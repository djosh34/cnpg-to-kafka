package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"maps"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/otelcol"

	"github.com/djosh34/cnpg-to-kafka/internal/inspect"
	"github.com/djosh34/cnpg-to-kafka/internal/replay"
	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
)

// TestRecording replays the recorded CloudNativePG pod logs, with a Kafka outage
// in the middle, and checks the events that arrive in the topic.
func TestRecording(t *testing.T) {
	p := startPipeline(t, "capture")
	recording, err := os.Open("../../testdata/capture/operations.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer recording.Close()

	// The recording is 300 seconds long, so this replay takes 15 seconds.
	replayed := make(chan error, 1)
	go func() { replayed <- replay.Run(t.Context(), p.pods, recording, 20) }()
	time.Sleep(5 * time.Second)
	p.kafka.Close()
	time.Sleep(5 * time.Second)
	p.startKafka(t)
	if err := <-replayed; err != nil {
		t.Fatal(err)
	}

	// testdata/capture/README.md says where these counts come from. Each of the
	// three instances saw the same client activity. The logins and logouts of
	// the roles postgres and streaming_replica are in the recording too, and
	// config.yaml excludes them.
	want := map[string]int{
		"included LOGIN": 3 * 17, "included LOGOUT": 3 * 17, "included LOGIN_FAILED": 3 * 28,
		"excluded LOGIN": 3 * 14, "excluded LOGOUT": 3 * 14, "excluded LOGIN_FAILED": 3 * 28,
		"unknown_role LOGIN_FAILED": 3 * 28,
	}
	total := 0
	for _, n := range want {
		total += n
	}
	got := map[string]int{}
	for _, event := range p.events(t, total) {
		if event.Hostname != "10.42.0.6" || event.Context.Database != "app" {
			t.Errorf("event %+v: want hostname 10.42.0.6 and database app", event)
		}
		got[event.Role+" "+event.EventType]++
	}
	if !maps.Equal(got, want) {
		t.Errorf("events per role and type:\n got %v\nwant %v", got, want)
	}
}

// TestEventRules checks which event the operators in config.yaml make of each
// log line.
func TestEventRules(t *testing.T) {
	type record = map[string]any
	postgres := func(r record) string {
		line, err := json.Marshal(record{"logger": "postgres", "msg": "record", "record": r})
		if err != nil {
			t.Fatal(err)
		}
		return string(line)
	}
	session := func(message, role, host string) string {
		return postgres(record{
			"message": message, "user_name": role, "connection_from": host, "database_name": "app",
			"error_severity": "LOG", "sql_state_code": "00000",
		})
	}
	failed := func(code, role, host string) string {
		return postgres(record{
			"message": "password authentication failed", "user_name": role, "connection_from": host,
			"database_name": "app", "error_severity": "FATAL", "sql_state_code": code,
		})
	}
	event := func(eventType, role, host, database string) *avroprocessor.Event {
		return &avroprocessor.Event{Role: role, Hostname: host, EventType: eventType, Context: avroprocessor.EventContext{Database: database}}
	}
	cases := []struct {
		line string
		want *avroprocessor.Event // nil if the line is not an event
	}{
		{session("connection authorized: user=alice database=app", "alice", "10.1.2.3:54321"), event("LOGIN", "alice", "10.1.2.3", "app")},
		{session("disconnection: session time: 0:00:01", "alice", "[2001:db8::7]:54321"), event("LOGOUT", "alice", "2001:db8::7", "app")},
		{session("replication connection authorized: user=replicator", "replicator", "10.1.2.6:54321"), event("LOGIN", "replicator", "10.1.2.6", "app")},
		{session("connection authorized: user=local", "local", "[local]"), event("LOGIN", "local", "[local]", "app")},
		{session("disconnection: session time: 0:00:01", "bare", "client.example"), event("LOGOUT", "bare", "client.example", "app")},

		// Steps before the login, and other messages.
		{session("connection received: host=10.1.2.3 port=54321", "alice", "10.1.2.3:54321"), nil},
		{session("connection authenticated: identity=alice method=scram-sha-256", "alice", "10.1.2.3:54321"), nil},
		{session("checkpoint starting: time", "", ""), nil},
		{postgres(record{"message": "connection authorized: user=nobody", "connection_from": "10.1.2.8:42"}), nil},

		// Excluded roles. The match is case-sensitive.
		{session("connection authorized: user=postgres", "postgres", "10.1.2.3:54321"), nil},
		{session("disconnection: session time: 0:00:01", "streaming_replica", "10.1.2.3:54321"), nil},
		{session("replication connection authorized: user=streaming_replica", "streaming_replica", "10.1.2.6:54321"), nil},
		{session("connection authorized: user=Postgres", "Postgres", "client.example:54321"), event("LOGIN", "Postgres", "client.example", "app")},

		// Failed logins, also of excluded roles and without a role.
		{failed("28P01", "alice", "10.1.2.4:54321"), event("LOGIN_FAILED", "alice", "10.1.2.4", "app")},
		{failed("28000", "streaming_replica", "::1:54321"), event("LOGIN_FAILED", "streaming_replica", "::1", "app")},
		{postgres(record{"error_severity": "FATAL", "sql_state_code": "28000"}), event("LOGIN_FAILED", "", "", "")},
		{postgres(record{"message": `permission denied for database "app"`, "error_severity": "FATAL", "sql_state_code": "42501", "user_name": "postgres"}), event("LOGIN_FAILED", "postgres", "", "")},
		{postgres(record{"message": "permission denied for table things", "error_severity": "FATAL", "sql_state_code": "42501"}), nil},
		{failed("57P01", "alice", "10.1.2.3:54321"), nil},

		// Lines that are not PostgreSQL records.
		{`{"logger":"other","record":{"message":"connection authorized: user=alice"}}`, nil},
		{`{"logger":"postgres","record":42}`, nil},
		{`{"logger":"postgres","record":`, nil},
		{"plain text", nil},

		{session("disconnection: session time: 0:00:02", "last", "10.1.2.3:54321"), event("LOGOUT", "last", "10.1.2.3", "app")},
	}

	p := startPipeline(t, "database")
	write := func(namespace string, lines ...string) {
		t.Helper()
		dir := filepath.Join(p.pods, namespace+"_cnpg-1_00000000-0000-0000-0000-000000000001", "postgres")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(filepath.Join(dir, "0.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		for _, line := range lines {
			// The container runtime's log format: time, stream, F for a full line.
			if _, err := fmt.Fprintln(file, "2026-10-01T12:00:00.000000000Z stdout F "+line); err != nil {
				t.Fatal(err)
			}
		}
	}
	var want []avroprocessor.Event
	for i, c := range cases {
		if c.want != nil {
			want = append(want, *c.want)
		}
		if i == len(cases)-1 {
			// Wait for the events before the last one. An event from a line
			// that should give none then shows up ahead of the last event.
			p.events(t, len(want)-1)
		}
		write("database", c.line)
		if i == 0 {
			write("other", session("connection authorized: user=mallory", "mallory", "10.1.2.3:54321"))
		}
	}
	if got := p.events(t, len(want)); !slices.Equal(got, want) {
		t.Errorf("events:\n got %+v\nwant %+v", got, want)
	}
}

// pipeline is the Collector running inside the test with the settings of main
// and the repository's config.yaml. Its Kafka is a kfake broker and its registry
// an HTTP test server, both of which require a client certificate.
type pipeline struct {
	pods      string // directory the Collector reads pod logs from
	config    string // the config file the Collector runs with
	kafka     *kfake.Cluster
	kafkaPort int
	kafkaDir  string
	serverTLS *tls.Config
}

// startPipeline starts a Collector that reads the pod logs of one namespace.
func startPipeline(t *testing.T, namespace string) *pipeline {
	dir := t.TempDir()
	p := &pipeline{pods: filepath.Join(dir, "pods"), config: filepath.Join(dir, "config.json"), kafkaDir: filepath.Join(dir, "kafka")}
	if err := os.Mkdir(p.pods, 0o755); err != nil {
		t.Fatal(err)
	}
	var clientTLS map[string]any
	clientTLS, p.serverTLS = testCertificate(t, dir)
	p.startKafka(t)

	schema, err := os.ReadFile("../../schema/connection-event.avsc")
	if err != nil {
		t.Fatal(err)
	}
	// The same answer serves the subject lookup of the Collector and the
	// lookup by schema ID of the inspector.
	registry := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"id": 7, "version": 1, "schema": string(schema)})
	}))
	registry.TLS = p.serverTLS.Clone()
	registry.StartTLS()
	t.Cleanup(registry.Close)

	file, err := fileprovider.NewFactory().Create(confmap.ProviderSettings{}).Retrieve(t.Context(), "file:../../config.yaml", nil)
	if err != nil {
		t.Fatal(err)
	}
	conf, err := file.AsConf()
	if err != nil {
		t.Fatal(err)
	}
	err = conf.Merge(confmap.NewFromStringMap(map[string]any{
		"receivers::file_log/cnpg::include":       []any{filepath.Join(p.pods, namespace+"_*/*/*.log*")},
		"receivers::file_log/cnpg::exclude":       []any{filepath.Join(p.pods, "*/*/*.gz")},
		"receivers::file_log/cnpg::poll_interval": "10ms",
		"processors::avro::registry::urls":        []any{registry.URL},
		"processors::avro::registry::tls":         clientTLS,
		"exporters::kafka/cnpg::brokers":          []any{"127.0.0.1:" + strconv.Itoa(p.kafkaPort)},
		"exporters::kafka/cnpg::tls":              clientTLS,
		"extensions::file_storage::directory":     filepath.Join(dir, "state"),
		"service::telemetry::logs::level":         "warn",
	}))
	if err != nil {
		t.Fatal(err)
	}
	// JSON is YAML, so the Collector reads this file like any config file.
	data, err := json.Marshal(conf.ToStringMap())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.config, data, 0o600); err != nil {
		t.Fatal(err)
	}

	set := settings()
	set.ConfigProviderSettings.ResolverSettings.URIs = []string{p.config}
	collector, err := otelcol.NewCollector(set)
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- collector.Run(context.Background()) }()
	// The Collector delivers what is in its queue before it stops, so Kafka
	// has to outlive it.
	t.Cleanup(func() {
		collector.Shutdown()
		if err := <-stopped; err != nil {
			t.Error(err)
		}
		p.kafka.Close()
	})
	for collector.GetState() != otelcol.StateRunning {
		select {
		case err := <-stopped:
			stopped <- err
			t.Fatalf("Collector stopped during startup: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	return p
}

// startKafka starts the broker. After p.kafka.Close it starts it again on the
// same port with the messages it had.
func (p *pipeline) startKafka(t *testing.T) {
	opts := []kfake.Opt{kfake.NumBrokers(1), kfake.DataDir(p.kafkaDir), kfake.TLS(p.serverTLS.Clone())}
	if p.kafka == nil {
		opts = append(opts, kfake.SeedTopics(1, "cnpg-connections"))
	} else {
		opts = append(opts, kfake.Ports(p.kafkaPort))
	}
	var err error
	if p.kafka, err = kfake.NewCluster(opts...); err != nil {
		t.Fatal(err)
	}
	_, port, _ := net.SplitHostPort(p.kafka.ListenAddrs()[0])
	p.kafkaPort, _ = strconv.Atoi(port)
}

// events reads the first n events of the topic with the inspector and logs them.
func (p *pipeline) events(t *testing.T, n int) []avroprocessor.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var out bytes.Buffer
	runErr := inspect.Run(ctx, p.config, &out, n)
	var events []avroprocessor.Event
	for line := range bytes.Lines(out.Bytes()) {
		t.Logf("%s", bytes.TrimSpace(line))
		var event avroprocessor.Event
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if runErr != nil {
		t.Errorf("read %d of %d events: %v", len(events), n, runErr)
	}
	return events
}

// testCertificate writes one self-signed certificate that the test uses as
// certificate authority, server certificate and client certificate. It returns
// the Collector's client TLS settings and the TLS config for the servers.
func testCertificate(t *testing.T, dir string) (map[string]any, *tls.Config) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	client := map[string]any{"ca_file": certFile, "cert_file": certFile, "key_file": keyFile}
	return client, &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
}
