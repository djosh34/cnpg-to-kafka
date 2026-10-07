package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/otelcol"

	"github.com/djosh34/cnpg-to-kafka/internal/event"
	"github.com/djosh34/cnpg-to-kafka/internal/fixture"
	"github.com/djosh34/cnpg-to-kafka/internal/replay"
)

// TestPodLogs runs the Collector on the sample pod logs in testdata/pods and
// checks every message that arrives in the topic.
func TestPodLogs(t *testing.T) {
	pods, err := filepath.Abs("../../testdata/pods")
	require.NoError(t, err)
	p := startPipeline(t, pods)

	// at returns a time on 2026-10-01 after 22:00 UTC in unix milliseconds.
	at := func(minute, second, milli int) int64 {
		return time.Date(2026, 10, 1, 22, minute, second, milli*int(time.Millisecond), time.UTC).UnixMilli()
	}
	// client returns an event of the recorded client at 10.42.0.6.
	client := func(eventType event.Type, timestamp int64, role string, cn, method *string) event.Event {
		return event.Event{
			Timestamp:       timestamp,
			EventType:       eventType,
			AccountType:     event.AccountNPA,
			ApplicationName: "payments",
			HostData:        event.HostData{SourceHostname: "127.0.0.1", SourceIP: "127.0.0.1"},
			ConnectionData: event.ConnectionData{
				Role: role, Database: "app", CN: cn, AuthMethod: method, ClientAddress: "10.42.0.6",
			},
		}
	}
	scram := new("scram-sha-256")
	// The logins and logouts of postgres with peer and of streaming_replica
	// with its certificate match trusted_connections and are not published.
	// Neither is the FATAL for too many connections.
	want := []event.Event{
		client(event.Login, at(28, 29, 982), "included", new("included"), scram),
		client(event.Logout, at(28, 29, 983), "included", nil, nil),
		// One wrong password gives two records, because psql retries without TLS.
		client(event.LoginFailed, at(28, 30, 18), "included", nil, scram),
		client(event.LoginFailed, at(28, 30, 22), "included", nil, scram),
		client(event.Login, at(28, 30, 56), "excluded", new("excluded"), scram),
		client(event.Logout, at(28, 30, 57), "excluded", nil, nil),
		client(event.LoginFailed, at(28, 30, 90), "excluded", nil, scram),
		client(event.LoginFailed, at(28, 30, 95), "excluded", nil, scram),
		client(event.LoginFailed, at(28, 30, 128), "unknown_role", nil, scram),
		client(event.LoginFailed, at(28, 30, 132), "unknown_role", nil, scram),
		// The hand-written cases: a valid certificate with the wrong CN, then
		// no database, CONNECT denied and NOLOGIN.
		client(event.LoginFailed, at(29, 0, 11), "streaming_replica", new("CN=mallory"), new("cert")),
		client(event.LoginFailed, at(29, 1, 0), "included", new("included"), scram),
		client(event.LoginFailed, at(29, 1, 0), "included", new("included"), scram),
		client(event.LoginFailed, at(29, 1, 0), "included", new("included"), scram),
		// The initdb pod logs in as postgres with trust, which no trusted
		// connection matches. Its logout is not published, because postgres is
		// in a trusted connection.
		{
			Timestamp:       at(27, 30, 702),
			EventType:       event.Login,
			AccountType:     event.AccountHA,
			ApplicationName: "payments",
			HostData:        event.HostData{SourceHostname: "127.0.0.1", SourceIP: "127.0.0.1"},
			ConnectionData: event.ConnectionData{
				Role: "postgres", Database: "postgres", CN: new("postgres"), AuthMethod: new("trust"), ClientAddress: "[local]",
			},
		},
	}

	values := p.values(t, len(want))
	got := make([]event.Event, len(values))
	for i, value := range values {
		got[i] = p.decode(t, value)
	}
	// Each pod log is read in order, but the order between files is not fixed.
	assert.ElementsMatch(t, want, got)
	for i, value := range values {
		// Unmarshal accepts a payload that ends early, so compare the bytes too.
		// ElementsMatch showed that got[i] is one of the expected events.
		payload, err := avro.Marshal(p.schema, got[i])
		require.NoError(t, err)
		assert.Equal(t, payload, value[5:])
	}
}

// TestRecording replays the recorded CloudNativePG pod logs, with a Kafka outage
// in the middle, and counts the events that arrive in the topic.
func TestRecording(t *testing.T) {
	p := startPipeline(t, t.TempDir())
	recording, err := os.Open("../../testdata/capture/operations.jsonl")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, recording.Close()) })

	// The recording is 300 seconds long, so this replay takes 15 seconds.
	replayed := make(chan error, 1)
	go func() { replayed <- replay.Run(t.Context(), p.pods, recording, 20) }()
	time.Sleep(5 * time.Second)
	p.kafka.Close()
	time.Sleep(5 * time.Second)
	p.startKafka(t)
	require.NoError(t, <-replayed)

	// testdata/capture/README.md says where these counts come from. Each of the
	// three instances saw the same client activity. The method of a failed
	// login comes from the matched pg_hba rule.
	want := map[string]int{
		"included LOGIN scram-sha-256":            3 * 17,
		"included LOGOUT":                         3 * 17,
		"included LOGIN_FAILED scram-sha-256":     3 * 28,
		"excluded LOGIN scram-sha-256":            3 * 14,
		"excluded LOGOUT":                         3 * 14,
		"excluded LOGIN_FAILED scram-sha-256":     3 * 28,
		"unknown_role LOGIN_FAILED scram-sha-256": 3 * 28,
		"postgres LOGIN trust":                    5,
	}
	total := 0
	for _, n := range want {
		total += n
	}
	got := map[string]int{}
	for _, value := range p.values(t, total) {
		e := p.decode(t, value)
		payload, err := avro.Marshal(p.schema, e)
		require.NoError(t, err)
		assert.Equal(t, payload, value[5:])

		data := e.ConnectionData
		key := data.Role + " " + string(e.EventType)
		if data.AuthMethod != nil {
			key += " " + *data.AuthMethod
		}
		got[key]++
		// A login carries the identity from its "connection authenticated"
		// record, and nothing else has a CN in the recording.
		if e.EventType == event.Login {
			assert.Equal(t, new(data.Role), data.CN, "%+v", e)
		} else {
			assert.Nil(t, data.CN, "%+v", e)
		}
	}
	assert.Equal(t, want, got)
}

// pipeline is the Collector running inside the test with the settings of main
// and the repository's config.yaml. Its Kafka is a kfake broker and its registry
// an HTTP test server, both of which require a client certificate.
type pipeline struct {
	pods      string // directory the Collector reads pod logs from
	schema    avro.Schema
	kafka     *kfake.Cluster
	kafkaPort int
	kafkaDir  string
	serverTLS *tls.Config
	clientTLS *tls.Config
}

// startPipeline starts a Collector that reads the pod logs of the namespace
// capture in the directory pods.
func startPipeline(t *testing.T, pods string) *pipeline {
	t.Helper()
	dir := t.TempDir()
	p := &pipeline{pods: pods, kafkaDir: filepath.Join(dir, "kafka")}
	var clientFiles map[string]any
	clientFiles, p.serverTLS, p.clientTLS = testCertificate(t, dir)
	p.startKafka(t)

	text, err := os.ReadFile("../../schema/connection-event.avsc")
	require.NoError(t, err)
	p.schema, err = avro.Parse(string(text))
	require.NoError(t, err)
	registry := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		err := json.NewEncoder(w).Encode(map[string]any{"id": fixture.SchemaID, "version": 1, "schema": string(text)})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	registry.TLS = p.serverTLS.Clone()
	registry.StartTLS()
	t.Cleanup(registry.Close)

	file, err := fileprovider.NewFactory().Create(confmap.ProviderSettings{}).Retrieve(t.Context(), "file:../../config.yaml", nil)
	require.NoError(t, err)
	conf, err := file.AsConf()
	require.NoError(t, err)
	require.NoError(t, conf.Merge(confmap.NewFromStringMap(map[string]any{
		"receivers::file_log/cnpg::include":       []any{filepath.Join(pods, "capture_*/*/*.log*")},
		"receivers::file_log/cnpg::exclude":       []any{filepath.Join(pods, "*/*/*.gz")},
		"receivers::file_log/cnpg::poll_interval": "10ms",
		// An address, so that the lookup needs no DNS.
		"processors::cnpg::source_hostname":   "127.0.0.1",
		"processors::avro::registry::urls":    []any{registry.URL},
		"processors::avro::registry::tls":     clientFiles,
		"exporters::kafka/cnpg::brokers":      []any{"127.0.0.1:" + strconv.Itoa(p.kafkaPort)},
		"exporters::kafka/cnpg::tls":          clientFiles,
		"extensions::file_storage::directory": filepath.Join(dir, "state"),
		"service::telemetry::logs::level":     "warn",
	})))
	// JSON is YAML, so the Collector reads this file like any config file.
	data, err := json.Marshal(conf.ToStringMap())
	require.NoError(t, err)
	config := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(config, data, 0o600))

	set := settings()
	set.ConfigProviderSettings.ResolverSettings.URIs = []string{config}
	require.NoError(t, enableGates())
	collector, err := otelcol.NewCollector(set)
	require.NoError(t, err)
	stopped := make(chan error, 1)
	go func() { stopped <- collector.Run(context.Background()) }()
	// The Collector delivers what is in its queue before it stops, so Kafka
	// has to outlive it.
	t.Cleanup(func() {
		collector.Shutdown()
		assert.NoError(t, <-stopped)
		p.kafka.Close()
	})
	for collector.GetState() != otelcol.StateRunning {
		select {
		case err := <-stopped:
			stopped <- err
			require.FailNow(t, "Collector stopped during startup", "%v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	return p
}

// startKafka starts the broker. After p.kafka.Close it starts it again on the
// same port with the messages it had.
func (p *pipeline) startKafka(t *testing.T) {
	t.Helper()
	opts := []kfake.Opt{kfake.NumBrokers(1), kfake.DataDir(p.kafkaDir), kfake.TLS(p.serverTLS.Clone())}
	if p.kafka == nil {
		opts = append(opts, kfake.SeedTopics(1, "cnpg-connections"))
	} else {
		opts = append(opts, kfake.Ports(p.kafkaPort))
	}
	var err error
	p.kafka, err = kfake.NewCluster(opts...)
	require.NoError(t, err)
	_, port, err := net.SplitHostPort(p.kafka.ListenAddrs()[0])
	require.NoError(t, err)
	p.kafkaPort, err = strconv.Atoi(port)
	require.NoError(t, err)
}

// values reads the raw message values in the topic. It waits up to 30 seconds
// for n messages, then two more seconds for a message that should not be there.
func (p *pipeline) values(t *testing.T, n int) [][]byte {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:"+strconv.Itoa(p.kafkaPort)),
		kgo.DialTLSConfig(p.clientTLS.Clone()),
		kgo.ConsumeTopics("cnpg-connections"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer client.Close()
	var values [][]byte
	read := func(timeout time.Duration, enough func() bool) {
		ctx, cancel := context.WithTimeout(t.Context(), timeout)
		defer cancel()
		for !enough() {
			fetches := client.PollFetches(ctx)
			if ctx.Err() != nil {
				return
			}
			require.NoError(t, fetches.Err())
			for _, record := range fetches.Records() {
				values = append(values, record.Value)
			}
		}
	}
	read(30*time.Second, func() bool { return len(values) >= n })
	read(2*time.Second, func() bool { return false })
	require.Len(t, values, n)
	return values
}

// decode checks the Confluent framing of a message value and decodes the Avro
// payload with the schema.
func (p *pipeline) decode(t *testing.T, value []byte) event.Event {
	t.Helper()
	require.Greater(t, len(value), 5)
	assert.Equal(t, byte(0), value[0])
	assert.Equal(t, uint32(fixture.SchemaID), binary.BigEndian.Uint32(value[1:5]))
	var e event.Event
	require.NoError(t, avro.Unmarshal(p.schema, value[5:], &e))
	return e
}

// testCertificate writes one self-signed certificate that the test uses as
// certificate authority, server certificate and client certificate. It returns
// the Collector's client TLS settings, and the TLS configs of the servers and
// of the test's own Kafka client.
func testCertificate(t *testing.T, dir string) (clientFiles map[string]any, server, client *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certFile, keyFile := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	clientFiles = map[string]any{"ca_file": certFile, "cert_file": certFile, "key_file": keyFile}
	server = &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	client = &tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: pool}
	return clientFiles, server, client
}
