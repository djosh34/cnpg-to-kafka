package inspect

import (
	"bytes"
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
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/collector/config/configtls"
)

// These focused inspector tests consume actual kfake records containing
// explicitly synthetic Avro events. They do not replace recording/Redpanda CI.
func TestRunConsumesWireSchemasAndRemainsReadOnly(t *testing.T) {
	clientTLS, serverTLS := inspectTestTLS(t)
	cluster := inspectTestBroker(t, serverTLS)
	var inspecting atomic.Bool
	var writes atomic.Int32
	cluster.Control(func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if inspecting.Load() {
			switch r := req.(type) {
			case *kmsg.MetadataRequest:
				if r.AllowAutoTopicCreation {
					t.Error("inspector requested automatic topic creation")
				}
			case *kmsg.ProduceRequest, *kmsg.CreateTopicsRequest, *kmsg.OffsetCommitRequest, *kmsg.JoinGroupRequest:
				writes.Add(1)
			}
		}
		return nil, nil, false
	})
	schemaBytes, err := os.ReadFile("../../schema/connection-event.avsc")
	if err != nil {
		t.Fatal(err)
	}
	// The second wire ID has a different field order; using the configured
	// current subject/version or the first cached schema would decode wrongly.
	var definition map[string]any
	if err := json.Unmarshal(schemaBytes, &definition); err != nil {
		t.Fatal(err)
	}
	fields := definition["fields"].([]any)
	fields[0], fields[1] = fields[1], fields[0]
	reordered, err := json.Marshal(definition)
	if err != nil {
		t.Fatal(err)
	}
	schemas := map[uint32]string{73: string(schemaBytes), 91: string(reordered)}
	var firstLookups, secondLookups atomic.Int32
	server := inspectTestRegistry(t, serverTLS, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("inspector registry write: %s", r.Method)
		}
		var schema string
		switch r.URL.Path {
		case "/schemas/ids/73":
			firstLookups.Add(1)
			schema = schemas[73]
		case "/schemas/ids/91":
			secondLookups.Add(1)
			schema = schemas[91]
		default:
			t.Errorf("lookup must use actual wire ID, got %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": schema})
	})
	want := []avroprocessor.Event{
		{Role: "app", Hostname: "192.0.2.10", EventType: "LOGIN", Context: avroprocessor.EventContext{Database: "db"}},
		{Role: "replica-app", Hostname: "client.example", EventType: "LOGOUT"},
		{EventType: "LOGIN_FAILED"},
	}
	var frames [][]byte
	for i, id := range []uint32{73, 91, 73} {
		frames = append(frames, inspectTestFrame(t, id, schemas[id], want[i]))
	}
	inspectTestProduce(t, cluster, clientTLS, frames...)
	inspecting.Store(true)
	path := writeInspectTestConfig(t, inspectTestConfig(clientTLS, cluster.ListenAddrs(), server.URL))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var out bytes.Buffer
	if err := Run(ctx, path, &out, len(want)); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&out)
	for i, expected := range want {
		var got avroprocessor.Event
		if err := decoder.Decode(&got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("event %d = %+v, want %+v", i, got, expected)
		}
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("output beyond requested limit: %v, %v", extra, err)
	}
	if firstLookups.Load() != 1 || secondLookups.Load() != 1 {
		t.Fatalf("schema-ID cache lookups = %d/%d, want one per ID", firstLookups.Load(), secondLookups.Load())
	}
	if writes.Load() != 0 {
		t.Fatalf("inspector made %d Kafka writes/group joins", writes.Load())
	}
}

func TestRunRejectsMalformedFrames(t *testing.T) {
	clientTLS, serverTLS := inspectTestTLS(t)
	schemaBytes, err := os.ReadFile("../../schema/connection-event.avsc")
	if err != nil {
		t.Fatal(err)
	}
	server := inspectTestRegistry(t, serverTLS, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected registry method: %s", r.Method)
		}
		if r.URL.Path != "/schemas/ids/73" {
			http.Error(w, `{"error_code":40403,"message":"unknown schema ID"}`, http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": string(schemaBytes)})
	})
	good := inspectTestFrame(t, 73, string(schemaBytes), avroprocessor.Event{EventType: "LOGIN_FAILED"})
	badMagic := append([]byte(nil), good...)
	badMagic[0] = 1
	unknownID := append([]byte(nil), good...)
	binary.BigEndian.PutUint32(unknownID[1:5], 999)
	for name, frame := range map[string][]byte{
		"short header": {0, 0, 0, 0}, "magic byte": badMagic,
		"unknown ID": unknownID, "truncated datum": {0, 0, 0, 0, 73, 0xff},
		"invalid enum": {0, 0, 0, 0, 73, 0, 0, 6, 0},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := inspectTestBroker(t, serverTLS)
			inspectTestProduce(t, cluster, clientTLS, frame)
			path := writeInspectTestConfig(t, inspectTestConfig(clientTLS, cluster.ListenAddrs(), server.URL))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var out bytes.Buffer
			if err := Run(ctx, path, &out, 1); err == nil || !strings.Contains(err.Error(), "decode inspect-events/") {
				t.Fatalf("bad frame returned %v, want contextual decode error", err)
			}
			if out.Len() != 0 {
				t.Fatalf("bad frame emitted JSON: %s", out.String())
			}
		})
	}
}

func TestLoadConfigReusesNativeFileReferences(t *testing.T) {
	clientTLS, _ := inspectTestTLS(t)
	cfg := inspectTestConfig(clientTLS, []string{"broker.example:9093"}, "https://registry.example")
	path := writeInspectTestConfig(t, cfg)
	kafka, topic, mapper, err := loadConfig(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	if topic != "inspect-events" || !reflect.DeepEqual(kafka.Brokers, []string{"broker.example:9093"}) {
		t.Fatalf("native Kafka configuration changed: %s %+v", topic, kafka)
	}
	if kafka.TLS.CAFile != clientTLS.CAFile || kafka.TLS.CertFile != clientTLS.CertFile || kafka.TLS.KeyFile != clientTLS.KeyFile ||
		mapper.Registry.TLS.CAFile != clientTLS.CAFile || mapper.Registry.TLS.CertFile != clientTLS.CertFile || mapper.Registry.TLS.KeyFile != clientTLS.KeyFile {
		t.Fatal("mounted native TLS file references changed")
	}
	if mapper.Registry.Version != "latest" || mapper.Registry.RequestTimeout != time.Second || mapper.Registry.Subject != "unused-current-subject" {
		t.Fatalf("native registry configuration changed: %+v", mapper.Registry)
	}
	// Kafka server-auth TLS can omit client identity; registry remains mTLS.
	kafkaTLS := cfg["exporters"].(map[string]any)["kafka/cnpg"].(map[string]any)["tls"].(map[string]any)
	delete(kafkaTLS, "cert_file")
	delete(kafkaTLS, "key_file")
	if _, _, _, err := loadConfig(t.Context(), writeInspectTestConfig(t, cfg)); err != nil {
		t.Fatalf("Kafka CA-only TLS rejected: %v", err)
	}
}

func TestLoadConfigUsesCollectorRegistryDefaults(t *testing.T) {
	clientTLS, _ := inspectTestTLS(t)
	for _, omitted := range []string{"version", "request_timeout", "both"} {
		t.Run(omitted, func(t *testing.T) {
			cfg := inspectTestConfig(clientTLS, []string{"broker.example:9093"}, "https://registry.example")
			registry := cfg["processors"].(map[string]any)["avro"].(map[string]any)["registry"].(map[string]any)
			registry["version"] = "7"
			if omitted == "version" || omitted == "both" {
				delete(registry, "version")
			}
			if omitted == "request_timeout" || omitted == "both" {
				delete(registry, "request_timeout")
			}
			_, _, mapper, err := loadConfig(t.Context(), writeInspectTestConfig(t, cfg))
			if err != nil {
				t.Fatalf("valid native configuration with omitted %s: %v", omitted, err)
			}
			defaults := avroprocessor.NewFactory().CreateDefaultConfig().(*avroprocessor.Config)
			wantVersion, wantTimeout := "7", time.Second
			if omitted == "version" || omitted == "both" {
				wantVersion = defaults.Registry.Version
			}
			if omitted == "request_timeout" || omitted == "both" {
				wantTimeout = defaults.Registry.RequestTimeout
			}
			if mapper.Registry.Version != wantVersion || mapper.Registry.RequestTimeout != wantTimeout {
				t.Fatalf("inspector defaults differ from Collector: %+v", mapper.Registry)
			}
		})
	}
}

func TestLoadConfigRejectsUnsafeKafkaSettings(t *testing.T) {
	clientTLS, _ := inspectTestTLS(t)
	for _, invalid := range []string{"no CA", "cert without key", "key without cert", "insecure", "skip verify", "system roots", "inline CA", "inline cert", "inline key", "no topic", "no brokers"} {
		t.Run(invalid, func(t *testing.T) {
			cfg := inspectTestConfig(clientTLS, []string{"broker.example:9093"}, "https://registry.example")
			kafka := cfg["exporters"].(map[string]any)["kafka/cnpg"].(map[string]any)
			tlsFields := kafka["tls"].(map[string]any)
			switch invalid {
			case "no CA":
				delete(tlsFields, "ca_file")
			case "cert without key":
				delete(tlsFields, "key_file")
			case "key without cert":
				delete(tlsFields, "cert_file")
			case "insecure":
				tlsFields["insecure"] = true
			case "skip verify":
				tlsFields["insecure_skip_verify"] = true
			case "system roots":
				tlsFields["include_system_ca_certs_pool"] = true
			case "inline CA":
				tlsFields["ca_pem"] = "inline material forbidden"
			case "inline cert":
				tlsFields["cert_pem"] = "inline material forbidden"
			case "inline key":
				tlsFields["key_pem"] = "inline material forbidden"
			case "no topic":
				delete(kafka, "logs")
			case "no brokers":
				delete(kafka, "brokers")
			}
			if _, _, _, err := loadConfig(t.Context(), writeInspectTestConfig(t, cfg)); err == nil {
				t.Fatal("unsafe/incomplete native configuration accepted")
			}
		})
	}
}

func TestRunRejectsNegativeLimitBeforeOpeningConfig(t *testing.T) {
	if err := Run(t.Context(), "missing-config.yaml", io.Discard, -1); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("negative limit returned %v", err)
	}
}

func inspectTestConfig(clientTLS configtls.ClientConfig, brokers []string, registryURL string) map[string]any {
	tlsFields := func() map[string]any {
		return map[string]any{"ca_file": clientTLS.CAFile, "cert_file": clientTLS.CertFile, "key_file": clientTLS.KeyFile}
	}
	return map[string]any{
		"processors": map[string]any{"avro": map[string]any{"registry": map[string]any{
			"urls": []string{registryURL}, "subject": "unused-current-subject", "version": "latest",
			"request_timeout": "1s", "tls": tlsFields(),
		}}},
		"exporters": map[string]any{"kafka/cnpg": map[string]any{
			"brokers": brokers, "tls": tlsFields(), "logs": map[string]any{"topic": "inspect-events", "encoding": "raw"},
			"producer": map[string]any{"required_acks": -1}, // Ordinary exporter-only fields are ignored by the reader.
		}},
	}
}

func writeInspectTestConfig(t *testing.T, cfg map[string]any) string {
	t.Helper()
	data, err := json.Marshal(cfg) // JSON is valid YAML for the native file provider.
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func inspectTestFrame(t *testing.T, id uint32, schemaText string, event avroprocessor.Event) []byte {
	t.Helper()
	schema, err := avro.Parse(schemaText)
	if err != nil {
		t.Fatal(err)
	}
	data, err := avro.Marshal(schema, event)
	if err != nil {
		t.Fatal(err)
	}
	frame := make([]byte, 5, 5+len(data))
	binary.BigEndian.PutUint32(frame[1:5], id)
	return append(frame, data...)
}

func inspectTestBroker(t *testing.T, serverTLS *tls.Config) *kfake.Cluster {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.TLS(serverTLS.Clone()), kfake.SeedTopics(1, "inspect-events"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	return cluster
}

func inspectTestProduce(t *testing.T, cluster *kfake.Cluster, clientTLS configtls.ClientConfig, frames ...[]byte) {
	t.Helper()
	tlsConfig, err := clientTLS.LoadTLSConfig(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DialTLSConfig(tlsConfig))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	var records []*kgo.Record
	for _, frame := range frames {
		records = append(records, &kgo.Record{Topic: "inspect-events", Value: frame})
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func inspectTestRegistry(t *testing.T, serverTLS *tls.Config, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.TLS = serverTLS.Clone()
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func inspectTestTLS(t *testing.T) (configtls.ClientConfig, *tls.Config) {
	t.Helper()
	// One ephemeral, dual-purpose identity keeps this synthetic TLS fixture
	// small. Production identities/trust remain separate mounted files.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "synthetic inspector TLS"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("cannot load generated test trust")
	}
	dir := t.TempDir()
	for name, data := range map[string][]byte{"ca.pem": certPEM, "client.pem": certPEM, "client-key.pem": keyPEM} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	client := configtls.ClientConfig{Config: configtls.Config{
		CAFile: filepath.Join(dir, "ca.pem"), CertFile: filepath.Join(dir, "client.pem"), KeyFile: filepath.Join(dir, "client-key.pem"),
	}}
	server := &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	return client, server
}
