package avroprocessor_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
	"go.opentelemetry.io/collector/config/configtls"
)

// These synthetic HTTPS responses isolate startup lookup failures; real
// Redpanda acceptance is separate. A primitive schema deliberately differs
// from the checked-in event sample: registry schemas are authoritative.
func writeRegistrySchema(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": 73, "version": 7, "schema": `"string"`,
	})
}

func TestRegistryLatestAndFixedLookup(t *testing.T) {
	clientTLS, serverTLS := registryTestTLS(t)
	for _, version := range []string{"latest", "7"} {
		t.Run(version, func(t *testing.T) {
			var calls atomic.Int32
			server := registryTestServer(t, serverTLS, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodGet || r.URL.Path != "/subjects/connections-value/versions/"+version {
					t.Errorf("unexpected registry request: %s %s", r.Method, r.URL.Path)
				}
				if len(r.TLS.VerifiedChains) == 0 {
					t.Error("registry request did not authenticate the client")
				}
				writeRegistrySchema(w)
			})
			cfg := registryTestConfig(clientTLS, server.URL)
			cfg.Version = version
			info, err := avroprocessor.LoadSchema(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			if info.ID != 73 || info.Version != 7 || info.Schema.String() != `"string"` {
				t.Fatalf("unexpected fetched schema metadata: %+v", info)
			}
			if calls.Load() != 1 {
				t.Fatalf("lookup made %d requests, want one read-only GET", calls.Load())
			}
		})
	}
}

func TestRegistryOrderedFallback(t *testing.T) {
	clientTLS, serverTLS := registryTestTLS(t)
	_, untrustedTLS := registryTestTLS(t)
	for _, failure := range []string{"transport", "TLS", "400", "503", "response JSON", "Avro schema", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			attempts := make(chan string, 3)
			badTLS := serverTLS
			if failure == "TLS" {
				badTLS = untrustedTLS
			}
			bad := registryTestServer(t, badTLS, func(w http.ResponseWriter, r *http.Request) {
				attempts <- "bad"
				switch failure {
				case "400":
					http.Error(w, `{"error_code":40001,"message":"bad subject"}`, http.StatusBadRequest)
				case "503":
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
				case "response JSON":
					_, _ = io.WriteString(w, "not JSON")
				case "Avro schema":
					_, _ = io.WriteString(w, `{"id":73,"version":7,"schema":"not an Avro schema"}`)
				case "timeout":
					<-r.Context().Done()
				default:
					t.Error("unexpected request to failed endpoint")
				}
			})
			if failure == "transport" {
				bad.Close()
			}
			good := registryTestServer(t, serverTLS, func(w http.ResponseWriter, r *http.Request) {
				attempts <- "good"
				if r.Method != http.MethodGet {
					t.Errorf("runtime registry write: %s", r.Method)
				}
				writeRegistrySchema(w)
			})
			unused := registryTestServer(t, serverTLS, func(w http.ResponseWriter, _ *http.Request) {
				attempts <- "unused"
				t.Error("lookup continued after first success")
			})
			cfg := registryTestConfig(clientTLS, bad.URL, good.URL, unused.URL)
			if failure == "timeout" {
				cfg.RequestTimeout = 100 * time.Millisecond
			}
			info, err := avroprocessor.LoadSchema(context.Background(), cfg)
			if err != nil || info.ID != 73 {
				t.Fatalf("fallback lookup = %+v, %v", info, err)
			}
			var got []string
			for len(attempts) > 0 {
				got = append(got, <-attempts)
			}
			want := []string{"bad", "good"}
			if failure == "transport" || failure == "TLS" {
				want = []string{"good"} // No HTTP handler runs before transport/TLS succeeds.
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("request order = %v, want %v", got, want)
			}
		})
	}
}

func TestRegistryAllEndpointsDown(t *testing.T) {
	clientTLS, serverTLS := registryTestTLS(t)
	attempts := make(chan int, 2)
	var urls []string
	for i := range 2 {
		server := registryTestServer(t, serverTLS, func(w http.ResponseWriter, _ *http.Request) {
			attempts <- i
			http.Error(w, "registry unavailable", http.StatusServiceUnavailable)
		})
		urls = append(urls, server.URL)
	}
	if _, err := avroprocessor.LoadSchema(context.Background(), registryTestConfig(clientTLS, urls...)); err == nil {
		t.Fatal("all failing registries must fail startup")
	}
	if len(attempts) != 2 || <-attempts != 0 || <-attempts != 1 {
		t.Fatal("startup did not try each endpoint once in configured order")
	}
}

func TestRegistryCancellationStopsFallback(t *testing.T) {
	clientTLS, serverTLS := registryTestTLS(t)
	entered := make(chan struct{})
	server := registryTestServer(t, serverTLS, func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
	})
	var nextCalls atomic.Int32
	next := registryTestServer(t, serverTLS, func(w http.ResponseWriter, _ *http.Request) {
		nextCalls.Add(1)
		writeRegistrySchema(w)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := avroprocessor.LoadSchema(ctx, registryTestConfig(clientTLS, server.URL, next.URL))
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first registry request did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled lookup returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lookup did not honor cancellation")
	}
	if nextCalls.Load() != 0 {
		t.Fatal("cancellation contacted another endpoint")
	}
}

func TestRegistryFileOnlyMutualTLS(t *testing.T) {
	clientTLS, serverTLS := registryTestTLS(t)
	otherClientTLS, _ := registryTestTLS(t)
	var calls atomic.Int32
	server := registryTestServer(t, serverTLS, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeRegistrySchema(w)
	})
	for _, failure := range []string{"wrong server CA", "untrusted client certificate"} {
		t.Run(failure, func(t *testing.T) {
			cfg := registryTestConfig(clientTLS, server.URL)
			if failure == "wrong server CA" {
				cfg.TLS.CAFile = otherClientTLS.CAFile
			} else {
				cfg.TLS.CertFile = otherClientTLS.CertFile
				cfg.TLS.KeyFile = otherClientTLS.KeyFile
			}
			if _, err := avroprocessor.LoadSchema(context.Background(), cfg); err == nil {
				t.Fatal("untrusted TLS identity accepted")
			}
		})
	}
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeRegistrySchema(w)
	}))
	t.Cleanup(plain.Close)
	for _, invalid := range []string{"no CA", "no cert", "no key", "system roots", "insecure", "skip verify", "inline CA", "inline cert", "inline key", "HTTP"} {
		t.Run(invalid, func(t *testing.T) {
			cfg := registryTestConfig(clientTLS, server.URL)
			switch invalid {
			case "no CA":
				cfg.TLS.CAFile = ""
			case "no cert":
				cfg.TLS.CertFile = ""
			case "no key":
				cfg.TLS.KeyFile = ""
			case "system roots":
				cfg.TLS.IncludeSystemCACertsPool = true
			case "insecure":
				cfg.TLS.Insecure = true
			case "skip verify":
				cfg.TLS.InsecureSkipVerify = true
			case "inline CA":
				cfg.TLS.CAPem = "inline material forbidden"
			case "inline cert":
				cfg.TLS.CertPem = "inline material forbidden"
			case "inline key":
				cfg.TLS.KeyPem = "inline material forbidden"
			case "HTTP":
				cfg.URLs = []string{plain.URL}
			}
			if _, err := avroprocessor.LoadSchema(context.Background(), cfg); err == nil {
				t.Fatal("file-only verified mTLS policy not enforced")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatalf("untrusted or invalid configuration made %d HTTP calls", calls.Load())
	}
}

func registryTestConfig(clientTLS configtls.ClientConfig, urls ...string) avroprocessor.RegistryConfig {
	return avroprocessor.RegistryConfig{
		URLs: urls, Subject: "connections-value", Version: "latest",
		RequestTimeout: time.Second, TLS: clientTLS,
	}
}

func registryTestServer(t *testing.T, serverTLS *tls.Config, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.TLS = serverTLS.Clone()
	server.Config.ErrorLog = log.New(io.Discard, "", 0) // Expected TLS rejection cases.
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

// Use standard x509/httptest tools and temporary file references only. No
// bundled test keys or CA material enters the application or its image.
func registryTestTLS(t *testing.T) (configtls.ClientConfig, *tls.Config) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "registry test CA"},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("could not load generated CA")
	}
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	var serverCert tls.Certificate
	for i, name := range []string{"server", "client"} {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		cert := &x509.Certificate{
			SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: name},
			NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}
		if name == "server" {
			cert.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			cert.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
		}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		if name == "server" {
			serverCert, err = tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			if err := os.WriteFile(filepath.Join(dir, "client.pem"), certPEM, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "client-key.pem"), keyPEM, 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	client := configtls.ClientConfig{Config: configtls.Config{
		CAFile: caFile, CertFile: filepath.Join(dir, "client.pem"), KeyFile: filepath.Join(dir, "client-key.pem"),
	}}
	server := &tls.Config{
		Certificates: []tls.Certificate{serverCert}, ClientCAs: pool,
		ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12,
	}
	return client, server
}
