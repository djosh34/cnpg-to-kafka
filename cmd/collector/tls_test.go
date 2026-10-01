package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/confmap"
)

func TestKafkaTLSPolicyDuringResolution(t *testing.T) {
	for _, tc := range []struct {
		name, exporter, tls string
		wantError           bool
	}{
		{"absent", "kafka", "", true},
		{"null", "kafka/cnpg", "tls: null", true},
		{"default roots", "kafka/cnpg", "tls: {}", true},
		{"insecure", "kafka/cnpg", "tls: {ca_file: ca.pem, insecure_skip_verify: true}", true},
		{"inline", "kafka/cnpg", "tls: {ca_file: ca.pem, cert_pem: inline}", true},
		{"CA-only", "kafka", "tls: {ca_file: ca.pem}", false},
		{"mTLS", "kafka/cnpg", "tls: {ca_file: ca.pem, cert_file: cert.pem, key_file: key.pem}", false},
		{"other exporter", "not_kafka", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			yaml := "exporters:\n  " + tc.exporter + ":\n    brokers: [broker:9093]\n    " + tc.tls + "\n"
			if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
				t.Fatal(err)
			}
			rs := settings().ConfigProviderSettings.ResolverSettings
			rs.URIs = []string{"file:" + path}
			resolver, err := confmap.NewResolver(rs)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := resolver.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			cfg, err := resolver.Resolve(context.Background())
			if (err != nil) != tc.wantError {
				t.Fatalf("Resolve error = %v, wantError = %v", err, tc.wantError)
			}
			if tc.wantError && !strings.Contains(err.Error(), "exporters."+tc.exporter+".tls") {
				t.Fatalf("error must identify Kafka TLS config: %v", err)
			}
			if !tc.wantError && tc.exporter != "not_kafka" && cfg.Get("exporters::"+tc.exporter+"::tls::ca_file") != "ca.pem" {
				t.Fatal("validator must preserve the supplied TLS settings")
			}
		})
	}
}
