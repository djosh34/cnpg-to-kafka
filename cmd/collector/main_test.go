package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

func TestMinimalFactories(t *testing.T) {
	f, err := factories()
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Receivers) != 2 || f.Receivers[component.MustNewType("file_log")] == nil ||
		f.Receivers[component.MustNewType("filelog")] == nil {
		t.Fatalf("unexpected receivers: %v", f.Receivers)
	}
	if len(f.Processors) != 1 || f.Processors[component.MustNewType("avro")] == nil {
		t.Fatalf("unexpected processors: %v", f.Processors)
	}
	if len(f.Exporters) != 1 || f.Exporters[component.MustNewType("kafka")] == nil {
		t.Fatalf("unexpected exporters: %v", f.Exporters)
	}
	if len(f.Extensions) != 1 || f.Extensions[component.MustNewType("file_storage")] == nil {
		t.Fatalf("unexpected extensions: %v", f.Extensions)
	}
	if len(f.Connectors) != 0 || f.Telemetry == nil {
		t.Fatal("unexpected connectors or missing native telemetry")
	}
}

func TestFileOnlyConfiguration(t *testing.T) {
	set := settings()
	if set.BuildInfo.Command != "cnpg-to-kafka" {
		t.Fatal(set.BuildInfo.Command)
	}
	resolverSet := set.ConfigProviderSettings.ResolverSettings
	if len(resolverSet.URIs) != 1 || resolverSet.URIs[0] != "file:/etc/cnpg-to-kafka/config.yaml" {
		t.Fatal(resolverSet.URIs)
	}
	if len(resolverSet.ProviderFactories) != 1 || resolverSet.DefaultScheme != "file" {
		t.Fatal("configuration must use only the file provider")
	}

	for _, tc := range []struct {
		name, yaml string
		wantError  bool
	}{
		{"literal", "value: literal\n", false},
		{"environment", "value: ${env:CNPG_TEST_VALUE}\n", true},
		{"remote", "value: ${https://example.invalid/config}\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0600); err != nil {
				t.Fatal(err)
			}
			rs := resolverSet
			rs.URIs = []string{"file:" + path}
			r, err := confmap.NewResolver(rs)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := r.Shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			cfg, err := r.Resolve(context.Background())
			if (err != nil) != tc.wantError {
				t.Fatalf("Resolve error = %v, wantError = %v", err, tc.wantError)
			}
			if !tc.wantError && cfg.ToStringMap()["value"] != "literal" {
				t.Fatal(cfg.ToStringMap())
			}
		})
	}
}
