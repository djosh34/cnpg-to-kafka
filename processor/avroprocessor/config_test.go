package avroprocessor_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
	"go.opentelemetry.io/collector/confmap"
)

func TestNativeConfigDecoding(t *testing.T) {
	for _, version := range []string{"latest", "7"} {
		t.Run(version, func(t *testing.T) {
			cfg := avroprocessor.NewFactory().CreateDefaultConfig().(*avroprocessor.Config)
			urls := []string{"https://first.example", "https://second.example"}
			conf := confmap.NewFromStringMap(map[string]any{"registry": map[string]any{
				"urls": urls, "subject": "connections-value", "version": version,
				"request_timeout": "3s", "tls": map[string]any{
					"ca_file": "/tls/ca.crt", "cert_file": "/tls/client.crt", "key_file": "/tls/client.key",
				},
			}})
			if err := conf.Unmarshal(cfg); err != nil {
				t.Fatal(err)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
			r := cfg.Registry
			if !reflect.DeepEqual(r.URLs, urls) || r.Subject != "connections-value" ||
				r.Version != version || r.RequestTimeout != 3*time.Second ||
				r.TLS.CAFile != "/tls/ca.crt" || r.TLS.CertFile != "/tls/client.crt" || r.TLS.KeyFile != "/tls/client.key" {
				t.Fatalf("native config fields not decoded: %+v", r)
			}
		})
	}
}

func TestNativeConfigKeepsDefaultsAndRejectsUnknownSettings(t *testing.T) {
	cfg := avroprocessor.NewFactory().CreateDefaultConfig().(*avroprocessor.Config)
	if err := confmap.NewFromStringMap(map[string]any{"registry": map[string]any{"subject": "connections-value"}}).Unmarshal(cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Registry.Version != "latest" || cfg.Registry.RequestTimeout != 10*time.Second {
		t.Fatalf("native unmarshal lost factory defaults: %+v", cfg.Registry)
	}
	if err := confmap.NewFromStringMap(map[string]any{"registry": map[string]any{"request_timout": "3s"}}).Unmarshal(cfg); err == nil {
		t.Fatal("native confmap must reject misspelled runtime settings")
	}
}
