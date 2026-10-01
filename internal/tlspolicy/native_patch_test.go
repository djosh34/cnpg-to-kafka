package tlspolicy

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

// These exercise native dependency rejection, not just our config policy.
// They fail against unpatched upstream sources; use scripts/prepare-go.sh and
// -mod=vendor for exactly the same source as the final image.
func TestNativePatchRejectsTPM(t *testing.T) {
	cfg := configtls.NewDefaultConfig()
	cfg.TPMConfig.Enabled = true
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "TPM is not supported") {
		t.Fatalf("native TPM config must fail explicitly: %v", err)
	}
}

func TestNativePatchRejectsKerberos(t *testing.T) {
	factory := kafkaexporter.NewFactory()
	cfg := factory.CreateDefaultConfig().(*kafkaexporter.Config)
	cfg.ClientConfig.Brokers = []string{"127.0.0.1:1"}
	cfg.ClientConfig.Authentication.Kerberos = &configkafka.KerberosConfig{}
	cfg.Logs.Encoding = "raw"
	exp, err := factory.CreateLogs(t.Context(), exportertest.NewNopSettings(factory.Type()), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := exp.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := exp.Start(t.Context(), componenttest.NewNopHost()); err == nil || !strings.Contains(err.Error(), "Kerberos is not supported") {
		t.Fatalf("native Kerberos startup must fail explicitly before broker access: %v", err)
	}
}
