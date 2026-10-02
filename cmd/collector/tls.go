package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/djosh34/cnpg-to-kafka/internal/tlspolicy"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
)

// kafkaTLSValidator uses the native resolver hook only to reject unsafe Kafka
// settings before components start. It does not convert or rewrite the config.
// The exporter and configtls remain responsible for TLS file loading/networking.
type kafkaTLSValidator struct{}

func (kafkaTLSValidator) Convert(_ context.Context, cfg *confmap.Conf) error {
	exporters, err := cfg.Sub("exporters")
	if err != nil {
		return err
	}
	for name := range exporters.ToStringMap() {
		if strings.SplitN(name, "/", 2)[0] != "kafka" {
			continue
		}
		tlsConf, err := exporters.Sub(name + "::tls")
		if err != nil {
			return fmt.Errorf("exporters.%s.tls: %w", name, err)
		}
		var tlsCfg configtls.ClientConfig
		if err := tlsConf.Unmarshal(&tlsCfg); err != nil {
			return fmt.Errorf("exporters.%s.tls: %w", name, err)
		}
		if err := tlspolicy.ValidateClient(&tlsCfg); err != nil {
			return fmt.Errorf("exporters.%s.tls: %w", name, err)
		}
	}
	return nil
}
