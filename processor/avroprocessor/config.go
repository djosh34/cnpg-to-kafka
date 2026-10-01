// Package avroprocessor maps native-selected connection attributes to Avro.
package avroprocessor

import (
	"errors"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/config/configtls"
)

// Config is the native Collector processor configuration.
type Config struct {
	Registry RegistryConfig `mapstructure:"registry"`
}

// RegistryConfig selects one authoritative schema at startup. All endpoints must
// share a logical schema-ID space and use the same mounted trust and identity.
type RegistryConfig struct {
	URLs           []string               `mapstructure:"urls"`
	Subject        string                 `mapstructure:"subject"`
	Version        string                 `mapstructure:"version"`
	RequestTimeout time.Duration          `mapstructure:"request_timeout"`
	TLS            configtls.ClientConfig `mapstructure:"tls"`
}

func (c *Config) Validate() error { return c.Registry.Validate() }

func (c RegistryConfig) Validate() error {
	if len(c.URLs) == 0 || c.Subject == "" {
		return errors.New("registry requires a nonempty ordered urls list and subject")
	}
	if c.Version != "latest" {
		version, err := strconv.Atoi(c.Version)
		if err != nil || version <= 0 {
			return errors.New("registry version must be latest or a positive decimal version")
		}
	}
	if c.RequestTimeout <= 0 {
		return errors.New("registry request_timeout must be positive and finite")
	}
	if c.TLS.CAFile == "" || c.TLS.CertFile == "" || c.TLS.KeyFile == "" {
		return errors.New("registry mTLS requires ca_file, cert_file and key_file")
	}
	if c.TLS.Insecure || c.TLS.InsecureSkipVerify || c.TLS.IncludeSystemCACertsPool ||
		c.TLS.CAPem != "" || c.TLS.CertPem != "" || c.TLS.KeyPem != "" || c.TLS.TPMConfig.Enabled {
		return errors.New("registry TLS requires verified peers and file-only trust/identity, without system CA fallback")
	}
	return nil
}
