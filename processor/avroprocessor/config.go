// Package avroprocessor encodes the connection events in log records as Avro.
package avroprocessor

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/config/configtls"
)

// Config is the configuration of the avro processor.
type Config struct {
	Registry RegistryConfig `mapstructure:"registry"`
}

// RegistryConfig says where to fetch the schema at startup. The URLs are tried
// in order and must all serve the same registry, so that schema IDs agree.
type RegistryConfig struct {
	URLs    []string `mapstructure:"urls"`
	Subject string   `mapstructure:"subject"`
	// Version is "latest" or a version number.
	Version        string                 `mapstructure:"version"`
	RequestTimeout time.Duration          `mapstructure:"request_timeout"`
	TLS            configtls.ClientConfig `mapstructure:"tls"`
}

// Validate checks the configuration when the Collector starts.
func (c *Config) Validate() error {
	if len(c.Registry.URLs) == 0 || c.Registry.Subject == "" {
		return errors.New("registry requires urls and subject")
	}
	return nil
}
