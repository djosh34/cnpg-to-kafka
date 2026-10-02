package avroprocessor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/confluentinc/confluent-avro-go/v2/registry"
)

// NewRegistryClient returns a Schema Registry client for one URL that uses the
// configured TLS settings and request timeout.
func NewRegistryClient(ctx context.Context, url string, cfg RegistryConfig) (*registry.Client, error) {
	tlsConfig, err := cfg.TLS.LoadTLSConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("registry TLS: %w", err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return registry.NewClient(url, registry.WithHTTPClient(&http.Client{
		Transport: transport,
		Timeout:   cfg.RequestTimeout,
	}))
}

// LoadSchema fetches the configured subject version from the first registry URL
// that answers with a schema it can parse.
func LoadSchema(ctx context.Context, cfg RegistryConfig) (registry.SchemaInfo, error) {
	var failures []error
	for _, url := range cfg.URLs {
		info, err := loadSchema(ctx, url, cfg)
		if err == nil {
			return info, nil
		}
		failures = append(failures, fmt.Errorf("registry %s: %w", url, err))
	}
	return registry.SchemaInfo{}, errors.Join(failures...)
}

func loadSchema(ctx context.Context, url string, cfg RegistryConfig) (registry.SchemaInfo, error) {
	client, err := NewRegistryClient(ctx, url, cfg)
	if err != nil {
		return registry.SchemaInfo{}, err
	}
	if cfg.Version == "latest" {
		return client.GetLatestSchemaInfo(ctx, cfg.Subject)
	}
	version, err := strconv.Atoi(cfg.Version)
	if err != nil {
		return registry.SchemaInfo{}, fmt.Errorf("version %q is neither latest nor a number", cfg.Version)
	}
	return client.GetSchemaInfo(ctx, cfg.Subject, version)
}
