package avroprocessor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"

	"github.com/confluentinc/confluent-avro-go/v2/registry"
)

// NewRegistryClient reuses the codec's registry client and native Collector TLS
// loader. No system trust, insecure verification, registration, or custom retry.
func NewRegistryClient(ctx context.Context, endpoint string, cfg RegistryConfig) (*registry.Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("registry endpoint must be an absolute HTTPS URL: %q", endpoint)
	}
	tlsConfig, err := cfg.TLS.LoadTLSConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load registry mTLS files: %w", err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	client := &http.Client{
		Transport: transport,
		Timeout:   cfg.RequestTimeout,
		// Treat redirects as endpoint failures, not another URL routing layer.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return registry.NewClient(endpoint, registry.WithHTTPClient(client))
}

// LoadSchema makes complete lookups in configured order, once at startup. The
// client parses the fetched schema; parsing failures advance to the next URL.
func LoadSchema(ctx context.Context, cfg RegistryConfig) (registry.SchemaInfo, error) {
	if err := cfg.Validate(); err != nil {
		return registry.SchemaInfo{}, err
	}
	var failures []error
	for _, endpoint := range cfg.URLs {
		if err := ctx.Err(); err != nil {
			return registry.SchemaInfo{}, err
		}
		client, err := NewRegistryClient(ctx, endpoint, cfg)
		var info registry.SchemaInfo
		if err == nil {
			if cfg.Version == "latest" {
				info, err = client.GetLatestSchemaInfo(ctx, cfg.Subject)
			} else {
				version, _ := strconv.Atoi(cfg.Version) // validated above
				info, err = client.GetSchemaInfo(ctx, cfg.Subject, version)
			}
			// Registry IDs are positive; zero also catches an omitted JSON ID,
			// which the library otherwise decodes to the int zero value.
			if err == nil && (info.ID <= 0 || uint64(info.ID) > math.MaxUint32) {
				err = fmt.Errorf("schema ID %d is missing or outside positive uint32", info.ID)
			}
		}
		if err == nil {
			return info, nil
		}
		failures = append(failures, fmt.Errorf("registry %s: %w", endpoint, err))
	}
	if err := ctx.Err(); err != nil {
		return registry.SchemaInfo{}, err
	}
	return registry.SchemaInfo{}, fmt.Errorf("startup schema lookup failed for every registry URL: %w", errors.Join(failures...))
}
