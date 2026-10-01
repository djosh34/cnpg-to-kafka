// Package tlspolicy rejects TLS settings outside the distribution's file-only
// trust/identity contract. Native configtls still loads files and owns TLS.
package tlspolicy

import (
	"errors"

	"go.opentelemetry.io/collector/config/configtls"
)

// ValidateClient requires verified TLS with an explicit CA file. Client identity
// is optional, but if supplied its certificate and key must both be files.
// Callers requiring mTLS must additionally require cert_file and key_file.
func ValidateClient(c *configtls.ClientConfig) error {
	if c == nil || c.CAFile == "" {
		return errors.New("TLS requires an explicit ca_file; plaintext and system CA fallback are not allowed")
	}
	if c.Insecure || c.InsecureSkipVerify || c.IncludeSystemCACertsPool ||
		c.CAPem != "" || c.CertPem != "" || c.KeyPem != "" || c.TPMConfig.Enabled {
		return errors.New("TLS requires verified peers and file-only trust/identity, without system CA fallback")
	}
	return c.Config.Validate()
}
