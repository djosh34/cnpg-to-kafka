package tlspolicy

import (
	"testing"

	"go.opentelemetry.io/collector/config/configtls"
)

func TestValidateClient(t *testing.T) {
	if err := ValidateClient(nil); err == nil {
		t.Fatal("absent TLS must not enable plaintext")
	}
	// Paths deliberately need not exist here: native configtls owns file loading.
	for _, tc := range []struct {
		name      string
		change    func(*configtls.ClientConfig)
		wantError bool
	}{
		{"CA-only", func(*configtls.ClientConfig) {}, false},
		{"mTLS", func(c *configtls.ClientConfig) { c.CertFile, c.KeyFile = "cert.pem", "key.pem" }, false},
		{"missing CA", func(c *configtls.ClientConfig) { c.CAFile = "" }, true},
		{"plaintext", func(c *configtls.ClientConfig) { c.Insecure = true }, true},
		{"skip verification", func(c *configtls.ClientConfig) { c.InsecureSkipVerify = true }, true},
		{"system roots", func(c *configtls.ClientConfig) { c.IncludeSystemCACertsPool = true }, true},
		{"inline CA", func(c *configtls.ClientConfig) { c.CAPem = "PEM" }, true},
		{"inline cert", func(c *configtls.ClientConfig) { c.CertPem = "PEM" }, true},
		{"inline key", func(c *configtls.ClientConfig) { c.KeyPem = "PEM" }, true},
		{"TPM", func(c *configtls.ClientConfig) { c.TPMConfig.Enabled = true }, true},
		{"cert without key", func(c *configtls.ClientConfig) { c.CertFile = "cert.pem" }, true},
		{"key without cert", func(c *configtls.ClientConfig) { c.KeyFile = "key.pem" }, true},
		{"invalid native TLS version", func(c *configtls.ClientConfig) { c.MinVersion = "invalid" }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := configtls.NewDefaultClientConfig()
			c.CAFile = "ca.pem"
			tc.change(&c)
			if err := ValidateClient(&c); (err != nil) != tc.wantError {
				t.Fatalf("ValidateClient error = %v, wantError = %v", err, tc.wantError)
			}
		})
	}
}
