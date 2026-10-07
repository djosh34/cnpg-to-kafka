// Package cnpgprocessor turns CloudNativePG log records into connection events.
package cnpgprocessor

import (
	"errors"
	"fmt"

	"github.com/djosh34/cnpg-to-kafka/internal/event"
)

// Config is the configuration of the cnpg processor.
type Config struct {
	// SourceHostname is the host name of the database. Its IP address is looked
	// up once, when the processor starts.
	SourceHostname   string           `mapstructure:"source_hostname"`
	AdditionalFields AdditionalFields `mapstructure:"additional_fields"`
	// HighPrivilegeRoles are matched exactly against the role of an event.
	HighPrivilegeRoles []string `mapstructure:"high_privilege_roles"`
	// TrustedConnections are logins that are not published. A logout is not
	// published when its role is in any of them. A cert entry matches the
	// common name of the certificate, any other entry the full identity.
	TrustedConnections []event.TrustedConnection `mapstructure:"trusted_connections"`
}

// AdditionalFields are added to every event.
type AdditionalFields struct {
	ApplicationName string `mapstructure:"application_name"`
}

// Validate checks the configuration when the Collector starts.
func (c *Config) Validate() error {
	var errs []error
	if c.SourceHostname == "" {
		errs = append(errs, errors.New("source_hostname is required"))
	}
	for i, trusted := range c.TrustedConnections {
		switch {
		case trusted.Role == "" || trusted.Method == "":
			errs = append(errs, fmt.Errorf("trusted_connections[%d] requires role and method", i))
		case trusted.Method == "cert" && (trusted.CommonName == "" || trusted.Identity != ""):
			errs = append(errs, fmt.Errorf("trusted_connections[%d] has method cert, so it requires common_name and no identity", i))
		case trusted.Method != "cert" && (trusted.Identity == "" || trusted.CommonName != ""):
			errs = append(errs, fmt.Errorf("trusted_connections[%d] has method %s, so it requires identity and no common_name", i, trusted.Method))
		}
	}
	return errors.Join(errs...)
}
