package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/otelcol"
)

func TestEnableGates(t *testing.T) {
	require.NoError(t, enableGates())
	enabled := map[string]bool{}
	featuregate.GlobalRegistry().VisitAll(func(gate *featuregate.Gate) {
		enabled[gate.ID()] = gate.IsEnabled()
	})
	cases := []string{"stanza.synchronousLogEmitter"}
	for _, gate := range cases {
		t.Run(gate, func(t *testing.T) {
			assert.True(t, enabled[gate])
		})
	}
}

// TestConfig checks that the Collector accepts the example config.yaml, and
// rejects it after a change.
func TestConfig(t *testing.T) {
	example, err := os.ReadFile("../../config.yaml")
	require.NoError(t, err)
	cases := []struct {
		name  string
		old   string
		new   string
		error string
	}{
		{name: "example"},
		{
			name:  "unknown cnpg setting",
			old:   "    high_privilege_roles:",
			new:   "    high_privilege_role:",
			error: "high_privilege_role",
		},
		{
			name:  "cert connection with identity",
			old:   "common_name: streaming_replica}",
			new:   `identity: "CN=streaming_replica"}`,
			error: "trusted_connections[1] has method cert, so it requires common_name and no identity",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			config := string(example)
			if c.old != "" {
				require.Contains(t, config, c.old)
				config = strings.Replace(config, c.old, c.new, 1)
			}
			err := dryRun(t, config)
			if c.error == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, c.error)
			}
		})
	}
}

// TestDebugExporter checks that the Collector accepts the example config.yaml
// with the debug exporter added as docs/operations.md shows.
func TestDebugExporter(t *testing.T) {
	example, err := os.ReadFile("../../config.yaml")
	require.NoError(t, err)
	config := string(example)
	for _, change := range [][2]string{
		{"exporters:\n", "exporters:\n  debug:\n    verbosity: normal\n"},
		{"exporters: [kafka/cnpg]", "exporters: [kafka/cnpg, debug]"},
	} {
		require.Contains(t, config, change[0])
		config = strings.Replace(config, change[0], change[1], 1)
	}
	require.NoError(t, dryRun(t, config))
}

// dryRun writes config to a file and returns the error of the Collector's dry
// run with it.
func dryRun(t *testing.T, config string) error {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(config), 0o600))
	set := settings()
	set.ConfigProviderSettings.ResolverSettings.URIs = []string{path}
	collector, err := otelcol.NewCollector(set)
	require.NoError(t, err)
	return collector.DryRun(t.Context())
}
