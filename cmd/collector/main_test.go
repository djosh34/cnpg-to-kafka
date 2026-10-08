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

// TestConfig checks that the Collector accepts the example config.yaml, with
// and without the debug exporter that docs/operations.md shows, and rejects it
// after a wrong change.
func TestConfig(t *testing.T) {
	example, err := os.ReadFile("../../config.yaml")
	require.NoError(t, err)
	cases := []struct {
		name    string
		changes [][2]string
		error   string
	}{
		{name: "example"},
		{
			name: "debug exporter",
			changes: [][2]string{
				{"exporters:\n", "exporters:\n  debug:\n    verbosity: normal\n"},
				{"exporters: [kafka/cnpg]", "exporters: [kafka/cnpg, debug]"},
			},
		},
		{
			name:    "unknown cnpg setting",
			changes: [][2]string{{"    high_privilege_roles:", "    high_privilege_role:"}},
			error:   "high_privilege_role",
		},
		{
			name:    "cert connection with identity",
			changes: [][2]string{{"common_name: streaming_replica}", `identity: "CN=streaming_replica"}`}},
			error:   "trusted_connections[1] has method cert, so it requires common_name and no identity",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			config := string(example)
			for _, change := range c.changes {
				require.Contains(t, config, change[0])
				config = strings.Replace(config, change[0], change[1], 1)
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(config), 0o600))

			set := settings()
			set.ConfigProviderSettings.ResolverSettings.URIs = []string{path}
			collector, err := otelcol.NewCollector(set)
			require.NoError(t, err)
			err = collector.DryRun(t.Context())
			if c.error == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, c.error)
			}
		})
	}
}
