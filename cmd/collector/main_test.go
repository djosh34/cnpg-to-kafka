package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/otelcol"
)

func TestEnableGates(t *testing.T) {
	require.NoError(t, enableGates())
	enabled := false
	featuregate.GlobalRegistry().VisitAll(func(gate *featuregate.Gate) {
		if gate.ID() == "stanza.synchronousLogEmitter" {
			enabled = gate.IsEnabled()
		}
	})
	assert.True(t, enabled)
}

// TestConfig checks that the Collector accepts the example config.yaml.
func TestConfig(t *testing.T) {
	set := settings()
	set.ConfigProviderSettings.ResolverSettings.URIs = []string{"../../config.yaml"}
	collector, err := otelcol.NewCollector(set)
	require.NoError(t, err)
	require.NoError(t, collector.DryRun(t.Context()))
}
