package avroprocessor_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/djosh34/cnpg-to-kafka/internal/event"
	"github.com/djosh34/cnpg-to-kafka/internal/fixture"
)

// TestKafka sends events through the avro processor and the Kafka exporter
// into a kfake broker and reads the raw message values back.
func TestKafka(t *testing.T) {
	failed := login
	failed.EventType, failed.AccountType, failed.ConnectionData.CN = event.LoginFailed, event.AccountHA, nil
	logout := login
	logout.EventType, logout.ConnectionData.CN, logout.ConnectionData.AuthMethod = event.Logout, nil, nil
	cases := []struct {
		name   string
		events []event.Event
	}{
		{"login", []event.Event{login}},
		{"failed login and logout", []event.Event{failed, logout}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			kafka, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "cnpg-connections"))
			require.NoError(t, err)
			t.Cleanup(kafka.Close)

			factory := kafkaexporter.NewFactory()
			cfg := factory.CreateDefaultConfig()
			require.NoError(t, confmap.NewFromStringMap(map[string]any{
				"brokers":       kafka.ListenAddrs(),
				"logs":          map[string]any{"topic": "cnpg-connections", "encoding": "raw"},
				"sending_queue": map[string]any{"enabled": false},
			}).Unmarshal(cfg))
			exporter, err := factory.CreateLogs(t.Context(), exportertest.NewNopSettings(factory.Type()), cfg)
			require.NoError(t, err)
			require.NoError(t, exporter.Start(t.Context(), componenttest.NewNopHost()))
			t.Cleanup(func() { assert.NoError(t, exporter.Shutdown(context.Background())) })

			url, schema := fixture.Registry(t)
			p, err := startProcessor(t, exporter, url)
			require.NoError(t, err)
			logs := plog.NewLogs()
			records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
			for _, e := range c.events {
				e.SetAttributes(records.AppendEmpty().Attributes())
			}
			require.NoError(t, p.ConsumeLogs(t.Context(), logs))

			client, err := kgo.NewClient(
				kgo.SeedBrokers(kafka.ListenAddrs()...),
				kgo.ConsumeTopics("cnpg-connections"),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			require.NoError(t, err)
			t.Cleanup(client.Close)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			var values [][]byte
			for len(values) < len(c.events) {
				fetches := client.PollFetches(ctx)
				require.NoError(t, fetches.Err())
				for _, record := range fetches.Records() {
					values = append(values, record.Value)
				}
			}

			require.Len(t, values, len(c.events))
			for i, want := range c.events {
				value := values[i]
				require.Greater(t, len(value), 5)
				assert.Equal(t, byte(0), value[0])
				assert.Equal(t, uint32(fixture.SchemaID), binary.BigEndian.Uint32(value[1:5]))
				var got event.Event
				require.NoError(t, avro.Unmarshal(schema.Schema, value[5:], &got))
				assert.Equal(t, want, got)
				// Unmarshal accepts a payload that ends early, so compare the bytes too.
				payload, err := avro.Marshal(schema.Schema, want)
				require.NoError(t, err)
				assert.Equal(t, payload, value[5:])
			}
		})
	}
}
