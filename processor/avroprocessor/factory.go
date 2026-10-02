package avroprocessor

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-avro-go/v2/registry"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
)

// NewFactory returns the factory of the avro processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(component.MustNewType("avro"), func() component.Config {
		return &Config{Registry: RegistryConfig{
			Version:        "latest",
			RequestTimeout: 10 * time.Second,
			TLS:            configtls.NewDefaultClientConfig(),
		}}
	}, processor.WithLogs(createLogs, component.StabilityLevelBeta))
}

func createLogs(ctx context.Context, set processor.Settings, config component.Config, next consumer.Logs) (processor.Logs, error) {
	cfg := config.(*Config)
	var schema registry.SchemaInfo
	return processorhelper.NewLogs(ctx, set, cfg, next,
		func(_ context.Context, logs plog.Logs) (plog.Logs, error) {
			for _, resource := range logs.ResourceLogs().All() {
				for _, scope := range resource.ScopeLogs().All() {
					scope.LogRecords().RemoveIf(func(record plog.LogRecord) bool {
						return !encode(record, schema, set.Logger)
					})
				}
			}
			return logs, nil
		},
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: true}),
		processorhelper.WithStart(func(ctx context.Context, _ component.Host) error {
			var err error
			schema, err = LoadSchema(ctx, cfg.Registry)
			return err
		}),
	)
}

// encode replaces the record body with the framed Avro event built from the
// record's attributes. It reports false for a record that is not a complete
// event or that the schema cannot encode. The attributes stay in place, so a
// record that the receiver sends again after a failed export encodes the same.
func encode(record plog.LogRecord, schema registry.SchemaInfo, logger *zap.Logger) bool {
	attribute := func(key string) string {
		value, ok := record.Attributes().Get(key)
		if !ok {
			return ""
		}
		return value.AsString()
	}
	event := Event{
		Role:      attribute("role"),
		Hostname:  attribute("hostname"),
		EventType: attribute("eventtype"),
		Context:   EventContext{Database: attribute("database")},
	}
	// A failed login can lack a role and a client host. A login or logout cannot.
	if event.EventType == "" || (event.EventType != "LOGIN_FAILED" && (event.Role == "" || event.Hostname == "")) {
		logger.Debug("skip record without event type, role or hostname")
		return false
	}
	payload, err := avro.Marshal(schema.Schema, event)
	if err != nil {
		logger.Warn("cannot encode connection event", zap.Error(err))
		return false
	}
	// Confluent wire format: a zero byte, the schema ID as four big-endian
	// bytes, then the Avro binary.
	frame := make([]byte, 5, 5+len(payload))
	binary.BigEndian.PutUint32(frame[1:], uint32(schema.ID))
	record.Body().SetEmptyBytes().FromRaw(append(frame, payload...))
	return true
}
