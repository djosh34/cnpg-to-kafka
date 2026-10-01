package avroprocessor

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-avro-go/v2/registry"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"
	"go.uber.org/zap"
)

// NewFactory supplies the only custom component in the Collector pipeline.
func NewFactory() processor.Factory {
	return processor.NewFactory(component.MustNewType("avro"), func() component.Config {
		return &Config{Registry: RegistryConfig{
			Version: "latest", RequestTimeout: 10 * time.Second,
			TLS: configtls.NewDefaultClientConfig(),
		}}
	}, processor.WithLogs(createLogs, component.StabilityLevelBeta))
}

func createLogs(ctx context.Context, set processor.Settings, config component.Config, next consumer.Logs) (processor.Logs, error) {
	cfg := config.(*Config)
	var schema registry.SchemaInfo
	return processorhelper.NewLogs(ctx, set, cfg, next,
		func(_ context.Context, input plog.Logs) (plog.Logs, error) {
			// Keep upstream data untouched, including on downstream queue failures:
			// a retry remaps the original attributes, never our encoded byte output.
			output := plog.NewLogs()
			input.CopyTo(output)
			var encodeErr error
			for _, resource := range output.ResourceLogs().All() {
				for _, scope := range resource.ScopeLogs().All() {
					scope.LogRecords().RemoveIf(func(record plog.LogRecord) bool {
						attrs := record.Attributes()
						event := Event{
							Role: stringAttribute(attrs, "role"), Hostname: stringAttribute(attrs, "hostname"),
							EventType: stringAttribute(attrs, "eventtype"),
							Context:   EventContext{Database: stringAttribute(attrs, "database")},
						}
						if event.EventType == "" || (event.EventType != "LOGIN_FAILED" && (event.Role == "" || event.Hostname == "")) {
							set.Logger.Debug("skip connection event missing necessary output attributes")
							return true
						}
						payload, err := avro.Marshal(schema.Schema, event)
						if err != nil {
							encodeErr = consumererror.NewPermanent(fmt.Errorf("encode connection event: %w", err))
							set.Logger.Warn("cannot encode connection event", zap.Error(err))
							return true
						}
						frame := make([]byte, 5, 5+len(payload))
						binary.BigEndian.PutUint32(frame[1:5], uint32(schema.ID))
						record.Body().SetEmptyBytes().FromRaw(append(frame, payload...))
						return false
					})
				}
			}
			return output, encodeErr
		},
		processorhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		processorhelper.WithStart(func(ctx context.Context, _ component.Host) error {
			var err error
			schema, err = LoadSchema(ctx, cfg.Registry)
			return err
		}),
	)
}

func stringAttribute(attrs pcommon.Map, key string) string {
	value, ok := attrs.Get(key)
	if !ok || value.Type() != pcommon.ValueTypeStr {
		return ""
	}
	return value.Str()
}
