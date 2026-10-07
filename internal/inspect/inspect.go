// Package inspect prints the connection events in the Kafka topic as JSON lines.
package inspect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-avro-go/v2/registry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"

	"github.com/djosh34/cnpg-to-kafka/internal/event"
	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
)

// Run reads the topic from its first record and writes one JSON line per event.
// It takes the brokers, the topic, the registry and the TLS settings from the
// Collector config file. It returns after limit events, or when ctx ends if
// limit is 0. It consumes the partitions directly.
func Run(ctx context.Context, configPath string, out io.Writer, limit int) error {
	kafka, topic, registryConfig, err := loadConfig(ctx, configPath)
	if err != nil {
		return err
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(kafka.Brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	if kafka.TLS != nil {
		tlsConfig, err := kafka.TLS.LoadTLSConfig(ctx)
		if err != nil {
			return fmt.Errorf("TLS for Kafka: %w", err)
		}
		if tlsConfig != nil {
			opts = append(opts, kgo.DialTLSConfig(tlsConfig))
		}
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer client.Close()

	// Each message names its schema by ID. A decoder fetches that schema from
	// its registry URL and caches it.
	var decoders []*registry.Decoder
	for _, url := range registryConfig.URLs {
		registryClient, err := avroprocessor.NewRegistryClient(ctx, url, registryConfig)
		if err != nil {
			return err
		}
		decoders = append(decoders, registry.NewDecoder(registryClient, registry.WithAPI(streamingAPI{avro.DefaultConfig})))
	}

	encoder := json.NewEncoder(out)
	for count := 0; ; {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("fetch from Kafka: %w", errs[0].Err)
		}
		for iter := fetches.RecordIter(); !iter.Done(); {
			record := iter.Next()
			var e event.Event
			var failures []error
			for _, decoder := range decoders {
				err := decoder.Decode(ctx, record.Value, &e)
				if err == nil {
					failures = nil
					break
				}
				failures = append(failures, err)
			}
			if len(failures) > 0 {
				return fmt.Errorf("decode offset %d of partition %d: %w", record.Offset, record.Partition, errors.Join(failures...))
			}
			if err := encoder.Encode(e); err != nil {
				return err
			}
			if count++; count == limit {
				return nil
			}
		}
	}
}

// streamingAPI decodes with the library's stream decoder. The library's
// Unmarshal reports no error for a record that ends too early.
type streamingAPI struct{ avro.API }

func (a streamingAPI) Unmarshal(schema avro.Schema, data []byte, v any) error {
	return a.NewDecoder(schema, bytes.NewReader(data)).Decode(v)
}

func loadConfig(ctx context.Context, path string) (configkafka.ClientConfig, string, avroprocessor.RegistryConfig, error) {
	kafka := configkafka.NewDefaultClientConfig()
	avro := avroprocessor.NewFactory().CreateDefaultConfig().(*avroprocessor.Config)
	fail := func(err error) (configkafka.ClientConfig, string, avroprocessor.RegistryConfig, error) {
		return kafka, "", avro.Registry, fmt.Errorf("config %s: %w", path, err)
	}
	resolver, err := confmap.NewResolver(confmap.ResolverSettings{
		URIs:              []string{path},
		DefaultScheme:     "file",
		ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory(), envprovider.NewFactory()},
	})
	if err != nil {
		return fail(err)
	}
	cfg, err := resolver.Resolve(ctx)
	if err := errors.Join(err, resolver.Shutdown(ctx)); err != nil {
		return fail(err)
	}
	avroConf, err := cfg.Sub("processors::avro")
	if err != nil {
		return fail(err)
	}
	if err := avroConf.Unmarshal(avro); err != nil {
		return fail(err)
	}
	kafkaConf, err := cfg.Sub("exporters::kafka/cnpg")
	if err != nil {
		return fail(err)
	}
	// The exporter settings that are not about the connection are ignored.
	if err := kafkaConf.Unmarshal(&kafka, confmap.WithIgnoreUnused()); err != nil {
		return fail(err)
	}
	topic, _ := kafkaConf.Get("logs::topic").(string)
	if topic == "" {
		return fail(errors.New("exporters.kafka/cnpg.logs.topic is not set"))
	}
	return kafka, topic, avro.Registry, nil
}
