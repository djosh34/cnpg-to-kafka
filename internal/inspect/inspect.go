// Package inspect reads Kafka records and prints Avro-decoded connection events.
// It has no consumer group, offset commits, topic creation or schema writes.
package inspect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/confluentinc/confluent-avro-go/v2/registry"
	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
)

// Run consumes from the beginning using the same native configuration as the
// Collector. limit=0 follows until ctx ends; positive limits return after that
// many decoded records. The caller supplies a deadline and owns the output file.
func Run(ctx context.Context, configPath string, out io.Writer, limit int) error {
	if limit < 0 {
		return errors.New("limit must not be negative")
	}
	kafka, topic, avro, err := loadConfig(ctx, configPath)
	if err != nil {
		return err
	}
	tlsConfig, err := kafka.TLS.LoadTLSConfig(ctx)
	if err != nil {
		return fmt.Errorf("load Kafka TLS files: %w", err)
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(kafka.Brokers...), kgo.DialTLSConfig(tlsConfig),
		kgo.ClientID("cnpg-to-kafka-inspect"), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	decoders := make([]*registry.Decoder, 0, len(avro.Registry.URLs))
	var clientErrors []error
	for _, endpoint := range avro.Registry.URLs {
		registryClient, err := avroprocessor.NewRegistryClient(ctx, endpoint, avro.Registry)
		if err != nil {
			clientErrors = append(clientErrors, err)
			continue
		}
		decoders = append(decoders, registry.NewDecoder(registryClient))
	}
	if len(decoders) == 0 {
		return errors.Join(clientErrors...)
	}
	encoder := json.NewEncoder(out)
	count := 0
	for {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("Kafka fetch: %w", errs[0].Err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			var event avroprocessor.Event
			var failures []error
			// Native Decoder validates the five-byte prefix, fetches the schema
			// by the actual wire ID (not configured version), and caches it.
			for _, decoder := range decoders {
				err = decoder.Decode(ctx, record.Value, &event)
				if err == nil {
					break
				}
				failures = append(failures, err)
			}
			if err != nil {
				return fmt.Errorf("decode %s/%d/%d: %w", record.Topic, record.Partition, record.Offset, errors.Join(failures...))
			}
			if err := encoder.Encode(event); err != nil {
				return err
			}
			count++
			if limit > 0 && count >= limit {
				return nil
			}
		}
	}
}

func loadConfig(ctx context.Context, path string) (configkafka.ClientConfig, string, avroprocessor.Config, error) {
	kafka := configkafka.NewDefaultClientConfig()
	var avro avroprocessor.Config
	resolver, err := confmap.NewResolver(confmap.ResolverSettings{
		URIs:              []string{"file:" + path},
		ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory()},
	})
	if err != nil {
		return kafka, "", avro, err
	}
	defer resolver.Shutdown(context.Background())
	cfg, err := resolver.Resolve(ctx)
	if err != nil {
		return kafka, "", avro, err
	}
	avroConf, err := cfg.Sub("processors::avro")
	if err != nil {
		return kafka, "", avro, err
	}
	if err := avroConf.Unmarshal(&avro); err != nil {
		return kafka, "", avro, err
	}
	if err := avro.Validate(); err != nil {
		return kafka, "", avro, err
	}
	kafkaConf, err := cfg.Sub("exporters::kafka/cnpg")
	if err != nil {
		return kafka, "", avro, err
	}
	// Reuse the native client configuration; producer/queue fields are irrelevant
	// to a read-only consumer. Do not introduce an inspector config format.
	if err := kafkaConf.Unmarshal(&kafka, confmap.WithIgnoreUnused()); err != nil {
		return kafka, "", avro, err
	}
	topic, ok := kafkaConf.Get("logs::topic").(string)
	if !ok || topic == "" || len(kafka.Brokers) == 0 {
		return kafka, "", avro, errors.New("exporters.kafka/cnpg requires brokers and logs.topic")
	}
	tls := kafka.TLS
	if tls == nil || tls.CAFile == "" || tls.CertFile == "" || tls.KeyFile == "" ||
		tls.Insecure || tls.InsecureSkipVerify || tls.IncludeSystemCACertsPool ||
		tls.CAPem != "" || tls.CertPem != "" || tls.KeyPem != "" || tls.TPMConfig.Enabled {
		return kafka, "", avro, errors.New("Kafka requires verified file-only mTLS without system CA fallback")
	}
	return kafka, topic, avro, nil
}
