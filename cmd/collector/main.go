// Command collector runs the minimal CNPG connection-event Collector.
package main

import (
	"os"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"

	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
)

func main() {
	if err := otelcol.NewCommand(settings()).Execute(); err != nil {
		os.Exit(1)
	}
}

func settings() otelcol.CollectorSettings {
	return otelcol.CollectorSettings{
		BuildInfo: component.BuildInfo{
			Command:     "cnpg-to-kafka",
			Description: "CNPG connection events to Kafka",
			Version:     "0.1.0",
		},
		Factories: factories,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:              []string{"file:/etc/cnpg-to-kafka/config.yaml"},
				DefaultScheme:     "file",
				ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory()},
			},
		},
	}
}

func factories() (otelcol.Factories, error) {
	// Preserve the upstream filelog alias for the canonical file_log type.
	receivers, err := otelcol.MakeFactoryMap[receiver.Factory](filelogreceiver.NewFactory())
	if err != nil {
		return otelcol.Factories{}, err
	}
	avro := avroprocessor.NewFactory()
	kafka := kafkaexporter.NewFactory()
	storage := filestorage.NewFactory()
	return otelcol.Factories{
		Receivers:  receivers,
		Processors: map[component.Type]processor.Factory{avro.Type(): avro},
		Exporters:  map[component.Type]exporter.Factory{kafka.Type(): kafka},
		Extensions: map[component.Type]extension.Factory{storage.Type(): storage},
		Telemetry:  otelconftelemetry.NewFactory(),
	}, nil
}
