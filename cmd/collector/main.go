// Command collector runs the OpenTelemetry Collector with the components that
// turn CloudNativePG connection logs into Kafka events.
package main

import (
	"fmt"
	"os"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/debugexporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"

	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
	"github.com/djosh34/cnpg-to-kafka/processor/cnpgprocessor"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := enableGates(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := otelcol.NewCommand(settings()).Execute(); err != nil {
		os.Exit(1)
	}
}

// enableGates turns on stanza.synchronousLogEmitter. The file log receiver then
// sends each batch from the goroutine that read it, so batches reach the cnpg
// processor in read order, and lines are marked as read only after they are
// sent.
func enableGates() error {
	return featuregate.GlobalRegistry().Set("stanza.synchronousLogEmitter", true)
}

func settings() otelcol.CollectorSettings {
	return otelcol.CollectorSettings{
		BuildInfo: component.BuildInfo{
			Command:     "cnpg-to-kafka",
			Description: "CloudNativePG connection events to Kafka",
			Version:     version,
		},
		Factories: factories,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				// The --config flag replaces this default.
				URIs:          []string{"file:/etc/cnpg-to-kafka/config.yaml"},
				DefaultScheme: "file",
				ProviderFactories: []confmap.ProviderFactory{
					fileprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
	}
}

func factories() (otelcol.Factories, error) {
	// MakeFactoryMap also registers the receiver under its older name, filelog.
	receivers, err := otelcol.MakeFactoryMap[receiver.Factory](filelogreceiver.NewFactory())
	if err != nil {
		return otelcol.Factories{}, err
	}
	cnpg := cnpgprocessor.NewFactory()
	avro := avroprocessor.NewFactory()
	kafka := kafkaexporter.NewFactory()
	debug := debugexporter.NewFactory()
	storage := filestorage.NewFactory()
	return otelcol.Factories{
		Receivers:  receivers,
		Processors: map[component.Type]processor.Factory{cnpg.Type(): cnpg, avro.Type(): avro},
		Exporters:  map[component.Type]exporter.Factory{kafka.Type(): kafka, debug.Type(): debug},
		Extensions: map[component.Type]extension.Factory{storage.Type(): storage},
		Telemetry:  otelconftelemetry.NewFactory(),
	}, nil
}
