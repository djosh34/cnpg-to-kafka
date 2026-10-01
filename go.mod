module github.com/djosh34/cnpg-to-kafka

go 1.27.1

require (
	github.com/confluentinc/confluent-avro-go/v2 v2.32.0
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter v0.162.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage v0.162.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka v0.162.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver v0.162.0
	github.com/twmb/franz-go v1.22.0
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20260421215025-4e7a1e1569ac
	go.opentelemetry.io/collector/component v1.68.0
	go.opentelemetry.io/collector/component/componenttest v0.162.0
	go.opentelemetry.io/collector/config/configtls v1.68.0
	go.opentelemetry.io/collector/confmap v1.68.0
	go.opentelemetry.io/collector/confmap/provider/fileprovider v1.68.0
	go.opentelemetry.io/collector/consumer v1.68.0
	go.opentelemetry.io/collector/exporter v1.68.0
	go.opentelemetry.io/collector/exporter/exportertest v0.162.0
	go.opentelemetry.io/collector/extension v1.68.0
	go.opentelemetry.io/collector/extension/extensiontest v0.162.0
	go.opentelemetry.io/collector/otelcol v0.162.0
	go.opentelemetry.io/collector/pdata v1.68.0
	go.opentelemetry.io/collector/processor v1.68.0
	go.opentelemetry.io/collector/processor/processorhelper v0.162.0
	go.opentelemetry.io/collector/receiver v1.68.0
	go.opentelemetry.io/collector/service v0.162.0
)
