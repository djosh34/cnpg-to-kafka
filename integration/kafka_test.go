package integration_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/kafkaexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/kafka/configkafka"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/pdata/plog"
)

// These supplementary synthetic byte-body tests exercise native delivery, not
// CNPG classification or Avro encoding. Real recording/Redpanda tests cover that
// pipeline. A protocol fake cannot establish broker replication or durability.
func TestKafkaRawBodiesAndAcknowledgements(t *testing.T) {
	cluster := kafkaCluster(t, "events")
	var produce, producerID atomic.Int32
	cluster.ControlKey(0, func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if req.(*kmsg.ProduceRequest).Acks != -1 {
			t.Error("producer did not request all-ISR acknowledgements")
		}
		produce.Add(1)
		return nil, nil, false
	})
	cluster.ControlKey(22, func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		producerID.Add(1)
		return nil, nil, false
	})
	cfg := kafkaConfig(cluster, "events")
	cfg.QueueBatchConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	exp := startKafka(t, cfg, componenttest.NewNopHost())
	defer shutdown(t, exp)

	bodies := [][]byte{{0, 0, 0, 0, 7, 6, 'a', 'p', 'p'}, {0, 0, 0, 0, 7, 0, 0, 2}}
	require.NoError(t, exp.ConsumeLogs(t.Context(), byteLogs(bodies...)))
	got := consumeBodies(t, cluster, "events", len(bodies))
	require.Equal(t, bodies, got, "raw exporter must emit one unchanged Kafka value per log")
	require.Positive(t, produce.Load())
	require.Positive(t, producerID.Load(), "native producer must initialize idempotent production")
}

func TestKafkaMissingTopicIsNotCreated(t *testing.T) {
	cluster := kafkaCluster(t, "existing")
	var metadata, writes atomic.Int32
	cluster.Control(func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		switch r := req.(type) {
		case *kmsg.MetadataRequest:
			metadata.Add(1)
			if r.AllowAutoTopicCreation {
				t.Error("runtime requested automatic topic creation")
			}
		case *kmsg.CreateTopicsRequest:
			writes.Add(1)
		}
		return nil, nil, false
	})
	cfg := kafkaConfig(cluster, "absent")
	cfg.QueueBatchConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	cfg.BackOffConfig.Enabled = false
	cfg.TimeoutSettings.Timeout = 300 * time.Millisecond
	exp := startKafka(t, cfg, componenttest.NewNopHost())
	defer shutdown(t, exp)
	require.Error(t, exp.ConsumeLogs(t.Context(), byteLogs([]byte{0, 0, 0, 0, 7, 0})))
	require.Positive(t, metadata.Load())
	require.Zero(t, writes.Load())

	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	name := "absent"
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: &name}}
	resp, err := req.RequestWith(ctx, client)
	require.NoError(t, err)
	require.Len(t, resp.Topics, 1)
	require.Equal(t, int16(3), resp.Topics[0].ErrorCode, "missing topic must stay missing")
}

func TestKafkaPersistentQueueSurvivesOutageRestart(t *testing.T) {
	cluster := kafkaCluster(t, "events")
	var outage atomic.Bool
	outage.Store(true)
	attempted := make(chan struct{}, 1)
	cluster.ControlKey(0, func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if outage.Load() {
			select {
			case attempted <- struct{}{}:
			default:
			}
			return nil, errors.New("synthetic Kafka transport outage"), true
		}
		return nil, nil, false
	})
	state := t.TempDir()
	cfg := kafkaConfig(cluster, "events")
	storageID := component.MustNewID("file_storage")
	queue := cfg.QueueBatchConfig.Get()
	queue.StorageID = &storageID
	queue.QueueSize = 4
	queue.NumConsumers = 1
	queue.BlockOnOverflow = true
	bodies := [][]byte{{0, 0, 0, 0, 7, 1}, {0, 0, 0, 0, 7, 2}, {0, 0, 0, 0, 7, 3}}

	// Abruptly terminate a real subprocess after all complete entries were
	// accepted into native fsync storage, while Kafka cannot acknowledge them.
	binary, err := os.Executable()
	require.NoError(t, err)
	child := exec.CommandContext(t.Context(), binary, "-test.run=^TestKafkaQueueCrashWriter$")
	child.Env = append(os.Environ(), "CNPG_QUEUE_TEST_STATE="+state, "CNPG_QUEUE_TEST_BROKER="+cluster.ListenAddrs()[0])
	var output bytes.Buffer
	child.Stdout, child.Stderr = &output, &output
	require.NoError(t, child.Start())
	t.Cleanup(func() {
		if child.ProcessState == nil {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	})
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(state, "queued"))
		return err == nil
	}, 8*time.Second, 10*time.Millisecond, "child did not durably enqueue its entries")
	select {
	case <-attempted:
	case <-time.After(5 * time.Second):
		t.Fatal("exporter did not attempt Kafka delivery during outage")
	}
	require.NoError(t, child.Process.Kill())
	require.Error(t, child.Wait(), "child must be abruptly terminated, not gracefully drained")
	t.Logf("queue subprocess output: %s", output.String())

	// Reopen the native store, not a custom spool. Complete entries survive;
	// broker-ack/local-delete crash duplicate windows remain accepted.
	outage.Store(false)
	host, storage := startStorage(t, state, storageID)
	exp := startKafka(t, cfg, host)
	require.ElementsMatch(t, bodies, consumeBodies(t, cluster, "events", len(bodies)))
	shutdown(t, exp)
	shutdown(t, storage)

	// Clean shutdown/restart must not republish acknowledged entries.
	host, storage = startStorage(t, state, storageID)
	exp = startKafka(t, cfg, host)
	defer shutdown(t, storage)
	defer shutdown(t, exp)
	marker := []byte{0, 0, 0, 0, 7, 4}
	require.NoError(t, exp.ConsumeLogs(t.Context(), byteLogs(marker)))
	want := append(bodies, marker)
	require.ElementsMatch(t, want, consumeBodies(t, cluster, "events", len(want)))
}

// TestKafkaQueueCrashWriter is invoked only by the persistence test subprocess.
func TestKafkaQueueCrashWriter(t *testing.T) {
	state := os.Getenv("CNPG_QUEUE_TEST_STATE")
	if state == "" {
		return
	}
	storageID := component.MustNewID("file_storage")
	host, _ := startStorage(t, state, storageID)
	cfg := kafkaConfigBrokers([]string{os.Getenv("CNPG_QUEUE_TEST_BROKER")}, "events")
	queue := cfg.QueueBatchConfig.Get()
	queue.StorageID = &storageID
	queue.QueueSize = 4
	queue.NumConsumers = 1
	queue.BlockOnOverflow = true
	exp := startKafka(t, cfg, host)
	for i := byte(1); i <= 3; i++ {
		require.NoError(t, exp.ConsumeLogs(t.Context(), byteLogs([]byte{0, 0, 0, 0, 7, i})))
	}
	require.NoError(t, os.WriteFile(filepath.Join(state, "queued"), nil, 0o600))
	// Parent kills this process without closing exporter or storage.
	<-t.Context().Done()
}

func TestKafkaQueueSaturationBackpressures(t *testing.T) {
	cluster := kafkaCluster(t, "events")
	var outage atomic.Bool
	outage.Store(true)
	cluster.ControlKey(0, func(kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		if outage.Load() {
			return nil, errors.New("synthetic Kafka transport outage"), true
		}
		return nil, nil, false
	})
	storageID := component.MustNewID("file_storage")
	host, storage := startStorage(t, t.TempDir(), storageID)
	defer shutdown(t, storage)
	cfg := kafkaConfig(cluster, "events")
	queue := cfg.QueueBatchConfig.Get()
	queue.StorageID = &storageID
	queue.Sizer = exporterhelper.RequestSizerTypeBytes
	queue.QueueSize = int64((&plog.ProtoMarshaler{}).LogsSize(byteLogs([]byte{0, 0, 0, 0, 7, 1})))
	queue.NumConsumers = 1
	queue.BlockOnOverflow = true
	exp := startKafka(t, cfg, host)
	defer shutdown(t, exp)

	// Match the example's native byte-sized persistent queue, with room for
	// one complete request (including in-flight entries). Further offers must
	// honor cancellation rather than silently evicting an already queued entry.
	accepted := make([][]byte, 0, 3)
	blocked := false
	for i := byte(1); i <= 3; i++ {
		body := []byte{0, 0, 0, 0, 7, i}
		ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
		err := exp.ConsumeLogs(ctx, byteLogs(body))
		cancel()
		if err != nil {
			require.ErrorIs(t, err, context.DeadlineExceeded)
			blocked = true
			break
		}
		accepted = append(accepted, body)
	}
	require.True(t, blocked, "finite queue must exert native backpressure")
	require.NotEmpty(t, accepted)
	outage.Store(false)
	got := consumeBodies(t, cluster, "events", len(accepted))
	require.Equal(t, accepted, got, "outage must not discard previously accepted entries")
}

func kafkaCluster(t *testing.T, topic string) *kfake.Cluster {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic), kfake.AllowAutoTopicCreation())
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return cluster
}

func kafkaConfig(cluster *kfake.Cluster, topic string) *kafkaexporter.Config {
	return kafkaConfigBrokers(cluster.ListenAddrs(), topic)
}

func kafkaConfigBrokers(brokers []string, topic string) *kafkaexporter.Config {
	cfg := kafkaexporter.NewFactory().CreateDefaultConfig().(*kafkaexporter.Config)
	cfg.ClientConfig.Brokers = brokers
	cfg.Producer.RequiredAcks = configkafka.WaitForAll
	cfg.Producer.AllowAutoTopicCreation = false
	cfg.Logs.Topic = topic
	cfg.Logs.Encoding = "raw"
	cfg.TimeoutSettings.Timeout = 250 * time.Millisecond
	cfg.BackOffConfig.InitialInterval = 10 * time.Millisecond
	cfg.BackOffConfig.MaxInterval = 30 * time.Millisecond
	cfg.BackOffConfig.MaxElapsedTime = 0
	return cfg
}

func startKafka(t *testing.T, cfg *kafkaexporter.Config, host component.Host) exporter.Logs {
	t.Helper()
	factory := kafkaexporter.NewFactory()
	settings := exportertest.NewNopSettings(factory.Type())
	// NewNopSettings randomizes the ID; persisted queue identity must not change
	// across exporter/process restarts, just as a native config component ID.
	settings.ID = component.NewID(factory.Type())
	exp, err := factory.CreateLogs(t.Context(), settings, cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), host))
	return exp
}

type storageHost struct {
	component.Host
	extensions map[component.ID]component.Component
}

func (h storageHost) GetExtensions() map[component.ID]component.Component { return h.extensions }

func startStorage(t *testing.T, dir string, id component.ID) (component.Host, component.Component) {
	t.Helper()
	factory := filestorage.NewFactory()
	cfg := factory.CreateDefaultConfig().(*filestorage.Config)
	cfg.Directory = dir
	cfg.FSync = true
	cfg.MaxSize = 16 << 20
	storage, err := factory.Create(t.Context(), extensiontest.NewNopSettings(factory.Type()), cfg)
	require.NoError(t, err)
	host := storageHost{Host: componenttest.NewNopHost(), extensions: map[component.ID]component.Component{id: storage}}
	require.NoError(t, storage.Start(t.Context(), host))
	return host, storage
}

func shutdown(t *testing.T, c component.Component) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, c.Shutdown(ctx))
}

func byteLogs(bodies ...[]byte) plog.Logs {
	logs := plog.NewLogs()
	records := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for _, body := range bodies {
		records.AppendEmpty().Body().SetEmptyBytes().FromRaw(body)
	}
	return logs
}

func consumeBodies(t *testing.T, cluster *kfake.Cluster, topic string, count int) [][]byte {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer client.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	var bodies [][]byte
	for len(bodies) < count {
		fetches := client.PollFetches(ctx)
		require.NoError(t, fetches.Err())
		fetches.EachRecord(func(r *kgo.Record) { bodies = append(bodies, bytes.Clone(r.Value)) })
	}
	return bodies
}
