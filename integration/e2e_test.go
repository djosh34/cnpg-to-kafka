package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/djosh34/cnpg-to-kafka/internal/inspect"
	"github.com/djosh34/cnpg-to-kafka/internal/replay"
	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/config/configtls"
)

// Opt-in: setup alone provisions the disposable topic/schema. Neither the
// Collector nor this read-only consumer provisions production objects.
func TestRealReplay(t *testing.T) {
	if os.Getenv("CNPG_E2E") != "1" {
		t.Skip("set CNPG_E2E=1 after scripts/e2e-setup.sh to run real Redpanda acceptance")
	}
	binaryPath := os.Getenv("CNPG_COLLECTOR_BIN")
	require.NotEmpty(t, binaryPath, "CNPG_COLLECTOR_BIN must name the actual Collector binary")
	binaryPath, err := filepath.Abs(binaryPath)
	require.NoError(t, err)
	root, err := filepath.Abs("..")
	require.NoError(t, err)
	certs := os.Getenv("CNPG_E2E_CERTS")
	if certs == "" {
		certs = filepath.Join(root, "integration", ".certs")
	}
	project := os.Getenv("CNPG_E2E_PROJECT")
	if project == "" {
		project = "cnpg-e2e"
	}
	composePath := os.Getenv("CNPG_E2E_COMPOSE")
	if composePath == "" {
		composePath = filepath.Join(root, "integration", "compose.yaml")
	}
	compose := func(t *testing.T, args ...string) {
		t.Helper()
		// testing cancels t.Context before cleanup; broker restoration must
		// still run there. The command always has its own finite deadline.
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-p", project, "-f", composePath}, args...)...)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
	}
	artifactPath := os.Getenv("CNPG_DECODED_EVENTS")
	if artifactPath == "" {
		artifactPath = filepath.Join(root, "decoded-events.jsonl")
	}
	artifact, err := os.Create(artifactPath)
	require.NoError(t, err)
	defer artifact.Close()
	output := json.NewEncoder(io.MultiWriter(os.Stdout, artifact))
	tlsFiles := configtls.ClientConfig{Config: configtls.Config{
		CAFile: filepath.Join(certs, "ca.crt"), CertFile: filepath.Join(certs, "client.crt"), KeyFile: filepath.Join(certs, "client.key"),
	}}
	regConfig := avroprocessor.RegistryConfig{
		URLs: []string{"https://localhost:18081"}, Subject: "cnpg-connections-value", Version: "latest", RequestTimeout: 3 * time.Second, TLS: tlsFiles,
	}
	regClient, err := avroprocessor.NewRegistryClient(t.Context(), regConfig.URLs[0], regConfig)
	require.NoError(t, err)
	info, err := avroprocessor.LoadSchema(t.Context(), regConfig)
	require.NoError(t, err)
	decoder := inspect.NewDecoder(regClient)
	tlsConfig, err := tlsFiles.LoadTLSConfig(t.Context())
	require.NoError(t, err)
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:19092"), kgo.DialTLSConfig(tlsConfig))
	require.NoError(t, err)
	defer client.Close()
	// Setup may preserve a previous run's topic. Snapshot native end offsets
	// before any Collector starts, without deleting/provisioning runtime objects.
	baselineCtx, baselineCancel := context.WithTimeout(t.Context(), 10*time.Second)
	ends, err := kadm.NewClient(client).ListEndOffsets(baselineCtx, "cnpg-connections")
	baselineCancel()
	require.NoError(t, err)
	require.NoError(t, ends.Error())
	require.NotEmpty(t, ends["cnpg-connections"])
	client.AddConsumePartitions(ends.KOffsets())

	// collect observes a bounded quiet period after filesystem work completes.
	// Every successfully decoded record is flushed to the failure-safe artifact
	// before content assertions, not only when the test eventually passes.
	collect := func(t *testing.T, quiet time.Duration) []avroprocessor.Event {
		t.Helper()
		var events []avroprocessor.Event
		until := time.Now().Add(quiet)
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(until) && time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
			fetches := client.PollFetches(ctx)
			cancel()
			for _, failure := range fetches.Errors() {
				if failure.Err != context.DeadlineExceeded {
					t.Fatalf("Kafka consume: %v", failure.Err)
				}
			}
			iter := fetches.RecordIter()
			for !iter.Done() {
				r := iter.Next()
				require.GreaterOrEqual(t, len(r.Value), 5)
				require.Equal(t, byte(0), r.Value[0], "Confluent magic byte")
				require.Equal(t, uint32(info.ID), binary.BigEndian.Uint32(r.Value[1:5]), "fetched authoritative schema ID")
				var event avroprocessor.Event
				require.NoError(t, decoder.Decode(t.Context(), r.Value, &event))
				require.NoError(t, output.Encode(event))
				require.NoError(t, artifact.Sync())
				events = append(events, event)
				until = time.Now().Add(quiet)
			}
		}
		return events
	}
	configSource, err := os.ReadFile(filepath.Join(root, "config.yaml"))
	require.NoError(t, err)
	makeConfig := func(t *testing.T, dir, podPattern, version string) string {
		t.Helper()
		replacements := strings.NewReplacer(
			"/var/log/pods/database_", filepath.Join(dir, "pods", "capture_"),
			"/var/log/pods/", filepath.Join(dir, "pods")+"/",
			`matches ".*"`, `matches "`+podPattern+`"`,
			`["postgres", "streaming_replica"]`, `["excluded"]`,
			"https://registry-1.example.com:8081, https://registry-2.example.com:8081", "https://localhost:1, https://localhost:18081",
			"version: latest", "version: \""+version+"\"",
			"request_timeout: 10s", "request_timeout: 1s",
			"/etc/cnpg-to-kafka/tls/ca.crt", filepath.Join(certs, "ca.crt"),
			"/etc/cnpg-to-kafka/tls/tls.crt", filepath.Join(certs, "client.crt"),
			"/etc/cnpg-to-kafka/tls/tls.key", filepath.Join(certs, "client.key"),
			"kafka.example.com:9093", "localhost:19092",
			"/var/lib/cnpg-to-kafka", filepath.Join(dir, "state"),
			"poll_interval: 200ms", "poll_interval: 10ms",
		)
		path := filepath.Join(dir, "config.yaml")
		require.NoError(t, os.WriteFile(path, []byte(replacements.Replace(string(configSource))), 0600))
		return path
	}
	start := func(t *testing.T, config string) func(bool) {
		t.Helper()
		log, err := os.CreateTemp(filepath.Dir(config), "collector-*.log")
		require.NoError(t, err)
		logPath := log.Name()
		cmd := exec.Command(binaryPath, "--config=file:"+config)
		cmd.Stdout, cmd.Stderr = log, log
		require.NoError(t, cmd.Start())
		done := make(chan error, 1)
		go func() { done <- cmd.Wait(); log.Close() }()
		stopped := false
		t.Cleanup(func() {
			if !stopped {
				_ = cmd.Process.Kill()
				<-done
			}
			if t.Failed() {
				data, _ := os.ReadFile(logPath)
				t.Logf("Collector log %s:\n%s", logPath, data)
			}
		})
		ready := false
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case err := <-done:
				stopped = true
				data, _ := os.ReadFile(logPath)
				t.Fatalf("Collector exited before readiness: %v\n%s", err, data)
			default:
			}
			data, _ := os.ReadFile(logPath)
			if bytes.Contains(data, []byte("Everything is ready")) {
				ready = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		require.True(t, ready, "Collector readiness: %s", logPath)
		return func(abrupt bool) {
			t.Helper()
			signal := syscall.SIGTERM
			if abrupt {
				signal = syscall.SIGKILL
			}
			require.NoError(t, cmd.Process.Signal(signal))
			select {
			case err := <-done:
				stopped = true
				if abrupt {
					var exit *exec.ExitError
					require.ErrorAs(t, err, &exit)
					require.Equal(t, syscall.SIGKILL, exit.Sys().(syscall.WaitStatus).Signal())
				} else {
					require.NoError(t, err, "clean shutdown must succeed, not be killed")
				}
			case <-time.After(10 * time.Second):
				t.Fatal("normal clean shutdown exceeded 10s")
			}
		}
	}
	recordingPath := os.Getenv("CNPG_RECORDING")
	if recordingPath == "" {
		recordingPath = filepath.Join(root, "testdata", "capture", "operations.jsonl")
	}
	recording, err := os.ReadFile(recordingPath)
	require.NoError(t, err, "canonical capture is required; synthetic inputs cannot replace it")

	// Each run narrows path metadata to one real instance so primary and replicas
	// are asserted independently although the public Avro schema has no pod field.
	for _, pod := range []string{"cnpg-1", "cnpg-2", "cnpg-3"} {
		t.Run("captured-"+pod, func(t *testing.T) {
			dir := t.TempDir()
			pods := filepath.Join(dir, "pods")
			require.NoError(t, os.Mkdir(pods, 0700))
			config := makeConfig(t, dir, "^"+pod+"$", "latest")
			stop := start(t, config)
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			require.NoError(t, replay.Run(ctx, pods, bytes.NewReader(recording), 5))
			events := collect(t, 3*time.Second)
			counts := map[string]int{}
			for _, e := range events {
				switch e.Role {
				case "included", "excluded", "unknown_role":
					require.Equal(t, "10.42.0.6", e.Hostname)
					require.Equal(t, "app", e.Context.Database)
					counts[e.Role+"/"+e.EventType]++
				}
			}
			// Manually inspected actual capture counts; SSL-prefer failed attempts
			// really generated two records. Do not deduplicate or halve them.
			require.Equal(t, map[string]int{"included/LOGIN": 17, "included/LOGOUT": 17,
				"included/LOGIN_FAILED": 28, "excluded/LOGIN_FAILED": 28, "unknown_role/LOGIN_FAILED": 28}, counts)
			stop(false)
			stop = start(t, config)
			require.Empty(t, collect(t, 3*time.Second), "persisted offsets: clean restart must not replay old logs")
			stop(false)
		})
	}

	t.Run("supplementary-synthetic-backlog-fields-and-outage", func(t *testing.T) {
		dir := t.TempDir()
		pods := filepath.Join(dir, "pods")
		require.NoError(t, os.Mkdir(pods, 0700))
		config := makeConfig(t, dir, "^cnpg-synthetic$", "1")
		// Native telemetry lets this test wait for successful queue acceptance,
		// not guess that a sleep or source write means the fsync queue is complete.
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		port := listener.Addr().(*net.TCPAddr).Port
		require.NoError(t, listener.Close())
		configBytes, err := os.ReadFile(config)
		require.NoError(t, err)
		metricsConfig := fmt.Sprintf("metrics:\n      level: normal\n      readers:\n        - pull:\n            exporter:\n              prometheus:\n                host: 127.0.0.1\n                port: %d", port)
		require.NoError(t, os.WriteFile(config, []byte(strings.Replace(string(configBytes), "metrics:\n      level: none", metricsConfig, 1)), 0600))
		path := filepath.Join(pods, "capture_cnpg-synthetic_00000000-0000-0000-0000-000000000001", "postgres", "0.log")
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
		// These are expressly invented edge cases, separate from real capture.
		line := func(role, host, database, message, severity, code string) string {
			r := map[string]any{"logger": "postgres", "record": map[string]any{
				"user_name": role, "connection_from": host, "database_name": database,
				"message": message, "error_severity": severity, "sql_state_code": code}, "extra": "ignored"}
			data, err := json.Marshal(r)
			require.NoError(t, err)
			return "2026-10-01T00:00:00.000000000Z stdout F " + string(data) + "\n"
		}
		appendLines := func(data string) {
			f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			require.NoError(t, err)
			_, err = io.WriteString(f, data)
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}
		appendLines(line("included", "client.example:123", "edge", "connection authorized: synthetic", "LOG", "00000") +
			line("included", "[2001:db8::1]:123", "edge", "disconnection: synthetic", "LOG", "00000") +
			line("excluded", "10.0.0.1:123", "", "password authentication failed", "FATAL", "28P01") +
			line("", "", "", "password authentication failed", "FATAL", "28P01") +
			line("excluded", "10.0.0.1:123", "edge", "connection authorized: excluded", "LOG", "00000") +
			line("excluded", "10.0.0.1:123", "edge", "disconnection: excluded", "LOG", "00000") +
			line("", "10.0.0.1:123", "edge", "connection authorized: missing role", "LOG", "00000") +
			line("included", "", "edge", "disconnection: missing client", "LOG", "00000") +
			line("included", "10.0.0.1:123", "edge", "server shutting down", "FATAL", "57P01") +
			line("included", "10.0.0.1:123", "edge", "connection received: progress", "LOG", "00000") +
			"2026-10-01T00:00:00.000000000Z stdout F {invalid JSON}\n")
		stop := start(t, config) // First-start backlog must be read from beginning.
		events := collect(t, 3*time.Second)
		require.ElementsMatch(t, []avroprocessor.Event{
			{Role: "included", Hostname: "client.example", EventType: "LOGIN", Context: avroprocessor.EventContext{Database: "edge"}},
			{Role: "included", Hostname: "2001:db8::1", EventType: "LOGOUT", Context: avroprocessor.EventContext{Database: "edge"}},
			{Role: "excluded", Hostname: "10.0.0.1", EventType: "LOGIN_FAILED"},
			{EventType: "LOGIN_FAILED"},
		}, events)
		stop(false)
		stop = start(t, config)
		require.Empty(t, collect(t, 3*time.Second))
		t.Cleanup(func() { compose(t, "start", "--wait", "--wait-timeout", "150", "redpanda") })
		compose(t, "stop", "-t", "5", "redpanda")
		// The loaded schema is still usable while the single broker AND registry
		// are down. Native retry/persistent queue own buffering, not our code.
		for i := 0; i < 40; i++ {
			appendLines(line("included", "10.0.0.2:321", fmt.Sprintf("outage-%d", i), "connection authorized: outage", "LOG", "00000"))
		}
		time.Sleep(3 * time.Second)
		compose(t, "start", "--wait", "--wait-timeout", "150", "redpanda")
		events = collect(t, 5*time.Second)
		require.Len(t, events, 40, "native persistent queue must recover after broker outage")
		for i, e := range events {
			require.Equal(t, avroprocessor.Event{Role: "included", Hostname: "10.0.0.2", EventType: "LOGIN", Context: avroprocessor.EventContext{Database: fmt.Sprintf("outage-%d", i)}}, e)
		}
		// A second outage deliberately kills the actual Collector, leaving its
		// fsync queue and source-offset databases intact. Complete CRI lines only;
		// accepted at-least-once duplicates are not mistaken for data loss.
		compose(t, "stop", "-t", "5", "redpanda")
		for i := 0; i < 40; i++ {
			appendLines(line("included", "10.0.0.3:321", fmt.Sprintf("crash-%d", i), "connection authorized: crash", "LOG", "00000"))
			// Separate native poll batches so one request is in flight while
			// other complete requests remain queued; telemetry below proves acceptance.
			time.Sleep(20 * time.Millisecond)
		}
		metricsClient := &http.Client{Timeout: time.Second}
		deadline := time.Now().Add(15 * time.Second)
		queued := false
		var metrics string
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/metrics", port), nil)
			require.NoError(t, err)
			resp, err := metricsClient.Do(req)
			if err == nil {
				data, readErr := io.ReadAll(resp.Body)
				resp.Body.Close()
				require.NoError(t, readErr)
				metrics = string(data)
				accepted, size := float64(0), float64(0)
				for _, row := range strings.Split(metrics, "\n") {
					fields := strings.Fields(row)
					if len(fields) < 2 || strings.HasPrefix(row, "#") {
						continue
					}
					value, err := strconv.ParseFloat(fields[1], 64)
					require.NoError(t, err)
					if strings.HasPrefix(fields[0], "otelcol_receiver_accepted_log_records") {
						accepted += value
					}
					if strings.HasPrefix(fields[0], "otelcol_exporter_queue_size") {
						size += value
					}
				}
				queued = accepted >= 80 && size > 0 // 40 prior outage + 40 crash records since restart.
			}
			cancel()
			if queued {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		require.True(t, queued, "all complete records must enter the native persistent queue before SIGKILL:\n%s", metrics)
		stop(true)
		compose(t, "start", "--wait", "--wait-timeout", "150", "redpanda")
		// One container supplies both services. Registry readiness must be restored
		// before new Collector startup performs its one read-only schema lookup.
		deadline = time.Now().Add(30 * time.Second)
		for {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			_, err := avroprocessor.LoadSchema(ctx, regConfig)
			cancel()
			if err == nil {
				break
			}
			require.True(t, time.Now().Before(deadline), "registry recovery: %v", err)
			time.Sleep(100 * time.Millisecond)
		}
		stop = start(t, config)
		events = collect(t, 5*time.Second)
		seen := map[string]bool{}
		for _, event := range events {
			require.Equal(t, "included", event.Role)
			require.Equal(t, "10.0.0.3", event.Hostname)
			require.Equal(t, "LOGIN", event.EventType)
			seen[event.Context.Database] = true
		}
		require.Len(t, seen, 40, "complete durably queued records survive actual process death; duplicates accepted")
		for i := 0; i < 40; i++ {
			require.True(t, seen[fmt.Sprintf("crash-%d", i)])
		}
		stop(false)
	})
}
