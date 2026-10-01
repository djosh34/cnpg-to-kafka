package avroprocessor_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
)

func TestProcessorFramesNestedEventsAndPreservesRetryInput(t *testing.T) {
	ctx := context.Background()
	clientTLS, serverTLS := registryTestTLS(t)
	schemaBytes, err := os.ReadFile("../../schema/connection-event.avsc")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := avro.Parse(string(schemaBytes))
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	server := registryTestServer(t, serverTLS, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodGet {
			t.Errorf("runtime registry write: %s", r.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 0x12345678, "version": 4, "schema": string(schemaBytes)})
	})
	cfg := &avroprocessor.Config{Registry: registryTestConfig(clientTLS, server.URL)}
	var batches []plog.Logs
	queueFull := errors.New("synthetic downstream queue full")
	next, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		batches = append(batches, logs)
		if len(batches) == 1 {
			// A mutating downstream consumer must not corrupt the retry input.
			logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Attributes().Clear()
			return queueFull
		}
		return nil
	}, consumer.WithCapabilities(consumer.Capabilities{MutatesData: true}))
	if err != nil {
		t.Fatal(err)
	}
	factory := avroprocessor.NewFactory()
	p, err := factory.CreateLogs(ctx, processor.Settings{
		ID: component.NewID(factory.Type()), TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, cfg, next)
	if err != nil {
		t.Fatal(err)
	}
	if p.Capabilities().MutatesData {
		t.Fatal("processor must preserve retry input")
	}
	if err := p.Start(ctx, componenttest.NewNopHost()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Shutdown(ctx) })
	server.Close() // Startup schema and ID must suffice for every following event.

	input := plog.NewLogs()
	records := input.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	want := []avroprocessor.Event{
		{Role: "app", Hostname: "192.0.2.10", EventType: "LOGIN", Context: avroprocessor.EventContext{Database: "db"}},
		{Role: "app", Hostname: "client.example", EventType: "LOGOUT"},
		{EventType: "LOGIN_FAILED"},
		{Role: "excluded-role", EventType: "LOGIN_FAILED", Context: avroprocessor.EventContext{Database: "db"}},
	}
	for _, event := range want {
		r := records.AppendEmpty()
		r.Body().SetStr("native body ignored, not reparsed")
		a := r.Attributes()
		a.PutStr("eventtype", event.EventType)
		if event.Role != "" {
			a.PutStr("role", event.Role)
		}
		if event.Hostname != "" {
			a.PutStr("hostname", event.Hostname)
		}
		a.PutStr("database", event.Context.Database)
		a.PutStr("unrelated-extra", "ignored")
	}
	// Successful events missing role/client, and absent eventtype, are skipped.
	for _, attrs := range []map[string]string{
		{"eventtype": "LOGIN", "hostname": "client"},
		{"eventtype": "LOGOUT", "role": "app"},
		{"role": "app", "hostname": "client"},
	} {
		r := records.AppendEmpty()
		r.Body().SetStr("unchanged missing-field input")
		for k, v := range attrs {
			r.Attributes().PutStr(k, v)
		}
	}
	if err := p.ConsumeLogs(ctx, input); !errors.Is(err, queueFull) {
		t.Fatalf("downstream error = %v", err)
	}
	if input.LogRecordCount() != 7 || records.At(0).Body().Type() != pcommon.ValueTypeStr {
		t.Fatal("processor altered retry input")
	}
	if _, ok := records.At(0).Attributes().Get("role"); !ok {
		t.Fatal("downstream mutation leaked into retry input")
	}
	if err := p.ConsumeLogs(ctx, input); err != nil {
		t.Fatal(err)
	}
	gotRecords := batches[1].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if gotRecords.Len() != len(want) {
		t.Fatalf("got %d events, want %d", gotRecords.Len(), len(want))
	}
	for i, expected := range want {
		frame := gotRecords.At(i).Body().Bytes().AsRaw()
		if len(frame) < 5 || frame[0] != 0 || binary.BigEndian.Uint32(frame[1:5]) != 0x12345678 {
			t.Fatalf("invalid Confluent frame: %x", frame)
		}
		var got avroprocessor.Event
		if err := avro.Unmarshal(schema, frame[5:], &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, expected) {
			t.Fatalf("event %d = %+v, want %+v", i, got, expected)
		}
	}
	if requests.Load() != 1 {
		t.Fatalf("%d registry requests, want startup lookup only", requests.Load())
	}

	// Invalid native event symbols are rejected by the schema-based codec,
	// rather than a duplicate Go classifier.
	invalid := plog.NewLogs()
	a := invalid.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Attributes()
	a.PutStr("role", "app")
	a.PutStr("hostname", "client")
	a.PutStr("eventtype", "NOT_AN_EVENT")
	if err := p.ConsumeLogs(ctx, invalid); !consumererror.IsPermanent(err) {
		t.Fatalf("invalid enum should be a permanent encoding error, got %v", err)
	}
}

func TestProcessorFailsStartupWhenAllRegistriesFail(t *testing.T) {
	clientTLS, serverTLS := registryTestTLS(t)
	server := registryTestServer(t, serverTLS, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "registry down", http.StatusServiceUnavailable)
	})
	next, err := consumer.NewLogs(func(context.Context, plog.Logs) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	factory := avroprocessor.NewFactory()
	p, err := factory.CreateLogs(context.Background(), processor.Settings{
		ID: component.NewID(factory.Type()), TelemetrySettings: componenttest.NewNopTelemetrySettings(),
	}, &avroprocessor.Config{Registry: registryTestConfig(clientTLS, server.URL)}, next)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(context.Background(), componenttest.NewNopHost()); err == nil {
		t.Fatal("all registry failures must fail processor startup")
	}
}
