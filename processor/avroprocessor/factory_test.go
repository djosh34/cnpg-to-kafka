package avroprocessor_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"testing"

	"github.com/confluentinc/confluent-avro-go/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"

	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
)

const schemaID = 0x01020304

// registry serves schema/connection-event.avsc for every request.
func registry(t *testing.T) (*httptest.Server, avro.Schema) {
	text, err := os.ReadFile("../../schema/connection-event.avsc")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"id": schemaID, "version": 1, "schema": string(text)})
	}))
	t.Cleanup(server.Close)
	schema, err := avro.Parse(string(text))
	if err != nil {
		t.Fatal(err)
	}
	return server, schema
}

// start starts the processor with the given registry URLs. The returned
// function sends one batch through it and returns what comes out.
func start(t *testing.T, urls ...string) (func(plog.Logs) plog.Logs, error) {
	factory := avroprocessor.NewFactory()
	cfg := factory.CreateDefaultConfig().(*avroprocessor.Config)
	cfg.Registry.URLs, cfg.Registry.Subject = urls, "cnpg-connections-value"
	var out plog.Logs
	next, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		out = logs
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	settings := processor.Settings{ID: component.NewID(factory.Type()), TelemetrySettings: componenttest.NewNopTelemetrySettings()}
	p, err := factory.CreateLogs(t.Context(), settings, cfg, next)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(t.Context(), componenttest.NewNopHost()); err != nil {
		return nil, err
	}
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return func(in plog.Logs) plog.Logs {
		if err := p.ConsumeLogs(t.Context(), in); err != nil {
			t.Fatal(err)
		}
		return out
	}, nil
}

func TestEncoding(t *testing.T) {
	server, schema := registry(t)
	process, err := start(t, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		attributes map[string]string
		want       *avroprocessor.Event // nil if the processor drops the record
	}{
		{map[string]string{"eventtype": "LOGIN", "role": "app", "hostname": "192.0.2.10", "database": "db"},
			&avroprocessor.Event{Role: "app", Hostname: "192.0.2.10", EventType: "LOGIN", Context: avroprocessor.EventContext{Database: "db"}}},
		// The schema's enum has no such symbol, so this record cannot be encoded.
		{map[string]string{"eventtype": "RESET", "role": "app", "hostname": "192.0.2.10"}, nil},
		{map[string]string{"eventtype": "LOGOUT", "role": "app", "hostname": "client.example", "other": "ignored"},
			&avroprocessor.Event{Role: "app", Hostname: "client.example", EventType: "LOGOUT"}},
		{map[string]string{"eventtype": "LOGIN_FAILED"}, &avroprocessor.Event{EventType: "LOGIN_FAILED"}},
		{map[string]string{"eventtype": "LOGIN", "hostname": "192.0.2.10"}, nil},
		{map[string]string{"eventtype": "LOGOUT", "role": "app"}, nil},
		{map[string]string{"role": "app", "hostname": "192.0.2.10"}, nil},
	}
	in := plog.NewLogs()
	records := in.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	var want []avroprocessor.Event
	for _, c := range cases {
		record := records.AppendEmpty()
		for key, value := range c.attributes {
			record.Attributes().PutStr(key, value)
		}
		if c.want != nil {
			want = append(want, *c.want)
		}
	}

	var got []avroprocessor.Event
	for _, record := range process(in).ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().All() {
		frame := record.Body().Bytes().AsRaw()
		if len(frame) < 5 || frame[0] != 0 || binary.BigEndian.Uint32(frame[1:5]) != schemaID {
			t.Fatalf("frame % x does not start with a zero byte and schema ID %#x", frame, schemaID)
		}
		var event avroprocessor.Event
		if err := avro.Unmarshal(schema, frame[5:], &event); err != nil {
			t.Fatal(err)
		}
		got = append(got, event)
	}
	if !slices.Equal(got, want) {
		t.Errorf("events:\n got %+v\nwant %+v", got, want)
	}
}

func TestRegistryFallback(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)
	up, _ := registry(t)

	if _, err := start(t, down.URL, up.URL); err != nil {
		t.Errorf("start with a working second registry: %v", err)
	}
	if _, err := start(t, down.URL); err == nil {
		t.Error("start without a working registry succeeded")
	}
}
