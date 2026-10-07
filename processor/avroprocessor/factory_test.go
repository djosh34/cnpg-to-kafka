package avroprocessor_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"

	"github.com/djosh34/cnpg-to-kafka/internal/event"
	"github.com/djosh34/cnpg-to-kafka/internal/fixture"
	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
)

var login = event.Event{
	Timestamp:       1790893709982,
	EventType:       event.Login,
	AccountType:     event.AccountNPA,
	ApplicationName: "payments",
	HostData:        event.HostData{SourceHostname: "db1.example.com", SourceIP: "192.0.2.1"},
	ConnectionData: event.ConnectionData{
		Role: "included", Database: "app", CN: new("included"), AuthMethod: new("scram-sha-256"), ClientAddress: "10.42.0.6",
	},
}

func TestEncode(t *testing.T) {
	_, schema := fixture.Registry(t)
	withoutJoin := login
	withoutJoin.EventType, withoutJoin.ConnectionData.CN, withoutJoin.ConnectionData.AuthMethod = event.Logout, nil, nil
	unknownType := login
	unknownType.EventType = "RESET"
	unknownAccount := login
	unknownAccount.AccountType = "other"
	cases := []struct {
		name  string
		event event.Event
		ok    bool
	}{
		{"every field set", login, true},
		{"CN and method null", withoutJoin, true},
		{"every field empty except the enums", event.Event{EventType: event.LoginFailed, AccountType: event.AccountHA}, true},
		{"event type not in the schema", unknownType, false},
		{"account type not in the schema", unknownAccount, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			frame, err := avroprocessor.Encode(schema, c.event)
			if !c.ok {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Greater(t, len(frame), 5)
			assert.Equal(t, []byte{0, 0, 0, 0, fixture.SchemaID}, frame[:5])
			var got event.Event
			require.NoError(t, avro.Unmarshal(schema.Schema, frame[5:], &got))
			assert.Equal(t, c.event, got)
		})
	}
}

// start starts the processor with the given registry URLs. The returned
// function sends one batch through it and returns what comes out.
func start(t *testing.T, urls ...string) (func(plog.Logs) plog.Logs, error) {
	t.Helper()
	factory := avroprocessor.NewFactory()
	cfg := factory.CreateDefaultConfig().(*avroprocessor.Config)
	cfg.Registry.URLs, cfg.Registry.Subject = urls, "cnpg-connections-value"
	var out plog.Logs
	next, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		out = logs
		return nil
	})
	require.NoError(t, err)
	settings := processor.Settings{ID: component.NewID(factory.Type()), TelemetrySettings: componenttest.NewNopTelemetrySettings()}
	p, err := factory.CreateLogs(t.Context(), settings, cfg, next)
	require.NoError(t, err)
	if err := p.Start(t.Context(), componenttest.NewNopHost()); err != nil {
		return nil, err
	}
	t.Cleanup(func() { assert.NoError(t, p.Shutdown(context.Background())) })
	return func(in plog.Logs) plog.Logs {
		require.NoError(t, p.ConsumeLogs(t.Context(), in))
		return out
	}, nil
}

func TestProcessor(t *testing.T) {
	url, schema := fixture.Registry(t)
	process, err := start(t, url)
	require.NoError(t, err)
	unknownType := login
	unknownType.EventType = "RESET"
	cases := []struct {
		name string
		// set changes the attributes of a record.
		set func(plog.LogRecord)
		ok  bool
	}{
		{"event", func(r plog.LogRecord) { login.SetAttributes(r.Attributes()) }, true},
		{"event and other attributes", func(r plog.LogRecord) {
			login.SetAttributes(r.Attributes())
			r.Attributes().PutStr("log.iostream", "stderr")
		}, true},
		{"event the schema cannot encode", func(r plog.LogRecord) { unknownType.SetAttributes(r.Attributes()) }, false},
		{"incomplete event", func(r plog.LogRecord) {
			login.SetAttributes(r.Attributes())
			r.Attributes().Remove("cnpg.connectiondata.role")
		}, false},
		{"no event", func(r plog.LogRecord) { r.Body().SetStr("checkpoint starting: time") }, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := plog.NewLogs()
			c.set(in.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty())
			records := process(in).ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
			if !c.ok {
				assert.Equal(t, 0, records.Len())
				return
			}
			require.Equal(t, 1, records.Len())
			want, err := avroprocessor.Encode(schema, login)
			require.NoError(t, err)
			assert.Equal(t, want, records.At(0).Body().Bytes().AsRaw())
		})
	}
}

func TestRegistryFallback(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(down.Close)
	up, _ := fixture.Registry(t)
	cases := []struct {
		name string
		urls []string
		ok   bool
	}{
		{"first registry works", []string{up}, true},
		{"second registry works", []string{down.URL, up}, true},
		{"no registry works", []string{down.URL}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := start(t, c.urls...)
			if c.ok {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
