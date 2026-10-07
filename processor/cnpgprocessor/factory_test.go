package cnpgprocessor_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-avro-go/v2/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"

	"github.com/djosh34/cnpg-to-kafka/internal/event"
	"github.com/djosh34/cnpg-to-kafka/internal/fixture"
	"github.com/djosh34/cnpg-to-kafka/processor/avroprocessor"
	"github.com/djosh34/cnpg-to-kafka/processor/cnpgprocessor"
)

// config is the cnpg part of config.yaml. The source host is an address, so
// that the lookup needs no DNS.
func config() *cnpgprocessor.Config {
	return &cnpgprocessor.Config{
		SourceHostname:     "127.0.0.1",
		AdditionalFields:   cnpgprocessor.AdditionalFields{ApplicationName: "payments"},
		HighPrivilegeRoles: []string{"postgres", "app_admin"},
		TrustedConnections: []event.TrustedConnection{
			{Role: "postgres", Method: "peer", Identity: "postgres"},
			{Role: "streaming_replica", Method: "cert", Identity: "CN=streaming_replica"},
		},
	}
}

// at returns a time on 2026-10-01 after 22:00 UTC in unix milliseconds.
func at(minute, second, milli int) int64 {
	return time.Date(2026, 10, 1, 22, minute, second, milli*int(time.Millisecond), time.UTC).UnixMilli()
}

// included returns an event of the role included from the recorded client.
func included(eventType event.Type, timestamp int64, cn, method *string) event.Event {
	return event.Event{
		Timestamp:       timestamp,
		EventType:       eventType,
		AccountType:     event.AccountNPA,
		ApplicationName: "payments",
		HostData:        event.HostData{SourceHostname: "127.0.0.1", SourceIP: "127.0.0.1"},
		ConnectionData: event.ConnectionData{
			Role: "included", Database: "app", CN: cn, AuthMethod: method, ClientAddress: "10.42.0.6",
		},
	}
}

var (
	login   = included(event.Login, at(28, 29, 982), new("included"), new("scram-sha-256"))
	logout  = included(event.Logout, at(28, 29, 983), nil, nil)
	failed  = included(event.LoginFailed, at(28, 30, 18), nil, new("scram-sha-256"))
	noLogin = included(event.LoginFailed, at(29, 1, 0), new("included"), new("scram-sha-256"))
	wrongCN = event.Event{
		Timestamp:       at(29, 0, 11),
		EventType:       event.LoginFailed,
		AccountType:     event.AccountNPA,
		ApplicationName: "payments",
		HostData:        event.HostData{SourceHostname: "127.0.0.1", SourceIP: "127.0.0.1"},
		ConnectionData: event.ConnectionData{
			Role: "streaming_replica", Database: "app", CN: new("CN=mallory"), AuthMethod: new("cert"), ClientAddress: "10.42.0.6",
		},
	}
)

// start creates and starts a processor that sends to next.
func start(t *testing.T, factory processor.Factory, cfg component.Config, next consumer.Logs) processor.Logs {
	t.Helper()
	settings := processor.Settings{ID: component.NewID(factory.Type()), TelemetrySettings: componenttest.NewNopTelemetrySettings()}
	p, err := factory.CreateLogs(t.Context(), settings, cfg, next)
	require.NoError(t, err)
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	t.Cleanup(func() { assert.NoError(t, p.Shutdown(context.Background())) })
	return p
}

// startAvro starts an avro processor with a test registry.
func startAvro(t *testing.T, next consumer.Logs) (processor.Logs, registry.SchemaInfo) {
	t.Helper()
	url, schema := fixture.Registry(t)
	factory := avroprocessor.NewFactory()
	cfg := factory.CreateDefaultConfig().(*avroprocessor.Config)
	cfg.Registry.URLs, cfg.Registry.Subject = []string{url}, "cnpg-connections-value"
	return start(t, factory, cfg, next), schema
}

// sink keeps every record that reaches it. It fails the first calls, as many
// as failures says.
type sink struct {
	failures int
	records  []plog.LogRecord
}

func (s *sink) consumer(t *testing.T) consumer.Logs {
	t.Helper()
	next, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		if s.failures > 0 {
			s.failures--
			return errors.New("export failed")
		}
		for _, resource := range logs.ResourceLogs().All() {
			for _, scope := range resource.ScopeLogs().All() {
				for _, record := range scope.LogRecords().All() {
					s.records = append(s.records, record)
				}
			}
		}
		return nil
	})
	require.NoError(t, err)
	return next
}

// batch joins the resources of several batches into one.
func batch(parts ...plog.Logs) plog.Logs {
	logs := plog.NewLogs()
	for _, part := range parts {
		part.ResourceLogs().MoveAndAppendTo(logs.ResourceLogs())
	}
	return logs
}

func TestProcessor(t *testing.T) {
	cnpg1 := func(lines ...string) plog.Logs { return fixture.Logs("capture", "cnpg-1", lines...) }
	cases := []struct {
		name    string
		batches []plog.Logs
		want    []event.Event
	}{
		{
			name:    "recorded login and logout",
			batches: []plog.Logs{cnpg1(fixture.IncludedLogin...)},
			want:    []event.Event{login, logout},
		},
		{
			name: "trusted connections",
			batches: []plog.Logs{
				cnpg1(slices.Concat(fixture.PeerLogin, fixture.ReplicaLogin, fixture.ReplicaLogout)...),
			},
		},
		{
			name: "failed logins",
			batches: []plog.Logs{
				cnpg1(slices.Concat(fixture.WrongPassword, fixture.WrongCN, fixture.NoLogin, fixture.TooManyConnections)...),
			},
			want: []event.Event{failed, wrongCN, noLogin},
		},
		{
			name:    "other lines",
			batches: []plog.Logs{cnpg1(fixture.NoEvent...)},
		},
		{
			name:    "session over two batches",
			batches: []plog.Logs{cnpg1(fixture.IncludedLogin[:2]...), cnpg1(fixture.IncludedLogin[2:]...)},
			want:    []event.Event{login, logout},
		},
		{
			// The ready line comes from another pod, so it has no join data.
			name: "same session on two pods",
			batches: []plog.Logs{batch(
				cnpg1(fixture.IncludedLogin[:3]...),
				fixture.Logs("capture", "cnpg-2", fixture.IncludedLogin[3]),
				fixture.Logs("other", "cnpg-1", fixture.IncludedLogin[3]),
			)},
			want: []event.Event{
				included(event.Login, at(28, 29, 982), nil, nil),
				included(event.Login, at(28, 29, 982), nil, nil),
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out sink
			p := start(t, cnpgprocessor.NewFactory(), config(), out.consumer(t))
			for _, logs := range c.batches {
				require.NoError(t, p.ConsumeLogs(t.Context(), logs))
			}
			var want, got []map[string]any
			for _, e := range c.want {
				attributes := pcommon.NewMap()
				e.SetAttributes(attributes)
				want = append(want, attributes.AsRaw())
			}
			for _, record := range out.records {
				got = append(got, record.Attributes().AsRaw())
			}
			assert.Equal(t, want, got)
		})
	}
}

func TestPipeline(t *testing.T) {
	cases := []struct {
		name  string
		lines []string
		want  []event.Event
	}{
		{"recorded login and logout", fixture.IncludedLogin, []event.Event{login, logout}},
		{"failed logins", slices.Concat(fixture.WrongPassword, fixture.WrongCN, fixture.NoLogin), []event.Event{failed, wrongCN, noLogin}},
		{"no events", slices.Concat(fixture.PeerLogin, fixture.TooManyConnections, fixture.NoEvent), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var out sink
			avroProcessor, schema := startAvro(t, out.consumer(t))
			p := start(t, cnpgprocessor.NewFactory(), config(), avroProcessor)
			require.NoError(t, p.ConsumeLogs(t.Context(), fixture.Logs("capture", "cnpg-1", c.lines...)))
			var got []event.Event
			for _, record := range out.records {
				frame := record.Body().Bytes().AsRaw()
				require.Greater(t, len(frame), 5)
				assert.Equal(t, []byte{0, 0, 0, 0, fixture.SchemaID}, frame[:5])
				var e event.Event
				require.NoError(t, avro.Unmarshal(schema.Schema, frame[5:], &e))
				got = append(got, e)
			}
			assert.Equal(t, c.want, got)
		})
	}
}

// TestRetry sends a batch again after the export failed, as the file log
// receiver does. The processors have already changed that batch.
func TestRetry(t *testing.T) {
	cases := []struct {
		name  string
		lines []string
		want  []event.Event
	}{
		{"login and logout", fixture.IncludedLogin, []event.Event{login, logout}},
		{"failed logins", slices.Concat(fixture.WrongCN, fixture.NoLogin), []event.Event{wrongCN, noLogin}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out := sink{failures: 1}
			avroProcessor, schema := startAvro(t, out.consumer(t))
			p := start(t, cnpgprocessor.NewFactory(), config(), avroProcessor)

			logs := fixture.Logs("capture", "cnpg-1", c.lines...)
			require.Error(t, p.ConsumeLogs(t.Context(), logs))
			require.NoError(t, p.ConsumeLogs(t.Context(), logs))

			require.Len(t, out.records, len(c.want))
			for i, want := range c.want {
				got, err := event.FromAttributes(out.records[i].Attributes())
				require.NoError(t, err)
				assert.Equal(t, want, got)
				frame, err := avroprocessor.Encode(schema, want)
				require.NoError(t, err)
				assert.Equal(t, frame, out.records[i].Body().Bytes().AsRaw())
			}
		})
	}
}

func TestStart(t *testing.T) {
	cases := []struct {
		name     string
		hostname string
		ok       bool
	}{
		{"address", "127.0.0.1", true},
		{"name that does not resolve", "db.invalid", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := config()
			cfg.SourceHostname = c.hostname
			factory := cnpgprocessor.NewFactory()
			settings := processor.Settings{ID: component.NewID(factory.Type()), TelemetrySettings: componenttest.NewNopTelemetrySettings()}
			var out sink
			p, err := factory.CreateLogs(t.Context(), settings, cfg, out.consumer(t))
			require.NoError(t, err)
			err = p.Start(t.Context(), componenttest.NewNopHost())
			if c.ok {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, `look up source_hostname "db.invalid"`)
			}
			require.NoError(t, p.Shutdown(context.Background()))
		})
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name   string
		change func(*cnpgprocessor.Config)
		want   string
	}{
		{"example", func(*cnpgprocessor.Config) {}, ""},
		{"no trusted connections", func(c *cnpgprocessor.Config) { c.TrustedConnections = nil }, ""},
		{"no source hostname", func(c *cnpgprocessor.Config) { c.SourceHostname = "" }, "source_hostname is required"},
		{"trusted connection without identity", func(c *cnpgprocessor.Config) {
			c.TrustedConnections[1].Identity = ""
		}, "trusted_connections[1] requires role, method and identity"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := config()
			c.change(cfg)
			if c.want == "" {
				require.NoError(t, cfg.Validate())
			} else {
				require.EqualError(t, cfg.Validate(), c.want)
			}
		})
	}
}
