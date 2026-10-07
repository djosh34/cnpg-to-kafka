package event_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/djosh34/cnpg-to-kafka/internal/event"
)

func TestAttributeRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		event event.Event
	}{
		{"every field set", event.Event{
			Timestamp:       1790893709982,
			EventType:       event.LoginFailed,
			AccountType:     event.AccountHA,
			ApplicationName: "payments",
			HostData:        event.HostData{SourceHostname: "db1.example.com", SourceIP: "192.0.2.1"},
			ConnectionData: event.ConnectionData{
				Role: "postgres", Database: "app", CN: new("CN=mallory"), AuthMethod: new("cert"), ClientAddress: "10.42.0.6",
			},
		}},
		{"CN and method null", event.Event{
			Timestamp:      -1,
			EventType:      event.Logout,
			AccountType:    event.AccountNPA,
			ConnectionData: event.ConnectionData{Role: "included", Database: "app", ClientAddress: "[local]"},
		}},
		{"CN and method empty", event.Event{
			EventType:      event.Login,
			ConnectionData: event.ConnectionData{CN: new(""), AuthMethod: new("")},
		}},
		{"every field empty", event.Event{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			attributes := pcommon.NewMap()
			assert.False(t, event.HasAttributes(attributes))
			c.event.SetAttributes(attributes)
			assert.True(t, event.HasAttributes(attributes))
			got, err := event.FromAttributes(attributes)
			require.NoError(t, err)
			assert.Equal(t, c.event, got)
		})
	}
}

func TestFromAttributesErrors(t *testing.T) {
	cases := []struct {
		name   string
		change func(pcommon.Map)
		want   string
	}{
		{"missing string", func(m pcommon.Map) { m.Remove("cnpg.connectiondata.role") }, "attribute cnpg.connectiondata.role is missing"},
		{"missing timestamp", func(m pcommon.Map) { m.Remove("cnpg.timestamp") }, "attribute cnpg.timestamp is missing"},
		{"string is an int", func(m pcommon.Map) { m.PutInt("cnpg.eventtype", 1) }, "attribute cnpg.eventtype is Int, not Str"},
		{"timestamp is a string", func(m pcommon.Map) { m.PutStr("cnpg.timestamp", "1") }, "attribute cnpg.timestamp is Str, not Int"},
		{"CN is an int", func(m pcommon.Map) { m.PutInt("cnpg.connectiondata.cn", 1) }, "attribute cnpg.connectiondata.cn is Int, not Str"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			attributes := pcommon.NewMap()
			event.Event{EventType: event.Login}.SetAttributes(attributes)
			c.change(attributes)
			_, err := event.FromAttributes(attributes)
			require.EqualError(t, err, c.want)
		})
	}
}
