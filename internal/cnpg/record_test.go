package cnpg_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/djosh34/cnpg-to-kafka/internal/cnpg"
	"github.com/djosh34/cnpg-to-kafka/internal/fixture"
)

func TestParse(t *testing.T) {
	cases := []struct {
		name string
		line string
		want cnpg.Record
		ok   bool
	}{
		{
			name: "recorded login",
			line: fixture.IncludedLogin[3],
			want: cnpg.Record{Logger: "postgres", Msg: "record", Record: cnpg.Fields{
				LogTime:         "2026-10-01 22:28:29.982 UTC",
				UserName:        "included",
				DatabaseName:    "app",
				ConnectionFrom:  "10.42.0.6:34964",
				SessionID:       "6abede8d.486",
				CommandTag:      "idle",
				ErrorSeverity:   "LOG",
				SQLStateCode:    "00000",
				Message:         "connection ready: setup total=9.668 ms, fork=0.475 ms, authentication=3.408 ms",
				ApplicationName: "psql",
			}},
			ok: true,
		},
		{
			name: "recorded failed login",
			line: fixture.UnknownRole[0],
			want: cnpg.Record{Logger: "postgres", Msg: "record", Record: cnpg.Fields{
				LogTime:        "2026-10-01 22:28:30.128 UTC",
				UserName:       "unknown_role",
				DatabaseName:   "app",
				ConnectionFrom: "10.42.0.6:35018",
				SessionID:      "6abede8e.48c",
				CommandTag:     "authentication",
				ErrorSeverity:  "FATAL",
				SQLStateCode:   "28P01",
				Message:        `password authentication failed for user "unknown_role"`,
				Detail:         "Role \"unknown_role\" does not exist.\nConnection matched file \"/var/lib/postgresql/data/pgdata/pg_hba.conf\" line 26: \"host all all all scram-sha-256\"",
			}},
			ok: true,
		},
		{
			name: "empty record",
			line: `{"logger":"postgres","record":{}}`,
			want: cnpg.Record{Logger: "postgres"},
			ok:   true,
		},
		{name: "postgres line without a record", line: fixture.NoEvent[1]},
		{name: "instance manager line", line: fixture.NoEvent[2]},
		{name: "plain text", line: fixture.NoEvent[3]},
		{name: "other logger", line: `{"logger":"other","record":{"message":"connection ready: x"}}`},
		{name: "record is null", line: `{"logger":"postgres","record":null}`},
		{name: "record is not an object", line: `{"logger":"postgres","record":42}`},
		{name: "cut off", line: `{"logger":"postgres","record":`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := cnpg.Parse([]byte(c.line))
			assert.Equal(t, c.ok, ok)
			assert.Equal(t, c.want, got)
		})
	}
}
