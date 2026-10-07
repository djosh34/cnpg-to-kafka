package event

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/djosh34/cnpg-to-kafka/internal/cnpg"
	"github.com/djosh34/cnpg-to-kafka/internal/fixture"
)

var testRules = Rules{
	ApplicationName:    "payments",
	Host:               HostData{SourceHostname: "db1.example.com", SourceIP: "192.0.2.1"},
	HighPrivilegeRoles: []string{"postgres", "app_admin"},
	TrustedConnections: []TrustedConnection{
		{Role: "postgres", Method: "peer", Identity: "postgres"},
		{Role: "streaming_replica", Method: "cert", CommonName: "streaming_replica"},
	},
}

// at returns 2026-10-01 at the given time of day in unix milliseconds.
func at(hour, minute, second, milli int) int64 {
	return time.Date(2026, 10, 1, hour, minute, second, milli*int(time.Millisecond), time.UTC).UnixMilli()
}

// testEvent returns an event of testRules for the role included from the
// recorded client.
func testEvent(eventType Type, timestamp int64, cn, method *string) Event {
	return Event{
		Timestamp:       timestamp,
		EventType:       eventType,
		AccountType:     AccountNPA,
		ApplicationName: "payments",
		HostData:        testRules.Host,
		ConnectionData: ConnectionData{
			Role: "included", Database: "app", CN: cn, AuthMethod: method, ClientAddress: "10.42.0.6",
		},
	}
}

func TestMake(t *testing.T) {
	scram := new("scram-sha-256")
	cases := []struct {
		name  string
		lines []string
		want  []Event
		// errors is the number of records that make an event whose log_time
		// does not parse.
		errors int
	}{
		{
			name:  "password login and logout",
			lines: fixture.IncludedLogin,
			want: []Event{
				testEvent(Login, at(22, 28, 29, 982), new("included"), scram),
				testEvent(Logout, at(22, 28, 29, 983), nil, nil),
			},
		},
		{name: "trusted peer login and logout", lines: fixture.PeerLogin},
		{name: "trusted replica login", lines: fixture.ReplicaLogin},
		{name: "replica logout with a walsender tag", lines: fixture.ReplicaLogout},
		{
			// The method does not match the trusted entry, but the role does.
			name:  "trust login is published, its logout is not",
			lines: fixture.TrustLogin,
			want: []Event{{
				Timestamp:       at(22, 27, 30, 702),
				EventType:       Login,
				AccountType:     AccountHA,
				ApplicationName: "payments",
				HostData:        testRules.Host,
				ConnectionData: ConnectionData{
					Role: "postgres", Database: "postgres", CN: new("postgres"), AuthMethod: new("trust"), ClientAddress: "[local]",
				},
			}},
		},
		{
			name:  "trusted role without join data",
			lines: fixture.PeerLogin[3:4],
			want: []Event{{
				Timestamp:       at(22, 28, 26, 455),
				EventType:       Login,
				AccountType:     AccountHA,
				ApplicationName: "payments",
				HostData:        testRules.Host,
				ConnectionData:  ConnectionData{Role: "postgres", Database: "postgres", ClientAddress: "[local]"},
			}},
		},
		{
			name:  "wrong password",
			lines: fixture.WrongPassword,
			want:  []Event{testEvent(LoginFailed, at(22, 28, 30, 18), nil, scram)},
		},
		{
			name:  "role does not exist",
			lines: fixture.UnknownRole,
			want: []Event{{
				Timestamp:       at(22, 28, 30, 128),
				EventType:       LoginFailed,
				AccountType:     AccountNPA,
				ApplicationName: "payments",
				HostData:        testRules.Host,
				ConnectionData:  ConnectionData{Role: "unknown_role", Database: "app", AuthMethod: scram, ClientAddress: "10.42.0.6"},
			}},
		},
		{
			name:  "certificate with a wrong CN",
			lines: fixture.WrongCN,
			want: []Event{{
				Timestamp:       at(22, 29, 0, 11),
				EventType:       LoginFailed,
				AccountType:     AccountNPA,
				ApplicationName: "payments",
				HostData:        testRules.Host,
				ConnectionData: ConnectionData{
					Role: "streaming_replica", Database: "app", CN: new("CN=mallory"), AuthMethod: new("cert"), ClientAddress: "10.42.0.6",
				},
			}},
		},
		{
			name:  "database does not exist",
			lines: fixture.NoDatabase,
			want:  []Event{testEvent(LoginFailed, at(22, 29, 1, 0), new("included"), scram)},
		},
		{
			name:  "CONNECT denied",
			lines: fixture.ConnectDenied,
			want:  []Event{testEvent(LoginFailed, at(22, 29, 1, 0), new("included"), scram)},
		},
		{
			name:  "role is NOLOGIN",
			lines: fixture.NoLogin,
			want:  []Event{testEvent(LoginFailed, at(22, 29, 1, 0), new("included"), scram)},
		},
		{name: "too many connections", lines: fixture.TooManyConnections},
		{
			// The ready line is the 999th later record of the pod, so it still
			// finds the entry.
			name:  "ready line 999 records after authentication",
			lines: slices.Concat(fixture.PeerLogin[1:2], slices.Repeat(fixture.NoEvent[:1], countdown-2), fixture.PeerLogin[3:4]),
		},
		{
			// The ready line is the 1000th later record and counts the entry
			// down to zero, so the trusted login has no join data and is
			// published.
			name:  "ready line 1000 records after authentication",
			lines: slices.Concat(fixture.PeerLogin[1:2], slices.Repeat(fixture.NoEvent[:1], countdown-1), fixture.PeerLogin[3:4]),
			want: []Event{{
				Timestamp:       at(22, 28, 26, 455),
				EventType:       Login,
				AccountType:     AccountHA,
				ApplicationName: "payments",
				HostData:        testRules.Host,
				ConnectionData:  ConnectionData{Role: "postgres", Database: "postgres", ClientAddress: "[local]"},
			}},
		},
		{name: "other records", lines: fixture.NoEvent},
		{
			name:   "log_time that does not parse",
			lines:  []string{strings.Replace(fixture.IncludedLogin[3], " UTC", " CEST", 1)},
			errors: 1,
		},
		{
			// A trusted login is no event, so its log_time does not matter.
			name:  "trusted login with a log_time that does not parse",
			lines: []string{fixture.PeerLogin[1], strings.Replace(fixture.PeerLogin[3], " UTC", " CEST", 1)},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sessions := NewSessions()
			var got []Event
			errors := 0
			for _, line := range c.lines {
				record, ok := cnpg.Parse([]byte(line))
				if !ok {
					continue
				}
				e, ok, err := testRules.Make(sessions, "capture/cnpg-1", record.Record)
				if err != nil {
					errors++
				}
				if ok {
					got = append(got, e)
				}
			}
			assert.Equal(t, c.want, got)
			assert.Equal(t, c.errors, errors)
			// Every case ends its sessions, also the ones it does not publish.
			assert.Empty(t, sessions.pods["capture/cnpg-1"])
		})
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name   string
		record cnpg.Fields
		want   Type
	}{
		{"connection ready", cnpg.Fields{CommandTag: "idle", Message: "connection ready: setup total=9.668 ms"}, Login},
		{"connection ready of a walsender", cnpg.Fields{CommandTag: "idle", Message: "connection ready: setup total=5.773 ms"}, Login},
		{"disconnection", cnpg.Fields{CommandTag: "idle", Message: "disconnection: session time: 0:00:00.010"}, Logout},
		{"disconnection of a walsender", cnpg.Fields{CommandTag: "streaming 0/605D368", Message: "disconnection: session time: 0:02:56.303"}, Logout},
		{"FATAL during authentication", cnpg.Fields{CommandTag: "authentication", ErrorSeverity: "FATAL", SQLStateCode: "28P01"}, LoginFailed},
		{"FATAL during startup", cnpg.Fields{CommandTag: "startup", ErrorSeverity: "FATAL", SQLStateCode: "3D000"}, LoginFailed},
		{"too many connections", cnpg.Fields{CommandTag: "startup", ErrorSeverity: "FATAL", SQLStateCode: "53300"}, ""},
		{"FATAL of an open session", cnpg.Fields{CommandTag: "idle", ErrorSeverity: "FATAL", SQLStateCode: "57P01"}, ""},
		{"ERROR during authentication", cnpg.Fields{CommandTag: "authentication", ErrorSeverity: "ERROR", SQLStateCode: "28P01"}, ""},
		{"connection authenticated", cnpg.Fields{CommandTag: "authentication", Message: `connection authenticated: identity="included" method=scram-sha-256`}, ""},
		{"connection authorized", cnpg.Fields{CommandTag: "authentication", Message: "connection authorized: user=included database=app"}, ""},
		{"replication connection authorized", cnpg.Fields{CommandTag: "authentication", Message: "replication connection authorized: user=streaming_replica"}, ""},
		{"connection received", cnpg.Fields{Message: "connection received: host=10.42.0.6 port=34964"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, Classify(c.record))
		})
	}
}

func TestAuthenticated(t *testing.T) {
	cases := []struct {
		message string
		want    Auth
		ok      bool
	}{
		{`connection authenticated: identity="included" method=scram-sha-256 (/var/lib/postgresql/data/pgdata/pg_hba.conf:26)`, Auth{"included", "scram-sha-256"}, true},
		{`connection authenticated: identity="CN=streaming_replica" method=cert (/var/lib/postgresql/data/pgdata/pg_hba.conf:12)`, Auth{"CN=streaming_replica", "cert"}, true},
		{`connection authenticated: identity="CN=a,OU=db,O=Example" method=cert`, Auth{"CN=a,OU=db,O=Example", "cert"}, true},
		{`connection authenticated: user="postgres" method=trust (/var/lib/postgresql/data/pgdata/pg_hba.conf:117)`, Auth{"postgres", "trust"}, true},
		{`connection authenticated: identity="odd" method=x" method=peer (pg_hba.conf:8)`, Auth{`odd" method=x`, "peer"}, true},
		{`connection authorized: user=included database=app`, Auth{}, false},
		{`connection authenticated: something else`, Auth{}, false},
		{`connection authenticated: identity="included"`, Auth{}, false},
	}
	for _, c := range cases {
		t.Run(c.message, func(t *testing.T) {
			got, ok := Authenticated(c.message)
			assert.Equal(t, c.ok, ok)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestHBAMethod(t *testing.T) {
	matched := func(rule string) string {
		return `Connection matched file "/var/lib/postgresql/data/pgdata/pg_hba.conf" line 26: "` + rule + `"`
	}
	cases := []struct {
		name   string
		detail string
		want   *string
	}{
		{"local rule", matched("local all all peer"), new("peer")},
		{"local rule with options", matched("local all all peer map=local"), new("peer")},
		{"host rule", matched("host all all all scram-sha-256"), new("scram-sha-256")},
		{"host rule with a prefix", matched("hostssl all all 10.0.0.0/8 cert clientcert=verify-full"), new("cert")},
		{"host rule with a separate netmask", matched("host all all 10.0.0.0 255.0.0.0 md5"), new("md5")},
		{"IPv6 rule with a separate netmask", matched("hostnossl all all ::1 ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff trust"), new("trust")},
		{"host name", matched("hostgssenc all all .example.com gss"), new("gss")},
		{"role named like a method", matched("host all cert all scram-sha-256"), new("scram-sha-256")},
		{"database named like a method", matched("local trust peer reject"), new("reject")},
		{"quoted name with a space", matched(`host "my db" all all md5`), new("md5")},
		{"list with spaces", matched("host all app, reader all scram-sha-256"), new("scram-sha-256")},
		{"list with two spaces", matched("host all app,  reader all scram-sha-256"), new("scram-sha-256")},
		{"list with a tab and a role named like a method", matched("local all app,\t peer trust"), new("trust")},
		{"list with spaces and a role named like a method", matched("host all app, reader, cert all scram-sha-256"), new("scram-sha-256")},
		{"quoted method", matched(`host all all all "scram-sha-256"`), new("scram-sha-256")},
		{"trailing comment", matched("local all all peer # instance manager"), new("peer")},
		{"role does not exist", "Role \"ghost\" does not exist.\n" + matched("host all all all scram-sha-256"), new("scram-sha-256")},
		{"method field is not a method", matched("host all all all password1"), nil},
		{"method field is missing", matched("host all all all"), nil},
		{"not a connection rule", matched("include_dir conf.d"), nil},
		{"no rule", "", nil},
		{"other detail", `Role "ghost" does not exist.`, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, HBAMethod(c.detail))
		})
	}
}

func TestClientAddress(t *testing.T) {
	cases := []struct{ from, want string }{
		{"10.42.0.6:34964", "10.42.0.6"},
		{"[2001:db8::7]:54321", "2001:db8::7"},
		{"2001:db8::7:54321", "2001:db8::7"},
		{"client.example:5432", "client.example"},
		{"[local]", "[local]"},
		{"", ""},
	}
	for _, c := range cases {
		t.Run(c.from, func(t *testing.T) {
			assert.Equal(t, c.want, ClientAddress(c.from))
		})
	}
}

func TestTimestamp(t *testing.T) {
	cases := []struct {
		logTime string
		want    int64
		ok      bool
	}{
		{"2026-10-01 22:28:26.382 UTC", at(22, 28, 26, 382), true},
		{"2026-10-01 00:00:00.000 UTC", at(0, 0, 0, 0), true},
		{"2026-10-01 22:28:26.382 CEST", 0, false},
		{"2026-10-01 22:28:26 UTC", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		t.Run(c.logTime, func(t *testing.T) {
			got, err := Timestamp(c.logTime)
			if c.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			assert.Equal(t, c.want, got)
		})
	}
}

func TestAccountType(t *testing.T) {
	cases := []struct{ role, want string }{
		{"postgres", AccountHA},
		{"app_admin", AccountHA},
		{"Postgres", AccountNPA},
		{"included", AccountNPA},
		{"", AccountNPA},
	}
	for _, c := range cases {
		t.Run(c.role, func(t *testing.T) {
			assert.Equal(t, c.want, testRules.AccountType(c.role))
		})
	}
}

func TestTrusted(t *testing.T) {
	cases := []struct {
		name        string
		role        string
		auth        Auth
		trusted     bool
		trustedRole bool
	}{
		{"instance manager", "postgres", Auth{"postgres", "peer"}, true, true},
		{"instance manager with another identity", "postgres", Auth{"app", "peer"}, false, true},
		{"instance manager with a subject", "postgres", Auth{"CN=postgres", "peer"}, false, true},
		{"replica", "streaming_replica", Auth{"CN=streaming_replica", "cert"}, true, true},
		{"replica with OU and O", "streaming_replica", Auth{"CN=streaming_replica,OU=Databases,O=Example Corp", "cert"}, true, true},
		{"replica in a multi-valued part", "streaming_replica", Auth{"CN=streaming_replica+OU=Databases", "cert"}, true, true},
		{"replica with another CN", "streaming_replica", Auth{"CN=mallory,OU=Databases", "cert"}, false, true},
		{"replica with the CN in another field", "streaming_replica", Auth{"CN=mallory,OU=streaming_replica", "cert"}, false, true},
		{"replica with two CNs", "streaming_replica", Auth{"CN=streaming_replica,CN=streaming_replica", "cert"}, false, true},
		{"replica without a CN", "streaming_replica", Auth{"OU=streaming_replica", "cert"}, false, true},
		{"replica with a CN of another case", "streaming_replica", Auth{"CN=Streaming_replica", "cert"}, false, true},
		{"replica with the identity of a peer entry", "streaming_replica", Auth{"streaming_replica", "peer"}, false, true},
		{"postgres with a password", "postgres", Auth{"postgres", "scram-sha-256"}, false, true},
		{"postgres with a certificate", "postgres", Auth{"CN=postgres", "cert"}, false, true},
		{"identity of another entry", "postgres", Auth{"CN=streaming_replica", "cert"}, false, true},
		{"other role", "included", Auth{"included", "scram-sha-256"}, false, false},
		{"case differs", "Postgres", Auth{"postgres", "peer"}, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.trusted, testRules.Trusted(c.role, c.auth))
			assert.Equal(t, c.trustedRole, testRules.TrustedRole(c.role))
		})
	}
}

func TestCommonName(t *testing.T) {
	cases := []struct {
		subject string
		want    string
		ok      bool
	}{
		// The identities PostgreSQL 18 logged for test certificates.
		{`CN=streaming_replica`, "streaming_replica", true},
		{`CN=streaming_replica,OU=Databases,O=Example Corp`, "streaming_replica", true},
		{`CN=Smith\, John`, "Smith, John", true},
		{`CN=a\+b=c\;d\<e\>f\"g\\h`, `a+b=c;d<e>f"g\h`, true},
		{`CN=\ lead and trail\ `, " lead and trail ", true},
		{`CN=\#hash`, "#hash", true},
		{`CN=J\C3\B6hn \C3\9Cn\C3\AFcode \C3\A9`, "Jöhn Ünïcode é", true},
		{`CN=J\C3\B6hn\, Smith\+Co,OU=Databases,O=Example Corp`, "Jöhn, Smith+Co", true},
		{`CN=J\C3\83\C2\B6hn \C3\83\C2\A9`, "JÃ¶hn Ã©", true},

		// A multi-valued part, an empty CN, and subjects that report false.
		{`CN=multi+OU=val`, "multi", true},
		{`OU=val+CN=multi,O=Example Corp`, "multi", true},
		{`CN=`, "", true},
		{`CN=second,CN=first`, "", false},
		{`CN=one+CN=two`, "", false},
		{`OU=Databases,O=Example Corp`, "", false},
		{`O=a\,CN=b`, "", false},
		{``, "", false},
		{`CN=broken\`, "", false},
	}
	for _, c := range cases {
		t.Run(c.subject, func(t *testing.T) {
			got, ok := CommonName(c.subject)
			assert.Equal(t, c.ok, ok)
			assert.Equal(t, c.want, got)
		})
	}
}
