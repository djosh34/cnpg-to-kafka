package event

import (
	"encoding/hex"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/djosh34/cnpg-to-kafka/internal/cnpg"
)

// Rules are the settings that turn records into events.
type Rules struct {
	ApplicationName    string
	Host               HostData
	HighPrivilegeRoles []string
	TrustedConnections []TrustedConnection
}

// TrustedConnection is a login that is not published. A cert entry has a
// CommonName and no Identity, any other entry an Identity and no CommonName.
// All fields must match exactly.
type TrustedConnection struct {
	Role       string `mapstructure:"role"`
	Method     string `mapstructure:"method"`
	Identity   string `mapstructure:"identity"`
	CommonName string `mapstructure:"common_name"`
}

// Make applies one PostgreSQL record of a pod to the session table and returns
// the event it makes. It reports false when the record makes no event, and
// returns an error when it makes one but its log_time does not parse.
func (r Rules) Make(sessions *Sessions, pod string, record cnpg.Fields) (Event, bool, error) {
	eventType := Classify(record)
	sessions.CountDown(pod)
	var auth Auth
	joined := false
	if eventType == Login || record.ErrorSeverity == "FATAL" {
		auth, joined = sessions.Take(pod, record.SessionID)
	}
	if a, ok := Authenticated(record.Message); ok {
		sessions.Add(pod, record.SessionID, a)
		return Event{}, false, nil
	}
	switch {
	case eventType == "",
		eventType == Login && joined && r.Trusted(record.UserName, auth),
		eventType == Logout && r.TrustedRole(record.UserName):
		return Event{}, false, nil
	}

	timestamp, err := Timestamp(record.LogTime)
	if err != nil {
		return Event{}, false, err
	}
	event := Event{
		Timestamp:       timestamp,
		EventType:       eventType,
		AccountType:     r.AccountType(record.UserName),
		ApplicationName: r.ApplicationName,
		HostData:        r.Host,
		ConnectionData: ConnectionData{
			Role:          record.UserName,
			Database:      record.DatabaseName,
			ClientAddress: ClientAddress(record.ConnectionFrom),
		},
	}
	switch {
	case joined:
		event.ConnectionData.CN, event.ConnectionData.AuthMethod = &auth.Identity, &auth.Method
	case eventType == LoginFailed:
		event.ConnectionData.AuthMethod = HBAMethod(record.Detail)
	}
	return event, true, nil
}

// Classify returns the event type of a record, or "" when it is no event.
func Classify(record cnpg.Fields) Type {
	switch {
	case strings.HasPrefix(record.Message, "connection ready:"):
		return Login
	case record.ErrorSeverity == "FATAL" &&
		(record.CommandTag == "authentication" || record.CommandTag == "startup") &&
		record.SQLStateCode != "53300": // too many connections
		return LoginFailed
	case strings.HasPrefix(record.Message, "disconnection:"):
		return Logout
	}
	return ""
}

// Authenticated reads the identity and method of a "connection authenticated"
// message. For trust, PostgreSQL logs user= in place of identity=.
func Authenticated(message string) (Auth, bool) {
	rest, ok := strings.CutPrefix(message, "connection authenticated: ")
	if !ok {
		return Auth{}, false
	}
	rest, ok = strings.CutPrefix(rest, `identity="`)
	if !ok {
		if rest, ok = strings.CutPrefix(rest, `user="`); !ok {
			return Auth{}, false
		}
	}
	// PostgreSQL does not escape the identity, so the last separator is the one.
	i := strings.LastIndex(rest, `" method=`)
	if i < 0 {
		return Auth{}, false
	}
	method, _, _ := strings.Cut(rest[i+len(`" method=`):], " ")
	return Auth{Identity: rest[:i], Method: method}, true
}

// hbaMatch finds the pg_hba rule that PostgreSQL quotes in the detail of a
// failed login.
var hbaMatch = regexp.MustCompile(`(?m)^Connection matched file ".*" line [0-9]+: "(.*)"$`)

// methods are the authentication methods of PostgreSQL 18.
var methods = []string{
	"trust", "reject", "scram-sha-256", "md5", "password", "gss", "sspi", "ident",
	"peer", "ldap", "radius", "cert", "pam", "bsd", "oauth",
}

var hostTypes = []string{"host", "hostssl", "hostnossl", "hostgssenc", "hostnogssenc"}

// HBAMethod returns the method of the pg_hba rule quoted in a detail, or nil
// when there is no such rule or its method field is not a method name.
func HBAMethod(detail string) *string {
	match := hbaMatch.FindStringSubmatch(detail)
	if match == nil {
		return nil
	}
	fields := hbaFields(match[1])
	if len(fields) == 0 {
		return nil
	}
	// local DATABASE USER METHOD, or host* DATABASE USER ADDRESS METHOD. An IP
	// address without a /prefix is followed by a separate netmask.
	i := 3
	switch {
	case fields[0] == "local":
	case slices.Contains(hostTypes, fields[0]) && len(fields) > 3:
		i = 4
		if _, err := netip.ParseAddr(fields[3]); err == nil {
			i = 5
		}
	default:
		return nil
	}
	if i >= len(fields) {
		return nil
	}
	method := strings.TrimSuffix(strings.TrimPrefix(fields[i], `"`), `"`)
	if !slices.Contains(methods, method) {
		return nil
	}
	return &method
}

// hbaFields splits a pg_hba rule at white space outside double quotes. As in
// PostgreSQL, a list such as "app, reader" stays one field when white space
// follows a comma.
func hbaFields(rule string) []string {
	var fields []string
	start, quoted := -1, false
	for i, r := range rule {
		if r == '"' {
			quoted = !quoted
		}
		separator := !quoted && unicode.IsSpace(r)
		switch {
		case separator && start >= 0 && !strings.HasSuffix(strings.TrimRightFunc(rule[start:i], unicode.IsSpace), ","):
			fields = append(fields, rule[start:i])
			start = -1
		case !separator && start < 0:
			start = i
		}
	}
	if start >= 0 {
		fields = append(fields, rule[start:])
	}
	return fields
}

// ClientAddress returns connection_from without the port and without the
// brackets of an IPv6 address. A value without a port, such as [local], stays
// as it is.
func ClientAddress(connectionFrom string) string {
	host, port, ok := strings.CutLast(connectionFrom, ":")
	if !ok {
		return connectionFrom
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return connectionFrom
	}
	return strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
}

// Timestamp returns a log_time such as "2026-10-01 22:28:26.382 UTC" in unix
// milliseconds. Only UTC is accepted, because Go cannot tell the offset of
// other zone abbreviations.
func Timestamp(logTime string) (int64, error) {
	t, err := time.Parse("2006-01-02 15:04:05.000 UTC", logTime)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

// AccountType returns AccountHA for a role in the high-privilege list, exact
// match, and AccountNPA otherwise.
func (r Rules) AccountType(role string) string {
	if slices.Contains(r.HighPrivilegeRoles, role) {
		return AccountHA
	}
	return AccountNPA
}

// Trusted reports whether a login matches a trusted connection on role, method
// and identity. For cert, the common name of the identity is matched in place
// of the identity.
func (r Rules) Trusted(role string, auth Auth) bool {
	if auth.Method == "cert" {
		name, ok := CommonName(auth.Identity)
		return ok && slices.Contains(r.TrustedConnections, TrustedConnection{Role: role, Method: auth.Method, CommonName: name})
	}
	return slices.Contains(r.TrustedConnections, TrustedConnection{Role: role, Method: auth.Method, Identity: auth.Identity})
}

// TrustedRole reports whether a role is in any trusted connection. A logout
// has no method and identity, so only its role is matched.
func (r Rules) TrustedRole(role string) bool {
	return slices.ContainsFunc(r.TrustedConnections, func(c TrustedConnection) bool { return c.Role == role })
}

// CommonName returns the common name of a certificate subject as PostgreSQL
// logs it, in RFC 2253 form such as "CN=app,OU=Databases,O=Example Corp". Its
// escapes are undone, so it is the name as typed when the certificate was
// made. It reports false when the subject has no CN, more than one CN, or a
// broken escape.
func CommonName(subject string) (string, bool) {
	// Split at unescaped , between parts and + inside a multi-valued part, and
	// undo the escapes. A backslash and two hex digits are one byte of UTF-8,
	// a backslash and any other character is that character.
	var attributes []string
	var attribute []byte
	for i := 0; i < len(subject); i++ {
		switch c := subject[i]; c {
		case ',', '+':
			attributes = append(attributes, string(attribute))
			attribute = nil
		case '\\':
			if i+1 == len(subject) {
				return "", false
			}
			if b, err := hex.DecodeString(subject[i+1 : min(i+3, len(subject))]); err == nil {
				attribute = append(attribute, b[0])
				i += 2
			} else {
				attribute = append(attribute, subject[i+1])
				i++
			}
		default:
			attribute = append(attribute, c)
		}
	}
	attributes = append(attributes, string(attribute))

	var names []string
	for _, a := range attributes {
		if name, ok := strings.CutPrefix(a, "CN="); ok {
			names = append(names, name)
		}
	}
	if len(names) != 1 {
		return "", false
	}
	return names[0], true
}
