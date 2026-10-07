// Package event turns PostgreSQL log records into connection events.
package event

// Type is the event type. Its values are the symbols of the schema's enum.
type Type string

// Event types.
const (
	Login       Type = "LOGIN"
	Logout      Type = "LOGOUT"
	LoginFailed Type = "LOGIN_FAILED"
)

// Account types. Their values are the symbols of the schema's enum.
const (
	AccountHA  = "ha"
	AccountNPA = "npa"
)

// Event is the Go form of schema/connection-event.avsc. Change both together.
type Event struct {
	// Timestamp is PostgreSQL's log_time in unix milliseconds.
	Timestamp       int64          `avro:"timestamp" json:"timestamp"`
	EventType       Type           `avro:"eventtype" json:"eventtype"`
	AccountType     string         `avro:"account_type" json:"account_type"`
	ApplicationName string         `avro:"application_name" json:"application_name"`
	HostData        HostData       `avro:"hostdata" json:"hostdata"`
	ConnectionData  ConnectionData `avro:"connectiondata" json:"connectiondata"`
}

// HostData describes the database host.
type HostData struct {
	SourceHostname string `avro:"source_hostname" json:"source_hostname"`
	SourceIP       string `avro:"source_ip" json:"source_ip"`
}

// ConnectionData describes the client connection. CN and AuthMethod are nil
// when they are unknown.
type ConnectionData struct {
	Role          string  `avro:"role" json:"role"`
	Database      string  `avro:"database" json:"database"`
	CN            *string `avro:"cn" json:"cn"`
	AuthMethod    *string `avro:"auth_method" json:"auth_method"`
	ClientAddress string  `avro:"client_address" json:"client_address"`
}
