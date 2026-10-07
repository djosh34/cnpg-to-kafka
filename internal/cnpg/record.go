// Package cnpg reads the JSON lines that CloudNativePG writes to the postgres
// container log.
package cnpg

import "encoding/json"

// Record is a PostgreSQL log record as CloudNativePG writes it: a JSON line
// with logger "postgres" and the csvlog columns in "record".
type Record struct {
	Logger string `json:"logger"`
	Msg    string `json:"msg"`
	Record Fields `json:"record"`
}

// Fields are the csvlog columns of a record that the events use. A column that
// PostgreSQL leaves empty is missing from the JSON and stays "".
type Fields struct {
	LogTime         string `json:"log_time"`
	UserName        string `json:"user_name"`
	DatabaseName    string `json:"database_name"`
	ConnectionFrom  string `json:"connection_from"`
	SessionID       string `json:"session_id"`
	CommandTag      string `json:"command_tag"`
	ErrorSeverity   string `json:"error_severity"`
	SQLStateCode    string `json:"sql_state_code"`
	Message         string `json:"message"`
	Detail          string `json:"detail"`
	ApplicationName string `json:"application_name"`
}

// Parse reads one log line. It reports false for a line that is not JSON, not
// logger "postgres", or has no "record" object.
func Parse(line []byte) (Record, bool) {
	var parsed struct {
		Logger string  `json:"logger"`
		Msg    string  `json:"msg"`
		Record *Fields `json:"record"`
	}
	if err := json.Unmarshal(line, &parsed); err != nil || parsed.Logger != "postgres" || parsed.Record == nil {
		return Record{}, false
	}
	return Record{Logger: parsed.Logger, Msg: parsed.Msg, Record: *parsed.Record}, true
}
