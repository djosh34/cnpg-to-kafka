package avroprocessor

// Event is the Go form of schema/connection-event.avsc. To publish a different
// schema, change this struct, the attribute mapping in factory.go and the schema
// together.
type Event struct {
	Role      string       `avro:"role" json:"role"`
	Hostname  string       `avro:"hostname" json:"hostname"`
	EventType string       `avro:"eventtype" json:"eventtype"`
	Context   EventContext `avro:"context" json:"context"`
}

type EventContext struct {
	Database string `avro:"database" json:"database"`
}
