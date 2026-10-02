package avroprocessor

// Event is the public example schema's handwritten mapping. Fork this and the
// example schema for a private schema; no private-schema compatibility is implied.
type Event struct {
	Role      string       `avro:"role" json:"role"`
	Hostname  string       `avro:"hostname" json:"hostname"`
	EventType string       `avro:"eventtype" json:"eventtype"`
	Context   EventContext `avro:"context" json:"context"`
}

// EventContext is the sample's single nested record.
type EventContext struct {
	Database string `avro:"database" json:"database"`
}
