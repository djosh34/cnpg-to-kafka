package event

import (
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// The log record attributes that carry an Event from the cnpg processor to the
// avro processor. CN and auth method are left out when they are nil.
const (
	keyTimestamp       = "cnpg.timestamp"
	keyEventType       = "cnpg.eventtype"
	keyAccountType     = "cnpg.account_type"
	keyApplicationName = "cnpg.application_name"
	keySourceHostname  = "cnpg.hostdata.source_hostname"
	keySourceIP        = "cnpg.hostdata.source_ip"
	keyRole            = "cnpg.connectiondata.role"
	keyDatabase        = "cnpg.connectiondata.database"
	keyCN              = "cnpg.connectiondata.cn"
	keyAuthMethod      = "cnpg.connectiondata.auth_method"
	keyClientAddress   = "cnpg.connectiondata.client_address"
)

// SetAttributes writes the event into a log record's attributes.
func (e Event) SetAttributes(attributes pcommon.Map) {
	attributes.PutInt(keyTimestamp, e.Timestamp)
	attributes.PutStr(keyEventType, string(e.EventType))
	attributes.PutStr(keyAccountType, e.AccountType)
	attributes.PutStr(keyApplicationName, e.ApplicationName)
	attributes.PutStr(keySourceHostname, e.HostData.SourceHostname)
	attributes.PutStr(keySourceIP, e.HostData.SourceIP)
	attributes.PutStr(keyRole, e.ConnectionData.Role)
	attributes.PutStr(keyDatabase, e.ConnectionData.Database)
	if e.ConnectionData.CN != nil {
		attributes.PutStr(keyCN, *e.ConnectionData.CN)
	}
	if e.ConnectionData.AuthMethod != nil {
		attributes.PutStr(keyAuthMethod, *e.ConnectionData.AuthMethod)
	}
	attributes.PutStr(keyClientAddress, e.ConnectionData.ClientAddress)
}

// HasAttributes reports whether a log record's attributes carry an event.
func HasAttributes(attributes pcommon.Map) bool {
	_, ok := attributes.Get(keyEventType)
	return ok
}

// FromAttributes reads the event that SetAttributes wrote. It returns an error
// when an attribute is missing or has the wrong type.
func FromAttributes(attributes pcommon.Map) (Event, error) {
	var errs []error
	// get returns false when the attribute is missing or has the wrong type.
	// Only a wrong type, or a missing attribute that is required, is an error.
	get := func(key string, valueType pcommon.ValueType, required bool) (pcommon.Value, bool) {
		value, ok := attributes.Get(key)
		switch {
		case !ok && required:
			errs = append(errs, fmt.Errorf("attribute %s is missing", key))
		case ok && value.Type() != valueType:
			errs = append(errs, fmt.Errorf("attribute %s is %s, not %s", key, value.Type(), valueType))
			ok = false
		}
		return value, ok
	}
	str := func(key string) string {
		if value, ok := get(key, pcommon.ValueTypeStr, true); ok {
			return value.Str()
		}
		return ""
	}
	optional := func(key string) *string {
		value, ok := get(key, pcommon.ValueTypeStr, false)
		if !ok {
			return nil
		}
		s := value.Str()
		return &s
	}

	var timestamp int64
	if value, ok := get(keyTimestamp, pcommon.ValueTypeInt, true); ok {
		timestamp = value.Int()
	}
	event := Event{
		Timestamp:       timestamp,
		EventType:       Type(str(keyEventType)),
		AccountType:     str(keyAccountType),
		ApplicationName: str(keyApplicationName),
		HostData: HostData{
			SourceHostname: str(keySourceHostname),
			SourceIP:       str(keySourceIP),
		},
		ConnectionData: ConnectionData{
			Role:          str(keyRole),
			Database:      str(keyDatabase),
			CN:            optional(keyCN),
			AuthMethod:    optional(keyAuthMethod),
			ClientAddress: str(keyClientAddress),
		},
	}
	return event, errors.Join(errs...)
}
