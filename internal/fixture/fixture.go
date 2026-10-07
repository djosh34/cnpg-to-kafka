// Package fixture holds the log lines and helpers that the tests share.
package fixture

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/confluentinc/confluent-avro-go/v2"
	"github.com/confluentinc/confluent-avro-go/v2/registry"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

// Logs returns one batch with the lines of one pod, as the file log receiver
// with the container operator makes it.
func Logs(namespace, pod string, lines ...string) plog.Logs {
	logs := plog.NewLogs()
	resource := logs.ResourceLogs().AppendEmpty()
	resource.Resource().Attributes().PutStr("k8s.namespace.name", namespace)
	resource.Resource().Attributes().PutStr("k8s.pod.name", pod)
	resource.Resource().Attributes().PutStr("k8s.container.name", "postgres")
	records := resource.ScopeLogs().AppendEmpty().LogRecords()
	for _, line := range lines {
		records.AppendEmpty().Body().SetStr(line)
	}
	return logs
}

// SchemaID is the ID that Registry serves the schema with.
const SchemaID = 7

// Registry starts a Schema Registry that serves schema/connection-event.avsc
// for every request. It returns its URL and the schema.
func Registry(t *testing.T) (string, registry.SchemaInfo) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	text, err := os.ReadFile(filepath.Join(filepath.Dir(file), "../../schema/connection-event.avsc"))
	require.NoError(t, err)
	schema, err := avro.Parse(string(text))
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		err := json.NewEncoder(w).Encode(map[string]any{"id": SchemaID, "version": 1, "schema": string(text)})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, registry.SchemaInfo{ID: SchemaID, Schema: schema}
}
