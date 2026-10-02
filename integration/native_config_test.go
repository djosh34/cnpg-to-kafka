package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/storage/filestorage"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/filelogreceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
)

// These are explicitly SYNTHETIC supplemental fault/selection cases, not the
// canonical real CNPG recording. The actual root YAML operators process CRI
// files through the upstream file receiver, not a copied classifier/parser.
func TestNativeConfigSelection(t *testing.T) {
	root, state := t.TempDir(), t.TempDir()
	input := func(record map[string]any) string {
		b, err := json.Marshal(map[string]any{"logger": "postgres", "msg": "record", "record": record, "extra": true})
		mustNative(t, err)
		return string(b)
	}
	record := func(message, role, host string) map[string]any {
		return map[string]any{"message": message, "user_name": role, "connection_from": host, "database_name": "app", "error_severity": "LOG", "sql_state_code": "00000", "extra": "ignored"}
	}
	failed := func(role, host, code string) map[string]any {
		r := record("password authentication failed for user", role, host)
		r["error_severity"], r["sql_state_code"] = "FATAL", code
		return r
	}
	lines := []string{
		input(record("connection authorized: user=alice database=app", "alice", "10.1.2.3:54321")),
		input(record("disconnection: session time: 0:00:01", "alice", "[2001:db8::7]:54321")),
		input(record("replication connection authorized: user=replicator", "replicator", "10.1.2.6:54321")),
		input(record("replication connection authorized: user=streaming_replica", "streaming_replica", "10.1.2.6:54321")),
		input(map[string]any{"message": `permission denied for database "app"`, "error_severity": "FATAL", "sql_state_code": "42501", "user_name": "postgres"}),
		input(map[string]any{"message": `permission denied for table things`, "error_severity": "FATAL", "sql_state_code": "42501"}),
		input(record("connection authorized: user=Postgres", "Postgres", "client.example:54321")),
		input(failed("postgres", "10.1.2.4:54321", "28P01")), // excluded failures pass
		input(failed("streaming_replica", "::1:54321", "28000")),
		input(map[string]any{"error_severity": "FATAL", "sql_state_code": "28000"}), // recognizable, no identity/message
		input(record("connection authorized: user=local", "local", "[local]")),
		input(record("disconnection: session time: 0:00:01", "bare", "client.example")),
		input(map[string]any{"message": "connection authorized:", "connection_from": "10.1.2.8:42"}), // mapper handles missing role
		input(record("connection authorized:", "postgres", "10.1.2.3:42")),
		input(record("disconnection:", "streaming_replica", "10.1.2.3:42")),
		input(record("connection received: host=10.1.2.3", "alice", "10.1.2.3:42")),
		input(record("connection authenticated: identity=alice method=scram-sha-256", "alice", "10.1.2.3:42")),
		input(failed("alice", "10.1.2.3:42", "XX000")), // arbitrary FATAL is not a failure
		input(record("ordinary server output", "alice", "10.1.2.3:42")),
		`{"logger":"other","record":{"message":"connection authorized:"}}`,
		`{"logger":"postgres"}`,
		`{"logger":"postgres","record":{"message":42}}`,
		`{"logger":"postgres","record":42}`,
		`{"logger":"postgres","record":`,
		`ordinary non-JSON output`,
	}
	path := writeNativeCRI(t, root, "database", "cnpg-1", "0.log", lines)
	writeNativeCRI(t, root, "other", "cnpg-1", "0.log", []string{input(record("connection authorized:", "wrong-namespace", "10.1.2.3:42"))})
	writeNativeCRI(t, root, "database", "unrelated", "0.log", []string{input(record("connection authorized:", "other-pod", "10.1.2.3:42"))})
	// Native container supports kubelet's timestamped rotated path as well.
	writeNativeCRI(t, root, "database", "cnpg-2", "0.log.20261001-120000", []string{input(record("connection authorized:", "replica", "10.1.2.9:42"))})
	got := make(chan map[string]string, 64)
	stop := startNativeReceiver(t, root, state, `^cnpg-[123]$`, got)
	want := []map[string]string{
		nativeAttrs("LOGIN", "alice", "10.1.2.3", "app"),
		nativeAttrs("LOGOUT", "alice", "2001:db8::7", "app"),
		nativeAttrs("LOGIN", "replicator", "10.1.2.6", "app"),
		nativeAttrs("LOGIN_FAILED", "postgres", "", ""),
		nativeAttrs("LOGIN", "Postgres", "client.example", "app"),
		nativeAttrs("LOGIN_FAILED", "postgres", "10.1.2.4", "app"),
		nativeAttrs("LOGIN_FAILED", "streaming_replica", "::1", "app"),
		nativeAttrs("LOGIN_FAILED", "", "", ""),
		nativeAttrs("LOGIN", "local", "[local]", "app"),
		nativeAttrs("LOGOUT", "bare", "client.example", "app"),
		nativeAttrs("LOGIN", "", "10.1.2.8", ""),
		nativeAttrs("LOGIN", "replica", "10.1.2.9", "app"),
	}
	assertNativeEvents(t, got, want)
	stop()
	// Persisted source checkpoint: append while stopped, then restart using
	// the same real storage. The old backlog must not be emitted again.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	mustNative(t, err)
	_, err = fmt.Fprintln(f, "2026-10-01T12:00:00.000000000Z stdout F "+input(record("disconnection:", "after-restart", "10.1.2.3:42")))
	mustNative(t, err)
	mustNative(t, f.Close())
	stop = startNativeReceiver(t, root, state, `^cnpg-[123]$`, got)
	defer stop()
	assertNativeEvents(t, got, []map[string]string{nativeAttrs("LOGOUT", "after-restart", "10.1.2.3", "app")})
}

func TestNativeConfigOptionalPodRegex(t *testing.T) {
	root := t.TempDir()
	writeNativeCRI(t, root, "database", "any-pod", "0.log", []string{`{"logger":"postgres","record":{"message":"connection authorized:","user_name":"alice","connection_from":"10.2.3.4:42"}}`})
	got := make(chan map[string]string, 8)
	stop := startNativeReceiver(t, root, t.TempDir(), ".*", got)
	defer stop()
	assertNativeEvents(t, got, []map[string]string{nativeAttrs("LOGIN", "alice", "10.2.3.4", "")})
}

// Exercise real CRI partial writes and a filesystem rename/new-file rotation.
// This remains supplemental synthetic input, not fabricated capture evidence.
func TestNativeConfigCRIFragmentsAndRotation(t *testing.T) {
	root := t.TempDir()
	path := writeNativeCRI(t, root, "database", "cnpg-1", "0.log", []string{`{"logger":"postgres","record":{"message":"connection authorized:","user_name":"before","connection_from":"10.2.3.4:42"}}`})
	got := make(chan map[string]string, 8)
	stop := startNativeReceiver(t, root, t.TempDir(), ".*", got)
	defer stop()
	assertNativeEvents(t, got, []map[string]string{nativeAttrs("LOGIN", "before", "10.2.3.4", "")})
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	mustNative(t, err)
	_, err = fmt.Fprintln(f, `2026-10-01T12:00:00.000000000Z stdout P {"logger":"postgres","record":{"message":"disconnection:",`)
	mustNative(t, err)
	mustNative(t, f.Close())
	// An incomplete CRI record must not produce an event.
	select {
	case event := <-got:
		t.Fatalf("incomplete fragment emitted: %#v", event)
	case <-time.After(100 * time.Millisecond):
	}
	f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	mustNative(t, err)
	_, err = fmt.Fprintln(f, `2026-10-01T12:00:00.000000000Z stdout F "user_name":"fragment","connection_from":"[2001:db8::1]:42"}}`)
	mustNative(t, err)
	mustNative(t, f.Close())
	assertNativeEvents(t, got, []map[string]string{nativeAttrs("LOGOUT", "fragment", "2001:db8::1", "")})
	mustNative(t, os.Rename(path, path+".20261001-120000"))
	writeNativeCRI(t, root, "database", "cnpg-1", "0.log", []string{`{"logger":"postgres","record":{"message":"connection authorized:","user_name":"after-rotation","connection_from":"10.2.3.5:42"}}`})
	assertNativeEvents(t, got, []map[string]string{nativeAttrs("LOGIN", "after-rotation", "10.2.3.5", "")})
}

func nativeAttrs(event, role, host, database string) map[string]string {
	return map[string]string{"eventtype": event, "role": role, "hostname": host, "database": database}
}

func writeNativeCRI(t *testing.T, root, ns, pod, name string, payloads []string) string {
	t.Helper()
	path := filepath.Join(root, ns+"_"+pod+"_00000000-0000-0000-0000-000000000001", "postgres", name)
	mustNative(t, os.MkdirAll(filepath.Dir(path), 0700))
	var text strings.Builder
	for _, payload := range payloads {
		fmt.Fprintln(&text, "2026-10-01T12:00:00.000000000Z stdout F "+payload)
	}
	mustNative(t, os.WriteFile(path, []byte(text.String()), 0600))
	return path
}

type nativeStorageHost struct {
	extensions map[component.ID]component.Component
}

func (h nativeStorageHost) GetExtensions() map[component.ID]component.Component { return h.extensions }

func startNativeReceiver(t *testing.T, root, state, podRegex string, got chan map[string]string) func() {
	t.Helper()
	p := fileprovider.NewFactory().Create(confmap.ProviderSettings{})
	retrieved, err := p.Retrieve(t.Context(), "file:../config.yaml", nil)
	mustNative(t, err)
	cm, err := retrieved.AsConf()
	mustNative(t, err)
	storageConf, err := cm.Sub("extensions::file_storage")
	mustNative(t, err)
	mustNative(t, storageConf.Merge(confmap.NewFromStringMap(map[string]any{"directory": state})))
	storageFactory := filestorage.NewFactory()
	storageCfg := storageFactory.CreateDefaultConfig()
	mustNative(t, storageConf.Unmarshal(storageCfg))
	storageID := component.MustNewID("file_storage")
	telemetry := componenttest.NewNopTelemetrySettings()
	ext, err := storageFactory.Create(t.Context(), extension.Settings{ID: storageID, TelemetrySettings: telemetry}, storageCfg)
	mustNative(t, err)
	host := nativeStorageHost{map[component.ID]component.Component{storageID: ext}}
	mustNative(t, ext.Start(t.Context(), host))
	receiverConf, err := cm.Sub("receivers::filelog/cnpg")
	mustNative(t, err)
	m := receiverConf.ToStringMap()
	m["include"] = []any{filepath.Join(root, "database_*", "*", "*.log*")}
	m["poll_interval"] = "20ms"
	for _, op := range m["operators"].([]any) {
		op := op.(map[string]any)
		if op["id"] == "select-pods" {
			op["expr"] = fmt.Sprintf(`not (resource["k8s.pod.name"] matches %q)`, podRegex)
		}
	}
	factory := filelogreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig()
	mustNative(t, confmap.NewFromStringMap(m).Unmarshal(cfg))
	sink, err := consumer.NewLogs(func(_ context.Context, logs plog.Logs) error {
		for _, rl := range logs.ResourceLogs().All() {
			for _, sl := range rl.ScopeLogs().All() {
				for _, log := range sl.LogRecords().All() {
					attrs := map[string]string{}
					for _, key := range []string{"eventtype", "role", "hostname", "database"} {
						v, _ := log.Attributes().Get(key)
						attrs[key] = v.AsString()
					}
					got <- attrs
				}
			}
		}
		return nil
	})
	mustNative(t, err)
	rcvr, err := factory.CreateLogs(t.Context(), receiver.Settings{ID: component.MustNewIDWithName(factory.Type().String(), "cnpg"), TelemetrySettings: telemetry}, cfg, sink)
	mustNative(t, err)
	mustNative(t, rcvr.Start(t.Context(), host))
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		mustNative(t, rcvr.Shutdown(ctx))
		mustNative(t, ext.Shutdown(ctx))
	}
	t.Cleanup(stop)
	return stop
}

func assertNativeEvents(t *testing.T, got <-chan map[string]string, want []map[string]string) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for len(want) > 0 {
		select {
		case event := <-got:
			found := -1
			for i, expected := range want {
				if reflect.DeepEqual(event, expected) {
					found = i
					break
				}
			}
			if found < 0 {
				t.Fatalf("unexpected native event: %#v; remaining %#v", event, want)
			}
			want = append(want[:found], want[found+1:]...)
		case <-deadline.C:
			t.Fatalf("timed out waiting for native events: %#v", want)
		}
	}
	select {
	case event := <-got:
		t.Fatalf("unexpected extra event: %#v", event)
	case <-time.After(300 * time.Millisecond):
	}
}

func mustNative(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
