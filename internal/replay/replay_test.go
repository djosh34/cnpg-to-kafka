package replay

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRotation(t *testing.T) {
	// The kubelet rotates a log by renaming it and creating a new file.
	recording := `
{"at_ms":0,"op":"mkdir","path":"ns_pod_uid/postgres"}
{"at_ms":1,"op":"create","path":"ns_pod_uid/postgres/0.log","data":"b25lCg=="}
{"at_ms":2,"op":"append","path":"ns_pod_uid/postgres/0.log","data":"dHdvCg=="}
{"at_ms":3,"op":"rename","path":"ns_pod_uid/postgres/0.log","to":"ns_pod_uid/postgres/0.log.20261001-120000"}
{"at_ms":4,"op":"create","path":"ns_pod_uid/postgres/0.log","data":"dGhyZWUK"}
{"at_ms":5,"op":"create","path":"ns_pod_uid/postgres/old.log.gz"}
{"at_ms":6,"op":"remove","path":"ns_pod_uid/postgres/old.log.gz"}
`
	root := t.TempDir()
	if err := Run(t.Context(), root, strings.NewReader(recording), 1000); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"0.log.20261001-120000": "one\ntwo\n", "0.log": "three\n"}
	entries, err := os.ReadDir(filepath.Join(root, "ns_pod_uid/postgres"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(want) {
		t.Errorf("%d files, want %d", len(entries), len(want))
	}
	for name, content := range want {
		got, err := os.ReadFile(filepath.Join(root, "ns_pod_uid/postgres", name))
		if err != nil || string(got) != content {
			t.Errorf("%s: got %q, error %v, want %q", name, got, err, content)
		}
	}
}
