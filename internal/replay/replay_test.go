package replay

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, Run(t.Context(), root, strings.NewReader(recording), 1000))

	dir := filepath.Join(root, "ns_pod_uid/postgres")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	got := map[string]string{}
	for _, entry := range entries {
		content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		got[entry.Name()] = string(content)
	}
	assert.Equal(t, map[string]string{"0.log.20261001-120000": "one\ntwo\n", "0.log": "three\n"}, got)
}
