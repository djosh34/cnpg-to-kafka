package replay

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	cases := []struct {
		name      string
		recording string
		// want is the content of each file in ns_pod_uid/postgres afterwards.
		want  map[string]string
		error string
	}{
		{
			// The kubelet rotates a log by renaming it and creating a new file.
			name: "rotation",
			recording: `
{"at_ms":0,"op":"mkdir","path":"ns_pod_uid/postgres"}
{"at_ms":1,"op":"create","path":"ns_pod_uid/postgres/0.log","data":"b25lCg=="}
{"at_ms":2,"op":"append","path":"ns_pod_uid/postgres/0.log","data":"dHdvCg=="}
{"at_ms":3,"op":"rename","path":"ns_pod_uid/postgres/0.log","to":"ns_pod_uid/postgres/0.log.20261001-120000"}
{"at_ms":4,"op":"create","path":"ns_pod_uid/postgres/0.log","data":"dGhyZWUK"}
{"at_ms":5,"op":"create","path":"ns_pod_uid/postgres/old.log.gz"}
{"at_ms":6,"op":"remove","path":"ns_pod_uid/postgres/old.log.gz"}
`,
			want: map[string]string{"0.log.20261001-120000": "one\ntwo\n", "0.log": "three\n"},
		},
		{
			name: "path outside the directory",
			recording: `
{"at_ms":0,"op":"mkdir","path":"ns_pod_uid/postgres"}
{"at_ms":1,"op":"create","path":"../escape.log","data":"b25lCg=="}
`,
			want:  map[string]string{},
			error: `operation 2 (create "../escape.log")`,
		},
		{
			name: "unknown operation",
			recording: `
{"at_ms":0,"op":"mkdir","path":"ns_pod_uid/postgres"}
{"at_ms":1,"op":"truncate","path":"ns_pod_uid/postgres/0.log"}
`,
			want:  map[string]string{},
			error: `operation 2 (truncate "ns_pod_uid/postgres/0.log"): unknown operation`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			err := Run(t.Context(), root, strings.NewReader(c.recording), 1000)
			if c.error == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, c.error)
			}

			dir := filepath.Join(root, "ns_pod_uid/postgres")
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			got := map[string]string{}
			for _, entry := range entries {
				content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
				require.NoError(t, err)
				got[entry.Name()] = string(content)
			}
			assert.Equal(t, c.want, got)
		})
	}
}
