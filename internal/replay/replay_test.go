package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These are synthetic producer unit tests, not the canonical real capture.
func recording(t *testing.T, ops ...Operation) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	for _, op := range ops {
		if err := json.NewEncoder(&buf).Encode(op); err != nil {
			t.Fatal(err)
		}
	}
	return &buf
}

func TestFilesystemRotationAndRepeatability(t *testing.T) {
	// Include partial writes and non-UTF8 bytes to check byte-for-byte replay.
	first := []byte("2026-01-01T00:00:00Z stdout P {\"message\":")
	last := append([]byte("\"login\"}\n"), 0xff, 0x00)
	ops := []Operation{
		{Op: "mkdir", Path: "selected_db-1_uid/postgres"},
		{Op: "create", Path: "selected_db-1_uid/postgres/0.log", Data: first},
		{Op: "append", Path: "selected_db-1_uid/postgres/0.log", Data: last},
		{Op: "rename", Path: "selected_db-1_uid/postgres/0.log", To: "selected_db-1_uid/postgres/0.log.rotated"},
		{Op: "create", Path: "selected_db-1_uid/postgres/0.log", Data: []byte("new file\n")},
		{Op: "append", Path: "selected_db-1_uid/postgres/0.log", Data: []byte("after rotation\n")},
		{Op: "mkdir", Path: "other_noise_uid/container"},
		{Op: "create", Path: "other_noise_uid/container/0.log", Data: []byte("noise\n")},
		{Op: "remove", Path: "other_noise_uid/container/0.log"},
		{Op: "remove", Path: "other_noise_uid/container"},
	}
	for i := 0; i < 2; i++ {
		root := t.TempDir()
		if err := Run(context.Background(), root, recording(t, ops...), 0); err != nil {
			t.Fatal(err)
		}
		for path, want := range map[string][]byte{
			"selected_db-1_uid/postgres/0.log.rotated": append(append([]byte{}, first...), last...),
			"selected_db-1_uid/postgres/0.log":         []byte("new file\nafter rotation\n"),
		} {
			got, err := os.ReadFile(filepath.Join(root, path))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s: got %q, want %q, error %v", path, got, want, err)
			}
		}
		if _, err := os.Stat(filepath.Join(root, "other_noise_uid/container")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("removed directory still exists: %v", err)
		}
	}
}

func TestMalformedRecordingAndOperations(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		want  string
	}{
		{"json", "{", "operation 1: decode"},
		{"unknown", `{"op":"truncate","path":"0.log"}`, "unsupported recorded operation"},
		{"negative time", `{"op":"mkdir","path":"pod","at_ms":-1}`, "negative capture time"},
		{"missing append target", `{"op":"append","path":"0.log","data":"YQ=="}`, "operation 1 (append"},
		{"duplicate create", "{\"op\":\"create\",\"path\":\"0.log\"}\n{\"op\":\"create\",\"path\":\"0.log\"}\n", "operation 2 (create"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := Run(context.Background(), t.TempDir(), strings.NewReader(tc.input), 0)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestRootConfinement(t *testing.T) {
	outside := t.TempDir()
	for _, tc := range []struct {
		name string
		op   Operation
	}{
		{"parent", Operation{Op: "create", Path: "../outside.log"}},
		{"absolute", Operation{Op: "create", Path: filepath.Join(outside, "outside.log")}},
		{"symlink", Operation{Op: "create", Path: "escape/outside.log"}},
		{"rename", Operation{Op: "rename", Path: "0.log", To: "escape/outside.log"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "0.log"), []byte("untouched"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := Run(context.Background(), root, recording(t, tc.op), 0); err == nil {
				t.Fatal("operation escaped replay root")
			}
			if _, err := os.Stat(filepath.Join(outside, "outside.log")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("outside file created: %v", err)
			}
		})
	}
}

func TestSpeed(t *testing.T) {
	for _, speed := range []float64{-1, math.NaN(), math.Inf(1)} {
		if err := Run(context.Background(), t.TempDir(), strings.NewReader(""), speed); err == nil {
			t.Fatalf("accepted speed %v", speed)
		}
	}
	// A minute of capture time is just a few milliseconds at this speed.
	start := time.Now()
	err := Run(context.Background(), t.TempDir(), recording(t, Operation{AtMS: 60_000, Op: "create", Path: "0.log"}), 10_000)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 6*time.Millisecond {
		t.Fatal("replay did not honor accelerated pacing")
	}
}

func TestCancellation(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := Run(ctx, root, recording(t, Operation{AtMS: 60_000, Op: "create", Path: "0.log"}), 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}
	if _, err := os.Stat(filepath.Join(root, "0.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled operation was applied: %v", err)
	}
	ctx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if err := Run(ctx, root, recording(t, Operation{Op: "mkdir", Path: "pod"}), 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want canceled", err)
	}
}
