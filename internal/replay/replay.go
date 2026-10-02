// Package replay writes a recording of pod-log file operations to a directory.
package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// Operation is one line of a recording. Path and To are relative to the pod-log
// directory, /var/log/pods on a node. AtMS is the time since the recording
// started. Data is base64 in the JSON.
type Operation struct {
	AtMS int64  `json:"at_ms"`
	Op   string `json:"op"`
	Path string `json:"path"`
	To   string `json:"to,omitempty"`
	Data []byte `json:"data,omitempty"`
}

// Run applies the recorded operations in order inside the existing directory
// rootPath. It waits between operations as the recording did, divided by speed.
// A speed of 0 does not wait.
func Run(ctx context.Context, rootPath string, recording io.Reader, speed float64) error {
	// os.Root refuses paths that leave the directory.
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return err
	}
	defer root.Close()
	decoder := json.NewDecoder(recording)
	start := time.Now()
	for n := 1; ; n++ {
		var op Operation
		if err := decoder.Decode(&op); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("operation %d: %w", n, err)
		}
		if speed > 0 {
			due := start.Add(time.Duration(float64(op.AtMS) * float64(time.Millisecond) / speed))
			select {
			case <-ctx.Done():
			case <-time.After(time.Until(due)):
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := apply(root, op); err != nil {
			return fmt.Errorf("operation %d (%s %q): %w", n, op.Op, op.Path, err)
		}
	}
}

func apply(root *os.Root, op Operation) error {
	switch op.Op {
	case "mkdir":
		return root.MkdirAll(op.Path, 0o755)
	case "create", "append":
		flags := os.O_WRONLY | os.O_APPEND
		if op.Op == "create" {
			flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL
		}
		file, err := root.OpenFile(op.Path, flags, 0o644)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(op.Data)
		return errors.Join(writeErr, file.Close())
	case "rename":
		return root.Rename(op.Path, op.To)
	case "remove":
		return root.Remove(op.Path)
	default:
		return errors.New("unknown operation")
	}
}
