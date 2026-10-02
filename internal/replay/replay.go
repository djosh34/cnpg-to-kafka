// Package replay applies recorded pod-log operations to a real filesystem.
// It is test tooling, not a Collector receiver or runtime file watcher.
package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"time"
)

// Operation records paths relative to /var/log/pods and unmodified file bytes.
// JSON encodes Data as standard base64. AtMS is elapsed capture time.
type Operation struct {
	AtMS int64  `json:"at_ms"`
	Op   string `json:"op"`
	Path string `json:"path"`
	To   string `json:"to,omitempty"`
	Data []byte `json:"data,omitempty"`
}

// Run applies operations in recorded order beneath an existing root directory.
// A speed of 10 replays at ten times capture speed; zero skips all waits.
// Start the actual Collector before Run so filelog observes ordinary disk changes.
func Run(ctx context.Context, rootPath string, recording io.Reader, speed float64) error {
	if speed < 0 || math.IsNaN(speed) || math.IsInf(speed, 0) {
		return errors.New("replay speed must be finite and nonnegative")
	}
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
			return fmt.Errorf("operation %d: decode: %w", n, err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if op.AtMS < 0 {
			return fmt.Errorf("operation %d: negative capture time", n)
		}
		if speed > 0 {
			delay := time.Duration(float64(op.AtMS)*float64(time.Millisecond)/speed) - time.Since(start)
			if delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
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
		return fmt.Errorf("unsupported recorded operation %q", op.Op)
	}
}
