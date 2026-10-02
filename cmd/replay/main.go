// Command replay writes a recording of pod-log file operations to a directory,
// so that a Collector that watches the directory sees the logs appear and rotate.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/djosh34/cnpg-to-kafka/internal/replay"
)

func main() {
	root := flag.String("root", "", "existing empty directory to write the pod logs to")
	recording := flag.String("recording", "testdata/capture/operations.jsonl", "recording to replay")
	speed := flag.Float64("speed", 10, "how many times faster than recorded; 0 does not wait at all")
	flag.Parse()
	if *root == "" {
		flag.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	file, err := os.Open(*recording)
	if err == nil {
		defer file.Close()
		err = replay.Run(ctx, *root, file, *speed)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
