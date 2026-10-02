// replay is deliberately separate test tooling; it never runs in the Collector.
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
	root := flag.String("root", "", "existing empty pod-log root to populate")
	recording := flag.String("recording", "testdata/capture/operations.jsonl", "captured JSONL operations")
	speed := flag.Float64("speed", 10, "capture-time acceleration; 0 disables waits")
	flag.Parse()
	if *root == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	file, err := os.Open(*recording)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = replay.Run(ctx, *root, file, *speed)
	stop()
	_ = file.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
