// Command inspect prints actual Kafka-consumed, Avro-decoded events as JSONL.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/djosh34/cnpg-to-kafka/internal/inspect"
)

func main() {
	config := flag.String("config", "config.yaml", "native Collector configuration file")
	limit := flag.Int("limit", 0, "stop after this many decoded events (0 follows)")
	timeout := flag.Duration("timeout", 30*time.Second, "maximum inspection duration")
	flag.Parse()
	if *timeout <= 0 || *limit < 0 || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "timeout must be positive; limit must be nonnegative; no positional arguments")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	if err := inspect.Run(ctx, *config, os.Stdout, *limit); err != nil {
		// Following is deliberately bounded; failing to reach an explicit count
		// before the deadline is an error, useful to local scripts and CI.
		if *limit == 0 && ctx.Err() != nil {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
