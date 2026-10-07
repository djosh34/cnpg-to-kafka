// Command inspect prints the connection events in the Kafka topic as JSON lines.
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
	config := flag.String("config", "config.yaml", "Collector config file")
	limit := flag.Int("limit", 0, "stop after this many events; 0 reads until the timeout")
	timeout := flag.Duration("timeout", 30*time.Second, "stop after this long")
	flag.Parse()
	if err := run(*config, *limit, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(config string, limit int, timeout time.Duration) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := inspect.Run(ctx, config, os.Stdout, limit)
	// Without a limit, the timeout or an interrupt is the normal way to stop.
	if limit == 0 && ctx.Err() != nil {
		return nil
	}
	return err
}
