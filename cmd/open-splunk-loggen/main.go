// Command open-splunk-loggen generates repeatable log fixtures and load for
// correctness and performance testing.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/loggen"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("open-splunk-loggen: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	defaults := loggen.DefaultConfig()
	flags := flag.NewFlagSet("open-splunk-loggen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	count := flags.Uint64("count", 1_000, "number of events; 0 runs until interrupted")
	format := flags.String("format", string(defaults.Format), "zap-json, nested-json, raw, cardinality-json, or mixed")
	seed := flags.Int64("seed", defaults.Seed, "deterministic fixture seed")
	start := flags.String("start", defaults.Start.Format(time.RFC3339Nano), "first event timestamp in RFC3339 format")
	interval := flags.Duration("interval", defaults.Interval, "timestamp interval between events")
	rate := flags.Float64("rate", 0, "maximum events per second; 0 emits as fast as possible")
	output := flags.String("output", "-", "output path, or - for stdout")
	service := flags.String("service", defaults.Service, "service field")
	environment := flags.String("environment", defaults.Environment, "environment field")
	host := flags.String("host", defaults.Host, "host field")
	cardinality := flags.Uint64("cardinality", defaults.Cardinality, "number of distinct bounded user IDs")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if ctx == nil {
		return errors.New("context is required")
	}
	if stdout == nil || stderr == nil {
		return errors.New("stdout and stderr writers are required")
	}

	startTime, err := time.Parse(time.RFC3339Nano, *start)
	if err != nil {
		return fmt.Errorf("parse -start: %w", err)
	}
	cfg := loggen.Config{
		Format:      loggen.Format(*format),
		Seed:        *seed,
		Start:       startTime,
		Interval:    *interval,
		Service:     *service,
		Environment: *environment,
		Host:        *host,
		Cardinality: *cardinality,
	}
	generator, err := loggen.New(cfg)
	if err != nil {
		return err
	}

	delay, err := rateDelay(*rate)
	if err != nil {
		return err
	}

	writer := stdout
	var outputFile *os.File
	if *output != "-" {
		outputFile, err = os.OpenFile(*output, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return fmt.Errorf("open output %q: %w", *output, err)
		}
		defer outputFile.Close()
		writer = outputFile
	}
	buffered := bufio.NewWriterSize(writer, 256*1024)

	var emitted uint64
	for *count == 0 || emitted < *count {
		if emitted > 0 && delay > 0 {
			if err := wait(ctx, delay); err != nil {
				_ = buffered.Flush()
				return err
			}
		} else if err := ctx.Err(); err != nil {
			return err
		}
		line, err := generator.Next()
		if err != nil {
			return err
		}
		if _, err := buffered.Write(line); err != nil {
			return fmt.Errorf("write event %d: %w", emitted, err)
		}
		if err := buffered.WriteByte('\n'); err != nil {
			return fmt.Errorf("write event %d delimiter: %w", emitted, err)
		}
		emitted++
	}
	if err := buffered.Flush(); err != nil {
		return fmt.Errorf("flush output: %w", err)
	}
	if outputFile != nil {
		if err := outputFile.Sync(); err != nil {
			return fmt.Errorf("sync output %q: %w", *output, err)
		}
	}
	return nil
}

func rateDelay(rate float64) (time.Duration, error) {
	if rate < 0 {
		return 0, errors.New("rate cannot be negative")
	}
	if rate == 0 {
		return 0, nil
	}
	delay := time.Duration(float64(time.Second) / rate)
	if delay <= 0 {
		return 0, errors.New("rate is too large")
	}
	return delay, nil
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
