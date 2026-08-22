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

	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	"github.com/Suhaibinator/open-splunk/internal/loggen"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if isOnlyCancellation(err) {
			return
		}
		log.Printf("open-splunk-loggen: %v", err)
		os.Exit(1)
	}
}

func isOnlyCancellation(err error) bool {
	if err == nil || !errors.Is(err, context.Canceled) {
		return false
	}

	type joinedError interface {
		error
		Unwrap() []error
	}
	if joined, ok := errors.AsType[joinedError](err); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !isOnlyCancellation(cause) {
				return false
			}
		}
		return true
	}

	type wrappedError interface {
		error
		Unwrap() error
	}
	if wrapped, ok := errors.AsType[wrappedError](err); ok {
		cause := wrapped.Unwrap()
		return cause != nil && isOnlyCancellation(cause)
	}
	return true
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) (returnedErr error) {
	defaults := loggen.DefaultConfig()
	flags := flag.NewFlagSet("open-splunk-loggen", flag.ContinueOnError)
	flags.SetOutput(stderr)
	count := flags.Uint64("count", 1_000, "number of events; 0 runs until interrupted")
	format := flags.String("format", string(defaults.Format), "zap-json, nested-json, raw, cardinality-json, or mixed")
	seed := flags.Int64("seed", defaults.Seed, "deterministic fixture seed")
	start := flags.String("start", defaults.Start.Format(time.RFC3339Nano), "first event timestamp in RFC3339 format")
	interval := flags.Duration("interval", defaults.Interval, "timestamp interval between events")
	rate := flags.Float64("rate", 0, "target events per second with bounded catch-up; 0 emits as fast as possible")
	output := flags.String("output", "-", "output path, or - for stdout")
	appendOutput := flags.Bool("append", false, "append to the output file instead of replacing it")
	flushEvents := flags.Uint64("flush-events", 0, "flush buffered output after this many events; 0 disables event-count flushing")
	service := flags.String("service", defaults.Service, "service field")
	environment := flags.String("environment", defaults.Environment, "environment field")
	host := flags.String("host", defaults.Host, "host field")
	cardinality := flags.Uint64("cardinality", defaults.Cardinality, "number of distinct bounded user IDs")
	version := flags.Bool("version", false, "print the compiled build identity")
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
	if *version {
		identity, err := buildinfo.Current()
		if err != nil {
			return err
		}
		return buildinfo.WriteIdentity(stdout, identity)
	}
	if *appendOutput && *output == "-" {
		return errors.New("-append requires a file output path")
	}
	if err := ctx.Err(); err != nil {
		return err
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

	pacer, err := loggen.NewPacer(*rate)
	if err != nil {
		return err
	}
	defer pacer.Close()
	if err := ctx.Err(); err != nil {
		return err
	}

	writer := stdout
	var outputFile *os.File
	if *output != "-" {
		outputFile, err = openOutputFile(*output, *appendOutput)
		if err != nil {
			return err
		}
		writer = outputFile
	}
	buffered := bufio.NewWriterSize(writer, 256*1024)
	defer func() {
		var finalizationErr error
		if err := buffered.Flush(); err != nil {
			finalizationErr = errors.Join(finalizationErr, fmt.Errorf("flush output: %w", err))
		}
		if outputFile != nil {
			if err := outputFile.Sync(); err != nil {
				finalizationErr = errors.Join(finalizationErr, fmt.Errorf("sync output %q: %w", *output, err))
			}
			if err := outputFile.Close(); err != nil {
				finalizationErr = errors.Join(finalizationErr, fmt.Errorf("close output %q: %w", *output, err))
			}
		}
		if finalizationErr == nil {
			return
		}
		returnedErr = errors.Join(returnedErr, finalizationErr)
	}()

	var emitted uint64
	for *count == 0 || emitted < *count {
		if waitErr := pacer.Wait(ctx, emitted); waitErr != nil {
			return waitErr
		}
		line, nextErr := generator.Next()
		if nextErr != nil {
			return nextErr
		}
		if _, err := buffered.Write(line); err != nil {
			return fmt.Errorf("write event %d: %w", emitted, err)
		}
		if err := buffered.WriteByte('\n'); err != nil {
			return fmt.Errorf("write event %d delimiter: %w", emitted, err)
		}
		emitted++
		if *flushEvents > 0 && emitted%*flushEvents == 0 {
			if err := buffered.Flush(); err != nil {
				return fmt.Errorf("flush output after event %d: %w", emitted-1, err)
			}
		}
	}
	return nil
}

func openOutputFile(path string, appendOutput bool) (*os.File, error) {
	flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if appendOutput {
		flags = os.O_CREATE | os.O_RDWR | os.O_APPEND
	}
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open output %q: %w", path, err)
	}
	if !appendOutput {
		return file, nil
	}

	info, err := file.Stat()
	if err == nil && info.Size() > 0 {
		var trailing [1]byte
		_, err = file.ReadAt(trailing[:], info.Size()-1)
		if err == nil && trailing[0] != '\n' {
			err = errors.New("existing output has an incomplete trailing record")
		}
	}
	if err != nil {
		return nil, errors.Join(fmt.Errorf("validate append output %q: %w", path, err), file.Close())
	}
	return file, nil
}
