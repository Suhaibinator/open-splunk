// Command open-splunk-collector tails local log files and forwards acknowledged,
// index-routed event batches to the Open Splunk server over gRPC.
//
// # Subcommands
//
//	open-splunk-collector [run] [-config PATH] [-log-level LEVEL]
//	                                                 start the collector (default)
//	open-splunk-collector validate [-config PATH]  check the config and exit
//	open-splunk-collector identity [-config PATH]  initialize and print the stable ID
//
// validate loads and validates the configuration, prints a redacted summary
// (never the bearer token) and the number of files each input's globs currently
// match, and exits non-zero with the precise error on failure.
//
// identity loads and validates the configuration without reading the bearer
// token, durably initializes the stable collector ID under state.directory, and
// prints only that ID. It does not open inputs, checkpoints, or the WAL and does
// not contact the server.
//
// run loads the configuration, builds the daemon, and runs until SIGINT or
// SIGTERM triggers a graceful shutdown.
//
// # Exit codes
//
//	0  clean shutdown (run) or valid configuration (validate)
//	1  runtime error, or invalid/unloadable configuration
//	2  usage error (unknown subcommand or bad flags)
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Suhaibinator/open-splunk/internal/buildinfo"
	"github.com/Suhaibinator/open-splunk/internal/collector"
	"github.com/Suhaibinator/open-splunk/internal/collector/config"
)

// defaultConfigPath is used when -config is not supplied.
const defaultConfigPath = "/etc/open-splunk/collector.yaml"

func main() {
	os.Exit(run(os.Args[1:]))
}

// run dispatches the subcommand and returns the process exit code.
func run(args []string) int {
	cmd := "run"
	if len(args) > 0 && len(args[0]) > 0 && args[0][0] != '-' {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "run":
		return runCollector(args)
	case "validate":
		return validateConfig(args)
	case "identity":
		return runIdentity(args, os.Stdout, os.Stderr)
	case "version":
		if len(args) != 0 {
			fmt.Fprintln(os.Stderr, "version does not accept arguments")
			return 2
		}
		if err := writeBuildIdentity(os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "load build identity: %v\n", err)
			return 1
		}
		return 0
	case "help", "-h", "-help", "--help":
		usage(os.Stdout)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", cmd)
		usage(os.Stderr)
		return 2
	}
}

// runIdentity initializes and prints the stable collector ID without reading
// the ingestion token or starting any collector runtime components.
func runIdentity(args []string, stdout, stderr io.Writer) int {
	if stdout == nil {
		return 1
	}
	if stderr == nil {
		stderr = io.Discard
	}
	fs := flag.NewFlagSet("identity", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("config", defaultConfigPath, "path to the collector configuration file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "identity does not accept positional arguments")
		return 2
	}

	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintf(stderr, "load configuration: %v\n", err)
		return 1
	}
	id, err := collector.InitializeCollectorID(cfg.State.Directory)
	if err != nil {
		fmt.Fprintf(stderr, "initialize collector identity: %v\n", err)
		return 1
	}
	if _, err := fmt.Fprintln(stdout, id); err != nil {
		fmt.Fprintf(stderr, "write collector identity: %v\n", err)
		return 1
	}
	return 0
}

func writeBuildIdentity(output io.Writer) error {
	if output == nil {
		return fmt.Errorf("build identity output is required")
	}
	identity, err := buildinfo.Current()
	if err != nil {
		return err
	}
	return buildinfo.WriteIdentity(output, identity)
}

// runCollector builds and runs the daemon until a termination signal arrives.
func runCollector(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	path := fs.String("config", defaultConfigPath, "path to the collector configuration file")
	logLevelText := fs.String("log-level", "info", "log level: debug, info, warn, or error")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	logLevel, err := parseLogLevel(*logLevelText)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

	cfg, err := config.Load(*path)
	if err != nil {
		logger.Error("load config", "error", err.Error())
		return 1
	}

	daemon, err := collector.New(cfg, collector.WithLogger(logger))
	if err != nil {
		logger.Error("build collector", "error", err.Error())
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("collector starting", "config", *path, "inputs", len(cfg.Inputs))
	if err := daemon.Run(ctx); err != nil {
		logger.Error("collector stopped with error", "error", err.Error())
		return 1
	}
	logger.Info("collector stopped cleanly")
	return 0
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(value) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log level %q: choose debug, info, warn, or error", value)
	}
}

// validateConfig loads and validates the config, printing a redacted summary and
// per-input glob match counts. It never prints the bearer token.
func validateConfig(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	path := fs.String("config", defaultConfigPath, "path to the collector configuration file")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		return 1
	}

	fmt.Printf("configuration %s is valid\n\n", *path)
	fmt.Print(cfg.String())
	fmt.Println("\ninput glob matches:")
	for _, in := range cfg.Inputs {
		matched := countMatches(in.Include, in.Exclude)
		fmt.Printf("  - %s: %d file(s)\n", in.ID, matched)
	}
	return 0
}

// countMatches returns the number of distinct files matched by include globs and
// not removed by exclude globs, mirroring the input manager's discovery rules.
func countMatches(include, exclude []string) int {
	set := make(map[string]struct{})
	for _, inc := range include {
		matches, err := filepath.Glob(inc)
		if err != nil {
			continue
		}
		for _, p := range matches {
			set[p] = struct{}{}
		}
	}
	for p := range set {
		base := filepath.Base(p)
		for _, exc := range exclude {
			if ok, _ := filepath.Match(exc, p); ok {
				delete(set, p)
				break
			}
			if ok, _ := filepath.Match(exc, base); ok {
				delete(set, p)
				break
			}
		}
	}
	return len(set)
}

// usage prints the command summary.
func usage(w *os.File) {
	fmt.Fprint(w, `open-splunk-collector: tail local logs and forward them to the Open Splunk server.

usage:
  open-splunk-collector [run] [-config PATH] [-log-level LEVEL]
                                                   start the collector (default)
  open-splunk-collector validate [-config PATH]    validate configuration and exit
  open-splunk-collector identity [-config PATH]    initialize and print stable ID
  open-splunk-collector version                    print the compiled build identity

flags:
  -config PATH       configuration file (default `+defaultConfigPath+`)
  -log-level LEVEL   run log level: debug, info, warn, or error (default info)
`)
}
