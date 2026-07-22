// Command open-splunk-collector tails local log files and forwards acknowledged,
// index-routed event batches to the Open Splunk server over gRPC.
//
// # Subcommands
//
//	open-splunk-collector [run] [-config PATH]     start the collector (default)
//	open-splunk-collector validate [-config PATH]  check the config and exit
//
// validate loads and validates the configuration, prints a redacted summary
// (never the bearer token) and the number of files each input's globs currently
// match, and exits non-zero with the precise error on failure.
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
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

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
	case "help", "-h", "-help", "--help":
		usage(os.Stdout)
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", cmd)
		usage(os.Stderr)
		return 2
	}
}

// runCollector builds and runs the daemon until a termination signal arrives.
func runCollector(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	path := fs.String("config", defaultConfigPath, "path to the collector configuration file")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

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
  open-splunk-collector [run] [-config PATH]      start the collector (default)
  open-splunk-collector validate [-config PATH]   validate configuration and exit

flags:
  -config PATH   configuration file (default `+defaultConfigPath+`)
`)
}
