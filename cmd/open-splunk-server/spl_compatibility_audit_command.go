package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/Suhaibinator/open-splunk/internal/splcompataudit"
)

type splCompatibilityAuditOptions struct {
	ControlDatabase string
	Repository      string
}

func runSPLCompatibilityAuditSubcommand(arguments []string) error {
	flags := flag.NewFlagSet("audit-spl-v0.2", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var options splCompatibilityAuditOptions
	flags.StringVar(
		&options.ControlDatabase,
		"control-db",
		"",
		"explicit existing control-plane SQLite database opened query-only",
	)
	flags.StringVar(
		&options.Repository,
		"repository",
		"",
		"explicit repository root scanned without following symbolic links",
	)
	if err := flags.Parse(arguments); err != nil {
		return fmt.Errorf("audit SPL v0.2 compatibility: parse flags: %w", err)
	}
	if flags.NArg() != 0 {
		return errors.New("audit SPL v0.2 compatibility: positional arguments are not allowed")
	}
	return runSPLCompatibilityAudit(
		context.Background(),
		options,
		os.Stdout,
	)
}

func runSPLCompatibilityAudit(
	ctx context.Context,
	options splCompatibilityAuditOptions,
	output io.Writer,
) error {
	if ctx == nil {
		return errors.New("audit SPL v0.2 compatibility: context is nil")
	}
	if output == nil {
		return errors.New("audit SPL v0.2 compatibility: output is nil")
	}
	report, err := splcompataudit.Audit(
		ctx,
		options.ControlDatabase,
		options.Repository,
	)
	if err != nil {
		return fmt.Errorf("audit SPL v0.2 compatibility: %w", err)
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(true)
	if err := encoder.Encode(report); err != nil {
		return errors.New("audit SPL v0.2 compatibility: write redacted report")
	}
	return nil
}
