package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/splcompataudit"
)

func TestRunSPLCompatibilityAuditWritesOnlyRedactedInventory(t *testing.T) {
	repository := t.TempDir()
	source := "index=main | eval value=request-bytes\n"
	if err := os.WriteFile(filepath.Join(repository, "affected.spl"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runSPLCompatibilityAudit(
		context.Background(),
		splCompatibilityAuditOptions{Repository: repository},
		&output,
	); err != nil {
		t.Fatalf("runSPLCompatibilityAudit(): %v", err)
	}
	var report splcompataudit.Report
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		t.Fatal(err)
	}
	if report.CompatibilityVersion != "0.2" || report.ScannedObjects != 1 ||
		len(report.Findings) != 1 || report.Findings[0].ObjectID != "repository:affected.spl" {
		t.Fatalf("audit report = %#v", report)
	}
	if strings.Contains(output.String(), "request-bytes") ||
		strings.Contains(output.String(), "index=main") ||
		strings.Contains(output.String(), "eval value") {
		t.Fatalf("audit output leaked authored SPL: %s", output.String())
	}
}

func TestSPLCompatibilityAuditSubcommandIsExplicitAndBounded(t *testing.T) {
	for _, arguments := range [][]string{
		{"audit-spl-v0.2", "-unknown"},
		{"audit-spl-v0.2", "positional"},
	} {
		handled, err := runDeploymentSubcommand(arguments)
		if !handled || err == nil {
			t.Fatalf("runDeploymentSubcommand(%q) = (%t, %v), want handled error", arguments, handled, err)
		}
	}
	if handled, err := runDeploymentSubcommand([]string{"audit-spl-v0.3"}); handled || err != nil {
		t.Fatalf("unknown audit subcommand = (%t, %v)", handled, err)
	}
}
