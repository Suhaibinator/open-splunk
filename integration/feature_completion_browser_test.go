//go:build !windows

package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

const featureCompletionIntegrationFlag = "OPEN_SPLUNK_FEATURE_COMPLETION_INTEGRATION"

func assertBrowserFeatureCompletion(
	t *testing.T,
	ctx context.Context,
	repository, baseURL string,
	fixtureStart, bulkStart time.Time,
	savedSearchID, appID, administratorToken string,
	serverProcess *managedProcess,
) {
	t.Helper()
	if os.Getenv(featureCompletionIntegrationFlag) != "1" {
		t.Log("set " + featureCompletionIntegrationFlag + "=1 to run the feature-completion browser flow")
		return
	}

	browserContext, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(
		browserContext,
		filepath.Join(repository, "node_modules", ".bin", "playwright"),
		"test",
		"integration/feature_completion.spec.ts",
		"--workers=1",
		"--reporter=line",
		"--output="+filepath.Join(repository, "test-results", "feature-completion"),
	)
	configureProcessGroup(command)
	command.Dir = repository
	environment := os.Environ()
	for _, flag := range browserE2EModeFlags {
		environment = environmentWithValue(environment, flag, "0")
	}
	for name, value := range map[string]string{
		"OPEN_SPLUNK_FEATURE_COMPLETION_ADMINISTRATOR_TOKEN": administratorToken,
		"OPEN_SPLUNK_FEATURE_COMPLETION_APP_ID":              appID,
		"OPEN_SPLUNK_FEATURE_COMPLETION_BASE_URL":            baseURL,
		"OPEN_SPLUNK_FEATURE_COMPLETION_BULK_START":          bulkStart.Format(time.RFC3339Nano),
		"OPEN_SPLUNK_FEATURE_COMPLETION_FIXTURE_START":       fixtureStart.Format(time.RFC3339Nano),
		"OPEN_SPLUNK_FEATURE_COMPLETION_SAVED_SEARCH_ID":     savedSearchID,
		"OPEN_SPLUNK_FEATURE_COMPLETION_EXPECTED_BULK_ROWS":  strconv.FormatUint(bulkEventCount-1, 10),
	} {
		environment = environmentWithValue(environment, name, value)
	}
	command.Env = environment
	logs, truncated, runErr := runCommandWithBoundedOutput(command, maximumHarnessOutputBytes)
	redactedLogs := redactForFailure(logs, administratorToken)
	if runErr != nil {
		diagnostics := managedProcessSearchFailureDiagnostics(serverProcess)
		if diagnostics != "" {
			diagnostics = "\nbackend diagnostics:\n" + formatBoundedCommandOutput(
				redactForFailure(diagnostics, administratorToken),
				len(diagnostics) > maximumHarnessOutputBytes,
				maximumHarnessOutputBytes,
			)
		}
		t.Fatalf(
			"verify integrated feature-completion browser flow: %v\n%s%s",
			runErr,
			formatBoundedCommandOutput(redactedLogs, truncated, maximumHarnessOutputBytes),
			diagnostics,
		)
	}
	if truncated {
		t.Fatalf(
			"feature-completion browser logs exceeded %d bytes\n%s",
			maximumHarnessOutputBytes,
			formatBoundedCommandOutput(redactedLogs, true, maximumHarnessOutputBytes),
		)
	}
}
