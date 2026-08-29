package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk"
)

func TestEmbeddedReleaseVerificationIncludesSourceIdentity(t *testing.T) {
	t.Parallel()

	release := opensplunk.Release{Metadata: opensplunk.ReleaseMetadata{
		SourceRevision: strings.Repeat("a", 40),
		ProductVersion: "0.4.5",
		UIBuildID:      "ui-build",
		UI: opensplunk.ComponentMetadata{
			SHA256: strings.Repeat("b", 64),
		},
	}}
	var output bytes.Buffer
	if err := writeEmbeddedReleaseVerification(&output, release); err != nil {
		t.Fatalf("writeEmbeddedReleaseVerification() error = %v", err)
	}
	want := "source_revision=" + strings.Repeat("a", 40) + "\n" +
		"product_version=0.4.5\n" +
		"ui_build_id=ui-build\n" +
		"ui_sha256=" + strings.Repeat("b", 64) + "\n"
	if output.String() != want {
		t.Fatalf("verification output = %q, want %q", output.String(), want)
	}
}

func TestEmbeddedReleaseVerificationRejectsNilOutput(t *testing.T) {
	t.Parallel()
	if err := writeEmbeddedReleaseVerification(nil, opensplunk.Release{}); err == nil {
		t.Fatal("writeEmbeddedReleaseVerification(nil) unexpectedly succeeded")
	}
}

func TestScrubOpenSplunkEnv(t *testing.T) {
	t.Parallel()
	got := scrubOpenSplunkEnv([]string{
		"PATH=/usr/bin",
		"OPEN_SPLUNK_SERVER_LOG_FORMAT=console",
		"OPEN_SPLUNK_COLLECTOR_STATE_DIR=/tmp/state",
		"OPEN_SPLUNK_TEST_VERIFY_PIPED_STDERR=1",
		"malformed",
		"HOME=/home/tester",
	})
	want := []string{"PATH=/usr/bin", "OPEN_SPLUNK_TEST_VERIFY_PIPED_STDERR=1", "HOME=/home/tester"}
	if !slices.Equal(got, want) {
		t.Fatalf("scrubOpenSplunkEnv() = %q, want %q", got, want)
	}
}

// verifyPipedStderrSubprocessEnv re-enters this test binary as the server
// process under test.
const verifyPipedStderrSubprocessEnv = "OPEN_SPLUNK_TEST_VERIFY_PIPED_STDERR"

// TestRunWithPipedStderrNeverFailsOnLoggerSync runs -verify-embedded-release in
// a child process whose stdout and stderr are real pipes, which is how
// scripts/build-release.sh invokes it under GitHub Actions. fsync on a pipe
// reports EBADF on macOS, and treating that as a flush failure previously
// turned a successful verification into exit 1.
//
// The embedded release payload is absent from a plain source checkout, so the
// child may legitimately fail on the release manifest. The invariant asserted
// here is independent of that: flushing the process logger must never report a
// failure, and must never be the reason the process exits non-zero.
func TestRunWithPipedStderrNeverFailsOnLoggerSync(t *testing.T) {
	if os.Getenv(verifyPipedStderrSubprocessEnv) == "1" {
		os.Args = []string{"open-splunk-server", "-verify-embedded-release"}
		os.Exit(run())
	}

	command := exec.CommandContext(
		t.Context(),
		os.Args[0],
		"-test.run=^TestRunWithPipedStderrNeverFailsOnLoggerSync$",
		"-test.count=1",
	)
	// runtime_options.go binds 33 OPEN_SPLUNK_SERVER_* variables (and the
	// collector binds its own OPEN_SPLUNK_COLLECTOR_* set). Inheriting one from
	// a developer shell or a CI job would change what the child does - a stray
	// OPEN_SPLUNK_SERVER_LOG_FORMAT alone rewrites the stderr this test reads -
	// so the child gets a scrubbed environment plus the sentinel.
	command.Env = append(scrubOpenSplunkEnv(os.Environ()), verifyPipedStderrSubprocessEnv+"=1")

	// StdoutPipe and StderrPipe hand the child ends of an os.Pipe, which is
	// exactly the descriptor type that makes fsync fail.
	var stdout, stderr bytes.Buffer
	stdoutPipe, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderrPipe, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	var drain sync.WaitGroup
	drain.Add(2)
	go func() { defer drain.Done(); _, _ = io.Copy(&stdout, stdoutPipe) }()
	go func() { defer drain.Done(); _, _ = io.Copy(&stderr, stderrPipe) }()
	drain.Wait()

	waitErr := command.Wait()
	exitCode := 0
	if exitError, ok := errors.AsType[*exec.ExitError](waitErr); ok {
		exitCode = exitError.ExitCode()
	} else if waitErr != nil {
		t.Fatalf("run child process: %v (stderr: %s)", waitErr, stderr.String())
	}

	if strings.Contains(stderr.String(), "sync server logger") {
		t.Fatalf(
			"flushing the process logger over a pipe reported a failure: %s",
			stderr.String(),
		)
	}

	// Without the embedded payload the child exits 1 on the release manifest.
	// With it, writeEmbeddedReleaseVerification prints the identity block to
	// stdout and the child must exit 0. Keying off that block rather than off
	// the absence of an error marker also catches the opposite failure: a
	// zero exit with no verification output would mean the flag silently did
	// nothing.
	verified := strings.Contains(stdout.String(), "source_revision=") &&
		strings.Contains(stdout.String(), "ui_sha256=")
	if verified && exitCode != 0 {
		t.Fatalf(
			"verification succeeded but exit code = %d (stderr: %s)",
			exitCode,
			stderr.String(),
		)
	}
	if exitCode == 0 && !verified {
		t.Fatalf(
			"child exited 0 without printing the verification identity block (stdout: %q, stderr: %s)",
			stdout.String(),
			stderr.String(),
		)
	}
	if !verified && !strings.Contains(stderr.String(), "open embedded release") {
		t.Fatalf(
			"child failed for an unexpected reason: exit=%d stdout=%q stderr=%s",
			exitCode,
			stdout.String(),
			stderr.String(),
		)
	}
	t.Logf("child exit=%d embedded release verified=%v", exitCode, verified)
}

// scrubOpenSplunkEnv removes every Open Splunk runtime binding from an
// inherited environment while keeping the rest (PATH, HOME, GOCOVERDIR, the
// Go toolchain's own variables) intact.
func scrubOpenSplunkEnv(environment []string) []string {
	scrubbed := make([]string, 0, len(environment))
	for _, binding := range environment {
		name, _, found := strings.Cut(binding, "=")
		if !found {
			continue
		}
		if strings.HasPrefix(name, "OPEN_SPLUNK_SERVER_") ||
			strings.HasPrefix(name, "OPEN_SPLUNK_COLLECTOR_") {
			continue
		}
		scrubbed = append(scrubbed, binding)
	}
	return scrubbed
}
