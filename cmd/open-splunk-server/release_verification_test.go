package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"os/exec"
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
	command.Env = append(os.Environ(), verifyPipedStderrSubprocessEnv+"=1")

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
	// With it, verification succeeds and the child must exit 0.
	releaseAvailable := !strings.Contains(stderr.String(), "open embedded release")
	if releaseAvailable && exitCode != 0 {
		t.Fatalf(
			"verification succeeded but exit code = %d (stderr: %s)",
			exitCode,
			stderr.String(),
		)
	}
	t.Logf("child exit=%d embedded release available=%v", exitCode, releaseAvailable)
}
