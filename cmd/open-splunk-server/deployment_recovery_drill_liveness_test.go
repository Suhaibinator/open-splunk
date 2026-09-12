//go:build linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Run with CGO_ENABLED=0 as well: the shipped drill helper is static, so a
// blocked nil channel can trigger Go's deadlock detector after native restore
// closes its last connection. A cgo binary can conceal that premature exit.
func TestDeploymentRecoveryDrillReceiptPauseSurvivesUntilKilled(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, executable, "-test.run=^TestDeploymentRecoveryDrillChild$", "-test.timeout=0", "-test.v")
	child.Env = append(os.Environ(), "OPEN_SPLUNK_RECOVERY_DRILL_CHILD=pause-after-receipt")
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	child.Stderr = &stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	published := false
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "RECOVERY_RECEIPT_PUBLISHED") {
			published = true
			break
		}
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	if !published {
		t.Fatalf("child exited before receipt marker: %v\n%s", <-done, stderr.String())
	}
	select {
	case err := <-done:
		t.Fatalf("receipt pause exited before parent kill: %v\n%s", err, stderr.String())
	case <-time.After(250 * time.Millisecond):
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill paused receipt child: %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("killed receipt child unexpectedly succeeded")
	}
	if status, ok := child.ProcessState.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("receipt child did not exit through parent SIGKILL: %v", child.ProcessState)
	}
}
