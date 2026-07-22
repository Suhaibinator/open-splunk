//go:build !windows

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRedactForFailure(t *testing.T) {
	const secret = "protected-value"
	redacted := redactForFailure("before "+secret+" after "+secret, secret)
	if strings.Contains(redacted, secret) || redacted != "before [REDACTED] after [REDACTED]" {
		t.Fatalf("redactForFailure() = %q", redacted)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	directory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if info, statErr := os.Stat(filepath.Join(directory, "go.mod")); statErr == nil && !info.IsDir() {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not locate repository go.mod")
		}
		directory = parent
	}
}

func buildBinary(t *testing.T, ctx context.Context, repository, output, pkg string) {
	t.Helper()
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", output, pkg)
	command.Dir = repository
	command.Env = append(os.Environ(), "CGO_ENABLED=0")
	combined, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build %s: %v\n%s", pkg, err, combined)
	}
}

func unusedLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func redactForFailure(value string, secrets ...string) string {
	for _, secret := range secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return value
}

type managedProcess struct {
	command *exec.Cmd
	logs    *lockedBuffer
	done    chan struct{}

	mu  sync.Mutex
	err error
}

func startProcess(t *testing.T, directory string, arguments []string, environment []string) *managedProcess {
	t.Helper()
	if len(arguments) == 0 {
		t.Fatal("process command is required")
	}
	logs := &lockedBuffer{maximum: 1 << 20}
	command := exec.Command(arguments[0], arguments[1:]...)
	command.Dir = directory
	command.Env = environment
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start %s: %v", arguments[0], err)
	}
	process := &managedProcess{command: command, logs: logs, done: make(chan struct{})}
	go func() {
		err := command.Wait()
		process.mu.Lock()
		process.err = err
		process.mu.Unlock()
		close(process.done)
	}()
	t.Cleanup(func() {
		if err := process.Kill(5 * time.Second); err != nil {
			t.Errorf("force process cleanup: %v", err)
		}
	})
	return process
}

func (process *managedProcess) Interrupt(timeout time.Duration) error {
	select {
	case <-process.done:
		return process.Err()
	default:
	}
	if err := process.command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-process.done:
		return process.Err()
	case <-timer.C:
		killErr := process.Kill(5 * time.Second)
		return fmt.Errorf("graceful shutdown timed out after %s (force cleanup: %v)", timeout, killErr)
	}
}

func (process *managedProcess) Kill(timeout time.Duration) error {
	select {
	case <-process.done:
		return nil
	default:
	}
	if err := process.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-process.done:
		return nil
	case <-timer.C:
		return fmt.Errorf("process did not exit within %s after kill", timeout)
	}
}

func (process *managedProcess) Exited() bool {
	select {
	case <-process.done:
		return true
	default:
		return false
	}
}

func (process *managedProcess) Err() error {
	process.mu.Lock()
	defer process.mu.Unlock()
	return process.err
}

func (process *managedProcess) Logs() string { return process.logs.String() }

type lockedBuffer struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	maximum int
}

func (buffer *lockedBuffer) Write(value []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	written := len(value)
	remaining := buffer.maximum - buffer.buffer.Len()
	if remaining > 0 {
		_, _ = buffer.buffer.Write(value[:min(len(value), remaining)])
	}
	return written, nil
}

func (buffer *lockedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}
