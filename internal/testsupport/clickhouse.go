// Package testsupport contains reusable, opt-in integration-test fixtures.
package testsupport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const DefaultClickHouseImage = "clickhouse/clickhouse-server:26.3.17.4"

// ClickHouseContainer is an ephemeral, loopback-only ClickHouse instance.
// The password is intentionally exposed only as data so callers can connect
// through the native driver; String must never be added because it could make
// accidental logging disclose the credential.
type ClickHouseContainer struct {
	Name     string
	Address  string
	Database string
	Username string
	Password string
	Image    string
}

// StartClickHouse starts a disposable ClickHouse container and waits for four
// consecutive successful health probes. An empty image selects the pinned
// release used by the repository integration suite.
func StartClickHouse(ctx context.Context, image string) (*ClickHouseContainer, error) {
	if ctx == nil {
		return nil, errors.New("start ClickHouse test container: context is required")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("start ClickHouse test container: docker CLI is unavailable: %w", err)
	}
	if strings.TrimSpace(image) == "" {
		image = DefaultClickHouseImage
	}
	nameSuffix, err := randomHex(6)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse test container: create name: %w", err)
	}
	password, err := randomHex(24)
	if err != nil {
		return nil, fmt.Errorf("start ClickHouse test container: create password: %w", err)
	}
	container := &ClickHouseContainer{
		Name:     "open-splunk-clickhouse-" + nameSuffix,
		Database: "open_splunk",
		Username: "open_splunk",
		Password: password,
		Image:    image,
	}
	if output, err := docker(ctx,
		"run", "--detach", "--rm", "--name", container.Name,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB="+container.Database,
		"--env", "CLICKHOUSE_USER="+container.Username,
		"--env", "CLICKHOUSE_PASSWORD="+container.Password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		container.Image,
	); err != nil {
		return nil, fmt.Errorf("start ClickHouse test container: %w: %s", err, boundedOutput(output))
	}
	started := true
	defer func() {
		if started {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = container.Close(cleanupCtx)
		}
	}()
	if err := container.waitReady(ctx); err != nil {
		return nil, err
	}
	address, err := container.nativeAddress(ctx)
	if err != nil {
		return nil, err
	}
	container.Address = address
	started = false
	return container, nil
}

// Close forcibly removes the disposable container. Docker --rm makes this
// idempotent after a process or daemon has already removed it.
func (container *ClickHouseContainer) Close(ctx context.Context) error {
	if container == nil || strings.TrimSpace(container.Name) == "" {
		return nil
	}
	output, err := docker(ctx, "rm", "--force", container.Name)
	if err != nil && !strings.Contains(string(output), "No such container") {
		return fmt.Errorf("remove ClickHouse test container: %w: %s", err, boundedOutput(output))
	}
	return nil
}

func (container *ClickHouseContainer) waitReady(ctx context.Context) error {
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	stable := 0
	last := "no health probe completed"
	for {
		output, err := docker(ctx, "exec", container.Name, "clickhouse-client",
			"--user", container.Username, "--password", container.Password,
			"--query", "SELECT 1",
		)
		last = fmt.Sprintf("%v: %s", err, boundedOutput(output))
		if err == nil && strings.TrimSpace(string(output)) == "1" {
			stable++
			if stable == 4 {
				return nil
			}
		} else {
			stable = 0
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for ClickHouse test container: %w", ctx.Err())
		case <-deadline.C:
			return fmt.Errorf("wait for ClickHouse test container: timed out: %s", last)
		case <-ticker.C:
		}
	}
}

func (container *ClickHouseContainer) nativeAddress(ctx context.Context) (string, error) {
	output, err := docker(ctx, "port", container.Name, "9000/tcp")
	if err != nil {
		return "", fmt.Errorf("resolve ClickHouse test native port: %w: %s", err, boundedOutput(output))
	}
	for _, line := range strings.Split(strings.TrimSpace(string(output)), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "127.0.0.1:") {
			return line, nil
		}
	}
	return "", fmt.Errorf("resolve ClickHouse test native port: no loopback mapping in %q", boundedOutput(output))
}

func docker(ctx context.Context, arguments ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return exec.CommandContext(ctx, "docker", arguments...).CombinedOutput()
}

func randomHex(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func boundedOutput(output []byte) string {
	const maximum = 4 << 10
	if len(output) > maximum {
		output = output[len(output)-maximum:]
	}
	return strings.TrimSpace(string(output))
}
