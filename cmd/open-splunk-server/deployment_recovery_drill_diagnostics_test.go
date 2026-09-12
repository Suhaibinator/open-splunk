//go:build linux

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

const (
	recoveryDrillDiagnosticLimit       = 16 << 10
	recoveryDrillDiagnosticOverlap     = 1 << 10
	recoveryDrillDiagnosticPlaceholder = "<redacted>"
	recoveryDrillDiagnosticTruncated   = "\n[diagnostics truncated]"
)

var recoveryDrillPrivateKeyPattern = regexp.MustCompile(
	`(?s)-----BEGIN [^\r\n]*PRIVATE KEY-----.*?(?:-----END [^\r\n]*PRIVATE KEY-----|$)`,
)

type recoveryDrillDiagnosticBuffer struct {
	contents  []byte
	limit     int
	truncated bool
}

func newRecoveryDrillDiagnosticBuffer(secrets []string) *recoveryDrillDiagnosticBuffer {
	overlap := recoveryDrillDiagnosticOverlap
	for _, secret := range secrets {
		overlap = max(overlap, len(secret))
	}
	return &recoveryDrillDiagnosticBuffer{
		contents: make([]byte, 0, recoveryDrillDiagnosticLimit+overlap),
		limit:    recoveryDrillDiagnosticLimit + overlap,
	}
}

func (buffer *recoveryDrillDiagnosticBuffer) Write(value []byte) (int, error) {
	written := len(value)
	available := buffer.limit - len(buffer.contents)
	if available <= 0 {
		buffer.truncated = buffer.truncated || written > 0
		return written, nil
	}
	if len(value) > available {
		buffer.contents = append(buffer.contents, value[:available]...)
		buffer.truncated = true
		return written, nil
	}
	buffer.contents = append(buffer.contents, value...)
	return written, nil
}

func (buffer *recoveryDrillDiagnosticBuffer) writeString(value string) {
	_, _ = buffer.Write([]byte(value))
}

func (fixture *recoveryDrill) apiErrorDiagnostic(body io.Reader, token string) string {
	secrets := append([]string{token}, fixture.diagnosticSecrets...)
	diagnostic := newRecoveryDrillDiagnosticBuffer(secrets)
	if _, err := io.Copy(diagnostic, io.LimitReader(body, int64(diagnostic.limit)+1)); err != nil {
		diagnostic.writeString("\nread error response: " + err.Error())
	}
	return recoveryDrillDiagnosticTextWithTruncation(diagnostic.contents, secrets, diagnostic.truncated)
}

func (fixture *recoveryDrill) reportDiagnostics(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	diagnostic := newRecoveryDrillDiagnosticBuffer(fixture.diagnosticSecrets)
	diagnostic.writeString("[last readiness]\n")
	if fixture.lastReadiness == "" {
		diagnostic.writeString("unavailable\n")
	} else {
		diagnostic.writeString(fixture.lastReadiness + "\n")
	}
	for _, command := range []struct {
		label     string
		arguments []string
	}{
		{label: "compose status", arguments: []string{"ps", "--all"}},
		{label: "server and ClickHouse logs", arguments: []string{"logs", "--no-color", "--tail", "40", "server", "clickhouse"}},
	} {
		diagnostic.writeString("\n[" + command.label + "]\n")
		process := exec.CommandContext(ctx, "docker", fixture.composeArguments(command.arguments...)...)
		process.Env = fixture.environment
		process.Stdout = diagnostic
		process.Stderr = diagnostic
		if err := process.Run(); err != nil {
			diagnostic.writeString("\ndiagnostic command failed: " + err.Error() + "\n")
		}
	}

	output := recoveryDrillDiagnosticTextWithTruncation(
		diagnostic.contents,
		fixture.diagnosticSecrets,
		diagnostic.truncated,
	)
	t.Logf("recovery drill diagnostics:\n%s", output)
}

func (fixture *recoveryDrill) reportChildDiagnostics(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	diagnostic := newRecoveryDrillDiagnosticBuffer(fixture.diagnosticSecrets)
	diagnostic.writeString("[crash child state]\n")
	process := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{json .State}}", name)
	process.Env = fixture.environment
	process.Stdout, process.Stderr = diagnostic, diagnostic
	if err := process.Run(); err != nil {
		diagnostic.writeString("\ninspect child: " + err.Error() + "\n")
	}
	diagnostic.writeString("\n[crash child output]\n")
	childOutput, err := fixture.readChildOutput()
	if err != nil {
		diagnostic.writeString("read child log: " + err.Error() + "\n")
	} else {
		_, _ = diagnostic.Write(childOutput.contents)
		diagnostic.truncated = diagnostic.truncated || childOutput.truncated
	}
	t.Logf("recovery drill child diagnostics:\n%s", recoveryDrillDiagnosticTextWithTruncation(
		diagnostic.contents, fixture.diagnosticSecrets, diagnostic.truncated))
}

func (fixture *recoveryDrill) readChildOutput() (*recoveryDrillDiagnosticBuffer, error) {
	logFile, err := os.Open(filepath.Join(fixture.work, "crash.log"))
	if err != nil {
		return nil, err
	}
	defer logFile.Close()
	diagnostic := newRecoveryDrillDiagnosticBuffer(fixture.diagnosticSecrets)
	_, err = io.Copy(diagnostic, io.LimitReader(logFile, int64(diagnostic.limit)+1))
	return diagnostic, err
}

func recoveryDrillDiagnosticText(output []byte, secrets []string) string {
	return recoveryDrillDiagnosticTextWithTruncation(output, secrets, false)
}

func recoveryDrillDiagnosticTextWithTruncation(
	output []byte,
	secrets []string,
	truncated bool,
) string {
	text := strings.ToValidUTF8(string(output), "�")
	orderedSecrets := slices.Clone(secrets)
	slices.SortFunc(orderedSecrets, func(left, right string) int {
		return len(right) - len(left)
	})
	if truncated {
		text = redactRecoveryDrillTruncatedSecret(text, orderedSecrets)
	}
	for _, secret := range orderedSecrets {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, recoveryDrillDiagnosticPlaceholder)
		}
	}
	text = recoveryDrillPrivateKeyPattern.ReplaceAllString(text, recoveryDrillDiagnosticPlaceholder)
	if truncated {
		text += recoveryDrillDiagnosticTruncated
	}
	return truncateRecoveryDrillDiagnostic(text)
}

func redactRecoveryDrillTruncatedSecret(text string, secrets []string) string {
	matched := 0
	for _, secret := range secrets {
		for length := min(len(secret), len(text)); length > matched; length-- {
			if strings.HasSuffix(text, secret[:length]) {
				matched = length
				break
			}
		}
	}
	if matched == 0 {
		return text
	}
	return text[:len(text)-matched] + recoveryDrillDiagnosticPlaceholder
}

func truncateRecoveryDrillDiagnostic(text string) string {
	if len(text) <= recoveryDrillDiagnosticLimit {
		return text
	}
	limit := recoveryDrillDiagnosticLimit - len(recoveryDrillDiagnosticTruncated)
	for limit > 0 && !utf8.ValidString(text[:limit]) {
		limit--
	}
	return text[:limit] + recoveryDrillDiagnosticTruncated
}

func TestRecoveryDrillDiagnosticTextRedactsSecretsAndPrivateKeys(t *testing.T) {
	t.Parallel()
	const secret = "administrator-token-that-must-not-be-logged"
	output := []byte(fmt.Sprintf(
		"password=%s\n-----BEGIN PRIVATE KEY-----\nprivate material\n-----END PRIVATE KEY-----\nafter\n-----BEGIN OPENSSH PRIVATE KEY-----\nunterminated material",
		secret,
	))
	got := recoveryDrillDiagnosticText(output, []string{"", "token", secret})
	for _, forbidden := range []string{secret, "private material", "unterminated material", "BEGIN PRIVATE KEY", "BEGIN OPENSSH PRIVATE KEY"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("diagnostics retained sensitive text %q: %s", forbidden, got)
		}
	}
	if strings.Count(got, recoveryDrillDiagnosticPlaceholder) != 3 || !strings.Contains(got, "after") {
		t.Fatalf("diagnostic redaction = %q", got)
	}
}

func TestRecoveryDrillAPIErrorDiagnosticPreservesReasonWithoutCredentials(t *testing.T) {
	t.Parallel()
	const token = "request-administrator-credential"
	const password = "fixture-clickhouse-password"
	fixture := &recoveryDrill{diagnosticSecrets: []string{password}}
	body := strings.NewReader(`{"error":"search authority is unavailable","token":"` + token + `","password":"` + password + `"}`)
	got := fixture.apiErrorDiagnostic(body, token)
	if !strings.Contains(got, "search authority is unavailable") || strings.Contains(got, token) || strings.Contains(got, password) {
		t.Fatalf("API error diagnostic lost its reason or exposed a credential: %q", got)
	}
}

func TestRecoveryDrillAPIErrorDiagnosticBoundsResponseConsumption(t *testing.T) {
	t.Parallel()
	fixture := &recoveryDrill{}
	body := strings.NewReader(strings.Repeat("x", 1<<20))
	before := body.Len()
	got := fixture.apiErrorDiagnostic(body, "")
	maximumRead := newRecoveryDrillDiagnosticBuffer(nil).limit + 1
	if before-body.Len() != maximumRead || len(got) > recoveryDrillDiagnosticLimit ||
		!strings.HasSuffix(got, recoveryDrillDiagnosticTruncated) {
		t.Fatalf("error response bounds: read=%d output=%d", before-body.Len(), len(got))
	}
}

func TestRecoveryDrillDiagnosticBufferProtectsSecretAcrossDisplayBoundary(t *testing.T) {
	t.Parallel()
	secret := strings.Repeat("sensitive", 160)
	buffer := newRecoveryDrillDiagnosticBuffer([]string{secret})
	prefix := bytes.Repeat([]byte{'x'}, recoveryDrillDiagnosticLimit-8)
	_, _ = buffer.Write(prefix)
	_, _ = buffer.Write([]byte(secret + strings.Repeat("tail", 1<<14)))
	got := recoveryDrillDiagnosticTextWithTruncation(buffer.contents, []string{secret}, buffer.truncated)
	if strings.Contains(got, secret[:32]) || len(got) > recoveryDrillDiagnosticLimit {
		t.Fatalf("bounded diagnostics exposed a partial secret or exceeded the limit: bytes=%d", len(got))
	}
	if !strings.HasSuffix(got, recoveryDrillDiagnosticTruncated) {
		t.Fatalf("bounded diagnostics omitted truncation disclosure: %q", got[len(got)-64:])
	}
}

func TestRecoveryDrillDiagnosticBufferRedactsPartialSecretAfterEarlierRedaction(t *testing.T) {
	t.Parallel()
	secret := strings.Repeat("boundary-secret-", 96)
	buffer := newRecoveryDrillDiagnosticBuffer([]string{secret})
	for range 8 {
		_, _ = buffer.Write([]byte(secret + "\n"))
	}
	partialLength := len(secret) / 2
	padding := buffer.limit - len(buffer.contents) - partialLength
	_, _ = buffer.Write(bytes.Repeat([]byte{'x'}, padding))
	_, _ = buffer.Write([]byte(secret + "uncaptured tail"))
	if !buffer.truncated {
		t.Fatal("fixture did not truncate the final secret")
	}
	got := recoveryDrillDiagnosticTextWithTruncation(buffer.contents, []string{secret}, true)
	if strings.Contains(got, secret[:partialLength]) || len(got) > recoveryDrillDiagnosticLimit {
		t.Fatalf("diagnostics exposed a partial secret after earlier redaction: bytes=%d", len(got))
	}
	if !strings.HasSuffix(got, recoveryDrillDiagnosticTruncated) {
		t.Fatal("diagnostics omitted truncation disclosure")
	}
}

func TestRecoveryDrillDiagnosticBufferRedactsWholeSecretAtCaptureBoundary(t *testing.T) {
	t.Parallel()
	secret := strings.Repeat("boundary-secret-", 96)
	buffer := newRecoveryDrillDiagnosticBuffer([]string{secret})
	for range 8 {
		_, _ = buffer.Write([]byte(secret + "\n"))
	}
	padding := buffer.limit - len(buffer.contents) - len(secret)
	_, _ = buffer.Write(bytes.Repeat([]byte{'x'}, padding))
	_, _ = buffer.Write([]byte(secret))
	_, _ = buffer.Write([]byte("uncaptured tail"))
	if !buffer.truncated {
		t.Fatal("fixture did not truncate after the boundary-aligned secret")
	}
	got := recoveryDrillDiagnosticTextWithTruncation(buffer.contents, []string{secret}, true)
	if strings.Contains(got, "boundary-secret-") || len(got) > recoveryDrillDiagnosticLimit {
		t.Fatalf("diagnostics exposed a repeating secret at the capture boundary: bytes=%d", len(got))
	}
}
