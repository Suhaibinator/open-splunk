package testsupport

import (
	"strings"
	"testing"
)

func TestStartClickHouseRejectsNilContextWithoutCallingDocker(t *testing.T) {
	if _, err := StartClickHouse(nil, ""); err == nil || !strings.Contains(err.Error(), "context is required") {
		t.Fatalf("StartClickHouse(nil) error = %v", err)
	}
}

func TestBoundedOutputRetainsDiagnosticTail(t *testing.T) {
	input := strings.Repeat("prefix", 1_000) + "diagnostic-tail\n"
	output := boundedOutput([]byte(input))
	if len(output) > 4<<10 || !strings.HasSuffix(output, "diagnostic-tail") {
		t.Fatalf("bounded output length = %d, suffix retained = %v", len(output), strings.HasSuffix(output, "diagnostic-tail"))
	}
}
