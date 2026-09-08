package config

import (
	"strings"
	"testing"
)

func TestReviewW1LogfmtRejectsExplicitEmptyPattern(t *testing.T) {
	t.Parallel()
	yaml := strings.Replace(
		validYAML,
		"    format: ndjson\n",
		"    format: logfmt\n    parser:\n      pattern: ''\n",
		1,
	)

	_, err := Load(writeFile(t, t.TempDir(), "collector.yaml", yaml))
	if err == nil {
		t.Fatal("accepted logfmt configuration with an explicit pattern option")
	}
}
