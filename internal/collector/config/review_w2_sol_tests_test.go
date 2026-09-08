package config

import (
	"strings"
	"testing"
)

func TestReviewW2FormatsRejectNullParserBlock(t *testing.T) {
	for _, format := range []string{"ndjson", "raw", "docker-json-file", "nginx-combined", "apache-common", "apache-combined", "logfmt", "log4j2-pattern", "logback-pattern"} {
		t.Run(format, func(t *testing.T) {
			source := strings.Replace(validYAML, "    format: ndjson\n", "    format: "+format+"\n    parser: null\n", 1)
			if _, err := Load(writeFile(t, t.TempDir(), "collector.yaml", source)); err == nil {
				t.Fatal("accepted an explicitly configured null parser block")
			}
		})
	}
}
