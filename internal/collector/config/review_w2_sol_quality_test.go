package config

import (
	"strings"
	"testing"
)

func TestReviewW2RejectsNullParserForFormatsWithoutOptions(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"ndjson", "raw", "docker-json-file", "nginx-combined", "apache-common", "apache-combined"} {
		t.Run(format, func(t *testing.T) {
			yaml := strings.Replace(
				validYAML,
				"    format: ndjson\n",
				"    format: "+format+"\n    parser: null\n",
				1,
			)

			if _, err := Load(writeFile(t, t.TempDir(), "collector.yaml", yaml)); err == nil {
				t.Fatal("accepted an explicitly supplied parser option on a format that does not accept parser options")
			}
		})
	}
}
