package config

import (
	"bytes"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

func TestParserPresenceRejectsNullAndWrongTypes(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"ndjson", "raw", "docker-json-file", "nginx-combined", "apache-common", "apache-combined", "logfmt", "log4j2-pattern", "logback-pattern"} {
		for name, block := range map[string]string{
			"null":                "    parser: null\n",
			"implicit-null":       "    parser:\n",
			"tilde":               "    parser: ~\n",
			"tagged-null":         "    parser: !!null null\n",
			"alias-null":          "    exclude: &empty null\n    parser: *empty\n",
			"merged-null":         "    <<: {parser: null}\n",
			"merged-alias-null":   "    exclude: &empty null\n    <<: {parser: *empty}\n",
			"merge-sequence-null": "    <<: [{parser: null}, {parser: {}}]\n",
			"string":              "    parser: ''\n",
			"sequence":            "    parser: []\n",
			"boolean":             "    parser: false\n",
		} {
			t.Run(format+"/"+name, func(t *testing.T) {
				if _, err := Load(writeFile(t, t.TempDir(), "collector.yaml", parserPresenceYAML(format, block))); err == nil {
					t.Fatal("accepted parser value that is not a mapping")
				}
			})
		}
	}
}

func TestParserPresencePreservesStrictYAML(t *testing.T) {
	t.Parallel()
	for name, block := range map[string]string{
		"unknown-input":                "    unexpected: true\n",
		"unknown-multiline":            "    multiline: {line_start_pattern: '^BEGIN', unexpected: true}\n",
		"unknown-parser":               "    parser: {unexpected: true}\n",
		"unknown-merged-input":         "    <<: {unexpected: true}\n",
		"unknown-merged-multiline":     "    multiline: {line_start_pattern: '^BEGIN', <<: {unexpected: true}}\n",
		"unknown-merged-parser":        "    parser: {<<: {unexpected: true}}\n",
		"duplicate-parser":             "    parser: {}\n    parser: {}\n",
		"duplicate-aliased-parser-key": "    &parser-key parser: {}\n    *parser-key: {}\n",
		"duplicate-input":              "    source: one\n    source: two\n",
		"duplicate-multiline":          "    multiline: {line_start_pattern: '^BEGIN', max_lines: 2, max_lines: 3}\n",
		"duplicate-parser-option":      "    parser: {fields: {}, fields: {}}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeFile(t, t.TempDir(), "collector.yaml", parserPresenceYAML("logfmt", block))); err == nil {
				t.Fatal("custom input decoding bypassed strict YAML validation")
			}
		})
	}
}

func TestParserPresenceKeepsOmissionAndMergePrecedence(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		format string
		block  string
		parser bool
	}{
		"omitted-fixed":           {format: "ndjson"},
		"omitted-logfmt":          {format: "logfmt"},
		"empty-logfmt":            {format: "logfmt", block: "    parser: {}\n", parser: true},
		"mapping-logfmt":          {format: "logfmt", block: "    parser: {fields: {message: msg}}\n", parser: true},
		"java-pattern":            {format: "log4j2-pattern", block: "    parser: {pattern: '%{message}'}\n", parser: true},
		"logback-pattern":         {format: "logback-pattern", block: "    parser: {pattern: '%{message}'}\n", parser: true},
		"parser-alias":            {format: "logfmt", block: "    fields: &options {}\n    parser: *options\n", parser: true},
		"mapping-merge":           {format: "logfmt", block: "    <<: {parser: {fields: {message: msg}}}\n", parser: true},
		"explicit-wins-over-null": {format: "logfmt", block: "    <<: {parser: null}\n    parser: {}\n", parser: true},
		"first-merge-wins":        {format: "logfmt", block: "    <<: [{parser: {}}, {parser: null}]\n", parser: true},
		"valid-multiline":         {format: "logfmt", block: "    multiline: {line_start_pattern: '^BEGIN', flush_after: 1s}\n"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := Load(writeFile(t, t.TempDir(), "collector.yaml", parserPresenceYAML(tc.format, tc.block)))
			if err != nil {
				t.Fatalf("valid input rejected: %v", err)
			}
			if (cfg.Inputs[0].Parser != nil) != tc.parser {
				t.Fatal("parser presence was not preserved")
			}
		})
	}
	for _, format := range []string{"ndjson", "raw", "docker-json-file", "nginx-combined", "apache-common", "apache-combined", "log4j2-pattern", "logback-pattern"} {
		t.Run("empty-rejected/"+format, func(t *testing.T) {
			if _, err := Load(writeFile(t, t.TempDir(), "collector.yaml", parserPresenceYAML(format, "    parser: {}\n"))); err == nil {
				t.Fatal("accepted empty parser for format that requires omission or a pattern")
			}
		})
	}
}

func parserPresenceYAML(format, block string) string {
	return strings.Replace(validYAML, "    format: ndjson\n", "    format: "+format+"\n"+block, 1)
}

func TestParserPresenceProgrammaticRoundTrip(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"ndjson", "raw", "docker-json-file", "nginx-combined", "apache-common", "apache-combined", "logfmt"} {
		input := InputConfig{ID: "app", Include: []string{"/var/log/app/*.log"}, Format: format, Index: "main"}
		t.Run(format+"/input", func(t *testing.T) {
			encoded, err := yaml.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			var decoded InputConfig
			decoder := yaml.NewDecoder(bytes.NewReader(encoded))
			decoder.KnownFields(true)
			if err := decoder.Decode(&decoded); err != nil {
				t.Fatalf("programmatic input does not round-trip: %v", err)
			}
			if decoded.Parser != nil || decoded.ID != input.ID || decoded.Format != format || decoded.Index != input.Index {
				t.Fatal("round-trip changed input metadata or parser presence")
			}
		})
		t.Run(format+"/config", func(t *testing.T) {
			cfg := Config{Server: ServerConfig{Address: "127.0.0.1:8443", TokenFile: "./collector.token"}, State: StateConfig{Directory: "./state"}, Inputs: []InputConfig{input}}
			encoded, err := yaml.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := Load(writeFile(t, t.TempDir(), "collector.yaml", string(encoded)))
			if err != nil {
				t.Fatalf("programmatic config does not round-trip: %v", err)
			}
			if len(decoded.Inputs) != 1 || decoded.Inputs[0].Parser != nil || decoded.Inputs[0].Format != format {
				t.Fatal("round-trip changed config inputs or parser presence")
			}
		})
	}
}
