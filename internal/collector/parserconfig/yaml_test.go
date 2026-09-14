package parserconfig

import (
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v3"
)

func compileYAML(format, source string) error {
	var options Options
	decoder := yaml.NewDecoder(strings.NewReader(source))
	decoder.KnownFields(true)
	if err := decoder.Decode(&options); err != nil {
		return err
	}
	_, err := Compile(format, &options)
	return err
}

func TestYAMLRejectsIrrelevantOptionPresence(t *testing.T) {
	for _, format := range []string{"ndjson", "raw", "docker-json-file", "nginx-combined", "apache-common", "apache-combined"} {
		for _, source := range []string{"{}", "pattern: ''", "fields: null", "timestamp_layout: ''", "timezone: null"} {
			if err := compileYAML(format, source); err == nil {
				t.Errorf("%s accepted %q", format, source)
			}
		}
	}
	for _, value := range []string{"''", "null", "'%{message}'"} {
		if err := compileYAML("logfmt", "pattern: "+value); err == nil {
			t.Errorf("logfmt accepted pattern %s", value)
		}
	}
	for _, format := range []string{"log4j2-pattern", "logback-pattern"} {
		for _, option := range []string{"fields: null", "fields: {}", "fields: {message: msg}", "timestamp_layout: ''", "timestamp_layout: null", "timezone: ''", "timezone: null"} {
			if err := compileYAML(format, "pattern: '%{message}'\n"+option); err == nil {
				t.Errorf("%s accepted %s", format, option)
			}
		}
	}
	for _, source := range []string{
		"timezone: ''", "timezone: null", "timestamp_layout: ''", "timestamp_layout: null",
		"timestamp_layout: '2006-01-02T15:04:05Z07:00'\ntimezone: ''",
		"timestamp_layout: '2006-01-02T15:04:05Z07:00'\ntimezone: null",
	} {
		if err := compileYAML("logfmt", source); err == nil {
			t.Errorf("logfmt accepted irrelevant/empty timestamp options %q", source)
		}
	}
}

func TestYAMLOptionsPreserveDefaultsAndValidValues(t *testing.T) {
	for _, source := range []string{
		"{}", "fields: {}", "fields: {message: msg}",
		"timestamp_layout: '2006-01-02T15:04:05Z07:00'",
		"timestamp_layout: '2006-01-02 15:04:05'\ntimezone: UTC",
		"&defaults {fields: {message: msg}}",
		"<<: &defaults {fields: {message: msg}}",
		"<<: [{fields: {message: msg}}, {timestamp_layout: '2006-01-02T15:04:05Z07:00'}]",
		"<<: {fields: {message: msg}}\nfields: {message: text}",
	} {
		if err := compileYAML("logfmt", source); err != nil {
			t.Errorf("rejected %q: %v", source, err)
		}
	}
	for _, format := range []string{"log4j2-pattern", "logback-pattern"} {
		for _, source := range []string{"pattern: '%{message}'", "pattern: '[%{timestamp}] %{message}'\ntimestamp_layout: '2006-01-02T15:04:05Z07:00'"} {
			if err := compileYAML(format, source); err != nil {
				t.Errorf("%s rejected %q: %v", format, source, err)
			}
		}
	}
}

func TestYAMLOptionsRejectUnknownDuplicateAndMergedInvalidKeys(t *testing.T) {
	for _, source := range []string{
		"unknown: x", "fields: {}\nfields: {}", "pattern: ''\npattern: ''",
		"fields: {message: msg, message: text}",
		"<<: {unknown: x}",
		"<<: [{fields: {}}, {unknown: x}]",
		"<<: &base {pattern: ''}",
		"<<: [{pattern: ''}, {fields: {}}]",
		"<<: {fields: {}}\npattern: ''",
		"<<: {fields: {}}\n<<: {timezone: UTC}",
		"&self {<<: *self}",
		"<<: 42", "<<: [null]", "fields: []", "pattern: []", "timezone: {}",
	} {
		if err := compileYAML("logfmt", source); err == nil {
			t.Errorf("accepted invalid YAML %q", source)
		}
	}
}

func TestYAMLOptionsAliasesPreservePresence(t *testing.T) {
	for _, source := range []string{
		"defaults: &defaults {pattern: ''}\nparser: *defaults",
		"defaults: &defaults {pattern: ''}\nparser: {<<: *defaults}",
		"defaults: &defaults {unknown: x}\nparser: {<<: *defaults}",
	} {
		var document struct {
			Defaults yaml.Node `yaml:"defaults"`
			Parser   Options   `yaml:"parser"`
		}
		err := yaml.Unmarshal([]byte(source), &document)
		if err == nil {
			_, err = Compile("logfmt", &document.Parser)
		}
		if err == nil {
			t.Errorf("accepted aliased invalid option %q", source)
		}
	}
}

func TestYAMLOptionsProgrammaticRoundTrip(t *testing.T) {
	for _, test := range []struct {
		format  string
		options Options
	}{
		{"logfmt", Options{}},
		{"logfmt", Options{Fields: map[string]string{"message": "msg"}}},
		{"logfmt", Options{TimestampLayout: "2006-01-02 15:04:05", Timezone: "UTC"}},
		{"log4j2-pattern", Options{Pattern: "%{message}"}},
		{"logback-pattern", Options{Pattern: "[%{timestamp}] %{message}", TimestampLayout: "2006-01-02T15:04:05Z07:00"}},
	} {
		encoded, err := yaml.Marshal(&test.options)
		if err != nil {
			t.Fatal(err)
		}
		if err := compileYAML(test.format, string(encoded)); err != nil {
			t.Errorf("%s failed round trip %q: %v", test.format, encoded, err)
		}
	}
}

func TestYAMLOptionsStrictFieldTypesAndMergePrecedence(t *testing.T) {
	for _, source := range []string{
		"fields: {message: null}", "fields: {message: 123}", "fields: {123: msg}",
		"fields: {<<: {message: msg, message: text}}",
		"<<: {unknown: x}\nfields: {}",
		"<<: [{fields: {message: msg}}, {unknown: x}]\nfields: {}",
		"!!merge wrong: {fields: {}}",
		"? &key fields\n: {}\n? *key\n: {}",
		"fields: {&key message: msg, *key: text}",
	} {
		if err := compileYAML("logfmt", source); err == nil {
			t.Errorf("accepted %q", source)
		}
	}
	var options Options
	if err := yaml.Unmarshal([]byte("fields: {<<: {message: msg}, message: text}"), &options); err != nil {
		t.Fatal(err)
	}
	compiled, err := Compile("logfmt", &options)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.SourceField("message") != "text" {
		t.Fatal("explicit field did not override merge")
	}
}
