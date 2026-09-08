// Package parserconfig validates and compiles immutable collector parser options.
package parserconfig

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

const (
	maxPatternBytes = 16 << 10
	maxCaptures     = eventfields.MaximumStoredFieldsPerEvent
	maxNameBytes    = eventfields.MaximumDynamicPathSegmentBytes
	maxEventBytes   = 1 << 20
)

// Options contains source-format options. A nil pointer means no parser block.
type Options struct {
	Fields          map[string]string `yaml:"fields"`
	Pattern         string            `yaml:"pattern"`
	TimestampLayout string            `yaml:"timestamp_layout"`
	Timezone        string            `yaml:"timezone"`
}

// Compiled is immutable after construction and safe for concurrent decoding.
type Compiled struct {
	fields         map[string]string
	canonical      map[string]string
	parts          []patternPart
	clock          timeParser
	explicitLayout bool
}

// Capture retains a pattern field's exact bytes in a Go string.
type Capture struct{ Name, Value string }

// Compile validates all options without reading log sources or opening network connections.
func Compile(format string, options *Options) (*Compiled, error) {
	c := &Compiled{}
	switch format {
	case "ndjson", "raw", "docker-json-file", "nginx-combined", "apache-common", "apache-combined":
		if options != nil {
			return nil, errors.New("this format does not accept parser options")
		}
		return c, nil
	case "logfmt", "log4j2-pattern", "logback-pattern":
	default:
		return nil, errors.New("format must be ndjson, raw, docker-json-file, nginx-combined, apache-common, apache-combined, logfmt, log4j2-pattern, or logback-pattern")
	}
	var o Options
	if options != nil {
		o = *options
	}
	if len(o.TimestampLayout) > maxPatternBytes || len(o.Timezone) > maxNameBytes {
		return nil, errors.New("timestamp configuration exceeds limit")
	}
	c.explicitLayout = o.TimestampLayout != ""
	if format == "logfmt" {
		if o.Pattern != "" {
			return nil, errors.New("logfmt does not accept pattern")
		}
		if err := c.compileFields(o.Fields); err != nil {
			return nil, err
		}
	} else {
		if o.Fields != nil {
			return nil, errors.New("pattern inputs do not accept fields")
		}
		parts, hasTimestamp, err := compilePattern(o.Pattern)
		if err != nil {
			return nil, err
		}
		c.parts = parts
		if hasTimestamp && o.TimestampLayout == "" {
			return nil, errors.New("timestamp capture requires timestamp_layout")
		}
		if !hasTimestamp && (o.TimestampLayout != "" || o.Timezone != "") {
			return nil, errors.New("timestamp options require timestamp capture")
		}
	}
	clock, err := compileTime(o.TimestampLayout, o.Timezone)
	if err != nil {
		return nil, err
	}
	c.clock = clock
	return c, nil
}

func (c *Compiled) compileFields(fields map[string]string) error {
	c.fields = map[string]string{"timestamp": "timestamp", "message": "message", "level": "level", "trace_id": "trace_id", "span_id": "span_id"}
	for role, key := range fields {
		if !semantic(role) {
			return errors.New("unknown canonical field mapping")
		}
		if !validName(key) || (eventfields.IsCollectorReservedRoot(key) && !roleAlias(role, key)) {
			return errors.New("invalid source field mapping")
		}
		c.fields[role] = key
	}
	c.canonical = make(map[string]string, len(c.fields))
	for role, key := range c.fields {
		if c.canonical[key] != "" {
			return errors.New("canonical field mappings must have distinct source keys")
		}
		c.canonical[key] = role
	}
	return nil
}

// SourceField returns the configured source key for a canonical logfmt role.
func (c *Compiled) SourceField(canonical string) string { return c.fields[canonical] }

// CanonicalField returns the canonical logfmt role for a configured source key.
func (c *Compiled) CanonicalField(source string) string { return c.canonical[source] }

// HasTimestampLayout distinguishes explicit layouts from default RFC3339 parsing.
func (c *Compiled) HasTimestampLayout() bool { return c.explicitLayout }

func semantic(name string) bool {
	switch name {
	case "timestamp", "message", "level", "trace_id", "span_id":
		return true
	}
	return false
}

func validName(name string) bool {
	if len(name) == 0 || len(name) > maxNameBytes || !utf8.ValidString(name) {
		return false
	}
	for _, r := range name {
		if unicode.IsSpace(r) || unicode.IsControl(r) || strings.ContainsRune("=\"\\{}%", r) {
			return false
		}
	}
	return true
}

func invalidPattern(reason string) error { return fmt.Errorf("invalid parser pattern: %s", reason) }

func roleAlias(role, key string) bool {
	switch role {
	case "timestamp":
		return key == "timestamp" || key == "ts" || key == "time" || key == "@timestamp"
	case "message":
		return key == "message" || key == "msg"
	case "level":
		return key == "level" || key == "severity" || key == "severity_text"
	case "trace_id":
		return key == "trace_id" || key == "traceid"
	case "span_id":
		return key == "span_id" || key == "spanid"
	}
	return false
}
