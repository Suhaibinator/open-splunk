package spl

import (
	"slices"
	"strings"
	"testing"
)

func TestMatchStatsFieldGlob(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		pattern string
		name    string
		want    bool
	}{
		{"*", "", true},
		{"*", "latency", true},
		{"*lay", "delay", true},
		{"*lay", "xdelay", true},
		{"*lay", "layer", false},
		{"http_*_bytes", "http_request_bytes", true},
		{"http_*_bytes", "http__bytes", true},
		{"http_*_bytes", "http_bytes", false},
		{"a**b***c", "axbyc", true},
		{"a**b***c", "ac", false},
		{"μ*秒", "μlatency秒", true},
		{"μ*秒", "Μlatency秒", false},
		{"literal", "literal", false},
		{"", "", false},
	} {
		t.Run(test.pattern+"/"+test.name, func(t *testing.T) {
			t.Parallel()
			if got := MatchStatsFieldGlob(test.pattern, test.name); got != test.want {
				t.Fatalf("MatchStatsFieldGlob(%q, %q) = %v, want %v", test.pattern, test.name, got, test.want)
			}
		})
	}
}

func TestIsStatsFieldGlob(t *testing.T) {
	t.Parallel()

	for _, pattern := range []string{"*", "*lay", "http_*_bytes", "μ*秒", "a**b"} {
		if !IsStatsFieldGlob(pattern) {
			t.Errorf("IsStatsFieldGlob(%q) = false", pattern)
		}
	}
	for _, pattern := range []string{"", "literal", "bad pattern*", "'quoted*'", "__os_*"} {
		if IsStatsFieldGlob(pattern) {
			t.Errorf("IsStatsFieldGlob(%q) = true", pattern)
		}
	}
}

func TestCaptureAndSubstituteStatsFieldGlob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern  string
		name     string
		captures []string
	}{
		{"*", "latency", []string{"latency"}},
		{"http_*_bytes", "http_request_bytes", []string{"request"}},
		{"a*b*c", "axbybc", []string{"x", "yb"}},
		{"a**b", "axxb", []string{"", "xx"}},
		{"*a*", "banana", []string{"b", "nana"}},
	}
	for _, test := range tests {
		captures, ok := CaptureStatsFieldGlob(test.pattern, test.name)
		if !ok || !slices.Equal(captures, test.captures) {
			t.Errorf("CaptureStatsFieldGlob(%q, %q) = %#v/%v, want %#v", test.pattern, test.name, captures, ok, test.captures)
			continue
		}
		outputPattern := strings.Repeat("prefix_*_", len(captures))
		outputPattern = strings.TrimSuffix(outputPattern, "_")
		output, substituted := SubstituteStatsFieldGlob(outputPattern, captures)
		if !substituted || strings.Count(output, "*") != 0 {
			t.Errorf("SubstituteStatsFieldGlob(%q, %#v) = %q/%v", outputPattern, captures, output, substituted)
		}
	}
	if _, ok := SubstituteStatsFieldGlob("one_*", []string{"a", "b"}); ok {
		t.Fatal("star-count mismatch substituted")
	}
}

func TestMatchStatsFieldGlobAdversarialStarsStaysBounded(t *testing.T) {
	t.Parallel()

	pattern := "*a*a*a*a*a*a*a*a*a*a*z"
	name := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for range 10_000 {
		if MatchStatsFieldGlob(pattern, name) {
			t.Fatal("nonmatching adversarial pattern matched")
		}
	}
}

func TestStatsSparklineWildcardValidationRejectsForgedUnusedAlias(t *testing.T) {
	t.Parallel()

	query, err := Parse(`| stats sparkline(avg(*lay),5m) AS trend_*`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	aggregate := query.Commands[0].(*StatsCommand).Aggregates[0]
	if !ValidStatsSparklineFieldGlobAggregate(aggregate) {
		t.Fatal("parser-owned sparkline wildcard is invalid")
	}
	aggregate.Alias = "forged"
	if ValidStatsSparklineFieldGlobAggregate(aggregate) {
		t.Fatal("sparkline wildcard accepted forged unused alias metadata")
	}
}
