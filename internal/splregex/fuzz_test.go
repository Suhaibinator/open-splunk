package splregex

import (
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

// regexFuzzProbes are the subjects every accepted pattern is executed
// against. Go's regexp is the oracle: the normalized program handed to
// ClickHouse must behave exactly like the authored one under Go, so a
// normalization that changes meaning shows up as a submatch disagreement.
var regexFuzzProbes = []string{
	"",
	"a",
	"abc",
	"a\nb",
	"abc\n",
	"\n",
	"host=web-1 status=500 bytes=1024",
	"2026-07-21T00:00:00Z GET /index.html 200",
	"名前=値 ünïcödé",
	"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	"x\ty z",
}

// regexFuzzProbeLimit keeps the Go oracle bounded: RE2 execution is linear
// in the subject, but the program size for a pattern the bounded compiler
// rejected is not something this harness should pay to measure.
const regexFuzzProbeLimit = 512

func FuzzCompileExtractionPattern(f *testing.F) {
	for _, seed := range []string{
		`(?<host>\S+)`,
		`(?P<host>[a-z]+)-(?<n>\d+)`,
		`status=(?<status>\d{3}) bytes=(?<bytes>\d+)`,
		`^(?<line>.*)$`,
		`(?s)(?<all>.*)`,
		`(?<a>a)(?<b>b)(?<c>c)(?<d>d)(?<e>e)(?<f>f)(?<g>g)(?<h>h)(?<i>i)(?<j>j)(?<k>k)(?<l>l)(?<m>m)(?<n>n)(?<o>o)(?<p>p)`,
		`(?<a>a)(?<b>b)(?<c>c)(?<d>d)(?<e>e)(?<f>f)(?<g>g)(?<h>h)(?<i>i)(?<j>j)(?<k>k)(?<l>l)(?<m>m)(?<n>n)(?<o>o)(?<p>p)(?<q>q)`,
		`(?<dup>a)(?<dup>b)`,
		`(\d+)`,
		`(?<x>a{1000}){1000}`,
		`(?<x>(a{100}){100})`,
		`(?<x>[`,
		`(?<x>\p{Greek}+)`,
		`(?i)(?<word>[a-z]+)`,
		"(?<nul>\x00)",
		"\xff",
		``,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, pattern string) {
		first, firstErr := CompileExtractionPattern(pattern)
		second, secondErr := CompileExtractionPattern(pattern)
		regexFuzzCheckDeterministic(t, "CompileExtractionPattern", first, firstErr, second, secondErr)
		if firstErr != nil {
			if !reflect.DeepEqual(first, ExtractionPattern{}) {
				t.Fatalf("error published a partial pattern: %#v", first)
			}
			regexFuzzCheckSentinel(t, firstErr,
				ErrInvalidExtractionPattern,
				ErrNoNamedCapture,
				ErrDuplicateNamedCapture,
				ErrExtractionPatternTooLarge,
				ErrTooManyExtractionCaptures,
			)
			if got, want := IsExtractionComplexityError(firstErr),
				errors.Is(firstErr, ErrExtractionPatternTooLarge) || errors.Is(firstErr, ErrTooManyExtractionCaptures); got != want {
				t.Fatalf("IsExtractionComplexityError(%v) = %t, want %t", firstErr, got, want)
			}
			return
		}

		if !strings.HasPrefix(first.Pattern, "(?-s)") {
			t.Fatalf("normalized pattern %q does not restore ordinary dot behavior", first.Pattern)
		}
		if len(first.Pattern) > MaximumExtractionPatternBytes {
			t.Fatalf("normalized pattern has %d bytes, more than %d", len(first.Pattern), MaximumExtractionPatternBytes)
		}
		if first.ProgramWorkUnits < 1 || first.ProgramWorkUnits > MaximumExtractionProgramWorkUnits {
			t.Fatalf("program work units %d outside (0, %d]", first.ProgramWorkUnits, MaximumExtractionProgramWorkUnits)
		}
		if first.GroupCount < 1 || first.GroupCount > MaximumExtractionCaptureGroups {
			t.Fatalf("group count %d outside [1, %d]", first.GroupCount, MaximumExtractionCaptureGroups)
		}
		if len(first.Captures) == 0 || len(first.Captures) > first.GroupCount {
			t.Fatalf("captures %#v do not fit %d groups", first.Captures, first.GroupCount)
		}
		normalized, err := regexp.Compile(first.Pattern)
		if err != nil {
			t.Fatalf("normalized pattern %q is not a valid Go regexp: %v", first.Pattern, err)
		}
		if got := normalized.NumSubexp(); got != first.GroupCount {
			t.Fatalf("normalized pattern has %d groups, metadata says %d", got, first.GroupCount)
		}
		names := normalized.SubexpNames()
		previousGroup := 0
		seen := make(map[string]struct{}, len(first.Captures))
		for _, capture := range first.Captures {
			if capture.Name == "" || capture.Group <= previousGroup || capture.Group > first.GroupCount {
				t.Fatalf("capture %#v is unnamed or out of order after group %d of %d", capture, previousGroup, first.GroupCount)
			}
			if names[capture.Group] != capture.Name {
				t.Fatalf("capture %#v names group %d, Go names it %q", capture, capture.Group, names[capture.Group])
			}
			if _, duplicate := seen[capture.Name]; duplicate {
				t.Fatalf("capture name %q is repeated in %#v", capture.Name, first.Captures)
			}
			seen[capture.Name] = struct{}{}
			previousGroup = capture.Group
		}
		for group, name := range names {
			if group == 0 || name == "" {
				continue
			}
			if _, named := seen[name]; !named {
				t.Fatalf("Go sees named group %d %q that the captures %#v omit", group, name, first.Captures)
			}
		}

		// The program handed to ClickHouse is itself an acceptable authored
		// pattern with the same named outputs. Go's serializer is not a
		// textual fixed point (it may re-factor an alternation into a class
		// or a fold flag), so equivalence is checked by behavior below rather
		// than by comparing the normalized text.
		again, err := CompileExtractionPattern(first.Pattern)
		if err != nil {
			t.Fatalf("normalized pattern %q does not recompile: %v", first.Pattern, err)
		}
		if again.GroupCount != first.GroupCount || !reflect.DeepEqual(again.Captures, first.Captures) {
			t.Fatalf("re-normalization changed the outputs:\nfirst:  %#v\nsecond: %#v", first, again)
		}
		renormalized, err := regexp.Compile(again.Pattern)
		if err != nil {
			t.Fatalf("re-normalized pattern %q is not a valid Go regexp: %v", again.Pattern, err)
		}

		authored, err := regexp.Compile(pattern)
		if err != nil {
			t.Fatalf("accepted pattern %q is not a valid Go regexp: %v", pattern, err)
		}
		for _, probe := range regexFuzzProbes {
			want := authored.FindStringSubmatchIndex(probe)
			for label, candidate := range map[string]*regexp.Regexp{"normalized": normalized, "re-normalized": renormalized} {
				got := candidate.FindStringSubmatchIndex(probe)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s %q and authored %q disagree on %q: got %v want %v",
						label, candidate.String(), pattern, probe, got, want)
				}
			}
		}
	})
}

func FuzzCompileMatchPattern(f *testing.F) {
	for _, seed := range []string{
		`^web-\d+$`,
		`error$`,
		`(?m)^status=500$`,
		`(?s)begin.*end`,
		`a|b|c`,
		`[[:alpha:]]+`,
		`(a{1000}){1000}`,
		`(a{100}){100}`,
		`a{2,5}$`,
		`(?i)HOST`,
		`\bword\b`,
		`(`,
		"\x00",
		"\xff",
		``,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, pattern string) {
		first, firstErr := CompileMatchPattern(pattern)
		second, secondErr := CompileMatchPattern(pattern)
		regexFuzzCheckDeterministic(t, "CompileMatchPattern", first, firstErr, second, secondErr)
		if firstErr != nil {
			if first != (MatchPattern{}) {
				t.Fatalf("error published a partial pattern: %#v", first)
			}
			regexFuzzCheckSentinel(t, firstErr, ErrInvalidMatchPattern, ErrMatchPatternTooLarge)
			if got, want := IsMatchComplexityError(firstErr), errors.Is(firstErr, ErrMatchPatternTooLarge); got != want {
				t.Fatalf("IsMatchComplexityError(%v) = %t, want %t", firstErr, got, want)
			}
			return
		}

		if !strings.HasPrefix(first.Pattern, "(?-s)") {
			t.Fatalf("normalized pattern %q does not restore ordinary dot behavior", first.Pattern)
		}
		if len(first.Pattern) > MaximumMatchPatternBytes {
			t.Fatalf("normalized pattern has %d bytes, more than %d", len(first.Pattern), MaximumMatchPatternBytes)
		}
		if first.ProgramWorkUnits < 1 || first.ProgramWorkUnits > MaximumMatchProgramWorkUnits {
			t.Fatalf("program work units %d outside (0, %d]", first.ProgramWorkUnits, MaximumMatchProgramWorkUnits)
		}
		normalized, err := regexp.Compile(first.Pattern)
		if err != nil {
			t.Fatalf("normalized pattern %q is not a valid Go regexp: %v", first.Pattern, err)
		}
		// The normalized program is itself an acceptable authored pattern.
		// Go's serializer is not a textual fixed point, so equivalence is
		// checked by behavior rather than by comparing the normalized text.
		again, err := CompileMatchPattern(first.Pattern)
		if err != nil {
			t.Fatalf("normalized pattern %q does not recompile: %v", first.Pattern, err)
		}
		renormalized, err := regexp.Compile(again.Pattern)
		if err != nil {
			t.Fatalf("re-normalized pattern %q is not a valid Go regexp: %v", again.Pattern, err)
		}

		// The $ rewrite deliberately lets the normalized program also accept
		// one final newline, so equivalence is asserted on subjects that do
		// not end in one; on those the programs must agree exactly.
		authored, err := regexp.Compile(pattern)
		if err != nil {
			t.Fatalf("accepted pattern %q is not a valid Go regexp: %v", pattern, err)
		}
		for _, probe := range regexFuzzProbes {
			want := authored.MatchString(probe)
			for label, candidate := range map[string]*regexp.Regexp{"normalized": normalized, "re-normalized": renormalized} {
				got := candidate.MatchString(probe)
				if strings.HasSuffix(probe, "\n") {
					if want && !got {
						t.Fatalf("%s %q rejects %q that authored %q accepts", label, candidate.String(), probe, pattern)
					}
					continue
				}
				if got != want {
					t.Fatalf("%s %q and authored %q disagree on %q: got %t want %t",
						label, candidate.String(), pattern, probe, got, want)
				}
			}
		}
	})
}

func FuzzValidateReplacePattern(f *testing.F) {
	for _, seed := range []string{
		`\d+`,
		`[aeiou]`,
		`a*`,
		`(a|)`,
		`a?b?`,
		`^`,
		`\b`,
		`a{0,3}`,
		`(?:x+)+`,
		`a{1,}|b`,
		`(`,
		"\x00",
		``,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, pattern string) {
		firstErr := ValidateReplacePattern(pattern)
		secondErr := ValidateReplacePattern(pattern)
		if (firstErr == nil) != (secondErr == nil) ||
			(firstErr != nil && firstErr.Error() != secondErr.Error()) {
			t.Fatalf("ValidateReplacePattern changed across identical input: first=%v second=%v", firstErr, secondErr)
		}
		if firstErr != nil {
			if !errors.Is(firstErr, ErrMayMatchEmpty) &&
				!strings.HasPrefix(firstErr.Error(), "invalid RE2 regular expression: ") {
				t.Fatalf("unclassified replace validation error: %v", firstErr)
			}
			return
		}
		if len(pattern) > regexFuzzProbeLimit {
			return
		}
		// Acceptance is a promise that every match consumes input, which is
		// what makes SPL's global replacement and ClickHouse's agree.
		compiled, err := regexp.Compile(pattern)
		if err != nil {
			t.Fatalf("accepted pattern %q is not a valid Go regexp: %v", pattern, err)
		}
		if compiled.MatchString("") {
			t.Fatalf("accepted pattern %q matches the empty subject", pattern)
		}
		for _, probe := range regexFuzzProbes {
			for _, match := range compiled.FindAllStringIndex(probe, -1) {
				if match[1] <= match[0] {
					t.Fatalf("accepted pattern %q matched the empty substring at %d of %q", pattern, match[0], probe)
				}
			}
		}
	})
}

func regexFuzzCheckDeterministic[Result any](
	t *testing.T,
	name string,
	first Result,
	firstErr error,
	second Result,
	secondErr error,
) {
	t.Helper()
	if (firstErr == nil) != (secondErr == nil) ||
		(firstErr != nil && firstErr.Error() != secondErr.Error()) {
		t.Fatalf("%s error changed across identical input: first=%v second=%v", name, firstErr, secondErr)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("%s result changed across identical input:\nfirst:  %#v\nsecond: %#v", name, first, second)
	}
}

// regexFuzzCheckSentinel requires every failure to carry exactly one of the
// package's published sentinels, because callers classify diagnostics by
// errors.Is and an unclassified failure would fall through to a generic code.
func regexFuzzCheckSentinel(t *testing.T, err error, sentinels ...error) {
	t.Helper()
	matched := 0
	for _, sentinel := range sentinels {
		if errors.Is(err, sentinel) {
			matched++
		}
	}
	if matched != 1 {
		t.Fatalf("error %v matches %d published sentinels, want exactly one", err, matched)
	}
}
