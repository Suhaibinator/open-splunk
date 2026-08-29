package spl

import (
	"errors"
	"slices"
	"testing"
)

func TestParseSortOfficialLimitsTypedFieldsAndTerminalDescending(t *testing.T) {
	t.Parallel()

	source := `* | sort LiMiT=12 + AuTo(host), -StR('Product Name'), +NuM(bytes), Ip(client_ip) DeSc`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command, ok := query.Commands[0].(*SortCommand)
	if !ok {
		t.Fatalf("command = %T, want *SortCommand", query.Commands[0])
	}
	if command.Limit != 12 || !command.LimitSpecified {
		t.Fatalf("limit = %d specified=%v, want 12 specified", command.Limit, command.LimitSpecified)
	}
	want := []struct {
		field      string
		quoted     bool
		descending bool
		mode       SortValueMode
		fieldText  string
		keyText    string
	}{
		{field: "host", descending: true, mode: SortValueModeAuto, fieldText: "host", keyText: "+ AuTo(host)"},
		{field: "Product Name", quoted: true, mode: SortValueModeString, fieldText: `'Product Name'`, keyText: `-StR('Product Name')`},
		{field: "bytes", descending: true, mode: SortValueModeNumber, fieldText: "bytes", keyText: "+NuM(bytes)"},
		{field: "client_ip", descending: true, mode: SortValueModeIP, fieldText: "client_ip", keyText: "Ip(client_ip)"},
	}
	if len(command.Fields) != len(want) {
		t.Fatalf("fields = %#v, want %d", command.Fields, len(want))
	}
	for index, expected := range want {
		got := command.Fields[index]
		if got.Field != expected.field || got.Quoted != expected.quoted ||
			got.Descending != expected.descending || got.Mode != expected.mode {
			t.Fatalf("field %d = %#v, want %#v", index, got, expected)
		}
		assertSourceRangeText(t, source, got.FieldRange, expected.fieldText)
		assertSourceRangeText(t, source, got.Range, expected.keyText)
	}
	assertSourceRangeText(t, source, command.Range, `sort LiMiT=12 + AuTo(host), -StR('Product Name'), +NuM(bytes), Ip(client_ip) DeSc`)
}

func TestParseSortLimitFormsAndExplicitUnlimited(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		limit  uint64
	}{
		{name: "bare", source: `* | sort 27 host`, limit: 27},
		{name: "option", source: `* | sort limit=27 host`, limit: 27},
		{name: "spaced option", source: `* | sort limit = 27 host`, limit: 27},
		{name: "bare unlimited", source: `* | sort 0 host`, limit: 0},
		{name: "option unlimited", source: `* | sort limit=0 host`, limit: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			query, err := Parse(test.source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			command := query.Commands[0].(*SortCommand)
			if !command.LimitSpecified || command.Limit != test.limit {
				t.Fatalf("sort = %#v, want explicit limit %d", command, test.limit)
			}
		})
	}
}

func TestParseSortQuotedFieldsWithAttachedAndSpacedDirections(t *testing.T) {
	t.Parallel()

	source := `* | sort -'Product Name', + 'request.path', 'plain alias'`
	query, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fields := query.Commands[0].(*SortCommand).Fields
	if len(fields) != 3 {
		t.Fatalf("fields = %#v", fields)
	}
	wantNames := []string{"Product Name", "request.path", "plain alias"}
	wantDescending := []bool{true, false, false}
	wantRanges := []string{`-'Product Name'`, `+ 'request.path'`, `'plain alias'`}
	for index := range fields {
		if fields[index].Field != wantNames[index] || !fields[index].Quoted ||
			fields[index].Descending != wantDescending[index] {
			t.Fatalf("field %d = %#v", index, fields[index])
		}
		assertSourceRangeText(t, source, fields[index].Range, wantRanges[index])
	}
}

func TestParseSortPreservesLegacyWhitespaceSeparatedKeys(t *testing.T) {
	t.Parallel()

	query, err := Parse(`* | sort +host -_time source`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fields := query.Commands[0].(*SortCommand).Fields
	if len(fields) != 3 || fields[0].Field != "host" || fields[0].Descending ||
		fields[1].Field != "_time" || !fields[1].Descending ||
		fields[2].Field != "source" || fields[2].Descending {
		t.Fatalf("fields = %#v", fields)
	}
}

func TestParseSortTerminalDescendingReversesEveryKey(t *testing.T) {
	t.Parallel()

	for _, suffix := range []string{"d", "D", "desc", "DeSc"} {
		query, err := Parse(`* | sort +host, -_time ` + suffix)
		if err != nil {
			t.Fatalf("Parse(%q): %v", suffix, err)
		}
		fields := query.Commands[0].(*SortCommand).Fields
		if len(fields) != 2 || !fields[0].Descending || fields[1].Descending {
			t.Fatalf("fields for %q = %#v, want reversed directions", suffix, fields)
		}
	}
}

func TestParseSortRejectsMalformedLimitsModesAndQuotedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, source, code, rangeText string
	}{
		{name: "negative limit", source: `* | sort limit=-1 host`, code: "SPL_INVALID_ARGUMENT", rangeText: "-1"},
		{name: "missing limit", source: `* | sort limit= | head 1`, code: "SPL_INVALID_ARGUMENT", rangeText: "limit"},
		{name: "overflowing limit", source: `* | sort limit=18446744073709551616 host`, code: "SPL_NUMBER_OUT_OF_RANGE", rangeText: "18446744073709551616"},
		{name: "empty wrapper", source: `* | sort num()`, code: "SPL_EXPECTED_FIELD", rangeText: ")"},
		{name: "multiple wrapper fields", source: `* | sort str(host,source)`, code: "SPL_EXPECTED_RIGHT_PAREN", rangeText: ","},
		{name: "missing wrapper close", source: `* | sort ip(host`, code: "SPL_EXPECTED_RIGHT_PAREN", rangeText: ""},
		{name: "unknown wrapper", source: `* | sort version(host)`, code: "SPL_UNSUPPORTED_SORT_SYNTAX", rangeText: "version"},
		{name: "nested wrapper", source: `* | sort auto(str(host))`, code: "SPL_EXPECTED_RIGHT_PAREN", rangeText: "("},
		{name: "invalid quoted escape", source: `* | sort str('bad\q')`, code: "SPL_INVALID_FIELD_QUOTE_ESCAPE", rangeText: `\q`},
		{name: "dangling typed direction", source: `* | sort - num()`, code: "SPL_EXPECTED_FIELD", rangeText: ")"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse(test.source)
			var diagnostic *Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != test.code {
				t.Fatalf("Parse error = %v, want %s", err, test.code)
			}
			assertSourceRangeText(t, test.source, diagnostic.Range, test.rangeText)
		})
	}
}

func TestParseFieldsExplicitIncludeModifier(t *testing.T) {
	t.Parallel()

	query, err := Parse(`* | fields + host, source`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	command := query.Commands[0].(*FieldsCommand)
	if command.Exclude || !slices.Equal(command.Fields, []string{"host", "source"}) {
		t.Fatalf("fields = %#v, want explicit inclusion", command)
	}
	assertSourceRangeText(t, `* | fields + host, source`, command.Range, `fields + host, source`)

	assertParseDiagnosticCode(t, `* | fields +`, "SPL_EXPECTED_FIELD")
}
