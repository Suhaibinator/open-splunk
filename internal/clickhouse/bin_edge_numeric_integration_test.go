package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

// This file probes the numeric-string and floating edges of the runtime
// Dynamic bin path.
//
// The padded cases are regressions for a ClickHouse text-to-double edge that
// once fabricated finite buckets after enough leading zeros. The compiler now
// canonicalizes a numeric spelling before choosing its exact-integer or
// bounded-Float64 arm, so padding and an explicit plus sign cannot change the
// bucket or make the text diverge from its numeric twin.
//
// binEdgeNumericCase is one numeric-text spelling of the Dynamic bin path and,
// when the spelling names a value ingestion could also have typed as a number,
// the identical value stored as a real number. The documented contract makes
// the two converge, so every case checks both.
type binEdgeNumericCase struct {
	name  string
	field string
	span  string
	// text is the String value stored on the text event.
	text string
	// number is the identical value stored as a real number, or nil when the
	// spelling has no numeric twin (NaN spellings, overflowing exponents, ...).
	number *opensplunkv1.TypedValue

	wantType  string
	wantValue string
	// wantTwinType and wantTwinValue override the text expectation for the
	// numeric twin. They are only set where the contract documents that the
	// two spellings legitimately diverge.
	wantTwinType  string
	wantTwinValue string
}

const (
	binEdgeNumericTextEvent   = "bin-edge-text"
	binEdgeNumericNumberEvent = "bin-edge-number"
	binEdgeNumericDoubleEvent = "bin-edge-double"
	binEdgeNumericDecimalBase = "bin-edge-decimal-base"
	binEdgeNumericDecimalLow  = "bin-edge-decimal-low"
	binEdgeNumericDecimalHigh = "bin-edge-decimal-high"
)

func binEdgeNumericCases() []binEdgeNumericCase {
	return []binEdgeNumericCase{
		// --- exact integer text ------------------------------------------
		{
			name: "explicit plus sign", field: "plus_sign", span: "10",
			text: "+21", number: typedSint(21),
			wantType: "Int64", wantValue: "20",
		},
		{
			name: "negative zero", field: "minus_zero", span: "10",
			text: "-0", number: typedSint(0),
			wantType: "Int64", wantValue: "0",
		},
		{
			name: "explicit plus zero", field: "plus_zero", span: "10",
			text: "+0", number: typedSint(0),
			wantType: "Int64", wantValue: "0",
		},
		{
			// Float64 negative zero is normalized on both sides.
			name: "negative zero float text", field: "minus_zero_float", span: "10",
			text: "-0.0", number: typedDouble(math.Copysign(0, -1)),
			wantType: "Float64", wantValue: "0",
		},
		{
			name: "leading zeros inside the width guard", field: "pad_short", span: "10",
			text: "000000000000000021", number: typedSint(21),
			wantType: "Int64", wantValue: "20",
		},
		{
			// A 21-byte spelling still canonicalizes to the ordinary integer 21.
			name: "leading zeros at the former raw-width boundary", field: "pad_21", span: "10",
			text: "000000000000000000021", number: typedSint(21),
			wantType: "Int64", wantValue: "20",
		},
		{
			// Regression: padding beyond the former raw-width guard must not
			// change the exact bucket or its signed result type.
			name: "leading zeros past the former raw-width boundary", field: "pad_22", span: "10",
			text: "0000000000000000000021", number: typedSint(21),
			wantType: "Int64", wantValue: "20",
		},
		{
			// Regression: canonicalization must happen before the bounded
			// Float64 conversion; 21.5 is exactly representable here.
			name: "padded fractional text", field: "pad_fraction", span: "10",
			text: "000000000000000000021.5", number: typedDouble(21.5),
			wantType: "Float64", wantValue: "20",
		},
		{
			// Regression: padding must not move this wide integer onto the
			// rounded Float64 path (compare the next case).
			name: "padded wide integer text", field: "pad_wide", span: "100",
			text: "0000009007199254740999", number: typedSint(9_007_199_254_740_999),
			wantType: "Int64", wantValue: "9007199254740900",
		},
		{
			// Exact widened arithmetic and Float64 rounding disagree here:
			// Float64 would round the input to 9007199254741000 and answer
			// 9007199254741000 instead of 9007199254740900.
			name: "wide integer text is exact, not rounded", field: "wide_exact", span: "100",
			text: "9007199254740999", number: typedSint(9_007_199_254_740_999),
			wantType: "Int64", wantValue: "9007199254740900",
		},
		{
			name: "wide integer text with a small span", field: "wide_exact", span: "10",
			text: "9007199254740999", number: typedSint(9_007_199_254_740_999),
			wantType: "Int64", wantValue: "9007199254740990",
		},
		{
			name: "two to the fifty-third minus one", field: "two53_minus", span: "2",
			text: "9007199254740991", number: typedSint(9_007_199_254_740_991),
			wantType: "Int64", wantValue: "9007199254740990",
		},
		{
			name: "two to the fifty-third", field: "two53", span: "2",
			text: "9007199254740992", number: typedSint(9_007_199_254_740_992),
			wantType: "Int64", wantValue: "9007199254740992",
		},
		{
			name: "two to the fifty-third plus one", field: "two53_plus", span: "2",
			text: "9007199254740993", number: typedSint(9_007_199_254_740_993),
			wantType: "Int64", wantValue: "9007199254740992",
		},
		// --- Int64/UInt64 promotion --------------------------------------
		{
			name: "signed maximum", field: "int64_max", span: "10",
			text: "9223372036854775807", number: typedSint(9_223_372_036_854_775_807),
			wantType: "Int64", wantValue: "9223372036854775800",
		},
		{
			name: "just above the signed maximum", field: "above_int64", span: "10",
			text: "9223372036854775810", number: typedUint(9_223_372_036_854_775_810),
			wantType: "UInt64", wantValue: "9223372036854775810",
		},
		{
			name: "unsigned maximum", field: "uint64_max", span: "10",
			text: "18446744073709551615", number: typedUint(^uint64(0)),
			wantType: "UInt64", wantValue: "18446744073709551610",
		},
		{
			// 21 bytes with the sign, the widest spelling the guard admits.
			name: "signed unsigned maximum", field: "uint64_max_plus", span: "10",
			text: "+18446744073709551615", number: typedUint(^uint64(0)),
			wantType: "UInt64", wantValue: "18446744073709551610",
		},
		{
			// Regression: a pad zero must not prevent convergence with the
			// UInt64 numeric twin.
			name: "padded unsigned maximum", field: "uint64_max_pad", span: "10",
			text: "+018446744073709551615", number: typedUint(^uint64(0)),
			wantType: "UInt64", wantValue: "18446744073709551610",
		},
		{
			// The mathematical floor lies below Int64 but remains inside the
			// bounded signed-Int256 contract.
			name: "integer text whose bucket is below Int64", field: "int64_min", span: "10",
			text:     "-9223372036854775808",
			wantType: "Decimal", wantValue: "-9223372036854775810",
		},
		{
			name: "integer text above the unsigned maximum", field: "above_uint64", span: "10",
			text:     "999999999999999999999",
			wantType: "Decimal", wantValue: "999999999999999999990",
		},
		{
			name: "signed Int256 maximum", field: "int256_max", span: "1",
			text:     exactNumericBinMaxInt256,
			wantType: "Decimal", wantValue: exactNumericBinMaxInt256,
		},
		{
			name: "signed Int256 minimum", field: "int256_min", span: "1",
			text:     "-" + exactNumericBinMinMagnitude,
			wantType: "Decimal", wantValue: "-" + exactNumericBinMinMagnitude,
		},
		{
			// The input is one above MaxInt256, but its span-ten boundary is
			// representable. Range validation belongs on the boundary.
			name: "above Int256 buckets back into range", field: "int256_max_plus_one", span: "10",
			text:      exactNumericBinMinMagnitude,
			wantType:  "Decimal",
			wantValue: "57896044618658097711785492504343953926634992332820282019728792003956564819960",
		},
		{
			name: "minimum with an unrepresentable floor", field: "int256_min_span_ten", span: "10",
			text:      "-" + exactNumericBinMinMagnitude,
			wantType:  "String",
			wantValue: "-" + exactNumericBinMinMagnitude,
		},
		{
			name: "below minimum with an unrepresentable floor", field: "int256_min_minus_one", span: "10",
			text:      "-57896044618658097711785492504343953926634992332820282019728792003956564819969",
			wantType:  "String",
			wantValue: "-57896044618658097711785492504343953926634992332820282019728792003956564819969",
		},
		{
			name: "seventy eight significant digits pass through", field: "coefficient_78", span: "10",
			text:      strings.Repeat("9", 78),
			wantType:  "String",
			wantValue: strings.Repeat("9", 78),
		},
		// --- fractional and exponent text ---------------------------------
		{
			name: "negative fractional text floors mathematically", field: "neg_frac", span: "10",
			text: "-21.5", number: typedDouble(-21.5),
			wantType: "Float64", wantValue: "-30",
		},
		{
			name: "negative fractional text with span one", field: "neg_frac_one", span: "1",
			text: "-0.5", number: typedDouble(-0.5),
			wantType: "Float64", wantValue: "-1",
		},
		{
			name: "fractional text with span one", field: "frac_one", span: "1",
			text: "21.7", number: typedDouble(21.7),
			wantType: "Float64", wantValue: "21",
		},
		{
			name: "uppercase exponent", field: "exp_upper", span: "10",
			text: "1E3", number: typedDouble(1000),
			wantType: "Float64", wantValue: "1000",
		},
		{
			name: "signed exponent", field: "exp_signed", span: "10",
			text: "+1.5e+1", number: typedDouble(15),
			wantType: "Float64", wantValue: "10",
		},
		{
			name: "negative exponent", field: "exp_negative", span: "1",
			text: "1e-2", number: typedDouble(0.01),
			wantType: "Float64", wantValue: "0",
		},
		{
			name: "trailing decimal point", field: "trailing_dot", span: "10",
			text: "5.", number: typedDouble(5),
			wantType: "Float64", wantValue: "0",
		},
		{
			name: "leading decimal point", field: "leading_dot", span: "1",
			text: ".5", number: typedDouble(0.5),
			wantType: "Float64", wantValue: "0",
		},
		{
			// Magnitude exactly at the fence is still bucketed.
			name: "fractional text at the exact fence", field: "fence_at", span: "2",
			text: "9007199254740992.0", number: typedDouble(9007199254740992),
			wantType: "Float64", wantValue: "9007199254740992",
		},
		{
			// Above the Float64 fence the lexical exact path emits Decimal,
			// while the genuinely stored double retains Float64.
			name: "fractional text above the exact fence", field: "fence_above", span: "2",
			text: "9007199254740994.0", number: typedDouble(9007199254740994),
			wantType: "Decimal", wantValue: "9007199254740994",
			wantTwinType: "Float64", wantTwinValue: "9007199254740994",
		},
		{
			// The result fits exactly in Float64 even though the source
			// fraction does not. Lexical bucketing avoids rounding the input up
			// to 2^53 before applying floor.
			name: "fractional text below the fence avoids input rounding", field: "fence_round", span: "1",
			text:     "9007199254740991.5",
			wantType: "Float64", wantValue: "9007199254740991",
		},
		{
			name: "numeric text just below the byte ceiling", field: "text_bytes_4095", span: "1",
			text:     "1." + strings.Repeat("0", MaximumExactNumericBinTextBytes-3),
			wantType: "Float64", wantValue: "1",
		},
		{
			name: "numeric text at the byte ceiling", field: "text_bytes_4096", span: "1",
			text:     "1." + strings.Repeat("0", MaximumExactNumericBinTextBytes-2),
			wantType: "Float64", wantValue: "1",
		},
		{
			name: "numeric text above the byte ceiling passes through", field: "text_bytes_4097", span: "1",
			text:      "1." + strings.Repeat("0", MaximumExactNumericBinTextBytes-1),
			wantType:  "String",
			wantValue: "1." + strings.Repeat("0", MaximumExactNumericBinTextBytes-1),
		},
		// --- spans --------------------------------------------------------
		{
			name: "maximum span over positive text", field: "span_max_pos", span: "9007199254740991",
			text: "9007199254740991", number: typedSint(9_007_199_254_740_991),
			wantType: "Int64", wantValue: "9007199254740991",
		},
		{
			name: "maximum span over negative text", field: "span_max_neg", span: "9007199254740991",
			text: "-1", number: typedSint(-1),
			wantType: "Int64", wantValue: "-9007199254740991",
		},
		{
			name: "maximum span over fractional text", field: "span_max_frac", span: "9007199254740991",
			text: "-1.5", number: typedDouble(-1.5),
			wantType: "Float64", wantValue: "-9007199254740991",
		},
		{
			name: "span one over integer text", field: "span_one", span: "1",
			text: "-7", number: typedSint(-7),
			wantType: "Int64", wantValue: "-7",
		},
		// --- text that must pass through unharmed --------------------------
		{
			name: "overflowing exponent", field: "big_exp", span: "10",
			text: "1e9999", wantType: "String", wantValue: "1e9999",
		},
		{
			name: "negative overflowing exponent", field: "big_exp_neg", span: "10",
			text: "-1e9999", wantType: "String", wantValue: "-1e9999",
		},
		{
			name: "underflowing exponent", field: "small_exp", span: "10",
			text: "1e-9999", wantType: "Float64", wantValue: "0",
		},
		{
			name: "NaN spelling", field: "nan_upper", span: "10",
			text: "NaN", wantType: "String", wantValue: "NaN",
		},
		{
			name: "lowercase nan spelling", field: "nan_lower", span: "10",
			text: "nan", wantType: "String", wantValue: "nan",
		},
		{
			name: "Infinity spelling", field: "inf_word", span: "10",
			text: "Infinity", wantType: "String", wantValue: "Infinity",
		},
		{
			name: "negative inf spelling", field: "inf_short", span: "10",
			text: "-inf", wantType: "String", wantValue: "-inf",
		},
		{
			name: "leading whitespace", field: "ws_lead", span: "10",
			text: " 21", wantType: "String", wantValue: " 21",
		},
		{
			name: "trailing whitespace", field: "ws_trail", span: "10",
			text: "21 ", wantType: "String", wantValue: "21 ",
		},
		{
			name: "leading tab", field: "ws_tab", span: "10",
			text: "\t21", wantType: "String", wantValue: "\t21",
		},
		{
			name: "unit suffix", field: "unit_suffix", span: "10",
			text: "21ms", wantType: "String", wantValue: "21ms",
		},
		{
			name: "hexadecimal spelling", field: "hex_value", span: "10",
			text: "0x15", wantType: "String", wantValue: "0x15",
		},
		{
			name: "underscore separators", field: "underscored", span: "10",
			text: "1_000", wantType: "String", wantValue: "1_000",
		},
		{
			name: "thousands separators", field: "comma_value", span: "10",
			text: "1,000", wantType: "String", wantValue: "1,000",
		},
		{
			name: "empty text", field: "empty_text", span: "10",
			text: "", wantType: "String", wantValue: "",
		},
		{
			name: "sign only", field: "sign_only", span: "10",
			text: "-", wantType: "String", wantValue: "-",
		},
		{
			name: "decimal point only", field: "dot_only", span: "10",
			text: ".", wantType: "String", wantValue: ".",
		},
		{
			name: "fullwidth digits", field: "fullwidth", span: "10",
			text: "２１", wantType: "String", wantValue: "２１",
		},
	}
}

// TestBinEdgeNumericDynamicStringsAgainstClickHouse probes the numeric-string
// and floating edges of the runtime Dynamic bin path against the pinned
// ClickHouse image. It is opt-in because it starts its own container.
func TestBinEdgeNumericDynamicStringsAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	store, connection := binEdgeNumericStore(t, ctx)
	indexTime := time.Date(2026, 7, 21, 3, 4, 6, 987654321, time.UTC)
	cases := binEdgeNumericCases()

	textFields := make([]*opensplunkv1.TypedObjectField, 0, len(cases)+2)
	numberFields := make([]*opensplunkv1.TypedObjectField, 0, len(cases)+2)
	seenText := make(map[string]struct{}, len(cases))
	seenNumber := make(map[string]struct{}, len(cases))
	for _, testCase := range cases {
		if _, seen := seenText[testCase.field]; !seen {
			seenText[testCase.field] = struct{}{}
			textFields = append(textFields, typedField(testCase.field, typedString(testCase.text)))
		}
		if testCase.number == nil {
			continue
		}
		if _, seen := seenNumber[testCase.field]; !seen {
			seenNumber[testCase.field] = struct{}{}
			numberFields = append(numberFields, typedField(testCase.field, testCase.number))
		}
	}
	// converge carries the same value spelled as text, as a signed integer, and
	// as a double so the documented convergence can be observed under stats.
	textFields = append(textFields, typedField("converge", typedString("25")))
	numberFields = append(numberFields, typedField("converge", typedSint(25)))

	textEvent := binEdgeNumericEvent(binEdgeNumericTextEvent, textFields...)
	numberEvent := binEdgeNumericEvent(binEdgeNumericNumberEvent, numberFields...)
	doubleEvent := binEdgeNumericEvent(binEdgeNumericDoubleEvent, typedField("converge", typedDouble(25)))
	decimalBaseEvent := binEdgeNumericEvent(binEdgeNumericDecimalBase,
		typedField("decimal_basic", typedDecimal("123.4500")),
		typedField("decimal_negative", typedDecimal("-21.5")),
		typedField("decimal_max", typedDecimal(exactNumericBinMaxInt256)),
		typedField("decimal_min", typedDecimal("-"+exactNumericBinMinMagnitude)),
		typedField("decimal_max_plus_one", typedDecimal(exactNumericBinMinMagnitude)),
		typedField("decimal_min_minus_one", typedDecimal(
			"-57896044618658097711785492504343953926634992332820282019728792003956564819969",
		)),
		typedField("decimal_78", typedDecimal(strings.Repeat("9", 78))),
		typedField("decimal_bytes_4095", typedDecimal(
			"1."+strings.Repeat("0", MaximumExactNumericBinTextBytes-3),
		)),
		typedField("decimal_bytes_4096", typedDecimal(
			"1."+strings.Repeat("0", MaximumExactNumericBinTextBytes-2),
		)),
		typedField("decimal_bytes_4097", typedDecimal(
			"1."+strings.Repeat("0", MaximumExactNumericBinTextBytes-1),
		)),
	)
	decimalLowEvent := binEdgeNumericEvent(binEdgeNumericDecimalLow,
		typedField("decimal_basic", typedDecimal("129.99")),
		typedField("decimal_adjacent", typedDecimal("9007199254740992")),
		// Keep the exponent spelling in its original envelope. A later bin
		// preserves this destination on the row where its source is missing,
		// so comparisons and sorting must recognize that it is the exact
		// integer 2^53 rather than round it together with 2^53+1.
		typedField("mixed_destination", typedDecimal("9007199254740992e0")),
	)
	decimalHighEvent := binEdgeNumericEvent(binEdgeNumericDecimalHigh,
		typedField("decimal_adjacent", typedDecimal("9007199254740993")),
		typedField("mixed_source", typedDecimal("9007199254740993")),
	)
	allEvents := []*ingest.StoredEvent{
		textEvent,
		numberEvent,
		doubleEvent,
		decimalBaseEvent,
		decimalLowEvent,
		decimalHighEvent,
	}
	for _, event := range allEvents {
		event.IndexTime = indexTime
		event.BatchID = "bin-edge-numeric-batch"
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "bin-edge-numeric-batch", BatchSequence: 1,
		SourceBatchSHA256: testSourceBatchDigest("bin-edge-numeric-batch"),
		ReceivedAt:        indexTime,
		Events:            allEvents,
	}); err != nil {
		t.Fatalf("store bin edge fixtures: %v", err)
	}
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture bin edge visibility cutoff: %v", err)
	}
	cutoff := indexTime.Add(10 * time.Second)

	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			gotType, gotValue := binEdgeNumericBucket(
				t, ctx, connection, cutoff, visibilityCutoff,
				binEdgeNumericTextEvent, testCase.field, testCase.span,
			)
			if gotType != testCase.wantType || gotValue != testCase.wantValue {
				t.Errorf(
					"bin %s span=%s over text %q = %s/%q, want %s/%q",
					testCase.field, testCase.span, testCase.text,
					gotType, gotValue, testCase.wantType, testCase.wantValue,
				)
			}
			if testCase.number == nil {
				return
			}
			wantTwinType, wantTwinValue := testCase.wantType, testCase.wantValue
			if testCase.wantTwinType != "" {
				wantTwinType, wantTwinValue = testCase.wantTwinType, testCase.wantTwinValue
			}
			twinType, twinValue := binEdgeNumericBucket(
				t, ctx, connection, cutoff, visibilityCutoff,
				binEdgeNumericNumberEvent, testCase.field, testCase.span,
			)
			if twinType != wantTwinType || twinValue != wantTwinValue {
				t.Errorf(
					"bin %s span=%s over the stored number = %s/%q, want %s/%q",
					testCase.field, testCase.span, twinType, twinValue, wantTwinType, wantTwinValue,
				)
			}
		})
	}

	t.Run("wide integer text publishes Decimal metadata", func(t *testing.T) {
		source := `index=compiler event_id=` + binEdgeNumericTextEvent +
			` | bin above_uint64 span=10 AS band`
		logical := buildIntegrationPlan(t, source, cutoff, visibilityCutoff)
		profile := binEdgeMetadataCatalogProfile(t, ctx, connection, logical, "band")
		wantProfile := binEdgeMetadataProfile{
			rows: 1, total: 1, events: 1, nulls: 0, missing: 0,
			types: []uint8{uint8(eventfields.StoredValueTypeDecimal)},
		}
		if !reflect.DeepEqual(profile, wantProfile) {
			t.Fatalf("wide integer catalog profile = %#v, want %#v", profile, wantProfile)
		}

		invalid, unsupported, encoded := binEdgeMetadataSummaryValues(
			t,
			ctx,
			connection,
			logical,
			"band",
		)
		wantEncoded := []string{
			fmt.Sprintf("%d:999999999999999999990:1", uint8(eventfields.StoredValueTypeDecimal)),
		}
		if invalid != 0 || unsupported != 0 || !reflect.DeepEqual(encoded, wantEncoded) {
			t.Fatalf(
				"wide integer summary = invalid:%d unsupported:%d values:%#v, want %#v",
				invalid,
				unsupported,
				encoded,
				wantEncoded,
			)
		}
	})

	t.Run("tagged decimals bucket exactly and retain Decimal representation", func(t *testing.T) {
		for _, testCase := range []struct {
			name, eventID, field, span, want string
		}{
			{
				name:    "positive fraction",
				eventID: binEdgeNumericDecimalBase, field: "decimal_basic", span: "10", want: "120",
			},
			{
				name:    "negative floor",
				eventID: binEdgeNumericDecimalBase, field: "decimal_negative", span: "10", want: "-30",
			},
		} {
			testCase := testCase
			t.Run(testCase.name, func(t *testing.T) {
				gotType, gotValue := binEdgeNumericBucket(
					t, ctx, connection, cutoff, visibilityCutoff,
					testCase.eventID, testCase.field, testCase.span,
				)
				if gotType != "Decimal" || gotValue != testCase.want {
					t.Fatalf(
						"bin %s span=%s = %s/%q, want Decimal %q",
						testCase.field, testCase.span, gotType, gotValue, testCase.want,
					)
				}
			})
		}
	})

	t.Run("signed Int256 and lexical ceilings are enforced on declared Decimals", func(t *testing.T) {
		for _, testCase := range []struct {
			name, field, span, want string
		}{
			{
				name: "maximum", field: "decimal_max", span: "1",
				want: exactNumericBinMaxInt256,
			},
			{
				name: "minimum", field: "decimal_min", span: "1",
				want: "-" + exactNumericBinMinMagnitude,
			},
			{
				name: "above maximum buckets into range", field: "decimal_max_plus_one", span: "10",
				want: "57896044618658097711785492504343953926634992332820282019728792003956564819960",
			},
			{name: "4095 bytes", field: "decimal_bytes_4095", span: "1", want: "1"},
			{name: "4096 bytes", field: "decimal_bytes_4096", span: "1", want: "1"},
		} {
			testCase := testCase
			t.Run(testCase.name, func(t *testing.T) {
				compiled := compileIntegrationSPL(
					t,
					`index=compiler event_id=`+binEdgeNumericDecimalBase+
						` | bin `+testCase.field+` span=`+testCase.span+` AS band | table band`,
					cutoff,
					visibilityCutoff,
				)
				var physicalType, value string
				if err := connection.QueryRow(ctx,
					`SELECT dynamicType(band), toString(band) FROM (`+compiled.SQL+`)`,
					compiled.Args...,
				).Scan(&physicalType, &value); err != nil {
					t.Fatalf("execute declared Decimal boundary: %v\nSQL: %s", err, compiled.SQL)
				}
				if physicalType != "Int256" || value != testCase.want {
					t.Fatalf("declared Decimal boundary = %s/%q, want Int256/%q",
						physicalType, value, testCase.want)
				}
			})
		}

		t.Run("driver transports Dynamic Int256 through its typed wrapper", func(t *testing.T) {
			compiled := compileIntegrationSPL(
				t,
				`index=compiler event_id=`+binEdgeNumericDecimalBase+
					` | bin decimal_max span=1 AS band | table band`,
				cutoff,
				visibilityCutoff,
			)
			var scanned chcol.Dynamic
			if err := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(&scanned); err != nil {
				t.Fatalf("scan Dynamic(Int256) through clickhouse-go: %v\nSQL: %s", err, compiled.SQL)
			}
			var got string
			switch value := scanned.Any().(type) {
			case big.Int:
				got = value.String()
			case *big.Int:
				if value != nil {
					got = value.String()
				}
			default:
				t.Fatalf("Dynamic(Int256) driver value = %T, want big.Int or *big.Int", scanned.Any())
			}
			if got != exactNumericBinMaxInt256 {
				t.Fatalf("Dynamic(Int256) driver value = %q, want %q", got, exactNumericBinMaxInt256)
			}
		})

		for _, testCase := range []struct {
			name, field string
		}{
			{name: "minimum floors out of range", field: "decimal_min"},
			{name: "below minimum floors out of range", field: "decimal_min_minus_one"},
			{name: "78 significant digits", field: "decimal_78"},
			{name: "4097 bytes", field: "decimal_bytes_4097"},
		} {
			testCase := testCase
			t.Run(testCase.name, func(t *testing.T) {
				compiled := compileIntegrationSPL(
					t,
					`index=compiler event_id=`+binEdgeNumericDecimalBase+
						` | bin `+testCase.field+` span=10 AS band | table band`,
					cutoff,
					visibilityCutoff,
				)
				binEdgeMetadataRequireMarker(t, ctx, connection, compiled, testCase.name)
			})
		}
	})

	t.Run("published exact Decimals can be binned again", func(t *testing.T) {
		compiled := compileIntegrationSPL(
			t,
			`index=compiler event_id=`+binEdgeNumericDecimalBase+
				` | bin decimal_basic span=10 AS band | bin band span=7 | table band`,
			cutoff,
			visibilityCutoff,
		)
		queryContext, cancelQuery := binEdgeNumericBoundedAnalyzerContext(ctx)
		defer cancelQuery()
		var gotType, gotValue string
		if err := connection.QueryRow(queryContext,
			`SELECT dynamicType(band), toString(band) FROM (`+compiled.SQL+`)`,
			compiled.Args...,
		).Scan(&gotType, &gotValue); err != nil {
			t.Fatalf("execute chained exact Decimal bins: %v\nSQL: %s", err, compiled.SQL)
		}
		if gotType != "Int256" || gotValue != "119" {
			t.Fatalf("chained exact Decimal bin = %s/%q, want Int256/119", gotType, gotValue)
		}
	})

	t.Run("consecutive bins retry recoverable numeric text", func(t *testing.T) {
		for _, testCase := range []struct {
			name, field, firstSpan, secondSpan, wantType, wantValue string
		}{
			{
				name:       "positive text first outside then inside Int256",
				field:      "int256_max_plus_one",
				firstSpan:  "1",
				secondSpan: "10",
				wantType:   "Int256",
				wantValue:  "57896044618658097711785492504343953926634992332820282019728792003956564819960",
			},
			{
				name:       "nonnumeric text remains text",
				field:      "nan_upper",
				firstSpan:  "1",
				secondSpan: "10",
				wantType:   "String",
				wantValue:  "NaN",
			},
		} {
			testCase := testCase
			t.Run(testCase.name, func(t *testing.T) {
				compiled := compileIntegrationSPL(
					t,
					`index=compiler event_id=`+binEdgeNumericTextEvent+
						` | bin `+testCase.field+` span=`+testCase.firstSpan+
						` AS band | bin band span=`+testCase.secondSpan+` | table band`,
					cutoff,
					visibilityCutoff,
				)
				queryContext, cancelQuery := binEdgeNumericBoundedAnalyzerContext(ctx)
				defer cancelQuery()
				var gotType, gotValue string
				if err := connection.QueryRow(
					queryContext,
					`SELECT dynamicType(band), toString(band) FROM (`+compiled.SQL+`)`,
					compiled.Args...,
				).Scan(&gotType, &gotValue); err != nil {
					t.Fatalf("execute consecutive numeric-text bins: %v\nSQL: %s", err, compiled.SQL)
				}
				if gotType != testCase.wantType || gotValue != testCase.wantValue {
					t.Fatalf("consecutive bins = %s/%q, want %s/%q",
						gotType, gotValue, testCase.wantType, testCase.wantValue)
				}
			})
		}
	})

	t.Run("exact Decimal buckets survive every downstream consumer", func(t *testing.T) {
		adjacentScope := `index=compiler (event_id=` + binEdgeNumericDecimalLow +
			` OR event_id=` + binEdgeNumericDecimalHigh + `)`
		for _, predicate := range []struct {
			name, command string
		}{
			{name: "where", command: `where band>9007199254740992`},
			{name: "search", command: `search band>9007199254740992`},
		} {
			predicate := predicate
			t.Run(predicate.name, func(t *testing.T) {
				compiled := compileIntegrationSPL(
					t,
					adjacentScope+` | bin decimal_adjacent span=1 AS band | `+
						predicate.command+` | stats count`,
					cutoff,
					visibilityCutoff,
				)
				var matched uint64
				if err := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(&matched); err != nil {
					t.Fatalf("execute exact Decimal %s: %v\nSQL: %s", predicate.name, err, compiled.SQL)
				}
				if matched != 1 {
					t.Fatalf("exact Decimal %s matched %d rows, want 1", predicate.name, matched)
				}
			})
		}

		sorted := compileIntegrationSPL(
			t,
			adjacentScope+` | bin decimal_adjacent span=1 AS band | sort band | table event_id`,
			cutoff,
			visibilityCutoff,
		)
		rows, err := connection.Query(ctx, sorted.SQL, sorted.Args...)
		if err != nil {
			t.Fatalf("execute exact Decimal sort: %v\nSQL: %s", err, sorted.SQL)
		}
		var sortedIDs []string
		for rows.Next() {
			var eventID string
			if err := rows.Scan(&eventID); err != nil {
				t.Fatalf("scan exact Decimal sort: %v", err)
			}
			sortedIDs = append(sortedIDs, eventID)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("iterate exact Decimal sort: %v", err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close exact Decimal sort: %v", err)
		}
		if want := []string{binEdgeNumericDecimalLow, binEdgeNumericDecimalHigh}; !reflect.DeepEqual(sortedIDs, want) {
			t.Fatalf("exact Decimal sort = %v, want %v", sortedIDs, want)
		}

		grouped := compileIntegrationSPL(
			t,
			adjacentScope+` | bin decimal_adjacent span=1 AS band | stats count BY band`,
			cutoff,
			visibilityCutoff,
		)
		groupRows, err := connection.Query(ctx, grouped.SQL, grouped.Args...)
		if err != nil {
			t.Fatalf("execute exact Decimal grouping: %v\nSQL: %s", err, grouped.SQL)
		}
		gotGroups := make(map[string]uint64, 2)
		for groupRows.Next() {
			var band string
			var count uint64
			if err := groupRows.Scan(&band, &count); err != nil {
				t.Fatalf("scan exact Decimal group: %v", err)
			}
			gotGroups[band] = count
		}
		if err := groupRows.Err(); err != nil {
			_ = groupRows.Close()
			t.Fatalf("iterate exact Decimal groups: %v", err)
		}
		if err := groupRows.Close(); err != nil {
			t.Fatalf("close exact Decimal groups: %v", err)
		}
		wantGroups := map[string]uint64{"9007199254740992": 1, "9007199254740993": 1}
		if !reflect.DeepEqual(gotGroups, wantGroups) {
			t.Fatalf("exact Decimal groups = %#v, want %#v", gotGroups, wantGroups)
		}

		equivalent := compileIntegrationSPL(
			t,
			`index=compiler (event_id=`+binEdgeNumericDecimalBase+
				` OR event_id=`+binEdgeNumericDecimalLow+
				`) | bin decimal_basic span=10 AS band | stats count BY band`,
			cutoff,
			visibilityCutoff,
		)
		var band string
		var count uint64
		if err := connection.QueryRow(ctx, equivalent.SQL, equivalent.Args...).Scan(&band, &count); err != nil {
			t.Fatalf("execute equivalent Decimal grouping: %v\nSQL: %s", err, equivalent.SQL)
		}
		if band != "120" || count != 2 {
			t.Fatalf("equivalent Decimal group = %q/%d, want 120/2", band, count)
		}
	})

	t.Run("preserved Decimal envelopes compare and sort with new Int256 buckets", func(t *testing.T) {
		scope := `index=compiler (event_id=` + binEdgeNumericDecimalLow +
			` OR event_id=` + binEdgeNumericDecimalHigh +
			`) | bin mixed_source span=1 AS mixed_destination`
		for _, predicate := range []struct {
			name, command, wantEventID string
		}{
			{
				name:        "where orders adjacent values exactly",
				command:     `where mixed_destination<9007199254740993`,
				wantEventID: binEdgeNumericDecimalLow,
			},
			{
				name:        "search orders adjacent values exactly",
				command:     `search mixed_destination<9007199254740993`,
				wantEventID: binEdgeNumericDecimalLow,
			},
			{
				name:        "where equality accepts an integral exponent",
				command:     `where mixed_destination=9007199254740992`,
				wantEventID: binEdgeNumericDecimalLow,
			},
			{
				name:        "search equality accepts an integral exponent",
				command:     `search mixed_destination=9007199254740992`,
				wantEventID: binEdgeNumericDecimalLow,
			},
		} {
			predicate := predicate
			t.Run(predicate.name, func(t *testing.T) {
				compiled := compileIntegrationSPL(
					t,
					scope+` | `+predicate.command+` | table event_id`,
					cutoff,
					visibilityCutoff,
				)
				var eventID string
				if err := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(&eventID); err != nil {
					t.Fatalf(
						"execute mixed Decimal predicate %q: %v\nSQL: %s",
						predicate.command,
						err,
						compiled.SQL,
					)
				}
				if eventID != predicate.wantEventID {
					t.Fatalf(
						"mixed Decimal predicate %q returned %q, want %q",
						predicate.command,
						eventID,
						predicate.wantEventID,
					)
				}
			})
		}

		sorted := compileIntegrationSPL(
			t,
			scope+` | sort mixed_destination | table event_id`,
			cutoff,
			visibilityCutoff,
		)
		rows, err := connection.Query(ctx, sorted.SQL, sorted.Args...)
		if err != nil {
			t.Fatalf("execute mixed Decimal sort: %v\nSQL: %s", err, sorted.SQL)
		}
		var got []string
		for rows.Next() {
			var eventID string
			if err := rows.Scan(&eventID); err != nil {
				t.Fatalf("scan mixed Decimal sort: %v", err)
			}
			got = append(got, eventID)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("iterate mixed Decimal sort: %v", err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close mixed Decimal sort: %v", err)
		}
		want := []string{binEdgeNumericDecimalLow, binEdgeNumericDecimalHigh}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("mixed Decimal sort = %v, want %v", got, want)
		}
	})

	t.Run("exact Decimal comparison parser handles integral exponent forms", func(t *testing.T) {
		maxScientific := exactNumericBinMaxInt256[:len(exactNumericBinMaxInt256)-1] + "." +
			exactNumericBinMaxInt256[len(exactNumericBinMaxInt256)-1:] + "e1"
		minScientific := "-" +
			exactNumericBinMinMagnitude[:len(exactNumericBinMinMagnitude)-1] + "." +
			exactNumericBinMinMagnitude[len(exactNumericBinMinMagnitude)-1:] + "e1"
		hugeExponent := strings.Repeat("9", 100)
		want := map[string]string{
			"9007199254740992e0":        "9007199254740992",
			"90071992547409920e-1":      "9007199254740992",
			"9007199254740992.0e0":      "9007199254740992",
			"1.20e1":                    "12",
			"1.20e0":                    "<null>",
			"-21.50e1":                  "-215",
			maxScientific:               exactNumericBinMaxInt256,
			minScientific:               "-" + exactNumericBinMinMagnitude,
			exactNumericBinMinMagnitude: "<null>",
			"-57896044618658097711785492504343953926634992332820282019728792003956564819969": "<null>",
			"0e" + hugeExponent: "0",
			"1e" + hugeExponent: "<null>",
		}
		payloads := make([]string, 0, len(want))
		for payload := range want {
			payloads = append(payloads, quoteStringLiteralForBinEdge(payload))
		}
		envelope := "CAST(map(" +
			"concat(char(0), 'open_splunk_type'), 'decimal/v1', " +
			"concat(char(0), 'open_splunk_value'), payload) AS Dynamic)"
		exact := dynamicTaggedDecimalIntegralSQL(compiledScalar{
			valueSQL:       envelope,
			dynamicTypeSQL: "dynamicType(" + envelope + ")",
			kind:           fieldKindDynamic,
		})
		query := "SELECT payload, ifNull(toString(" + exact +
			"), '<null>') FROM (SELECT arrayJoin([" +
			strings.Join(payloads, ", ") + "]) AS payload)"
		rows, err := connection.Query(ctx, query)
		if err != nil {
			t.Fatalf("execute exact Decimal comparison parser: %v\nSQL: %s", err, query)
		}
		got := make(map[string]string, len(want))
		for rows.Next() {
			var payload, exactValue string
			if err := rows.Scan(&payload, &exactValue); err != nil {
				_ = rows.Close()
				t.Fatalf("scan exact Decimal comparison parser: %v", err)
			}
			got[payload] = exactValue
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatalf("iterate exact Decimal comparison parser: %v", err)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("close exact Decimal comparison parser: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("exact Decimal comparison parser = %#v, want %#v", got, want)
		}
	})

	t.Run("numeric text converges with its numeric twin under stats", func(t *testing.T) {
		compiled := compileIntegrationSPL(
			t,
			`index=compiler | bin converge span=10 AS band | stats count BY band`,
			cutoff,
			visibilityCutoff,
		)
		rows, err := connection.Query(ctx, compiled.SQL, compiled.Args...)
		if err != nil {
			t.Fatalf("execute convergence query: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
		}
		defer func() { _ = rows.Close() }()
		type group struct {
			band  string
			count uint64
		}
		var groups []group
		for rows.Next() {
			var row group
			if err := rows.Scan(&row.band, &row.count); err != nil {
				t.Fatalf("scan convergence group: %v", err)
			}
			groups = append(groups, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate convergence groups: %v", err)
		}
		if len(groups) != 1 || groups[0].band != "20" || groups[0].count != 3 {
			t.Fatalf("convergence groups = %#v, want one 20 group of three events", groups)
		}
	})

	// A bucket is not cosmetic: it is the value every later numeric predicate
	// and aggregation sees. Keep the former fabricated-bucket cases pinned
	// through downstream predicates as well as direct result inspection.
	t.Run("a bucket never contradicts the value it buckets", func(t *testing.T) {
		for _, probe := range []struct {
			name, field, span, filter string
			wantCount                 uint64
		}{
			{
				name: "padded integer text", field: "pad_22", span: "10",
				filter: "band<10", wantCount: 0,
			},
			{
				name: "padded fractional text", field: "pad_fraction", span: "10",
				filter: "band<10", wantCount: 0,
			},
			{
				name: "padded wide integer text", field: "pad_wide", span: "100",
				filter: "band<9007199254740900", wantCount: 0,
			},
		} {
			probe := probe
			t.Run(probe.name, func(t *testing.T) {
				compiled := compileIntegrationSPL(
					t,
					`index=compiler event_id=`+binEdgeNumericTextEvent+
						` | bin `+probe.field+` span=`+probe.span+` AS band | where `+probe.filter+
						` | stats count`,
					cutoff,
					visibilityCutoff,
				)
				var matched uint64
				if err := connection.QueryRow(ctx, compiled.SQL, compiled.Args...).Scan(&matched); err != nil {
					t.Fatalf("execute bucket filter probe: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
				}
				if matched != probe.wantCount {
					t.Errorf(
						"bin %s span=%s | where %s matched %d events, want %d",
						probe.field, probe.span, probe.filter, matched, probe.wantCount,
					)
				}
			})
		}
	})

	t.Run("anomalous text never fails the search for other rows", func(t *testing.T) {
		compiled := compileIntegrationSPL(
			t,
			`index=compiler | bin pad_22 span=10 AS band | table event_id band`,
			cutoff,
			visibilityCutoff,
		)
		var rowCount uint64
		if err := connection.QueryRow(ctx,
			"SELECT count() FROM ("+compiled.SQL+")", compiled.Args...,
		).Scan(&rowCount); err != nil {
			t.Fatalf("execute mixed-row bin: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
		}
		if rowCount != uint64(len(allEvents)) {
			t.Fatalf("mixed-row bin returned %d rows, want %d", rowCount, len(allEvents))
		}
	})

	t.Run("malformed Decimal envelopes fail only inside the authorized snapshot", func(t *testing.T) {
		const field = "malformed_decimal"
		typeKey := "\x00open_splunk_type"
		valueKey := "\x00open_splunk_value"
		inScope := []binEdgeRawDecimalEnvelope{
			{
				eventID: "bin-edge-poison-missing-type",
				envelope: map[string]string{
					valueKey: "123",
				},
			},
			{
				eventID: "bin-edge-poison-missing-value",
				envelope: map[string]string{
					typeKey: "decimal/v1",
				},
			},
			{
				eventID: "bin-edge-poison-extra-key",
				envelope: map[string]string{
					typeKey: "decimal/v1", valueKey: "123", "extra": "forbidden",
				},
			},
			{
				eventID: "bin-edge-poison-wrong-tag",
				envelope: map[string]string{
					typeKey: "bytes/v1", valueKey: "MTIz",
				},
			},
			{
				eventID:   "bin-edge-poison-wrong-semantic-type",
				fieldType: eventfields.StoredValueTypeString,
				envelope: map[string]string{
					typeKey: "decimal/v1", valueKey: "123",
				},
			},
			{
				eventID: "bin-edge-poison-noncanonical",
				envelope: map[string]string{
					typeKey: "decimal/v1", valueKey: "01",
				},
			},
			{
				eventID: "bin-edge-poison-invalid-grammar",
				envelope: map[string]string{
					typeKey: "decimal/v1", valueKey: "malformed-secret-1e",
				},
			},
			{
				eventID: "bin-edge-poison-oversized",
				envelope: map[string]string{
					typeKey:  "decimal/v1",
					valueKey: strings.Repeat("9", MaximumExactNumericBinTextBytes+1),
				},
			},
			{
				eventID: "bin-edge-poison-invalid-utf8",
				envelope: map[string]string{
					typeKey: "decimal/v1", valueKey: string([]byte{0xff, '1'}),
				},
			},
		}
		for index := range inScope {
			inScope[index].tenantID = "tenant"
			inScope[index].indexName = "compiler"
			inScope[index].eventTime = indexTime
			inScope[index].indexTime = indexTime
			inScope[index].visibilitySeq = visibilityCutoff
			inScope[index].fieldName = field
			if inScope[index].fieldType == 0 {
				inScope[index].fieldType = eventfields.StoredValueTypeDecimal
			}
		}

		// Every out-of-scope poison row deliberately reuses the valid event's ID.
		// The event-id predicate therefore cannot hide it: only one of the
		// tenant, authorized-index, event-time, index-time, or immutable
		// visibility fences can remove each row before bin classifies it.
		scopePoison := map[string]string{
			typeKey: "decimal/v1", valueKey: "not-a-decimal",
		}
		outOfScope := []binEdgeRawDecimalEnvelope{
			{
				eventID: binEdgeNumericDecimalBase, tenantID: "other-tenant", indexName: "compiler",
				eventTime: indexTime, indexTime: indexTime, visibilitySeq: visibilityCutoff,
			},
			{
				eventID: binEdgeNumericDecimalBase, tenantID: "tenant", indexName: "hidden",
				eventTime: indexTime, indexTime: indexTime, visibilitySeq: visibilityCutoff,
			},
			{
				eventID: binEdgeNumericDecimalBase, tenantID: "tenant", indexName: "compiler",
				eventTime: time.Date(2026, time.July, 19, 23, 59, 59, 0, time.UTC),
				indexTime: indexTime, visibilitySeq: visibilityCutoff,
			},
			{
				eventID: binEdgeNumericDecimalBase, tenantID: "tenant", indexName: "compiler",
				eventTime: time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC),
				indexTime: indexTime, visibilitySeq: visibilityCutoff,
			},
			{
				eventID: binEdgeNumericDecimalBase, tenantID: "tenant", indexName: "compiler",
				eventTime: indexTime, indexTime: cutoff.Add(time.Second), visibilitySeq: visibilityCutoff,
			},
			{
				eventID: binEdgeNumericDecimalBase, tenantID: "tenant", indexName: "compiler",
				eventTime: indexTime, indexTime: indexTime, visibilitySeq: visibilityCutoff + 1,
			},
		}
		for index := range outOfScope {
			outOfScope[index].fieldName = "decimal_basic"
			outOfScope[index].fieldType = eventfields.StoredValueTypeDecimal
			outOfScope[index].envelope = scopePoison
		}
		binEdgeInsertRawDecimalEnvelopes(
			t,
			ctx,
			connection,
			"bin-edge-numeric-decimal-envelope-scope",
			append(inScope, outOfScope...),
		)

		var rawRows, mapRows, alignedRows uint64
		if err := connection.QueryRow(ctx,
			`SELECT count(),
				countIf(dynamicType(fields.malformed_decimal) = 'Map(String, String)'),
				countIf(field_metadata_version = ?
					AND field_names = ['malformed_decimal']
					AND length(field_types) = 1)
			FROM open_splunk.events
			WHERE startsWith(event_id, 'bin-edge-poison-')`,
			eventfields.CurrentFieldMetadataVersion,
		).Scan(&rawRows, &mapRows, &alignedRows); err != nil {
			t.Fatalf("verify raw malformed Decimal fixtures: %v", err)
		}
		if want := uint64(len(inScope)); rawRows != want || mapRows != want || alignedRows != want {
			t.Fatalf(
				"raw malformed Decimal fixtures = rows:%d maps:%d aligned:%d, want %d/%d/%d",
				rawRows,
				mapRows,
				alignedRows,
				want,
				want,
				want,
			)
		}

		var (
			scopedRows, scopedMaps, scopedMetadata, scopedMalformed uint64
			foreignTenant, foreignIndex, earlyTime, lateTime        uint64
			lateIndexTime, futureVisibility                         uint64
		)
		scopeControlSQL := fmt.Sprintf(
			`SELECT countIf(has(field_names, 'decimal_basic')),
				countIf(has(field_names, 'decimal_basic')
					AND dynamicType(fields.decimal_basic) = 'Map(String, String)'),
				countIf(has(field_names, 'decimal_basic')
					AND field_metadata_version = toUInt8(%d)
					AND length(field_names) = 1
					AND length(field_types) = 1
					AND arrayElement(field_types, 1) = toUInt8(%d)),
				countIf(has(field_names, 'decimal_basic')
					AND length(dynamicElement(fields.decimal_basic, 'Map(String, String)')) = 2
					AND mapContains(
						dynamicElement(fields.decimal_basic, 'Map(String, String)'),
						concat(char(0), 'open_splunk_type'))
					AND mapContains(
						dynamicElement(fields.decimal_basic, 'Map(String, String)'),
						concat(char(0), 'open_splunk_value'))
					AND dynamicElement(
						fields.decimal_basic,
						'Map(String, String)')[concat(char(0), 'open_splunk_type')] = 'decimal/v1'
					AND dynamicElement(
						fields.decimal_basic,
						'Map(String, String)')[concat(char(0), 'open_splunk_value')] = 'not-a-decimal'),
				countIf(has(field_names, 'decimal_basic') AND tenant_id != 'tenant'),
				countIf(has(field_names, 'decimal_basic') AND index_name != 'compiler'),
				countIf(has(field_names, 'decimal_basic')
					AND event_time < parseDateTime64BestEffort('2026-07-20 00:00:00', 9, 'UTC')),
				countIf(has(field_names, 'decimal_basic')
					AND event_time >= parseDateTime64BestEffort('2026-07-22 00:00:00', 9, 'UTC')),
				countIf(has(field_names, 'decimal_basic')
					AND index_time > parseDateTime64BestEffort(%s, 3, 'UTC')),
				countIf(has(field_names, 'decimal_basic') AND visibility_seq > toUInt64(%d))
			FROM open_splunk.events
			WHERE batch_id = 'poison-batch'`,
			eventfields.CurrentFieldMetadataVersion,
			uint8(eventfields.StoredValueTypeDecimal),
			quoteStringLiteralForBinEdge(cutoff.UTC().Format("2006-01-02 15:04:05.000")),
			visibilityCutoff,
		)
		if err := connection.QueryRow(ctx, scopeControlSQL).Scan(
			&scopedRows,
			&scopedMaps,
			&scopedMetadata,
			&scopedMalformed,
			&foreignTenant,
			&foreignIndex,
			&earlyTime,
			&lateTime,
			&lateIndexTime,
			&futureVisibility,
		); err != nil {
			t.Fatalf("verify raw out-of-scope Decimal fixtures: %v\nSQL: %s", err, scopeControlSQL)
		}
		wantScopedRows := uint64(len(outOfScope))
		if scopedRows != wantScopedRows ||
			scopedMaps != wantScopedRows ||
			scopedMetadata != wantScopedRows ||
			scopedMalformed != wantScopedRows ||
			foreignTenant != 1 ||
			foreignIndex != 1 ||
			earlyTime != 1 ||
			lateTime != 1 ||
			lateIndexTime != 1 ||
			futureVisibility != 1 {
			t.Fatalf(
				"raw scope poison = rows/maps/metadata/malformed:%d/%d/%d/%d tenant/index/time:%d/%d/%d/%d index_time/visibility:%d/%d, want %d/%d/%d/%d 1/1/1/1 1/1",
				scopedRows,
				scopedMaps,
				scopedMetadata,
				scopedMalformed,
				foreignTenant,
				foreignIndex,
				earlyTime,
				lateTime,
				lateIndexTime,
				futureVisibility,
				wantScopedRows,
				wantScopedRows,
				wantScopedRows,
				wantScopedRows,
			)
		}
		// Force valid and poison fixtures into the same sorted part/granule.
		// The application query below must therefore rely on row-level scope
		// predicates rather than merely pruning the direct-insert part.
		if err := connection.Exec(ctx, "OPTIMIZE TABLE open_splunk.events FINAL"); err != nil {
			t.Fatalf("merge raw Decimal poison fixtures: %v", err)
		}

		for _, malformed := range inScope {
			malformed := malformed
			t.Run(malformed.eventID, func(t *testing.T) {
				compiled := compileIntegrationSPL(
					t,
					`index=compiler event_id=`+malformed.eventID+
						` | bin `+field+` span=10 | table event_id `+field,
					cutoff,
					visibilityCutoff,
				)
				queryErr := executeCompiledExpectingNoRows(ctx, connection, compiled)
				var exception *clickhousedriver.Exception
				if !errors.As(queryErr, &exception) ||
					exception.Code != 395 ||
					!strings.Contains(exception.Message, UnsupportedNumericBinValueMarker) {
					t.Fatalf(
						"malformed Decimal %q error = %v, want the sanitized unsupported marker",
						malformed.eventID,
						queryErr,
					)
				}
				if strings.Contains(queryErr.Error(), "malformed-secret") {
					t.Fatalf("malformed Decimal %q leaked its payload: %v", malformed.eventID, queryErr)
				}
			})
		}

		authorized := compileIntegrationSPL(
			t,
			`index=compiler event_id=`+binEdgeNumericDecimalBase+
				` | bin decimal_basic span=10 AS band | table event_id band`,
			cutoff,
			visibilityCutoff,
		)
		var authorizedRows uint64
		var authorizedID, authorizedType, authorizedBand string
		if err := connection.QueryRow(
			ctx,
			"SELECT count(), any(event_id), any(dynamicType(band)), any(toString(band)) FROM ("+
				authorized.SQL+")",
			authorized.Args...,
		).Scan(&authorizedRows, &authorizedID, &authorizedType, &authorizedBand); err != nil {
			t.Fatalf(
				"execute authorized Decimal with out-of-scope poison: %v\nSQL: %s\nargs: %#v",
				err,
				authorized.SQL,
				authorized.Args,
			)
		}
		if authorizedRows != 1 ||
			authorizedID != binEdgeNumericDecimalBase ||
			authorizedType != "Int256" ||
			authorizedBand != "120" {
			t.Fatalf(
				"authorized Decimal result = rows:%d id:%q type:%q band:%q, want 1/%q/Int256/120",
				authorizedRows,
				authorizedID,
				authorizedType,
				authorizedBand,
				binEdgeNumericDecimalBase,
			)
		}
	})
}

type binEdgeRawDecimalEnvelope struct {
	eventID       string
	tenantID      string
	indexName     string
	eventTime     time.Time
	indexTime     time.Time
	visibilitySeq uint64
	fieldName     string
	fieldType     eventfields.StoredValueType
	envelope      map[string]string
}

func binEdgeInsertRawDecimalEnvelopes(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	deduplicationToken string,
	fixtures []binEdgeRawDecimalEnvelope,
) {
	t.Helper()
	if len(fixtures) == 0 {
		t.Fatal("insert raw Decimal envelopes: empty fixture set")
	}
	if deduplicationToken == "" {
		t.Fatal("insert raw Decimal envelopes: empty deduplication token")
	}
	insertContext := clickhousedriver.Context(ctx, clickhousedriver.WithSettings(
		insertSettings(deduplicationToken),
	))
	batch, err := connection.PrepareBatch(insertContext, eventsInsertSQL)
	if err != nil {
		t.Fatalf("prepare raw Decimal envelope batch: %v", err)
	}
	defer func() { _ = batch.Close() }()

	for _, fixture := range fixtures {
		document := clickhousedriver.NewJSON()
		document.SetValueAtPath(
			fixture.fieldName,
			clickhousedriver.NewDynamicWithType(fixture.envelope, "Map(String, String)"),
		)
		if err := batch.Append(
			fixture.eventID,
			fixture.tenantID,
			fixture.indexName,
			fixture.eventTime.UTC(),
			fixture.indexTime.UTC().Truncate(time.Millisecond),
			nil,
			uint8(opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED),
			"poison-host",
			"poison-source",
			"poison",
			nil,
			uint8(0),
			nil,
			nil,
			[]byte(fixture.eventID),
			uint8(opensplunkv1.RawEncoding_RAW_ENCODING_UTF8),
			nil,
			nil,
			document,
			[]string{fixture.fieldName},
			"poison-collector",
			"poison-batch",
			uint64(1),
			time.Date(2099, time.January, 1, 0, 0, 0, 0, time.UTC),
			fixture.visibilitySeq,
			[]uint8{uint8(fixture.fieldType)},
			eventfields.CurrentFieldMetadataVersion,
		); err != nil {
			t.Fatalf("append raw Decimal envelope %q: %v", fixture.eventID, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send raw Decimal envelope batch: %v", err)
	}
}

func binEdgeNumericBoundedAnalyzerContext(parent context.Context) (context.Context, context.CancelFunc) {
	queryContext := clickhousedriver.Context(parent, clickhousedriver.WithSettings(clickhousedriver.Settings{
		"max_memory_usage":                  uint64(256 << 20),
		"max_execution_time":                uint64(15),
		"timeout_overflow_mode":             "throw",
		"max_threads":                       uint64(1),
		"short_circuit_function_evaluation": "enable",
	}))
	return context.WithTimeout(queryContext, 20*time.Second)
}

func binEdgeNumericBucket(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	cutoff time.Time,
	visibilityCutoff uint64,
	eventID, field, span string,
) (string, string) {
	t.Helper()
	compiled := compileIntegrationSPL(
		t,
		`index=compiler event_id=`+eventID+` | bin `+field+` span=`+span+` AS band | table band`,
		cutoff,
		visibilityCutoff,
	)
	var gotType, gotValue string
	if err := connection.QueryRow(ctx,
		`SELECT multiIf(
				dynamicType(band) IN ('Int128', 'Int256', 'UInt128', 'UInt256')
					OR startsWith(dynamicType(band), 'Decimal'),
				'Decimal',
				dynamicType(band) = 'Map(String, String)'
					AND length(dynamicElement(band, 'Map(String, String)')) = 2
					AND dynamicElement(band, 'Map(String, String)')[concat(char(0), 'open_splunk_type')] = 'decimal/v1',
				'Decimal',
				dynamicType(band)),
			multiIf(
				dynamicType(band) = 'None', '<none>',
				dynamicType(band) = 'Map(String, String)'
					AND length(dynamicElement(band, 'Map(String, String)')) = 2
					AND dynamicElement(band, 'Map(String, String)')[concat(char(0), 'open_splunk_type')] = 'decimal/v1',
				dynamicElement(band, 'Map(String, String)')[concat(char(0), 'open_splunk_value')],
				toString(band))
		FROM (`+compiled.SQL+`)`,
		compiled.Args...,
	).Scan(&gotType, &gotValue); err != nil {
		t.Fatalf(
			"execute bin %s span=%s for %s: %v\nSQL: %s\nargs: %#v",
			field, span, eventID, err, compiled.SQL, compiled.Args,
		)
	}
	return gotType, gotValue
}

func binEdgeNumericEvent(id string, fields ...*opensplunkv1.TypedObjectField) *ingest.StoredEvent {
	event := testStoredEvent(id, "compiler", time.Date(2026, 7, 21, 3, 4, 6, 987654321, time.UTC))
	event.Event.Host = "api"
	event.Event.Raw = []byte("bin edge numeric fixture")
	event.Event.Fields = typedObjectValue(fields...)
	return event
}

// binEdgeNumericStore starts a dedicated pinned ClickHouse container, applies
// the repository migrations, and returns a writer store plus a raw query
// connection. The container name is randomized so concurrent authors in this
// tree never collide.
func binEdgeNumericStore(t *testing.T, ctx context.Context) (*Store, clickhousedriver.Conn) {
	t.Helper()

	container := "open-splunk-bin-edge-numeric-" + integrationRandomHex(t, 6)
	password := integrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = storeIntegrationImage
	}
	integrationDocker(t, ctx, nil,
		"run", "--detach", "--rm", "--name", container,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB=open_splunk",
		"--env", "CLICKHOUSE_USER=open_splunk",
		"--env", "CLICKHOUSE_PASSWORD="+password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		image,
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", "--volumes", container).Run()
	})
	integrationWaitForClickHouse(t, ctx, container, password)

	migrationPaths, err := filepath.Glob(
		filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"),
	)
	if err != nil || len(migrationPaths) == 0 {
		t.Fatalf("discover migrations: paths=%v err=%v", migrationPaths, err)
	}
	var migrations bytes.Buffer
	for _, migrationPath := range migrationPaths {
		migration, readErr := os.ReadFile(migrationPath)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", migrationPath, readErr)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	integrationDocker(t, ctx, bytes.NewReader(migrations.Bytes()),
		"exec", "--interactive", container, "clickhouse-client",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)

	config := DefaultConfig()
	config.Addresses = []string{integrationNativeAddress(t, ctx, container)}
	config.Username = "open_splunk"
	config.Password = password
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open visibility control database: %v", err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatalf("create visibility sequencer: %v", err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	store, err := Open(config, fixedRetention(30*24*time.Hour), sequencer)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	options, _, err := config.clickHouseOptions()
	if err != nil {
		t.Fatal(err)
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatalf("open query connection: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return store, connection
}
