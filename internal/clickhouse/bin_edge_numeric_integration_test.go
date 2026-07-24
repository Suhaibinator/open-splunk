package clickhouse

import (
	"bytes"
	"context"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

// This file probes the numeric-string and floating edges of the runtime
// Dynamic bin path.
//
// Four cases currently fail and are left failing on purpose; each is a reported
// defect, not a wrong expectation. Every spelling longer than 21 bytes leaves
// the exact Int256 arm (`length(<value>) <= 21`) and is handed to the Float64
// arm, which has no width bound of its own. ClickHouse's `toFloat64OrNull`
// silently drops digits once an integer part carries about twenty leading
// zeros, so those spellings become a fabricated finite bucket — `bin` answers 0
// for a field that spells 21 — or fall through as text. Both outcomes
// contradict `docs/spl-compatibility-v0.1.md`, which says numeric text becomes
// the number it spells and converges with its numeric twin.
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
			// Exactly 21 bytes, the documented width bound.
			name: "leading zeros at the width guard", field: "pad_21", span: "10",
			text: "000000000000000000021", number: typedSint(21),
			wantType: "Int64", wantValue: "20",
		},
		{
			// DEFECT (failing): one byte past the width guard. The spelling
			// still names an integer whose exact bucket start is
			// representable, so the contract requires the same signed number
			// as its twin. Observed: Float64 0.
			name: "leading zeros past the width guard", field: "pad_22", span: "10",
			text: "0000000000000000000021", number: typedSint(21),
			wantType: "Int64", wantValue: "20",
		},
		{
			// DEFECT (failing): fractional text has no width bound at all, so
			// the Float64 arm trusts whatever ClickHouse's text-to-double
			// parser returns. 21.5 is exactly representable and its bucket is
			// 20. Observed: Float64 0.
			name: "padded fractional text", field: "pad_fraction", span: "10",
			text: "000000000000000000021.5", number: typedDouble(21.5),
			wantType: "Float64", wantValue: "20",
		},
		{
			// DEFECT (failing): the identical digits without padding bucket
			// exactly (see the next case). Observed: Float64 9007199254740000.
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
			// DEFECT (failing): 22 bytes with one pad zero. Observed: the text
			// is kept, so the bucket never converges with its numeric twin.
			name: "padded unsigned maximum", field: "uint64_max_pad", span: "10",
			text: "+018446744073709551615", number: typedUint(^uint64(0)),
			wantType: "UInt64", wantValue: "18446744073709551610",
		},
		{
			// An integer bucket that lands outside both Int64 and UInt64 keeps
			// its text; the twin fails the search, so it is checked separately.
			name: "integer text whose bucket is unrepresentable", field: "int64_min", span: "10",
			text:     "-9223372036854775808",
			wantType: "String", wantValue: "-9223372036854775808",
		},
		{
			name: "integer text above the unsigned maximum", field: "above_uint64", span: "10",
			text:     "999999999999999999999",
			wantType: "String", wantValue: "999999999999999999999",
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
			// Above the fence the text is kept, while the stored double is
			// bucketed: the contract documents that divergence explicitly.
			name: "fractional text above the exact fence", field: "fence_above", span: "2",
			text: "9007199254740994.0", number: typedDouble(9007199254740994),
			wantType: "String", wantValue: "9007199254740994.0",
			wantTwinType: "Float64", wantTwinValue: "9007199254740994",
		},
		{
			// ClickHouse rounds this spelling to 2^53 while parsing, so the
			// emitted bucket is one greater than the value being binned. The
			// documented fence is a magnitude fence and this magnitude is below
			// it, so the observed double semantics are recorded here rather
			// than reported as a contract break.
			name: "fractional text that rounds up onto the fence", field: "fence_round", span: "1",
			text:     "9007199254740991.5",
			wantType: "Float64", wantValue: "9007199254740992",
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
	for _, event := range []*ingest.StoredEvent{textEvent, numberEvent, doubleEvent} {
		event.IndexTime = indexTime
		event.BatchID = "bin-edge-numeric-batch"
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "bin-edge-numeric-batch", BatchSequence: 1,
		SourceBatchSHA256: testSourceBatchDigest("bin-edge-numeric-batch"),
		ReceivedAt:        indexTime,
		Events:            []*ingest.StoredEvent{textEvent, numberEvent, doubleEvent},
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

	// DEFECT (failing): a bucket is not cosmetic. It is the value every later
	// numeric predicate and aggregation sees, so a fabricated bucket silently
	// changes which events a search matches: a field spelling 21 matches
	// `where band<10`.
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
		if rowCount != 3 {
			t.Fatalf("mixed-row bin returned %d rows, want 3", rowCount)
		}
	})
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
		`SELECT dynamicType(band), if(dynamicType(band) = 'None', '<none>', toString(band)) FROM (`+
			compiled.SQL+`)`,
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
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", container).Run()
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
