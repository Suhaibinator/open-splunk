package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

const (
	rawTextIndexFixtureName  = "raw-text-index"
	rawTextIndexGranuleRows  = 8_192
	rawTextIndexFixtureRows  = 3 * rawTextIndexGranuleRows
	rawTextIndexFirstSpecial = rawTextIndexGranuleRows
)

const (
	rawTextIndexCaseFoldRow = rawTextIndexFirstSpecial + iota
	rawTextIndexPunctuationRow
	rawTextIndexUnderscoreBeforeRow
	rawTextIndexUnderscoreAfterRow
	rawTextIndexAlphanumericAdjacencyRow
	rawTextIndexUnicodeBoundaryRow
	rawTextIndexBinaryBoundaryRow
	rawTextIndexSecondTokenRow
	rawTextIndexOtherTermRow
	rawTextIndexNeedleGateRow
	rawTextIndexPunctuatedTermRow
	rawTextIndexUnderscoredTermRow
	rawTextIndexUnicodeTermRow
	rawTextIndexUnicodeExpansionBoundaryRow
	rawTextIndexLongSFoldRow
	rawTextIndexKelvinFoldRow
	rawTextIndexLongSAdjacencyRow
	rawTextIndexKelvinAdjacencyRow
	rawTextIndexCombinedFoldRow
)

func rawTextIndexQuerySettings(useOnRead uint8) clickhousedriver.Settings {
	return clickhousedriver.Settings{
		"enable_full_text_index":                 uint8(1),
		"query_plan_direct_read_from_text_index": uint8(1),
		"use_skip_indexes_on_data_read":          useOnRead,
		"use_skip_indexes":                       uint8(1),
	}
}

func rawTextIndexEventSearch(term string, row int) string {
	return fmt.Sprintf(
		"index=%s %s event_id=raw-token-%05d",
		rawTextIndexFixtureName,
		term,
		row,
	)
}

// testRawTextTokenIndexAgainstClickHouse proves both halves of the native
// _raw acceleration contract. A token candidate may only discard impossible
// granules; the existing regex remains authoritative for every SPL match.
func testRawTextTokenIndexAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	insertRawTextIndexFixtures(t, ctx, connection, indexTime)
	runtimeContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithSettings(rawTextIndexQuerySettings(1)),
	)
	// Production executes with on-read skip-index analysis. The administrator
	// EXPLAIN path deliberately disables it and the condition cache so selected
	// granules are stable, visible plan evidence rather than a runtime-only
	// optimization.
	explainSettings := rawTextIndexQuerySettings(0)
	explainSettings["use_query_condition_cache"] = uint8(0)
	explainContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithSettings(explainSettings),
	)
	compile := func(t *testing.T, source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPLForIndex(
			t,
			source,
			indexTime.Add(10*time.Second),
			1,
			rawTextIndexFixtureName,
		)
	}
	assertCount := func(t *testing.T, source string, want uint64) {
		t.Helper()
		compiled := compile(t, source+" | stats count")
		var got uint64
		if err := connection.QueryRow(
			runtimeContext,
			compiled.SQL,
			compiled.Args...,
		).Scan(&got); err != nil {
			t.Fatalf(
				"execute raw-text-index SPL %q: %v\nSQL: %s\nargs: %#v",
				source,
				err,
				compiled.SQL,
				compiled.Args,
			)
		}
		if got != want {
			t.Fatalf("raw-text-index SPL %q count = %d, want %d", source, got, want)
		}
	}

	// Pin the exact residual semantics before looking at the optimization plan.
	// In particular, POSIX alnum/underscore boundaries intentionally differ
	// from substring matching, while non-UTF-8 raw bytes remain searchable.
	for _, test := range []struct {
		name   string
		source string
		want   uint64
	}{
		{
			name:   "ASCII case fold at both raw boundaries",
			source: rawTextIndexEventSearch("needle42", rawTextIndexCaseFoldRow),
			want:   1,
		},
		{
			name:   "ASCII punctuation is a boundary",
			source: rawTextIndexEventSearch("needle42", rawTextIndexPunctuationRow),
			want:   1,
		},
		{
			name:   "underscore before term is not a boundary",
			source: rawTextIndexEventSearch("needle42", rawTextIndexUnderscoreBeforeRow),
			want:   0,
		},
		{
			name:   "underscore after term is not a boundary",
			source: rawTextIndexEventSearch("needle42", rawTextIndexUnderscoreAfterRow),
			want:   0,
		},
		{
			name:   "ASCII alphanumeric adjacency is not a boundary",
			source: rawTextIndexEventSearch("needle42", rawTextIndexAlphanumericAdjacencyRow),
			want:   0,
		},
		{
			name:   "Unicode bytes delimit an ASCII POSIX token",
			source: rawTextIndexEventSearch("needle42", rawTextIndexUnicodeBoundaryRow),
			want:   1,
		},
		{
			name:   "Unicode lowercase expansion remains a token boundary",
			source: rawTextIndexEventSearch("needle42", rawTextIndexUnicodeExpansionBoundaryRow),
			want:   1,
		},
		{
			name:   "Unicode long-s folds inside an ASCII search term",
			source: rawTextIndexEventSearch("test", rawTextIndexLongSFoldRow),
			want:   1,
		},
		{
			name:   "Unicode Kelvin sign folds inside an ASCII search term",
			source: rawTextIndexEventSearch("check", rawTextIndexKelvinFoldRow),
			want:   1,
		},
		{
			name:   "all non-ASCII simple-fold aliases normalize together",
			source: rawTextIndexEventSearch("mask", rawTextIndexCombinedFoldRow),
			want:   1,
		},
		{
			name:   "Unicode long-s alias is not a token boundary",
			source: rawTextIndexEventSearch("needle42", rawTextIndexLongSAdjacencyRow),
			want:   0,
		},
		{
			name:   "Unicode Kelvin alias is not a token boundary",
			source: rawTextIndexEventSearch("needle42", rawTextIndexKelvinAdjacencyRow),
			want:   0,
		},
		{
			name:   "binary raw delimiters preserve the ASCII token",
			source: rawTextIndexEventSearch("needle42", rawTextIndexBinaryBoundaryRow),
			want:   1,
		},
		{
			name:   "punctuated term remains an exact residual",
			source: rawTextIndexEventSearch("panic-code", rawTextIndexPunctuatedTermRow),
			want:   1,
		},
		{
			name:   "underscored term remains an exact residual",
			source: rawTextIndexEventSearch("panic_code", rawTextIndexUnderscoredTermRow),
			want:   1,
		},
		{
			name:   "Unicode term remains an exact residual",
			source: rawTextIndexEventSearch("café", rawTextIndexUnicodeTermRow),
			want:   1,
		},
		{
			name:   "quoted term retains substring semantics",
			source: rawTextIndexEventSearch(`"needle42"`, rawTextIndexAlphanumericAdjacencyRow),
			want:   1,
		},
		{
			name:   "wildcard retains token-scoped residual semantics",
			source: rawTextIndexEventSearch("needle*", rawTextIndexUnderscoreAfterRow),
			want:   1,
		},
		{
			name:   "negation does not invert a lossy candidate",
			source: rawTextIndexEventSearch("NOT needle42", rawTextIndexAlphanumericAdjacencyRow),
			want:   1,
		},
		{
			name:   "positive conjunction",
			source: `index=raw-text-index needle42 AND secondtoken`,
			want:   1,
		},
		{
			name:   "positive disjunction",
			source: `index=raw-text-index needle42 OR otherterm`,
			want:   8,
		},
		{
			name:   "mixed positive and negative conjunction",
			source: `index=raw-text-index needle42 NOT secondtoken`,
			want:   6,
		},
		{
			name:   "parenthesized Boolean combination",
			source: `index=raw-text-index (needle42 OR otherterm) AND gate`,
			want:   2,
		},
	} {
		t.Run("semantics/"+test.name, func(t *testing.T) {
			assertCount(t, test.source, test.want)
		})
	}
	assertCount(
		t,
		`index=raw-text-index | eval _raw="needle42" | search needle42`,
		rawTextIndexFixtureRows,
	)

	// These forms have no safe native token candidate. Their compiled SQL pins
	// that fallback independently of any optimizer inference ClickHouse may make
	// from the residual predicate itself.
	for _, source := range []string{
		`index=raw-text-index "needle42"`,
		`index=raw-text-index needle*`,
		`index=raw-text-index NOT needle42`,
		`index=raw-text-index café`,
		`index=raw-text-index panic-code`,
		`index=raw-text-index panic_code`,
		`index=raw-text-index | eval _raw="needle42" | search needle42`,
	} {
		compiled := compile(t, source)
		if strings.Contains(compiled.SQL, rawTokenIndexCandidateSQL) {
			t.Fatalf("ineligible raw term %q compiled a native token candidate:\n%s", source, compiled.SQL)
		}
	}

	positive := compile(t, `index=raw-text-index needle42 | stats count`)
	if !strings.Contains(positive.SQL, rawTokenIndexCandidateSQL) {
		t.Fatalf("eligible positive bare term lacks a native token candidate:\n%s", positive.SQL)
	}
	planText := explainCompiledQuery(
		t,
		explainContext,
		connection,
		"EXPLAIN PLAN json = 1, description = 1, indexes = 1, actions = 0, header = 1 ",
		positive,
	)
	assertRawTextIndexPrunesGranules(t, planText)
}

func insertRawTextIndexFixtures(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	batch, err := connection.PrepareBatch(ctx, `INSERT INTO open_splunk.events
		(event_id, tenant_id, index_name, event_time, index_time, raw, raw_encoding,
		 collector_id, batch_id, batch_sequence, visibility_seq, expires_at)`)
	if err != nil {
		t.Fatalf("prepare raw text index fixtures: %v", err)
	}
	for row := 0; row < rawTextIndexFixtureRows; row++ {
		raw := []byte("quiet filler")
		encoding := uint8(opensplunkv1.RawEncoding_RAW_ENCODING_UTF8)
		switch row {
		case rawTextIndexCaseFoldRow:
			raw = []byte("NEEDLE42")
		case rawTextIndexPunctuationRow:
			raw = []byte("x-needle42.y")
		case rawTextIndexUnderscoreBeforeRow:
			raw = []byte("x_needle42")
		case rawTextIndexUnderscoreAfterRow:
			raw = []byte("needle42_y")
		case rawTextIndexAlphanumericAdjacencyRow:
			raw = []byte("xneedle42y")
		case rawTextIndexUnicodeBoundaryRow:
			raw = []byte("éneedle42é")
		case rawTextIndexBinaryBoundaryRow:
			raw = []byte{0xff, ' ', 'N', 'E', 'E', 'D', 'L', 'E', '4', '2', ' ', 0x00}
			encoding = uint8(opensplunkv1.RawEncoding_RAW_ENCODING_BINARY)
		case rawTextIndexSecondTokenRow:
			raw = []byte("needle42 secondtoken")
		case rawTextIndexOtherTermRow:
			raw = []byte("otherterm gate")
		case rawTextIndexNeedleGateRow:
			raw = []byte("needle42 gate")
		case rawTextIndexPunctuatedTermRow:
			raw = []byte("prefix PANIC-code suffix")
		case rawTextIndexUnderscoredTermRow:
			raw = []byte("prefix PANIC_code suffix")
		case rawTextIndexUnicodeTermRow:
			raw = []byte("prefix CAFÉ suffix")
		case rawTextIndexUnicodeExpansionBoundaryRow:
			raw = []byte("needle42İ")
		case rawTextIndexLongSFoldRow:
			raw = []byte("prefix teſt suffix")
		case rawTextIndexKelvinFoldRow:
			raw = []byte("prefix checK suffix")
		case rawTextIndexLongSAdjacencyRow:
			raw = []byte("ſneedle42")
		case rawTextIndexKelvinAdjacencyRow:
			raw = []byte("needle42K")
		case rawTextIndexCombinedFoldRow:
			raw = []byte("prefix maſK suffix")
		}
		if err := batch.Append(
			fmt.Sprintf("raw-token-%05d", row),
			"tenant",
			rawTextIndexFixtureName,
			indexTime,
			indexTime,
			raw,
			encoding,
			"raw-text-index-collector",
			"raw-text-index-batch",
			uint64(1),
			uint64(1),
			time.Date(2100, time.January, 1, 0, 0, 0, 0, time.UTC),
		); err != nil {
			t.Fatalf("append raw text index fixture row %d: %v", row, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send raw text index fixtures: %v", err)
	}
}

type rawTextExplainEnvelope struct {
	Plan rawTextExplainNode `json:"Plan"`
}

type rawTextExplainNode struct {
	Plans   []rawTextExplainNode  `json:"Plans"`
	Indexes []rawTextExplainIndex `json:"Indexes"`
}

type rawTextExplainIndex struct {
	Type             string `json:"Type"`
	Name             string `json:"Name"`
	InitialParts     uint64 `json:"Initial Parts"`
	SelectedParts    uint64 `json:"Selected Parts"`
	InitialGranules  uint64 `json:"Initial Granules"`
	SelectedGranules uint64 `json:"Selected Granules"`
}

func assertRawTextIndexPrunesGranules(t *testing.T, planText string) {
	t.Helper()

	var envelopes []rawTextExplainEnvelope
	if err := json.Unmarshal([]byte(planText), &envelopes); err != nil {
		t.Fatalf("decode raw text structured EXPLAIN: %v\n%s", err, planText)
	}
	if len(envelopes) != 1 {
		t.Fatalf("raw text structured EXPLAIN envelope count = %d, want 1", len(envelopes))
	}
	var evidence []rawTextExplainIndex
	var visit func(rawTextExplainNode)
	visit = func(node rawTextExplainNode) {
		for _, index := range node.Indexes {
			if index.Type == "Skip" && index.Name == "idx_raw_text" {
				evidence = append(evidence, index)
			}
		}
		for _, child := range node.Plans {
			visit(child)
		}
	}
	visit(envelopes[0].Plan)
	if len(evidence) != 1 {
		t.Fatalf(
			"structured EXPLAIN selected idx_raw_text %d times, want once\n%s",
			len(evidence),
			planText,
		)
	}
	index := evidence[0]
	if index.InitialParts == 0 || index.SelectedParts == 0 ||
		index.SelectedParts > index.InitialParts {
		t.Fatalf("idx_raw_text part evidence is invalid: %+v", index)
	}
	if index.InitialGranules < 3 || index.SelectedGranules == 0 ||
		index.SelectedGranules >= index.InitialGranules {
		t.Fatalf("idx_raw_text did not prune the multi-granule fixture: %+v", index)
	}
}
