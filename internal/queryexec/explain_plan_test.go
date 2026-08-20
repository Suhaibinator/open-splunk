package queryexec

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const validStructuredExplainPlan = `[
  {
    "Plan": {
      "Node Type": "Expression",
      "Node Id": "Expression_6",
      "Description": "private description",
      "Header": [
        {"Name": "event_time", "Type": "DateTime64(9, 'UTC')"}
      ],
      "Plans": [
        {
          "Node Type": "ReadFromMergeTree",
          "Node Id": "ReadFromMergeTree_0",
          "Description": "open_splunk.events",
          "Header": [
            {"Name": "event_time", "Type": "DateTime64(9, 'UTC')"},
            {"Name": "trace_id", "Type": "Nullable(String)"}
          ],
          "Indexes": [
            {
              "Type": "MinMax",
              "Keys": ["event_time"],
              "Condition": "private literal",
              "Initial Parts": 2,
              "Selected Parts": 1,
              "Initial Granules": 4,
              "Selected Granules": 3
            },
            {
              "Type": "Skip",
              "Name": "idx_trace_id",
              "Description": "bloom_filter GRANULARITY 1",
              "Initial Parts": 1,
              "Selected Parts": 1,
              "Initial Granules": 3,
              "Selected Granules": 1
            }
          ]
        }
      ]
    }
  }
]`

func TestParseExplainPlanProjectsBoundedPhysicalEvidence(t *testing.T) {
	t.Parallel()

	result := ExplainResult{
		Text:    validStructuredExplainPlan,
		QueryID: "open-splunk-explain-structured-test",
	}
	got, err := ParseExplainPlan(result)
	if err != nil {
		t.Fatal(err)
	}
	want := ExplainPlan{
		NodeTypes: []string{"Expression", "ReadFromMergeTree"},
		Reads: []ExplainRead{{
			Columns: []string{"event_time", "trace_id"},
			Indexes: []ExplainIndex{
				{
					Type:             "MinMax",
					Keys:             []string{"event_time"},
					InitialParts:     2,
					SelectedParts:    1,
					InitialGranules:  4,
					SelectedGranules: 3,
				},
				{
					Type:             "Skip",
					Name:             "idx_trace_id",
					Keys:             []string{},
					InitialParts:     1,
					SelectedParts:    1,
					InitialGranules:  3,
					SelectedGranules: 1,
				},
			},
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseExplainPlan() = %#v, want %#v", got, want)
	}
	rendered := fmt.Sprintf("%#v", got)
	if strings.Contains(rendered, "private") {
		t.Fatalf("safe projection retained a condition or description: %#v", got)
	}

	got.NodeTypes[0] = "mutated"
	got.Reads[0].Columns[0] = "mutated"
	got.Reads[0].Indexes[0].Keys[0] = "mutated"
	again, err := ParseExplainPlan(result)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(again, want) {
		t.Fatalf("ParseExplainPlan() retained caller mutations: %#v", again)
	}
}

func TestParseExplainPlanOmitsLiteralBearingHeaderAndKeyMetadata(
	t *testing.T,
) {
	t.Parallel()

	const private = "private-search-literal"
	const identifierShapedPrivate = "private_search_literal_7f2c"
	result := ExplainResult{
		Text: `[{"Plan":{"Node Type":"` + identifierShapedPrivate + `","Plans":[{` +
			`"Node Type":"ReadFromMergeTree","Header":[` +
			`{"Name":"event_time","Type":"DateTime64(9, 'UTC')"},` +
			`{"Name":"_part_offset","Type":"UInt64"},` +
			`{"Name":"equals(tenant_id, '` + private + `')","Type":"UInt8"},` +
			`{"Name":"` + identifierShapedPrivate + `","Type":"String"},` +
			`{"Name":"fields.` + identifierShapedPrivate + `","Type":"String"}],"Indexes":[{` +
			`"Type":"PrimaryKey","Keys":["tenant_id",` +
			`"equals(trace_id, '` + private + `')","` + private + `"],` +
			`"Initial Parts":2,"Selected Parts":1,` +
			`"Initial Granules":2,"Selected Granules":1},{` +
			`"Type":"` + identifierShapedPrivate + `",` +
			`"Name":"` + identifierShapedPrivate + `",` +
			`"Keys":["` + identifierShapedPrivate + `"],` +
			`"Initial Parts":1,"Selected Parts":1,` +
			`"Initial Granules":1,"Selected Granules":1}]}]}}]`,
		QueryID: "open-splunk-explain-literal-bearing-metadata",
	}
	got, err := ParseExplainPlan(result)
	if err != nil {
		t.Fatal(err)
	}
	want := ExplainPlan{
		NodeTypes: []string{"ReadFromMergeTree"},
		Reads: []ExplainRead{{
			Columns: []string{"event_time", "_part_offset"},
			Indexes: []ExplainIndex{{
				Type:             "PrimaryKey",
				Keys:             []string{"tenant_id"},
				InitialParts:     2,
				SelectedParts:    1,
				InitialGranules:  2,
				SelectedGranules: 1,
			}},
		}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseExplainPlan() = %#v, want %#v", got, want)
	}
	rendered := fmt.Sprintf("%#v", got)
	if strings.Contains(rendered, private) ||
		strings.Contains(rendered, identifierShapedPrivate) {
		t.Fatalf(
			"safe physical projection retained query metadata: %#v",
			got,
		)
	}
}

func TestParseExplainPlanAdmitsBoundedDroppedLookupExpressionHeader(
	t *testing.T,
) {
	t.Parallel()

	// Real lookup plans can render a MergeTree expression header above the
	// former 16 KiB ceiling. It remains private: only fixed physical names are
	// projected, while the raw value stays bounded by the EXPLAIN line limit.
	privateExpression := strings.Repeat("x", (16<<10)+1)
	result := ExplainResult{
		Text: "[\n" +
			`{"Plan":{"Node Type":"ReadFromMergeTree","Header":[` + "\n" +
			`{"Name":"` + privateExpression + `","Type":"UInt8"}` + "\n" +
			`],"Indexes":[]}}` + "\n]",
		QueryID: "open-splunk-explain-wide-lookup-header",
	}
	got, err := ParseExplainPlan(result)
	if err != nil {
		t.Fatal(err)
	}
	want := ExplainPlan{
		NodeTypes: []string{"ReadFromMergeTree"},
		Reads: []ExplainRead{{
			Columns: []string{},
			Indexes: []ExplainIndex{},
		}},
	}
	if !reflect.DeepEqual(got, want) ||
		strings.Contains(fmt.Sprintf("%#v", got), privateExpression) {
		t.Fatalf("wide private header projection = %#v, want %#v", got, want)
	}
	if !validExplainMetadata(strings.Repeat("x", maximumExplainPlanMetadataLen)) ||
		validExplainMetadata(strings.Repeat("x", maximumExplainPlanMetadataLen+1)) {
		t.Fatal("raw EXPLAIN metadata boundary is not exact")
	}
}

func TestParseExplainPlanProjectsOnlyKnownIndexMetadata(t *testing.T) {
	t.Parallel()

	index := func(
		indexType string,
		name string,
		keys []string,
	) rawExplainIndex {
		initialParts := uint64(5)
		selectedParts := uint64(3)
		initialGranules := uint64(5)
		selectedGranules := uint64(3)
		return rawExplainIndex{
			Type:             indexType,
			Name:             name,
			Keys:             keys,
			InitialParts:     &initialParts,
			SelectedParts:    &selectedParts,
			InitialGranules:  &initialGranules,
			SelectedGranules: &selectedGranules,
		}
	}
	tests := []struct {
		name string
		raw  rawExplainIndex
		want ExplainIndex
		safe bool
	}{
		{
			name: "MinMax",
			raw: index(
				"MinMax",
				"ignored_known_name",
				[]string{"event_time", "tenant_id"},
			),
			want: ExplainIndex{
				Type:             "MinMax",
				Keys:             []string{"event_time"},
				InitialParts:     5,
				SelectedParts:    3,
				InitialGranules:  5,
				SelectedGranules: 3,
			},
			safe: true,
		},
		{
			name: "Min-Max normalizes to MinMax",
			raw: index(
				"Min-Max",
				"ignored_known_name",
				[]string{"event_time", "tenant_id"},
			),
			want: ExplainIndex{
				Type:             "MinMax",
				Keys:             []string{"event_time"},
				InitialParts:     5,
				SelectedParts:    3,
				InitialGranules:  5,
				SelectedGranules: 3,
			},
			safe: true,
		},
		{
			name: "Partition",
			raw: index(
				"Partition",
				"",
				[]string{"toYYYYMM(event_time)", "event_time"},
			),
			want: ExplainIndex{
				Type:             "Partition",
				Keys:             []string{"toYYYYMM(event_time)"},
				InitialParts:     5,
				SelectedParts:    3,
				InitialGranules:  5,
				SelectedGranules: 3,
			},
			safe: true,
		},
		{
			name: "PrimaryKey",
			raw: index(
				"PrimaryKey",
				"",
				[]string{
					"tenant_id",
					"index_name",
					"toStartOfHour(event_time)",
					"event_time",
					"raw",
				},
			),
			want: ExplainIndex{
				Type: "PrimaryKey",
				Keys: []string{
					"tenant_id",
					"index_name",
					"toStartOfHour(event_time)",
					"event_time",
				},
				InitialParts:     5,
				SelectedParts:    3,
				InitialGranules:  5,
				SelectedGranules: 3,
			},
			safe: true,
		},
		{
			name: "known Skip",
			raw: index(
				"Skip",
				"idx_raw_text",
				[]string{"lowerUTF8(raw)", "raw"},
			),
			want: ExplainIndex{
				Type:             "Skip",
				Name:             "idx_raw_text",
				Keys:             []string{"lowerUTF8(raw)"},
				InitialParts:     5,
				SelectedParts:    3,
				InitialGranules:  5,
				SelectedGranules: 3,
			},
			safe: true,
		},
		{
			name: "unknown Skip name",
			raw: index(
				"Skip",
				"private_index_7f2c",
				[]string{"visibility_seq"},
			),
			want: ExplainIndex{
				Type:             "Skip",
				Keys:             []string{},
				InitialParts:     5,
				SelectedParts:    3,
				InitialGranules:  5,
				SelectedGranules: 3,
			},
			safe: true,
		},
		{
			name: "unknown type",
			raw: index(
				"private_index_type_7f2c",
				"idx_visibility_seq",
				[]string{"visibility_seq"},
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, safe, err := projectExplainIndex(test.raw)
			if err != nil {
				t.Fatal(err)
			}
			if safe != test.safe || !reflect.DeepEqual(got, test.want) {
				t.Fatalf(
					"projectExplainIndex() = (%#v, %v), want (%#v, %v)",
					got,
					safe,
					test.want,
					test.safe,
				)
			}
		})
	}
}

func TestParseExplainPlanAcceptsPlansWithoutPhysicalIndexEvidence(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name      string
		text      string
		nodeType  string
		readCount int
		columns   int
	}{
		{
			name:      "empty MergeTree becomes ReadNothing",
			text:      `[{"Plan":{"Node Type":"ReadNothing"}}]`,
			nodeType:  "ReadNothing",
			readCount: 0,
		},
		{
			name: "MergeTree read can omit Indexes",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree",` +
				`"Header":[{"Name":"event_time","Type":"UInt8"}]}}]`,
			nodeType:  "ReadFromMergeTree",
			readCount: 1,
			columns:   1,
		},
		{
			name: "MergeTree read can report empty Indexes",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree",` +
				`"Header":[{"Name":"event_time","Type":"UInt8"}],"Indexes":[]}}]`,
			nodeType:  "ReadFromMergeTree",
			readCount: 1,
			columns:   1,
		},
		{
			name: "literal-safe projection can omit every header name",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree",` +
				`"Header":[{"Name":"private_literal","Type":"UInt8"}]}}]`,
			nodeType:  "ReadFromMergeTree",
			readCount: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseExplainPlan(ExplainResult{
				Text:    test.text,
				QueryID: "open-splunk-explain-valid-indexless-plan",
			})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got.NodeTypes, []string{test.nodeType}) ||
				len(got.Reads) != test.readCount {
				t.Fatalf("ParseExplainPlan() = %#v", got)
			}
			if test.readCount != 0 &&
				(len(got.Reads[0].Columns) != test.columns ||
					len(got.Reads[0].Indexes) != 0) {
				t.Fatalf("indexless MergeTree read = %#v", got.Reads[0])
			}
		})
	}
}

func TestParseExplainPlanHonorsCancellationAndNodeBound(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := parseExplainPlanText(canceled, validStructuredExplainPlan)
	if !errors.Is(err, context.Canceled) ||
		!reflect.DeepEqual(got, ExplainPlan{}) {
		t.Fatalf("canceled parse = (%#v, %v)", got, err)
	}

	var nested strings.Builder
	nested.Grow(maximumExplainPlanNodes * 48)
	for range maximumExplainPlanNodes {
		nested.WriteString(`{"Node Type":"Expression","Plans":[`)
	}
	nested.WriteString(`{"Node Type":"ReadNothing"}`)
	for range maximumExplainPlanNodes {
		nested.WriteString(`]}`)
	}
	got, err = parseExplainPlanText(
		context.Background(),
		`[{"Plan":`+nested.String()+`}]`,
	)
	if !errors.Is(err, searchjobs.ErrExecutionLimit) ||
		!reflect.DeepEqual(got, ExplainPlan{}) {
		t.Fatalf("over-node parse = (%#v, %v)", got, err)
	}
}

func TestExplainPlanPreflightRejectsCollectionsBeforeTypedDecode(
	t *testing.T,
) {
	t.Parallel()

	repeatJSON := func(
		prefix string,
		value string,
		count int,
		suffix string,
	) string {
		t.Helper()
		var text strings.Builder
		text.Grow(len(prefix) + count*(len(value)+1) + len(suffix))
		text.WriteString(prefix)
		for index := range count {
			if index > 0 {
				text.WriteByte(',')
			}
			text.WriteString(value)
		}
		text.WriteString(suffix)
		return text.String()
	}

	tests := []struct {
		name string
		text string
	}{
		{
			name: "children",
			text: repeatJSON(
				`[{"Plan":{"Node Type":"Expression","Plans":[`,
				`{"Node Type":"ReadNothing"}`,
				maximumExplainPlanChildren+1,
				`]}}]`,
			),
		},
		{
			name: "headers",
			text: repeatJSON(
				`[{"Plan":{"Node Type":"ReadFromMergeTree","Header":[`,
				`{"Name":"x","Type":"UInt8"}`,
				maximumExplainPlanHeaders+1,
				`]}}]`,
			),
		},
		{
			name: "indexes",
			text: repeatJSON(
				`[{"Plan":{"Node Type":"ReadFromMergeTree",`+
					`"Header":[{"Name":"x","Type":"UInt8"}],"Indexes":[`,
				`{}`,
				maximumExplainPlanIndexes+1,
				`]}}]`,
			),
		},
		{
			name: "index keys",
			text: repeatJSON(
				`[{"Plan":{"Node Type":"ReadFromMergeTree",`+
					`"Header":[{"Name":"x","Type":"UInt8"}],"Indexes":[{"Keys":[`,
				`"x"`,
				maximumExplainPlanIndexKeys+1,
				`]}]}}]`,
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			err := preflightExplainPlan(context.Background(), test.text)
			if !errors.Is(err, searchjobs.ErrExecutionLimit) {
				t.Fatalf("preflightExplainPlan() error = %v", err)
			}
		})
	}
}

func TestParseExplainPlanRejectsMalformedOrIncompleteStructure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		text string
	}{
		{name: "plain text", text: "ReadFromMergeTree"},
		{name: "truncated JSON", text: `[{"Plan":`},
		{name: "empty envelope", text: `[]`},
		{
			name: "multiple envelopes",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree"}},{"Plan":{"Node Type":"ReadFromMergeTree"}}]`,
		},
		{name: "missing Plan", text: `[{}]`},
		{name: "missing node type", text: `[{"Plan":{"Plans":[]}}]`},
		{
			name: "actions present",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree","Actions":[],"Header":[{"Name":"x","Type":"UInt8"}],"Indexes":[{"Type":"PrimaryKey","Initial Parts":1,"Selected Parts":1,"Initial Granules":1,"Selected Granules":1}]}}]`,
		},
		{
			name: "duplicate header name",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree","Header":[{"Name":"x","Name":"y","Type":"UInt8"}]}}]`,
		},
		{
			name: "case variant duplicate header type",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree","Header":[{"Name":"x","Type":"UInt8","type":"String"}]}}]`,
		},
		{
			name: "duplicate index type",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree","Header":[{"Name":"x","Type":"UInt8"}],"Indexes":[{"Type":"PrimaryKey","Type":"Skip","Initial Parts":1,"Selected Parts":1,"Initial Granules":1,"Selected Granules":1}]}}]`,
		},
		{
			name: "case variant duplicate index counter",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree","Header":[{"Name":"x","Type":"UInt8"}],"Indexes":[{"Type":"PrimaryKey","Initial Parts":1,"initial parts":2,"Selected Parts":1,"Initial Granules":1,"Selected Granules":1}]}}]`,
		},
		{
			name: "read missing header",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree","Indexes":[{"Type":"PrimaryKey","Initial Parts":1,"Selected Parts":1,"Initial Granules":1,"Selected Granules":1}]}}]`,
		},
		{
			name: "selected parts exceed initial",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree","Header":[{"Name":"x","Type":"UInt8"}],"Indexes":[{"Type":"PrimaryKey","Initial Parts":1,"Selected Parts":2,"Initial Granules":1,"Selected Granules":1}]}}]`,
		},
		{
			name: "partial part counters",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree","Header":[{"Name":"x","Type":"UInt8"}],"Indexes":[{"Type":"PrimaryKey","Initial Parts":1,"Initial Granules":1,"Selected Granules":1}]}}]`,
		},
		{
			name: "empty header name",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree","Header":[{"Name":"","Type":"UInt8"}],"Indexes":[{"Type":"PrimaryKey","Initial Parts":1,"Selected Parts":1,"Initial Granules":1,"Selected Granules":1}]}}]`,
		},
		{
			name: "control in index name",
			text: `[{"Plan":{"Node Type":"ReadFromMergeTree","Header":[{"Name":"x","Type":"UInt8"}],"Indexes":[{"Type":"Skip","Name":"private\u0000index","Initial Parts":1,"Selected Parts":1,"Initial Granules":1,"Selected Granules":1}]}}]`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseExplainPlan(ExplainResult{
				Text:    test.text,
				QueryID: "open-splunk-explain-invalid-structure",
			})
			if !errors.Is(err, searchjobs.ErrInvalidResult) ||
				!reflect.DeepEqual(got, ExplainPlan{}) {
				t.Fatalf("ParseExplainPlan() = (%#v, %v)", got, err)
			}
			if strings.Contains(err.Error(), "private") ||
				strings.Contains(err.Error(), test.text) {
				t.Fatalf("ParseExplainPlan() leaked plan detail: %v", err)
			}
		})
	}
}
