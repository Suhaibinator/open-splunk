package queryexec

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	knowledgeCompatibilityPrimaryIndex   = "knowledge-compat-primary"
	knowledgeCompatibilitySecondaryIndex = "knowledge-compat-secondary"
	knowledgeCompatibilityHiddenIndex    = "knowledge-compat-hidden"
	knowledgeCompatibilityService        = "knowledge-compatibility"
)

var knowledgeCompatibilityRuntimeOutputFields = []string{
	"event_id",
	"regex_edge",
	"regex_preserve_edge",
	"json_edge",
	"alias_source",
	"alias_copy",
	"alias_preserve_source",
	"alias_preserve_dest",
	"calculated_source",
	"calculated_edge",
	"calculated_preserve_edge",
}

type compiledKnowledgeCompatibilityRuntime struct {
	all      clickhouse.CompiledQuery
	filtered clickhouse.CompiledQuery
	fixtures []knowledgeCompatibilityRuntimeFixture
}

type knowledgeCompatibilityRuntimeFixture struct {
	id          string
	indexName   string
	eventTime   time.Time
	indexTime   time.Time
	raw         []byte
	rawEncoding uint8
	fields      []knowledgeCompatibilityRuntimeField
}

type knowledgeCompatibilityRuntimeField struct {
	name       string
	value      any
	storedType eventfields.StoredValueType
}

type knowledgeCompatibilityRuntimeResult struct {
	rows         map[string][]searchjobs.Value
	filteredRows map[string][]searchjobs.Value
}

type knowledgeCompatibilityRuntimeObservation struct {
	caseID string
	assert func(*testing.T, knowledgeCompatibilityRuntimeResult)
}

func TestKnowledgeCompatibilityRuntimeEdgesAreCanonical(t *testing.T) {
	base := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	compileKnowledgeCompatibilityRuntime(
		t,
		"knowledge-compatibility-tenant",
		base,
		base.Add(10*time.Minute),
		base,
		base.Add(2*time.Minute),
	)
}

func compileKnowledgeCompatibilityRuntime(
	t *testing.T,
	tenantID string,
	base time.Time,
	indexTime time.Time,
	earliest time.Time,
	latest time.Time,
) compiledKnowledgeCompatibilityRuntime {
	t.Helper()
	program := knowledgeCompatibilityRuntimeProgram(t)
	if program.ObjectCount() != 7 || len(program.RegexExtractions()) != 2 ||
		len(program.JSONExtractions()) != 1 || len(program.Aliases()) != 2 ||
		len(program.CalculatedFields()) != 2 || len(program.OperatorKinds()) != 5 ||
		program.Charges().GeneratedFields != 7 {
		t.Fatalf(
			"compatibility runtime program = objects %d regex %d JSON %d aliases %d calculated %d operators %v charges %#v",
			program.ObjectCount(),
			len(program.RegexExtractions()),
			len(program.JSONExtractions()),
			len(program.Aliases()),
			len(program.CalculatedFields()),
			program.OperatorKinds(),
			program.Charges(),
		)
	}

	fixtures := knowledgeCompatibilityRuntimeFixtures(base, indexTime)
	knowledgeCompatibilityRequireCanonicalFixtures(t, fixtures)
	knowledgeCompatibilityRequireCanonicalObservations(t)

	compiler := clickhouse.Compiler{}
	compile := func(name string, source string, indexes []string) clickhouse.CompiledQuery {
		t.Helper()
		logical := knowledgeRuntimePlan(
			t,
			source,
			program,
			tenantID,
			indexes,
			indexTime,
			earliest,
			latest,
		)
		compiled, err := compiler.Compile(logical)
		if err != nil {
			t.Fatalf("compile compatibility runtime query %q: %v", name, err)
		}
		if !compiled.HasValidExecutionSeal() {
			t.Fatalf("compatibility runtime query %q has no valid execution seal", name)
		}
		for _, extraction := range program.RegexExtractions() {
			pattern := extraction.Pattern()
			if !slices.Contains(compiled.Args, any(pattern)) {
				t.Fatalf(
					"compatibility runtime query %q omits exact regex argument %q from %#v",
					name,
					pattern,
					compiled.Args,
				)
			}
		}
		if !slices.Contains(compiled.Args, any(knowledgeCompatibilityPrimaryIndex)) {
			t.Fatalf(
				"compatibility runtime query %q omits exact selector argument index=%q from %#v",
				name,
				knowledgeCompatibilityPrimaryIndex,
				compiled.Args,
			)
		}
		return compiled
	}

	all := compile(
		"all edges",
		`index=`+knowledgeCompatibilityPrimaryIndex+` OR index=`+knowledgeCompatibilitySecondaryIndex+
			` | search service=`+knowledgeCompatibilityService+` | sort 0 +event_id | table `+
			strings.Join(knowledgeCompatibilityRuntimeOutputFields, " "),
		[]string{knowledgeCompatibilityPrimaryIndex, knowledgeCompatibilitySecondaryIndex},
	)
	knowledgeCompatibilityRequireCompiledOutputs(t, "all edges", all, knowledgeCompatibilityRuntimeOutputFields)

	filtered := compile(
		"extraction before base filter",
		`index=`+knowledgeCompatibilityPrimaryIndex+` service=`+knowledgeCompatibilityService+
			` regex_edge=filter | table event_id regex_edge`,
		[]string{knowledgeCompatibilityPrimaryIndex},
	)
	knowledgeCompatibilityRequireCompiledOutputs(
		t,
		"extraction before base filter",
		filtered,
		[]string{"event_id", "regex_edge"},
	)
	if !slices.Contains(filtered.Args, any("filter")) {
		t.Fatalf("compatibility filtered query omits exact extracted-field predicate: %#v", filtered.Args)
	}

	return compiledKnowledgeCompatibilityRuntime{
		all:      all,
		filtered: filtered,
		fixtures: fixtures,
	}
}

func knowledgeCompatibilityRequireCompiledOutputs(
	t *testing.T,
	name string,
	compiled clickhouse.CompiledQuery,
	want []string,
) {
	t.Helper()
	if !slices.Equal(compiled.OutputFields, want) {
		t.Fatalf(
			"compatibility runtime query %q outputs = %#v, want %#v",
			name,
			compiled.OutputFields,
			want,
		)
	}
	validated, ok := compiled.ValidatedResultContainerOutputs()
	if !ok || !slices.Equal(validated, compiled.ContainerOutputs) {
		t.Fatalf(
			"compatibility runtime query %q container outputs = (%#v, %t), want exact validated %#v",
			name,
			validated,
			ok,
			compiled.ContainerOutputs,
		)
	}
}

func knowledgeCompatibilityRuntimeProgram(t *testing.T) knowledgeprogram.Program {
	t.Helper()
	replace := opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING
	preserve := opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING
	selector := func() *opensplunkv1.KnowledgeSelector {
		return &opensplunkv1.KnowledgeSelector{IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
			Value:     knowledgeCompatibilityPrimaryIndex,
			MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
		}}}
	}
	definitions := []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "knowledge-app", Name: "a-compat-regex",
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector:     selector(),
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
					InputField: "_raw", OverwriteBehavior: replace,
					Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
						Regex: &opensplunkv1.RegexFieldExtractionDefinition{
							Pattern:      `kind=(?P<regex_edge>[a-z]+)?$`,
							OutputFields: []string{"regex_edge"},
						},
					},
				},
			},
		},
		{
			AppId: "knowledge-app", Name: "b-compat-regex-preserve",
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector:     selector(),
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
					InputField: "_raw", OverwriteBehavior: preserve,
					Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
						Regex: &opensplunkv1.RegexFieldExtractionDefinition{
							Pattern:      `kind=(?P<regex_preserve_edge>[a-z]+)?$`,
							OutputFields: []string{"regex_preserve_edge"},
						},
					},
				},
			},
		},
		{
			AppId: "knowledge-app", Name: "c-compat-json",
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector:     selector(),
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
					InputField: "_raw", OverwriteBehavior: replace,
					Extraction: &opensplunkv1.FieldExtractionDefinition_Json{
						Json: &opensplunkv1.JsonFieldExtractionDefinition{
							Path: "nested.value", OutputField: "json_edge",
						},
					},
				},
			},
		},
		knowledgeRuntimeAliasDefinitionWithOverwrite(
			"a-compat-alias-replace",
			"alias_source",
			"alias_copy",
			selector(),
			replace,
		),
		knowledgeRuntimeAliasDefinitionWithOverwrite(
			"b-compat-alias-preserve",
			"alias_preserve_source",
			"alias_preserve_dest",
			selector(),
			preserve,
		),
		{
			AppId: "knowledge-app", Name: "a-compat-calculated",
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector:     selector(),
			Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
				CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
					DestinationField:  "calculated_edge",
					Expression:        "calculated_source",
					OverwriteBehavior: replace,
				},
			},
		},
		{
			AppId: "knowledge-app", Name: "b-compat-calculated-preserve",
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector:     selector(),
			Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
				CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
					DestinationField:  "calculated_preserve_edge",
					Expression:        "calculated_source",
					OverwriteBehavior: preserve,
				},
			},
		},
	}
	return knowledgeRuntimePrepareProgram(t, definitions)
}

func knowledgeCompatibilityRuntimeFixtures(
	base time.Time,
	indexTime time.Time,
) []knowledgeCompatibilityRuntimeFixture {
	utf8Encoding := uint8(opensplunkv1.RawEncoding_RAW_ENCODING_UTF8)
	binaryEncoding := uint8(opensplunkv1.RawEncoding_RAW_ENCODING_BINARY)
	fixture := func(
		id string,
		indexName string,
		offset time.Duration,
		raw string,
		rawEncoding uint8,
		fields ...knowledgeCompatibilityRuntimeField,
	) knowledgeCompatibilityRuntimeFixture {
		return knowledgeCompatibilityRuntimeFixture{
			id:          id,
			indexName:   indexName,
			eventTime:   base.Add(offset),
			indexTime:   indexTime,
			raw:         []byte(raw),
			rawEncoding: rawEncoding,
			fields:      fields,
		}
	}
	stringField := func(name string, value string) knowledgeCompatibilityRuntimeField {
		return knowledgeCompatibilityRuntimeField{
			name:       name,
			value:      clickhousedriver.NewDynamic(value),
			storedType: eventfields.StoredValueTypeString,
		}
	}
	nullField := func(name string) knowledgeCompatibilityRuntimeField {
		return knowledgeCompatibilityRuntimeField{
			name:       name,
			value:      clickhousedriver.NewDynamic(nil),
			storedType: eventfields.StoredValueTypeNull,
		}
	}

	return []knowledgeCompatibilityRuntimeFixture{
		fixture("compat-regex-filter", knowledgeCompatibilityPrimaryIndex, time.Second, "kind=filter", utf8Encoding),
		fixture("compat-regex-overwrite", knowledgeCompatibilityPrimaryIndex, 2*time.Second, "kind=replaced", utf8Encoding,
			stringField("regex_edge", "old-regex")),
		fixture("compat-regex-preserve", knowledgeCompatibilityPrimaryIndex, 2500*time.Millisecond, "kind=must-not-replace", utf8Encoding,
			stringField("regex_preserve_edge", "keep-regex-preserve")),
		fixture("compat-regex-empty", knowledgeCompatibilityPrimaryIndex, 3*time.Second, "kind=", utf8Encoding,
			stringField("regex_edge", "old-empty")),
		fixture("compat-regex-no-match", knowledgeCompatibilityPrimaryIndex, 4*time.Second, "not-a-match", utf8Encoding,
			stringField("regex_edge", "keep-regex")),
		fixture("compat-regex-bytes", knowledgeCompatibilityPrimaryIndex, 5*time.Second, "kind=bytes", binaryEncoding,
			stringField("regex_edge", "keep-regex-bytes")),
		fixture("compat-json-no-match", knowledgeCompatibilityPrimaryIndex, 6*time.Second, `{"nested":{}}`, utf8Encoding,
			stringField("json_edge", "keep-json")),
		fixture("compat-json-null", knowledgeCompatibilityPrimaryIndex, 7*time.Second, `{"nested":{"value":null}}`, utf8Encoding,
			stringField("json_edge", "old-json-null")),
		fixture("compat-json-bytes", knowledgeCompatibilityPrimaryIndex, 8*time.Second, `{"nested":{"value":"bytes"}}`, binaryEncoding,
			stringField("json_edge", "keep-json-bytes")),
		fixture("compat-json-object", knowledgeCompatibilityPrimaryIndex, 9*time.Second, `{"nested":{"value":{"child":"object"}}}`, utf8Encoding,
			stringField("json_edge", "keep-json-object")),
		fixture("compat-json-array", knowledgeCompatibilityPrimaryIndex, 10*time.Second, `{"nested":{"value":["array"]}}`, utf8Encoding,
			stringField("json_edge", "keep-json-array")),
		fixture("compat-json-overwrite", knowledgeCompatibilityPrimaryIndex, 11*time.Second, `{"nested":{"value":"json-new"}}`, utf8Encoding,
			stringField("json_edge", "old-json")),
		fixture("compat-alias-overwrite", knowledgeCompatibilityPrimaryIndex, 12*time.Second, "alias", utf8Encoding,
			stringField("alias_source", "alias-new"), stringField("alias_copy", "alias-old")),
		fixture("compat-alias-null-source", knowledgeCompatibilityPrimaryIndex, 13*time.Second, "alias", utf8Encoding,
			nullField("alias_source"), stringField("alias_copy", "alias-old")),
		fixture("compat-alias-preserve-null", knowledgeCompatibilityPrimaryIndex, 14*time.Second, "alias", utf8Encoding,
			stringField("alias_preserve_source", "preserve-new"), nullField("alias_preserve_dest")),
		fixture("compat-alias-missing-source", knowledgeCompatibilityPrimaryIndex, 15*time.Second, "alias", utf8Encoding,
			stringField("alias_copy", "keep-alias")),
		fixture("compat-calculated-missing", knowledgeCompatibilityPrimaryIndex, 16*time.Second, "calculated", utf8Encoding,
			stringField("calculated_edge", "keep-calculated")),
		fixture("compat-calculated-null", knowledgeCompatibilityPrimaryIndex, 17*time.Second, "calculated", utf8Encoding,
			nullField("calculated_source"), stringField("calculated_edge", "old-calculated"),
			stringField("calculated_preserve_edge", "keep-calculated-null")),
		fixture("compat-selector-secondary", knowledgeCompatibilitySecondaryIndex, 18*time.Second, "kind=selector", utf8Encoding,
			stringField("regex_edge", "selector-kept")),
		fixture("compat-selector-hidden", knowledgeCompatibilityHiddenIndex, 19*time.Second, "kind=hidden", utf8Encoding),
	}
}

func knowledgeCompatibilityRequireCanonicalFixtures(
	t *testing.T,
	fixtures []knowledgeCompatibilityRuntimeFixture,
) {
	t.Helper()
	want := []string{
		"compat-regex-filter",
		"compat-regex-overwrite",
		"compat-regex-preserve",
		"compat-regex-empty",
		"compat-regex-no-match",
		"compat-regex-bytes",
		"compat-json-no-match",
		"compat-json-null",
		"compat-json-bytes",
		"compat-json-object",
		"compat-json-array",
		"compat-json-overwrite",
		"compat-alias-overwrite",
		"compat-alias-null-source",
		"compat-alias-preserve-null",
		"compat-alias-missing-source",
		"compat-calculated-missing",
		"compat-calculated-null",
		"compat-selector-secondary",
		"compat-selector-hidden",
	}
	got := make([]string, len(fixtures))
	seen := make(map[string]struct{}, len(fixtures))
	for index, fixture := range fixtures {
		got[index] = fixture.id
		if _, duplicate := seen[fixture.id]; duplicate {
			t.Fatalf("duplicate compatibility runtime fixture %q", fixture.id)
		}
		seen[fixture.id] = struct{}{}
		if fixture.indexName == "" || fixture.rawEncoding == 0 {
			t.Fatalf("incomplete compatibility runtime fixture %#v", fixture)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("compatibility runtime fixtures = %#v, want %#v", got, want)
	}
}

func insertKnowledgeCompatibilityRuntimeFixtures(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	tenantID string,
	fixtures []knowledgeCompatibilityRuntimeFixture,
) {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, field_types, field_metadata_version, " +
		"collector_id, batch_id, batch_sequence, expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatalf("prepare compatibility runtime fixture: %v", err)
	}
	for index, fixture := range fixtures {
		fields := slices.Clone(fixture.fields)
		slices.SortFunc(fields, func(left, right knowledgeCompatibilityRuntimeField) int {
			return strings.Compare(left.name, right.name)
		})
		document := clickhousedriver.NewJSON()
		fieldNames := make([]string, len(fields))
		fieldTypes := make([]uint8, len(fields))
		for fieldIndex, field := range fields {
			if fieldIndex > 0 && fields[fieldIndex-1].name == field.name {
				t.Fatalf("fixture %q duplicates field %q", fixture.id, field.name)
			}
			document.SetValueAtPath(field.name, field.value)
			fieldNames[fieldIndex] = field.name
			fieldTypes[fieldIndex] = uint8(field.storedType)
		}
		service := knowledgeCompatibilityService
		if err := batch.Append(
			fixture.id,
			tenantID,
			fixture.indexName,
			fixture.eventTime,
			fixture.indexTime,
			nil,
			uint8(1),
			"compatibility-host",
			"compatibility-source",
			"knowledge:compatibility",
			&service,
			uint8(1),
			nil,
			nil,
			fixture.raw,
			fixture.rawEncoding,
			nil,
			nil,
			document,
			fieldNames,
			fieldTypes,
			eventfields.CurrentFieldMetadataVersion,
			"knowledge-compatibility-collector",
			"knowledge-compatibility-batch",
			uint64(1_000+index),
			knowledgeRuntimeFixtureExpiresAt(),
			uint64(1),
		); err != nil {
			t.Fatalf("append compatibility runtime event %q: %v", fixture.id, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send compatibility runtime fixtures: %v", err)
	}
}

func runKnowledgeCompatibilityRuntime(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	executor *Executor,
	tenantID string,
	compiled compiledKnowledgeCompatibilityRuntime,
) {
	t.Helper()
	insertKnowledgeCompatibilityRuntimeFixtures(t, ctx, connection, tenantID, compiled.fixtures)

	all := &fakeSink{}
	if err := executor.Execute(ctx, compiled.all, all); err != nil {
		t.Fatalf("execute compatibility runtime edge query: %v", err)
	}
	knowledgeCompatibilityRequireResultSchema(t, all, knowledgeCompatibilityRuntimeOutputFields)
	rows := knowledgeCompatibilityRuntimeRows(t, all, len(compiled.fixtures)-1)
	if _, leaked := rows["compat-selector-hidden"]; leaked {
		t.Fatal("compatibility runtime query returned a row outside its authorized indexes")
	}

	filtered := &fakeSink{}
	if err := executor.Execute(ctx, compiled.filtered, filtered); err != nil {
		t.Fatalf("execute compatibility extracted-field base filter: %v", err)
	}
	knowledgeCompatibilityRequireResultSchema(t, filtered, []string{"event_id", "regex_edge"})
	filteredRows := knowledgeCompatibilityRuntimeRows(t, filtered, 1)

	result := knowledgeCompatibilityRuntimeResult{rows: rows, filteredRows: filteredRows}
	for _, observation := range knowledgeCompatibilityRuntimeObservations() {
		observation := observation
		t.Run(observation.caseID, func(t *testing.T) {
			observation.assert(t, result)
		})
	}
}

func knowledgeCompatibilityRequireResultSchema(t *testing.T, sink *fakeSink, fields []string) {
	t.Helper()
	want := make([]searchjobs.Column, len(fields))
	want[0] = searchjobs.Column{Name: fields[0], Kind: searchjobs.ValueKindString}
	for index := 1; index < len(fields); index++ {
		want[index] = searchjobs.Column{
			Name: fields[index], Kind: searchjobs.ValueKindMixed, Nullable: true,
		}
	}
	if sink.setCalls != 1 || !slices.Equal(sink.schema.Columns, want) {
		t.Fatalf(
			"compatibility runtime schema = %#v calls %d, want %#v/1",
			sink.schema,
			sink.setCalls,
			want,
		)
	}
}

func knowledgeCompatibilityRuntimeRows(
	t *testing.T,
	sink *fakeSink,
	wantCount int,
) map[string][]searchjobs.Value {
	t.Helper()
	if len(sink.rows) != wantCount {
		t.Fatalf("compatibility runtime rows = %d, want %d: %#v", len(sink.rows), wantCount, sink.rows)
	}
	rows := make(map[string][]searchjobs.Value, len(sink.rows))
	for _, row := range sink.rows {
		if len(row) != len(sink.schema.Columns) {
			t.Fatalf("compatibility runtime row width = %d, want %d: %#v", len(row), len(sink.schema.Columns), row)
		}
		id, ok := row[0].String()
		if !ok || id == "" {
			t.Fatalf("compatibility runtime row has invalid event_id: %#v", row)
		}
		if _, duplicate := rows[id]; duplicate {
			t.Fatalf("compatibility runtime result duplicates event_id %q", id)
		}
		rows[id] = row
	}
	return rows
}

func knowledgeCompatibilityRuntimeObservations() []knowledgeCompatibilityRuntimeObservation {
	return []knowledgeCompatibilityRuntimeObservation{
		{
			caseID: "selector.cross-index-per-row",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-regex-filter", "regex_edge", "filter")
				result.requireString(t, "compat-selector-secondary", "regex_edge", "selector-kept")
				result.requireMissingEvent(t, "compat-selector-hidden")
			},
		},
		{
			caseID: "extraction.before-base-filter",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireFilteredString(t, "compat-regex-filter", "regex_edge", "filter")
			},
		},
		{
			caseID: "extraction.no-match-preserves-destination",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-regex-no-match", "regex_edge", "keep-regex")
				result.requireString(t, "compat-json-no-match", "json_edge", "keep-json")
			},
		},
		{
			caseID: "alias.preserves-source",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-alias-overwrite", "alias_source", "alias-new")
				result.requireString(t, "compat-alias-overwrite", "alias_copy", "alias-new")
			},
		},
		{
			caseID: "alias.null-is-present",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-alias-preserve-null", "alias_preserve_source", "preserve-new")
				result.requireNull(t, "compat-alias-preserve-null", "alias_preserve_dest")
			},
		},
		{
			caseID: "alias.missing-source-does-not-erase",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-alias-missing-source", "alias_copy", "keep-alias")
			},
		},
		{
			caseID: "calculated.missing-does-not-erase",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-calculated-missing", "calculated_edge", "keep-calculated")
			},
		},
		{
			caseID: "extraction.optional-empty",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-regex-empty", "regex_edge", "")
			},
		},
		{
			caseID: "extraction.bytes-no-match",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-regex-bytes", "regex_edge", "keep-regex-bytes")
			},
		},
		{
			caseID: "extraction.json-null",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireNull(t, "compat-json-null", "json_edge")
			},
		},
		{
			caseID: "extraction.json-bytes-no-match",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-json-bytes", "json_edge", "keep-json-bytes")
			},
		},
		{
			caseID: "extraction.json-container-no-output",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-json-object", "json_edge", "keep-json-object")
				result.requireString(t, "compat-json-array", "json_edge", "keep-json-array")
			},
		},
		{
			caseID: "extraction.overwrite-true",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-regex-overwrite", "regex_edge", "replaced")
				result.requireString(t, "compat-regex-preserve", "regex_preserve_edge", "keep-regex-preserve")
				result.requireString(t, "compat-json-overwrite", "json_edge", "json-new")
			},
		},
		{
			caseID: "alias.overwrite-true",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireString(t, "compat-alias-overwrite", "alias_source", "alias-new")
				result.requireString(t, "compat-alias-overwrite", "alias_copy", "alias-new")
			},
		},
		{
			caseID: "alias.null-source",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireNull(t, "compat-alias-null-source", "alias_source")
				result.requireNull(t, "compat-alias-null-source", "alias_copy")
			},
		},
		{
			caseID: "calculated.present-null",
			assert: func(t *testing.T, result knowledgeCompatibilityRuntimeResult) {
				result.requireNull(t, "compat-calculated-null", "calculated_source")
				result.requireNull(t, "compat-calculated-null", "calculated_edge")
				result.requireString(t, "compat-calculated-null", "calculated_preserve_edge", "keep-calculated-null")
			},
		},
	}
}

func knowledgeCompatibilityRequireCanonicalObservations(t *testing.T) {
	t.Helper()
	want := []string{
		"selector.cross-index-per-row",
		"extraction.before-base-filter",
		"extraction.no-match-preserves-destination",
		"alias.preserves-source",
		"alias.null-is-present",
		"alias.missing-source-does-not-erase",
		"calculated.missing-does-not-erase",
		"extraction.optional-empty",
		"extraction.bytes-no-match",
		"extraction.json-null",
		"extraction.json-bytes-no-match",
		"extraction.json-container-no-output",
		"extraction.overwrite-true",
		"alias.overwrite-true",
		"alias.null-source",
		"calculated.present-null",
	}
	observations := knowledgeCompatibilityRuntimeObservations()
	got := make([]string, len(observations))
	seen := make(map[string]struct{}, len(observations))
	for index, observation := range observations {
		got[index] = observation.caseID
		if observation.caseID == "" || observation.assert == nil {
			t.Fatalf("invalid compatibility runtime observation %d: %#v", index, observation)
		}
		if _, duplicate := seen[observation.caseID]; duplicate {
			t.Fatalf("duplicate compatibility runtime observation %q", observation.caseID)
		}
		seen[observation.caseID] = struct{}{}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("compatibility runtime observations = %#v, want %#v", got, want)
	}
}

func (result knowledgeCompatibilityRuntimeResult) requireString(
	t *testing.T,
	eventID string,
	field string,
	want string,
) {
	t.Helper()
	value := result.requireValue(t, result.rows, eventID, field, knowledgeCompatibilityRuntimeOutputFields)
	got, ok := value.String()
	if !ok || got != want {
		t.Fatalf("compatibility %s.%s = %#v, want String %q", eventID, field, value, want)
	}
}

func (result knowledgeCompatibilityRuntimeResult) requireNull(
	t *testing.T,
	eventID string,
	field string,
) {
	t.Helper()
	value := result.requireValue(t, result.rows, eventID, field, knowledgeCompatibilityRuntimeOutputFields)
	if !value.IsNull() {
		t.Fatalf("compatibility %s.%s = %#v, want present null", eventID, field, value)
	}
}

func (result knowledgeCompatibilityRuntimeResult) requireFilteredString(
	t *testing.T,
	eventID string,
	field string,
	want string,
) {
	t.Helper()
	value := result.requireValue(
		t,
		result.filteredRows,
		eventID,
		field,
		[]string{"event_id", "regex_edge"},
	)
	got, ok := value.String()
	if !ok || got != want {
		t.Fatalf("compatibility filtered %s.%s = %#v, want String %q", eventID, field, value, want)
	}
}

func (result knowledgeCompatibilityRuntimeResult) requireMissingEvent(t *testing.T, eventID string) {
	t.Helper()
	if row, exists := result.rows[eventID]; exists {
		t.Fatalf("compatibility unauthorized event %q was returned: %#v", eventID, row)
	}
}

func (result knowledgeCompatibilityRuntimeResult) requireValue(
	t *testing.T,
	rows map[string][]searchjobs.Value,
	eventID string,
	field string,
	fields []string,
) searchjobs.Value {
	t.Helper()
	row, ok := rows[eventID]
	if !ok {
		t.Fatalf("compatibility result is missing event %q (events=%v)", eventID, sortedKnowledgeCompatibilityEventIDs(rows))
	}
	index := slices.Index(fields, field)
	if index < 0 || index >= len(row) {
		t.Fatalf("compatibility result has no field %q in %#v", field, fields)
	}
	return row[index]
}

func sortedKnowledgeCompatibilityEventIDs(rows map[string][]searchjobs.Value) []string {
	result := make([]string, 0, len(rows))
	for eventID := range rows {
		result = append(result, eventID)
	}
	slices.Sort(result)
	return result
}

func knowledgeCompatibilityRuntimeDebug(compiled compiledKnowledgeCompatibilityRuntime) string {
	return fmt.Sprintf(
		"all SQL=%d args=%d filtered SQL=%d args=%d fixtures=%d",
		len(compiled.all.SQL),
		len(compiled.all.Args),
		len(compiled.filtered.SQL),
		len(compiled.filtered.Args),
		len(compiled.fixtures),
	)
}
