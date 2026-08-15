package clickhouse

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileFillNullPreservesFlattenedContainerTransport(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | fillnull value="fallback-界" parent | table event_id parent`,
	)
	wantOutputs := []string{"event_id", "parent"}
	wantContainers := []ResultContainerOutput{canonicalResultContainerOutput(1)}
	if !reflect.DeepEqual(compiled.OutputFields, wantOutputs) {
		t.Fatalf("fillnull parent outputs = %#v, want %#v", compiled.OutputFields, wantOutputs)
	}
	assertFillNullContainerDescriptors(t, compiled, wantContainers)
	if !strings.Contains(compiled.SQL, `WITH arrayJoin([`) ||
		!strings.Contains(compiled.SQL, `JSONExtractRaw(toJSONString("__os_fields")`) ||
		!strings.Contains(compiled.SQL, `AS "__os_fillnull_names_`) ||
		!strings.Contains(compiled.SQL, `AS "__os_fillnull_types_`) ||
		!strings.Contains(compiled.SQL, `AS "__os_fillnull_metadata_version_`) {
		t.Fatalf("fillnull parent lacks one lossless stored-container binding:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "fallback-界") ||
		!v03ArgumentContainsString(compiled.Args, "fallback-界") ||
		!v03ArgumentContainsString(compiled.Args, "parent.") {
		t.Fatalf("fillnull parent argument authority = %#v\nSQL: %s", compiled.Args, compiled.SQL)
	}
}

func TestCompileFillNullDescendantAndOverlappingOrdersKeepIndependentSidecars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		fields         string
		wantContainers []ResultContainerOutput
		wantBindings   int
	}{
		{
			name:           "exact descendant",
			fields:         "parent.child",
			wantContainers: []ResultContainerOutput{canonicalResultContainerOutput(1)},
			wantBindings:   1,
		},
		{
			name:   "parent then descendant",
			fields: "parent parent.child",
			wantContainers: []ResultContainerOutput{
				canonicalResultContainerOutput(1),
				canonicalResultContainerOutput(2),
			},
			wantBindings: 2,
		},
		{
			name:   "descendant then parent",
			fields: "parent.child parent",
			wantContainers: []ResultContainerOutput{
				canonicalResultContainerOutput(1),
				canonicalResultContainerOutput(2),
			},
			wantBindings: 2,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileSPL(t,
				`index=gradethis | fillnull value="fallback-界" `+test.fields+
					` | table event_id `+test.fields+` parent.sibling`,
			)
			assertFillNullContainerDescriptors(t, compiled, test.wantContainers)
			if got := strings.Count(compiled.SQL, `WITH arrayJoin([`); got != test.wantBindings {
				t.Fatalf("lossless fillnull bindings = %d, want %d:\n%s", got, test.wantBindings, compiled.SQL)
			}
			if got := strings.Count(compiled.SQL,
				`JSONExtractRaw(toJSONString("__os_fields")`); got != test.wantBindings {
				t.Fatalf("immutable stored-path materializers = %d, want %d:\n%s", got, test.wantBindings, compiled.SQL)
			}
			if strings.Contains(compiled.SQL, `JSONExtractRaw(toJSONString("parent")`) {
				t.Fatalf("fillnull descendant rewrites through the calculated parent:\n%s", compiled.SQL)
			}
			if strings.Count(compiled.SQL, "?") != len(compiled.Args) {
				t.Fatalf("placeholder/argument mismatch = %d/%d\nSQL: %s\nargs: %#v",
					strings.Count(compiled.SQL, "?"), len(compiled.Args), compiled.SQL, compiled.Args)
			}
			if !v03ArgumentContainsString(compiled.Args, "parent.child") ||
				!v03ArgumentContainsString(compiled.Args, "parent.child.") {
				t.Fatalf("descendant path authority is absent: %#v", compiled.Args)
			}
			if test.wantBindings == 2 && !v03ArgumentContainsString(compiled.Args, "parent.") {
				t.Fatalf("parent path authority is absent: %#v", compiled.Args)
			}
		})
	}
}

func TestV03FillNullPreservesFixedStringSemanticsBranchByBranch(t *testing.T) {
	t.Parallel()

	sourceRange := v03CompilerRange()
	field := v03CompilerField(t, "value", sourceRange)
	input := fieldState{
		valueSQL:        `"value"`,
		existsSQL:       `"value_present" = ?`,
		existsArgs:      []any{"value.path"},
		kind:            fieldKindString,
		caseSensitive:   true,
		textEligibleSQL: `"value_is_text"`,
		storedTypeSQL:   `"value_type"`,
	}
	state := compileState{
		visible:         map[string]fieldState{"value": input},
		context:         &compileContext{},
		publicOrder:     []string{"value"},
		blocked:         make(map[string]struct{}),
		blockedPrefixes: make(map[string]struct{}),
	}
	result, next, args, err := compileFillNull(
		newScanRelation(`SELECT "value", "value_present", "value_is_text", "value_type"`, sourceRange),
		&plan.FillNull{
			Fields: []plan.FieldRef{field}, Value: "safe", Range: sourceRange,
		},
		state,
		1,
	)
	if err != nil {
		t.Fatalf("compileFillNull: %v", err)
	}
	output := next.visible["value"]
	if output.kind != fieldKindDynamic || !output.caseSensitive {
		t.Fatalf("fillnull fixed String metadata = %#v", output)
	}
	for _, required := range []string{
		`WITH arrayJoin([tuple("_stage_1_fillnull_1"."value", toUInt8(ifNull("value_present" = ?, 0)),`,
		`isValidUTF8("_stage_1_fillnull_1"."value")`,
		`'bytes/v1'`,
		`base64Encode(assumeNotNull(tupleElement("__os_fillnull_string_source_1_1", 1)))`,
		`CAST(CAST(? AS String) AS Dynamic)`,
		`tupleElement("__os_fillnull_string_source_1_1", 3) != 0, 1)) AS "__os_fillnull_text_eligible_1_1"`,
		`AS "__os_fillnull_stored_type_1_1"`,
	} {
		if !strings.Contains(result.sql, required) {
			t.Fatalf("fixed String fillnull lost branch-aware provenance %q:\n%s", required, result.sql)
		}
	}
	if output.textEligibleSQL != `"__os_fillnull_text_eligible_1_1"` {
		t.Fatalf("fillnull output text proof = %q", output.textEligibleSQL)
	}
	if output.storedTypeSQL != `"__os_fillnull_stored_type_1_1"` {
		t.Fatalf("fillnull output stored type = %q", output.storedTypeSQL)
	}
	if !strings.Contains(result.sql, `SELECT * REPLACE (`) ||
		strings.Index(result.sql, `AS "__os_fillnull_text_eligible_1_1"`) <
			strings.Index(result.sql, `SELECT * REPLACE (`) {
		t.Fatalf("fillnull did not publish text proof beside the replacement:\n%s", result.sql)
	}
	if got, want := strings.Count(result.sql, "?"), len(args); got != want {
		t.Fatalf("fillnull placeholder count = %d, want %d:\n%s", got, want, result.sql)
	}
	if !reflect.DeepEqual(args, []any{"value.path", "safe"}) {
		t.Fatalf("fillnull arguments = %#v, want source presence then fill", args)
	}
}

func TestV03FillNullMaximumFixedStringShapeKeepsOneLayerPerField(t *testing.T) {
	t.Parallel()

	sourceRange := v03CompilerRange()
	state := compileState{
		visible:         make(map[string]fieldState, 64),
		context:         &compileContext{},
		publicOrder:     make([]string, 0, 64),
		blocked:         make(map[string]struct{}),
		blockedPrefixes: make(map[string]struct{}),
	}
	fields := make([]plan.FieldRef, 64)
	columns := make([]string, 0, 64)
	for index := range fields {
		name := fmt.Sprintf("value_%02d", index)
		fields[index] = v03CompilerField(t, name, sourceRange)
		column := quoteIdentifier(name)
		state.visible[name] = fieldState{
			valueSQL: column, existsSQL: "1", kind: fieldKindString,
			textEligibleSQL: "1",
		}
		state.publicOrder = append(state.publicOrder, name)
		columns = append(columns, column)
	}
	inputDepth := relationalNodeDepth()
	result, _, _, err := compileFillNull(
		compiledRelation{
			sql:        "SELECT " + strings.Join(columns, ", "),
			depth:      inputDepth,
			ownerRange: sourceRange,
		},
		&plan.FillNull{Fields: fields, Value: "safe", Range: sourceRange},
		state,
		1,
	)
	if err != nil {
		t.Fatalf("compileFillNull(64 fixed Strings): %v", err)
	}
	if got, want := result.depth, inputDepth+len(fields); got != want {
		t.Fatalf("fillnull 64-field depth = %d, want %d", got, want)
	}
	if err := validateRelationalDepth(result.depth, sourceRange); err != nil {
		t.Fatalf("fillnull 64-field relational depth: %v", err)
	}
	if got := strings.Count(result.sql, `__os_fillnull_text_eligible_`); got != len(fields) {
		t.Fatalf("fillnull text-proof aliases = %d, want %d", got, len(fields))
	}
}

func TestCompileFillNullMaterializesPrivateAggregateStringUnderPublicName(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(t,
		`index=gradethis | stats count BY service`+
			` | fillnull value="fallback" service | table service count`,
	)
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("stats -> fillnull query lacks a valid execution seal")
	}
	for _, required := range []string{
		`fillnull_1"."__os_group_0"`,
		` AS "service",`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("stats -> fillnull lost private-to-public materialization %q:\n%s", required, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, `fillnull_1"."service"`) {
		t.Fatalf("stats -> fillnull addressed a nonexistent public aggregate column:\n%s", compiled.SQL)
	}
}

func TestV03FixedRawProvenancePipelinesCompileWithAtomicConsumers(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source string
		marker string
	}{
		{
			source: `index=gradethis | eval copied=_raw | makemv delim="," copied`,
			marker: UnsupportedMakeMVValueMarker,
		},
		{
			source: `index=gradethis | eval copied=_raw | mvexpand copied`,
			marker: UnsupportedMVExpandValueMarker,
		},
		{
			source: `index=gradethis | eval copied=_raw | fillnull value="safe" copied | makemv delim="," copied`,
			marker: UnsupportedMakeMVValueMarker,
		},
		{
			source: `index=gradethis | eval copied=if(event_id="never",_raw,null)` +
				` | fillnull value="safe" copied | makemv delim="," copied | mvexpand copied`,
			marker: UnsupportedMVExpandValueMarker,
		},
	} {
		compiled := compileSPL(t, test.source)
		if !compiled.RequiresAtomicResult() || !strings.Contains(compiled.SQL, test.marker) {
			t.Fatalf("fixed raw pipeline lost atomic marker %q:\n%s", test.marker, compiled.SQL)
		}
	}
}

func TestV03FillNullPreservesIndexCaseSensitiveComparison(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | fillnull value="fallback" index`+
			` | where index="GRADETHIS" | table index`,
	)
	if !strings.Contains(compiled.SQL, `"index" = ?`) ||
		strings.Contains(compiled.SQL, `lowerUTF8(toString("index"))`) {
		t.Fatalf("fillnull changed index comparison semantics:\n%s", compiled.SQL)
	}
}

func assertFillNullContainerDescriptors(
	t *testing.T,
	compiled CompiledQuery,
	want []ResultContainerOutput,
) {
	t.Helper()
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("fillnull container query lacks a valid execution seal")
	}
	got, ok := compiled.ValidatedResultContainerOutputs()
	if !ok || !reflect.DeepEqual(got, want) ||
		!reflect.DeepEqual(compiled.ContainerOutputs, want) {
		t.Fatalf("fillnull container descriptors = %#v / valid %t, want %#v", got, ok, want)
	}
	seen := make(map[string]struct{}, len(want)*3)
	for _, descriptor := range want {
		for _, hidden := range []string{
			descriptor.NamesColumn(),
			descriptor.TypesColumn(),
			descriptor.MetadataVersionColumn(),
		} {
			if _, duplicate := seen[hidden]; duplicate {
				t.Fatalf("fillnull hidden transport column collides at %q", hidden)
			}
			seen[hidden] = struct{}{}
			if !strings.Contains(compiled.SQL, quoteIdentifier(hidden)) {
				t.Fatalf("fillnull SQL omits sealed hidden transport %q:\n%s", hidden, compiled.SQL)
			}
			for _, public := range compiled.OutputFields {
				if public == hidden {
					t.Fatalf("fillnull hidden transport %q leaked publicly", hidden)
				}
			}
		}
	}
}
