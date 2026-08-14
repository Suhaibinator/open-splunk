package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestLookupResolutionValidatesAndDetachesAsset(t *testing.T) {
	t.Parallel()

	headers := []string{"service_id", "region", "owner"}
	rows := [][]string{{"api", "us", "platform"}}
	resolution, err := NewLookupResolution(
		"tenant-1",
		"service_catalog",
		"asset-1",
		7,
		uint64(len("service_id,region,owner\napi,us,platform\n")),
		sha256.Sum256([]byte("asset-v7")),
		headers,
		rows,
	)
	if err != nil {
		t.Fatalf("NewLookupResolution(): %v", err)
	}
	headers[0] = "changed"
	rows[0][0] = "changed"
	if got := resolution.Headers(); !slices.Equal(got, []string{"service_id", "region", "owner"}) {
		t.Fatalf("detached headers = %v", got)
	}
	if got := resolution.Rows(); len(got) != 1 ||
		!slices.Equal(got[0], []string{"api", "us", "platform"}) {
		t.Fatalf("detached rows = %v", got)
	}
	opened := resolution.Rows()
	opened[0][0] = "mutated"
	if resolution.Rows()[0][0] != "api" {
		t.Fatal("Rows returned an alias to retained authority")
	}
	internalClone := resolution.clone()
	if &internalClone.headers[0] != &resolution.headers[0] ||
		&internalClone.rows[0][0] != &resolution.rows[0][0] {
		t.Fatal("immutable internal clone duplicated the retained asset backing")
	}
}

func TestLookupResolutionFromImmutableAssetVersion(t *testing.T) {
	t.Parallel()

	asset, err := lookupasset.ParseCSV(
		strings.NewReader("service_id,owner\napi,platform\n"),
		lookupasset.Limits{},
	)
	if err != nil {
		t.Fatalf("ParseCSV(): %v", err)
	}
	version := lookupasset.Version{
		Ref: lookupasset.VersionRef{
			TenantID:      "tenant-1",
			LookupAssetID: "asset-1",
			Version:       4,
			SizeBytes:     uint64(len(asset.CanonicalCSV())),
			ContentSHA256: asset.ContentSHA256(),
		},
		Asset: asset,
	}
	resolution, err := NewLookupResolutionFromVersion("service_catalog", version)
	if err != nil {
		t.Fatalf("NewLookupResolutionFromVersion(): %v", err)
	}
	if resolution.TenantID() != "tenant-1" || resolution.ObjectID() != "asset-1" ||
		resolution.Version() != 4 || resolution.ContentSHA256() != asset.ContentSHA256() {
		t.Fatalf("bridged resolution identity = %#v", resolution)
	}
	if resolution.asset != asset || resolution.rows != nil {
		t.Fatal("immutable asset bridge duplicated the full row matrix")
	}

	version.Ref.ContentSHA256[0] ^= 0xff
	if _, err := NewLookupResolutionFromVersion("service_catalog", version); err == nil {
		t.Fatal("inconsistent asset version unexpectedly bridged")
	}
}

func TestLookupPreparationDetachesSelectedCSVCellBacking(t *testing.T) {
	largeUnselected := strings.Repeat("z", 60<<10)
	asset, err := lookupasset.ParseCSV(
		strings.NewReader("service_id,ignored,owner\napi,"+largeUnselected+",platform\n"),
		lookupasset.Limits{},
	)
	if err != nil {
		t.Fatalf("ParseCSV(): %v", err)
	}
	version := lookupasset.Version{
		Ref: lookupasset.VersionRef{
			TenantID:      "tenant-1",
			LookupAssetID: "asset-detached-selected",
			Version:       1,
			SizeBytes:     asset.CanonicalSizeBytes(),
			ContentSHA256: asset.ContentSHA256(),
		},
		Asset: asset,
	}
	resolution, err := NewLookupResolutionFromVersion("service_catalog", version)
	if err != nil {
		t.Fatalf("NewLookupResolutionFromVersion(): %v", err)
	}
	logical := buildPlan(
		t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner`,
	)
	resolution = bindTestLookupResolution(t, resolution, logical)
	scan, ok := logical.Operators[0].(*plan.Scan)
	if !ok {
		t.Fatalf("first operator = %T, want Scan", logical.Operators[0])
	}
	prepared, err := prepareLookupCompilation(
		logical,
		scan,
		[]LookupResolution{resolution},
	)
	if err != nil {
		t.Fatalf("prepareLookupCompilation(): %v", err)
	}
	source, ok := asset.Cell(0, 2)
	if !ok || source != "platform" {
		t.Fatalf("source owner cell = %q, %v", source, ok)
	}
	var selected string
	for _, column := range prepared.stages[0].selectedColumns {
		if column.headerIndex == 2 {
			selected = column.values[0]
		}
	}
	if selected != source {
		t.Fatalf("selected owner value = %q, want %q", selected, source)
	}
	if unsafe.StringData(selected) == unsafe.StringData(source) {
		t.Fatal("selected cell aliases encoding/csv row backing and can retain uncharged columns")
	}
}

func TestLookupResolutionRejectsEmptyCellAllocationAmplificationBeforeClone(t *testing.T) {
	// Intentionally not parallel: this walks the hard 6.4M-cell envelope and
	// should not contend with unrelated package tests.
	contract := testLookupContract(t, "service_catalog")
	headers := make([]string, 33)
	headers[0], headers[1] = "service_id", "owner"
	for index := 2; index < len(headers); index++ {
		headers[index] = "column_" + strconv.Itoa(index)
	}
	emptyRow := make([]string, len(headers))
	rows := make([][]string, MaximumLookupAssetRows)
	for index := range rows {
		// Shared backing deliberately makes the fixture small. The resolution
		// validator still charges every logical cell before any detached clone.
		rows[index] = emptyRow
	}
	resolution := func(objectID string) LookupResolution {
		return LookupResolution{
			tenantID:       "tenant-1",
			definitionName: contract.DefinitionName,
			logicalID:      "lookup-service-catalog",
			logicalVersion: 1,
			objectID:       objectID,
			version:        1,
			sizeBytes:      1,
			contentSHA256:  sha256.Sum256([]byte(objectID)),
			contract:       contract,
			contractSet:    true,
			headers:        headers,
			rows:           rows,
		}
	}
	_, err := (Compiler{}).WithLookupResolutions([]LookupResolution{
		resolution("asset-a"),
		resolution("asset-b"),
	})
	if err == nil || !strings.Contains(err.Error(), "cell-work budget") {
		t.Fatalf("WithLookupResolutions(empty-cell amplification) error = %v", err)
	}
}

func TestLookupSelectedCellBudgetIncludesPrivateMatchMarker(t *testing.T) {
	t.Parallel()

	contract := testLookupContract(t, "service_catalog")
	resolution := testLookupResolution(
		t,
		"tenant-1",
		[][]string{{"api", "platform"}},
	)
	cells, err := lookupSelectedCellCount(&contract, resolution)
	if err != nil {
		t.Fatalf("lookupSelectedCellCount(): %v", err)
	}
	// The key and output select two String cells; the sealed external table
	// also materializes one UInt8 match marker for this row.
	if cells != 3 {
		t.Fatalf("selected-cell work = %d, want 3 including match marker", cells)
	}

	perStage, ok := lookupExternalTableCellCount(MaximumLookupAssetRows, 4)
	if !ok || perStage != 500_000 {
		t.Fatalf("four-column maximum-row stage work = %d, %v", perStage, ok)
	}
	if 12*perStage > MaximumLookupSelectedCellsPerQuery ||
		13*perStage <= MaximumLookupSelectedCellsPerQuery {
		t.Fatalf(
			"aggregate marker-inclusive ceiling = %d, per-stage work = %d",
			MaximumLookupSelectedCellsPerQuery,
			perStage,
		)
	}
}

func TestLookupSealRevalidationDoesNotRematerializeSelectedCells(t *testing.T) {
	t.Parallel()

	logical := buildPlan(
		t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner`,
	)
	resolution := bindTestLookupResolution(
		t,
		testLookupResolution(t, "tenant-1", [][]string{{"api", "platform"}}),
		logical,
	)
	scan, ok := logical.Operators[0].(*plan.Scan)
	if !ok {
		t.Fatalf("first operator = %T, want Scan", logical.Operators[0])
	}
	prepared, err := prepareLookupCompilation(
		logical,
		scan,
		[]LookupResolution{resolution},
	)
	if err != nil {
		t.Fatalf("prepare lookup compilation: %v", err)
	}
	revalidated, err := prepareLookupCompilationForSeal(
		logical,
		scan,
		prepared.resolutions,
	)
	if err != nil {
		t.Fatalf("revalidate lookup compilation: %v", err)
	}
	if !preparedLookupCompilationEqual(prepared, revalidated) {
		t.Fatal("nonmaterializing seal preparation changed lookup authority")
	}
	if len(prepared.stages) != 1 || len(prepared.stages[0].selectedColumns) == 0 ||
		len(prepared.stages[0].selectedColumns[0].values) == 0 {
		t.Fatal("initial lookup preparation did not materialize selected values")
	}
	for _, column := range revalidated.stages[0].selectedColumns {
		if column.values != nil {
			t.Fatal("seal revalidation rematerialized selected lookup values")
		}
	}
}

func TestCompileLookupRequiresExactPinnedResolution(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner`,
	)
	_, err := (Compiler{}).Compile(logical)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_LOOKUP_UNRESOLVED" {
		t.Fatalf("Compile(unresolved) error = %#v", err)
	}

	wrongTenant := testLookupResolution(t, "other-tenant", [][]string{{"api", "platform"}})
	_, err = (Compiler{}).CompileWithLookupResolutions(logical, []LookupResolution{wrongTenant})
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_LOOKUP_UNAVAILABLE" {
		t.Fatalf("Compile(wrong tenant) error = %#v", err)
	}

	wrongName, createErr := NewLookupResolution(
		"tenant-1",
		"other_catalog",
		"asset-1",
		3,
		uint64(len("service_id,owner\napi,platform\n")),
		sha256.Sum256([]byte("asset-v3")),
		[]string{"service_id", "owner"},
		[][]string{{"api", "platform"}},
	)
	if createErr != nil {
		t.Fatalf("NewLookupResolution(wrong name): %v", createErr)
	}
	wrongName, createErr = wrongName.WithLogicalContract(
		testLookupContract(t, "other_catalog"),
		"lookup-other-catalog",
		1,
	)
	if createErr != nil {
		t.Fatalf("WithLogicalContract(wrong name): %v", createErr)
	}
	_, err = (Compiler{}).CompileWithLookupResolutions(logical, []LookupResolution{wrongName})
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_LOOKUP_UNAVAILABLE" {
		t.Fatalf("Compile(wrong name) error = %#v", err)
	}
}

func TestCompileLookupUsesSealedExternalTableAndExactLeftAnyJoin(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t,
		`index=gradethis | lookup service_catalog service_id AS service region AS host OUTPUT owner tier`,
	)
	resolution, err := NewLookupResolution(
		"tenant-1",
		"service_catalog",
		"asset-42",
		11,
		uint64(len("lookup-test-asset-v11")),
		sha256.Sum256([]byte("asset-v11")),
		[]string{"service_id", "region", "owner", "tier", "ignored"},
		[][]string{
			{"api", "us-west", "platform", "one", "not-bound"},
			{"worker", "us-east", "compute", "two", "not-bound-either"},
		},
	)
	if err != nil {
		t.Fatalf("NewLookupResolution(): %v", err)
	}
	resolution = bindTestLookupResolution(t, resolution, logical)
	compiled, err := (Compiler{}).CompileWithLookupResolutions(
		logical,
		[]LookupResolution{resolution},
	)
	if err != nil {
		t.Fatalf("CompileWithLookupResolutions(): %v", err)
	}
	for _, required := range []string{
		"LEFT ANY JOIN",
		`LEFT ANY JOIN "__os_lookup_table_`,
		`"service" = "__os_lookup_table_`,
		`"host" = "__os_lookup_table_`,
		`ifNull("__os_lookup_matched_`,
	} {
		if !strings.Contains(compiled.SQL, required) {
			t.Fatalf("compiled SQL missing %q:\n%s", required, compiled.SQL)
		}
	}
	for _, secret := range []string{"asset-42", "platform", "compute", "not-bound"} {
		if strings.Contains(compiled.SQL, secret) {
			t.Fatalf("compiled SQL contains bound lookup value %q", secret)
		}
	}
	if strings.Count(compiled.SQL, "?") != len(compiled.Args) {
		t.Fatalf("placeholder count = %d, args = %d", strings.Count(compiled.SQL, "?"), len(compiled.Args))
	}
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("compiled lookup query lacks a valid execution seal")
	}
	cloned, ok := compiled.CloneForExecution()
	if !ok || !cloned.EqualForExecution(compiled) {
		t.Fatal("compiled lookup authority did not clone for execution")
	}
	if containsLookupArgument(compiled.Args, "asset-42") ||
		containsLookupArgument(compiled.Args, []string{"api", "worker"}) {
		t.Fatalf("lookup rows were expanded into positional bind arguments: %#v", compiled.Args)
	}
	if len(compiled.lookupTables) != 1 {
		t.Fatalf("compiled lookup external tables = %#v", compiled.lookupTables)
	}
	payload := compiled.lookupTables[0]
	if payload.objectID != "asset-42" || payload.version != 11 ||
		len(payload.columns) != 4 ||
		!slices.Equal(payload.columns[0].values, []string{"api", "worker"}) ||
		!slices.Equal(payload.columns[2].values, []string{"platform", "compute"}) {
		t.Fatalf("compiled lookup external table = %#v", payload)
	}
	for _, column := range payload.columns {
		if slices.Equal(column.values, []string{"not-bound", "not-bound-either"}) {
			t.Fatal("compiler retained an unreferenced lookup column")
		}
	}
	tables, err := compiled.ExternalTablesForExecution(context.Background())
	if err != nil || len(tables) != 1 || tables[0].Name() != payload.name ||
		tables[0].Block().Rows() != 2 ||
		!strings.Contains(tables[0].Structure(), payload.matchedColumn+" UInt8") {
		t.Fatalf("materialized lookup table = (%#v, %v)", tables, err)
	}

	if &cloned.lookupTables[0].columns[0].values[0] !=
		&compiled.lookupTables[0].columns[0].values[0] {
		t.Fatal("compiled execution clone duplicated immutable lookup cell backing")
	}
	cloned.lookupTables[0].columns[0].values[0] = "tampered"
	if cloned.HasValidExecutionSeal() {
		t.Fatal("lookup payload mutation preserved the compiled execution seal")
	}
	if compiled.HasValidExecutionSeal() {
		t.Fatal("same-package corruption of shared immutable lookup backing preserved the source seal")
	}
}

func TestCompileLookupOutputNewWritesExplicitNullAndPreservesOccupiedValue(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUTNEW owner`,
	)
	resolution := bindTestLookupResolution(
		t,
		testLookupResolution(t, "tenant-1", [][]string{{"api", "platform"}}),
		logical,
	)
	compiled, err := (Compiler{}).CompileWithLookupResolutions(
		logical,
		[]LookupResolution{resolution},
	)
	if err != nil {
		t.Fatalf("CompileWithLookupResolutions(): %v", err)
	}
	if !strings.Contains(compiled.SQL, `AND NOT ifNull(`) {
		t.Fatalf("OUTPUTNEW did not guard the write by existing-field presence:\n%s", compiled.SQL)
	}
	if !strings.Contains(compiled.SQL, `isNotNull(`) {
		t.Fatalf("OUTPUTNEW did not treat an explicit null as writable:\n%s", compiled.SQL)
	}
	if strings.Count(compiled.SQL, "?") != len(compiled.Args) {
		t.Fatalf("placeholder count = %d, args = %d", strings.Count(compiled.SQL, "?"), len(compiled.Args))
	}
}

func TestLookupOutputNewPreservesDescendantOnlyContainer(t *testing.T) {
	t.Parallel()

	output, err := plan.ResolveField("payload", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(payload): %v", err)
	}
	compiled, err := compileLookupOutput(
		plan.LookupOutput{LookupField: "replacement", EventField: output},
		plan.LookupWriteModePreserveExisting,
		compileState{visible: map[string]fieldState{
			"payload": {
				valueSQL:      `"payload"`,
				existsSQL:     `"payload_exists"`,
				storedTypeSQL: `"payload_type"`,
				descendantSQL: `"payload_descendant"`,
				kind:          fieldKindDynamic,
			},
		}},
		`"matched" != 0`,
		`"replacement"`,
		1,
		0,
	)
	if err != nil {
		t.Fatalf("compileLookupOutput(): %v", err)
	}
	for _, expression := range []string{compiled.valueSQL, compiled.existsProjection} {
		if !strings.Contains(
			expression,
			`(("payload_exists") AND isNotNull("payload")) OR ("payload_descendant")`,
		) {
			t.Fatalf("OUTPUTNEW container occupancy is incomplete: %s", expression)
		}
	}
}

func TestCompileLookupPreservesSparseRawFieldsForMatchAndNoMatchRows(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner`,
	)
	compiled, err := (Compiler{}).CompileWithLookupResolutions(
		logical,
		[]LookupResolution{testLookupResolution(t, "tenant-1", [][]string{{"api", "platform"}})},
	)
	if err != nil {
		t.Fatalf("CompileWithLookupResolutions(): %v", err)
	}
	if !compiled.SparseFields || !slices.Contains(compiled.OutputFields, "fields") ||
		!slices.Contains(compiled.OutputFields, "owner") ||
		!strings.Contains(
			compiled.SQL,
			`"__os_field_names" AS "`+SparseEventFieldNamesColumn+`"`,
		) {
		t.Fatalf(
			"lookup discarded the complete sparse input row: fields %v sparse %v\n%s",
			compiled.OutputFields,
			compiled.SparseFields,
			compiled.SQL,
		)
	}
}

func TestCompileLookupRejectsStoredContractMismatch(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t,
		`index=gradethis | lookup service_catalog service_id AS service region AS host OUTPUT owner tier`,
	)
	var authored plan.Lookup
	for _, operator := range logical.Operators {
		if lookup, ok := operator.(*plan.Lookup); ok {
			authored = cloneLookupResolutionContract(*lookup)
			break
		}
	}
	if authored.DefinitionName == "" {
		t.Fatal("logical plan contains no lookup")
	}
	changedKey, err := plan.ResolveField("owner", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(changed key): %v", err)
	}
	changedOutput, err := plan.ResolveField("assignee", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(changed output): %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*plan.Lookup)
	}{
		{
			name: "ordered keys",
			mutate: func(contract *plan.Lookup) {
				contract.Keys[0], contract.Keys[1] = contract.Keys[1], contract.Keys[0]
			},
		},
		{
			name: "key mapping",
			mutate: func(contract *plan.Lookup) {
				contract.Keys[0].EventField = changedKey
			},
		},
		{
			name: "ordered outputs",
			mutate: func(contract *plan.Lookup) {
				contract.Outputs[0], contract.Outputs[1] = contract.Outputs[1], contract.Outputs[0]
			},
		},
		{
			name: "output mapping",
			mutate: func(contract *plan.Lookup) {
				contract.Outputs[0].EventField = changedOutput
			},
		},
		{
			name: "write mode",
			mutate: func(contract *plan.Lookup) {
				contract.WriteMode = plan.LookupWriteModePreserveExisting
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			stored := cloneLookupResolutionContract(authored)
			test.mutate(&stored)
			resolution, createErr := NewLookupResolutionWithContract(
				stored,
				"lookup-service-catalog",
				2,
				"tenant-1",
				"asset-1",
				3,
				uint64(len("service_id,region,owner,tier\napi,us,platform,one\n")),
				sha256.Sum256([]byte("asset-v3")),
				[]string{"service_id", "region", "owner", "tier"},
				[][]string{{"api", "us", "platform", "one"}},
			)
			if createErr != nil {
				t.Fatalf("NewLookupResolutionWithContract(): %v", createErr)
			}
			_, compileErr := (Compiler{}).CompileWithLookupResolutions(
				logical,
				[]LookupResolution{resolution},
			)
			var diagnostic *plan.Diagnostic
			if !errors.As(compileErr, &diagnostic) ||
				diagnostic.Code != "SPL_LOOKUP_DEFINITION_MISMATCH" {
				t.Fatalf("Compile(contract mismatch) error = %#v", compileErr)
			}
		})
	}
}

func TestLookupEvidenceDistinguishesLogicalReplaceAndDeduplicatesStages(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner`,
	)
	versionOne := testLookupResolution(
		t,
		"tenant-1",
		[][]string{{"api", "platform"}},
	)
	contract, ok := versionOne.LogicalContract()
	if !ok {
		t.Fatal("test resolution lacks logical contract")
	}
	versionTwo, err := versionOne.WithLogicalContract(
		contract,
		versionOne.LogicalID(),
		2,
	)
	if err != nil {
		t.Fatalf("WithLogicalContract(v2): %v", err)
	}
	compiledOne, err := (Compiler{}).CompileWithLookupResolutions(
		logical,
		[]LookupResolution{versionOne},
	)
	if err != nil {
		t.Fatalf("CompileWithLookupResolutions(v1): %v", err)
	}
	compiledTwo, err := (Compiler{}).CompileWithLookupResolutions(
		logical,
		[]LookupResolution{versionTwo},
	)
	if err != nil {
		t.Fatalf("CompileWithLookupResolutions(v2): %v", err)
	}
	first, firstOK := compiledOne.LookupAssetVersions()
	second, secondOK := compiledTwo.LookupAssetVersions()
	if !firstOK || !secondOK || len(first) != 1 || len(second) != 1 ||
		first[0].LookupID() != second[0].LookupID() ||
		first[0].LookupVersion() != 1 || second[0].LookupVersion() != 2 ||
		first[0].AssetID() != second[0].AssetID() ||
		first[0].AssetVersion() != second[0].AssetVersion() ||
		first[0].ContentSHA256() != second[0].ContentSHA256() ||
		compiledOne.EqualForExecution(compiledTwo) {
		t.Fatalf("logical replace evidence = first %#v/%v second %#v/%v", first, firstOK, second, secondOK)
	}

	repeated := buildPlan(t,
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner | lookup service_catalog service_id AS service OUTPUT owner`,
	)
	compiledRepeated, err := (Compiler{}).CompileWithLookupResolutions(
		repeated,
		[]LookupResolution{versionOne, versionOne},
	)
	if err != nil {
		t.Fatalf("CompileWithLookupResolutions(repeated): %v", err)
	}
	deduplicated, ok := compiledRepeated.LookupAssetVersions()
	if !ok || len(deduplicated) != 1 {
		t.Fatalf("repeated-stage evidence = %#v, %v", deduplicated, ok)
	}
}

func TestCompileLookupRejectsDuplicateCompositeKeysButUsesLengthFraming(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t,
		`index=gradethis | lookup service_catalog service_id AS service region AS host OUTPUT owner`,
	)
	valid, err := NewLookupResolution(
		"tenant-1",
		"service_catalog",
		"asset-1",
		1,
		uint64(len("lookup-test-asset")),
		sha256.Sum256([]byte("asset")),
		[]string{"service_id", "region", "owner"},
		[][]string{{"a", "bc", "first"}, {"ab", "c", "second"}},
	)
	if err != nil {
		t.Fatalf("NewLookupResolution(valid): %v", err)
	}
	valid = bindTestLookupResolution(t, valid, logical)
	if _, err := (Compiler{}).CompileWithLookupResolutions(
		logical,
		[]LookupResolution{valid},
	); err != nil {
		t.Fatalf("length-framed distinct keys rejected: %v", err)
	}

	duplicate, err := NewLookupResolution(
		"tenant-1",
		"service_catalog",
		"asset-2",
		2,
		uint64(len("lookup-test-asset-2")),
		sha256.Sum256([]byte("asset-2")),
		[]string{"service_id", "region", "owner"},
		[][]string{{"api", "us", "first"}, {"api", "us", "second"}},
	)
	if err != nil {
		t.Fatalf("NewLookupResolution(duplicate): %v", err)
	}
	duplicate = bindTestLookupResolution(t, duplicate, logical)
	_, err = (Compiler{}).CompileWithLookupResolutions(
		logical,
		[]LookupResolution{duplicate},
	)
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_LOOKUP_DUPLICATE_KEY" {
		t.Fatalf("Compile(duplicate key) error = %#v", err)
	}
}

func TestLookupExternalTablesPropagateThroughDerivedExecutables(t *testing.T) {
	t.Parallel()

	resolution := testLookupResolution(
		t,
		"tenant-1",
		[][]string{{"api", "platform"}},
	)
	compiler, err := (Compiler{}).WithLookupResolutions([]LookupResolution{resolution})
	if err != nil {
		t.Fatalf("WithLookupResolutions(): %v", err)
	}
	logical := func() *plan.Query {
		return buildPlan(t,
			`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner`,
		)
	}
	timeline, err := compiler.CompileTimeline(logical(), validTimelineSpec())
	if err != nil {
		t.Fatalf("CompileTimeline(): %v", err)
	}
	catalog, err := compiler.CompileFieldCatalog(
		logical(),
		FieldCatalogSpec{MaximumFields: 20},
	)
	if err != nil {
		t.Fatalf("CompileFieldCatalog(): %v", err)
	}
	summary, err := compiler.CompileFieldSummary(
		logical(),
		fieldSummaryTestSpec("owner"),
	)
	if err != nil {
		t.Fatalf("CompileFieldSummary(): %v", err)
	}
	suggestions, err := compiler.CompileFieldSuggestions(
		logical(),
		FieldSuggestionSpec{Prefix: "own", MaximumFields: 20},
	)
	if err != nil {
		t.Fatalf("CompileFieldSuggestions(): %v", err)
	}

	for name, executable := range map[string]interface {
		ExternalTablesForExecution(context.Context) ([]*ext.Table, error)
	}{
		"timeline":          timeline,
		"field catalog":     catalog,
		"field summary":     summary,
		"field suggestions": suggestions,
	} {
		tables, materializeErr := executable.ExternalTablesForExecution(context.Background())
		if materializeErr != nil || len(tables) != 1 || tables[0].Block().Rows() != 1 {
			t.Fatalf("%s external tables = (%#v, %v)", name, tables, materializeErr)
		}
	}

	parsed, err := spl.Parse(
		`index=gradethis | lookup service_catalog service_id AS service OUTPUT owner | stats avg(met*)`,
	)
	if err != nil {
		t.Fatalf("Parse(stats wildcard): %v", err)
	}
	preparation, err := plan.PrepareStatsWildcard(parsed, testChartScope())
	if err != nil {
		t.Fatalf("PrepareStatsWildcard(): %v", err)
	}
	inventory, err := compiler.CompileStatsWildcardInventory(
		preparation.Prefix(),
		preparation.Request(),
	)
	if err != nil {
		t.Fatalf("CompileStatsWildcardInventory(): %v", err)
	}
	tables, err := inventory.ExternalTablesForExecution(context.Background())
	if err != nil || len(tables) != 1 || tables[0].Block().Rows() != 1 {
		t.Fatalf("stats wildcard external tables = (%#v, %v)", tables, err)
	}
	clone, ok := inventory.CloneForExecution()
	if !ok {
		t.Fatal("CloneForExecution(stats wildcard) rejected lookup authority")
	}
	if &clone.lookupTables[0].columns[0].values[0] !=
		&inventory.lookupTables[0].columns[0].values[0] {
		t.Fatal("derived execution clone duplicated immutable lookup cell backing")
	}
	clone.lookupTables[0].columns[0].values[0] = "tampered"
	if clone.HasValidExecutionSeal() {
		t.Fatal("derived lookup payload mutation preserved execution authority")
	}
	if inventory.HasValidExecutionSeal() {
		t.Fatal("same-package corruption of shared derived lookup backing preserved the source seal")
	}
}

func testLookupResolution(t *testing.T, tenantID string, rows [][]string) LookupResolution {
	t.Helper()
	resolution, err := NewLookupResolution(
		tenantID,
		"service_catalog",
		"asset-1",
		3,
		uint64(len("service_id,owner\napi,platform\n")),
		sha256.Sum256([]byte("asset-v3")),
		[]string{"service_id", "owner"},
		rows,
	)
	if err != nil {
		t.Fatalf("NewLookupResolution(): %v", err)
	}
	resolution, err = resolution.WithLogicalContract(
		testLookupContract(t, "service_catalog"),
		"lookup-service-catalog",
		1,
	)
	if err != nil {
		t.Fatalf("WithLogicalContract(): %v", err)
	}
	return resolution
}

func bindTestLookupResolution(
	t *testing.T,
	resolution LookupResolution,
	logical *plan.Query,
) LookupResolution {
	t.Helper()
	for _, operator := range logical.Operators {
		if lookup, ok := operator.(*plan.Lookup); ok {
			bound, err := resolution.WithLogicalContract(
				*lookup,
				"lookup-"+lookup.DefinitionName,
				1,
			)
			if err != nil {
				t.Fatalf("WithLogicalContract(): %v", err)
			}
			return bound
		}
	}
	t.Fatal("logical plan contains no lookup")
	return LookupResolution{}
}

func testLookupContract(t *testing.T, name string) plan.Lookup {
	t.Helper()
	key, err := plan.ResolveField("service", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(service): %v", err)
	}
	output, err := plan.ResolveField("owner", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(owner): %v", err)
	}
	return plan.Lookup{
		DefinitionName: name,
		Keys: []plan.LookupKey{{
			LookupField: "service_id",
			EventField:  key,
		}},
		Outputs: []plan.LookupOutput{{
			LookupField: "owner",
			EventField:  output,
		}},
		WriteMode: plan.LookupWriteModeOverwrite,
	}
}

func containsLookupArgument(arguments []any, want any) bool {
	for _, argument := range arguments {
		switch want := want.(type) {
		case string:
			if got, ok := argument.(string); ok && got == want {
				return true
			}
		case uint64:
			if got, ok := argument.(uint64); ok && got == want {
				return true
			}
		case []string:
			if got, ok := argument.([]string); ok && slices.Equal(got, want) {
				return true
			}
		}
	}
	return false
}
