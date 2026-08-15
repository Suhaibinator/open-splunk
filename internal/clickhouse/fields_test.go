package clickhouse

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileFieldCatalogValidatesBound(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	for _, maximum := range []uint32{0, 10_001} {
		if _, err := (Compiler{}).CompileFieldCatalog(logical, FieldCatalogSpec{MaximumFields: maximum}); err == nil {
			t.Fatalf("CompileFieldCatalog(MaximumFields=%d) succeeded", maximum)
		}
	}
}

func TestCompileFieldCatalogPreservesImmutableScanScope(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	before := buildPlan(t, `index=gradethis`)
	compiled := compileFieldCatalog(t, logical, 100)
	if !reflect.DeepEqual(logical, before) {
		t.Fatalf("CompileFieldCatalog mutated plan\nbefore: %#v\nafter:  %#v", before, logical)
	}

	for _, predicate := range []string{
		`"tenant_id" = ?`,
		`"index_name" IN (?)`,
		`"event_time" >= parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"event_time" < parseDateTime64BestEffort(?, 9, 'UTC')`,
		`"index_time" <= parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"expires_at" > parseDateTime64BestEffort(?, 3, 'UTC')`,
		`"visibility_seq" <= ?`,
	} {
		if strings.Count(compiled.SQL, predicate) != 1 {
			t.Fatalf("security predicate %q count != 1:\n%s", predicate, compiled.SQL)
		}
	}
	if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 {
		t.Fatalf("catalog must contain exactly one physical source scan:\n%s", compiled.SQL)
	}
	wantScope := []any{
		"tenant-1",
		"gradethis",
		"2026-07-21 00:00:00.000000000",
		"2026-07-22 00:00:00.000000000",
		"2026-07-22 00:00:01.000",
		"2026-07-22 00:00:01.000",
		uint64(73),
	}
	if len(compiled.Args) < len(wantScope) || !reflect.DeepEqual(compiled.Args[:len(wantScope)], wantScope) {
		t.Fatalf("scan scope args = %#v, want prefix %#v", compiled.Args, wantScope)
	}
}

func TestCompileFieldCatalogIsDeterministicAndParameterized(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis | eval copied=status | rename copied AS target${x} | table target${x},literal\.dot`)
	first := compileFieldCatalog(t, logical, 73)
	second := compileFieldCatalog(t, logical, 73)
	if first.SQL != second.SQL || !reflect.DeepEqual(first.Args, second.Args) || first.Spec != second.Spec {
		t.Fatalf("recompilation differed\nfirst:  %#v\nsecond: %#v", first, second)
	}
	if got, want := strings.Count(first.SQL, "?"), len(first.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nargs: %#v\nSQL: %s", got, want, first.Args, first.SQL)
	}
	if strings.Contains(first.SQL, "target${x}") || strings.Contains(first.SQL, `literal\.dot`) {
		t.Fatalf("logical field name was interpolated into catalog SQL:\n%s", first.SQL)
	}
	if !containsArgument(first.Args, "target${x}") || !containsArgument(first.Args, `literal\.dot`) {
		t.Fatalf("logical field names were not bound: %#v", first.Args)
	}
	if got := first.Args[len(first.Args)-1]; got != uint64(74) {
		t.Fatalf("profile limit arg = %#v, want MaximumFields+1", got)
	}
}

func TestCompileFieldCatalogFixedResultContract(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t, `index=gradethis`), 50)
	if strings.Count(compiled.SQL, " AS MATERIALIZED (") != 1 {
		t.Fatalf("materialized source CTE count != 1:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "FROM "+quoteIdentifier(fieldCatalogSourceCTE)); got != 2 {
		t.Fatalf("materialized final relation read count = %d, want one known/header pass plus one dynamic pass:\n%s", got, compiled.SQL)
	}
	for _, column := range []string{
		FieldCatalogRowKindColumn,
		FieldCatalogNameColumn,
		FieldCatalogObservedTypesColumn,
		FieldCatalogEventCountColumn,
		FieldCatalogNullCountColumn,
		FieldCatalogMissingCountColumn,
		FieldCatalogTotalEventsColumn,
		FieldCatalogInvalidColumn,
	} {
		if !strings.Contains(compiled.SQL, quoteIdentifier(column)) {
			t.Fatalf("fixed output column %q is missing:\n%s", column, compiled.SQL)
		}
	}
	for _, fragment := range []string{
		"toUInt8(0) AS " + quoteIdentifier(FieldCatalogRowKindColumn),
		"toUInt8(1) AS " + quoteIdentifier(FieldCatalogRowKindColumn),
		"arraySort(groupUniqArrayIf(",
		"countIf(\"__os_field_catalog_present_",
		"LIMIT ?",
		"ORDER BY " + quoteIdentifier(FieldCatalogRowKindColumn) + " ASC, " + quoteIdentifier(FieldCatalogNameColumn) + " ASC",
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("catalog SQL is missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	if strings.Contains(compiled.SQL, "GROUP BY Dynamic") || strings.Contains(compiled.SQL, `GROUP BY "__os_fields"`) {
		t.Fatalf("catalog grouped a Dynamic value:\n%s", compiled.SQL)
	}
}

func TestCompileFieldCatalogPrerequisiteUsesSingleSourceSentinelChain(t *testing.T) {
	t.Parallel()

	logical := buildEventStatsMinimumPlan(
		t,
		`index=gradethis | eventstats min(payload) AS low BY host`,
	)
	compiled := compileFieldCatalog(t, logical, 73)

	for _, name := range []string{
		fieldCatalogSourceCTE,
		fieldCatalogKnownRowsCTE,
		fieldCatalogRowsCTE,
		fieldCatalogObservationsCTE,
		fieldCatalogGroupsCTE,
		fieldCatalogControlledCTE,
		fieldCatalogExpandedCTE,
		fieldCatalogLimitedCTE,
	} {
		if got := strings.Count(compiled.SQL, quoteIdentifier(name)+" AS ("); got != 1 {
			t.Errorf("single-consumer CTE %q definitions = %d, want one\nSQL: %s", name, got, compiled.SQL)
		}
		if got := strings.Count(compiled.SQL, " FROM "+quoteIdentifier(name)); got != 1 {
			t.Errorf("single-consumer CTE %q reads = %d, want one\nSQL: %s", name, got, compiled.SQL)
		}
		if strings.Contains(compiled.SQL, quoteIdentifier(name)+" AS MATERIALIZED (") {
			t.Errorf("single-consumer CTE %q was materialized\nSQL: %s", name, compiled.SQL)
		}
	}
	for _, fragment := range []string{
		"arrayJoin(arrayConcat([tuple(toUInt8(0)",
		"arrayMap(field_metadata -> tuple(toUInt8(1)",
		"UNION ALL SELECT toUInt8(0), CAST('' AS String)",
		quoteIdentifier(internalFieldMetadataVersionColumn) + " != ?",
		"length(" + quoteIdentifier(internalFieldNamesColumn) + ") > ?",
		"length(" + quoteIdentifier(internalFieldTypesColumn) + ") > ?",
		quoteIdentifier(internalFieldNamesColumn) + " != arraySort(arrayDistinct(" +
			quoteIdentifier(internalFieldNamesColumn) + "))",
		"arrayExists(stored_type -> stored_type < ? OR stored_type > ?, " +
			quoteIdentifier(internalFieldTypesColumn) + ")",
		"GROUP BY " + quoteIdentifier(fieldCatalogObservedKind) + ", " +
			quoteIdentifier(fieldCatalogObservedName),
		"sumForEach(" + quoteIdentifier(fieldCatalogKnownCounts) + ")",
		"groupBitOrForEach(" + quoteIdentifier(fieldCatalogKnownTypeMasks) + ")",
		"max(if(" + quoteIdentifier(fieldCatalogGroupKind) + " = toUInt8(0), " +
			quoteIdentifier(fieldCatalogGroupInvalid) + ", toUInt8(0))) OVER ()",
		"arrayResize(arrayMap((field_name, field_index) -> tuple(toUInt8(1)",
		"LIMIT ?",
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("prerequisite sentinel catalog is missing %q\nSQL: %s", fragment, compiled.SQL)
		}
	}
	for _, forbidden := range []string{
		quoteIdentifier(fieldCatalogTotalsCTE),
		quoteIdentifier(fieldCatalogProfilesCTE),
		"sumMap(",
		"GROUPING SETS",
		"groupArray(",
	} {
		if strings.Contains(compiled.SQL, forbidden) {
			t.Errorf("prerequisite catalog retained forbidden parallel or unbounded graph %q\nSQL: %s", forbidden, compiled.SQL)
		}
	}
	groupPosition := strings.Index(compiled.SQL, quoteIdentifier(fieldCatalogGroupsCTE)+" AS (SELECT")
	windowPosition := strings.Index(compiled.SQL, quoteIdentifier(fieldCatalogControlledCTE)+" AS (SELECT")
	rowsPosition := strings.Index(compiled.SQL, quoteIdentifier(fieldCatalogRowsCTE)+" AS (SELECT")
	if groupPosition < 0 || windowPosition < 0 || windowPosition < groupPosition ||
		rowsPosition < 0 || strings.Contains(compiled.SQL[rowsPosition:groupPosition], " OVER ()") {
		t.Fatalf("catalog control window is not exclusively downstream of the bounded group:\n%s", compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, " AS MATERIALIZED ("); got != 1 {
		t.Fatalf("prerequisite catalog materialized CTEs = %d, want the sole chronological input fence:\n%s", got, compiled.SQL)
	}
	if got := strings.Count(compiled.SQL, "__os_chronological_final_input_"); got != 2 {
		t.Fatalf("prerequisite catalog final-input textual uses = %d, want definition plus one main consumer:\n%s", got, compiled.SQL)
	}
	if strings.Contains(compiled.SQL, "any("+quoteIdentifier(FieldCatalogRowKindColumn)+")") ||
		strings.Contains(compiled.SQL, " LIMIT 0") {
		t.Fatalf("prerequisite catalog inferred its validation schema from the complete final input:\n%s", compiled.SQL)
	}
	for _, fragment := range fieldCatalogValidationDummyProjection() {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Errorf("typed validation dummy is missing %q\nSQL: %s", fragment, compiled.SQL)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nargs: %#v\nSQL: %s", got, want, compiled.Args, compiled.SQL)
	}
	if got := compiled.Args[len(compiled.Args)-1]; got != uint64(compiled.Spec.MaximumFields)+2 {
		t.Fatalf("bounded result limit = %#v, want MaximumFields+2", got)
	}

	ordinaryPolicy := eventAnalysisFinalizationPolicy{materializeSharedCTEs: true}
	prerequisitePolicy := eventAnalysisFinalizationPolicy{materializeSharedCTEs: false}
	if got := fieldCatalogResultContractFor(ordinaryPolicy).sourceFanout; got != eventStatsCatalogSourceFanout {
		t.Fatalf("ordinary field-catalog source fanout = %d, want %d", got, eventStatsCatalogSourceFanout)
	}
	if got := fieldCatalogResultContractFor(prerequisitePolicy).sourceFanout; got != eventStatsOrdinarySourceFanout {
		t.Fatalf("prerequisite field-catalog source fanout = %d, want one", got)
	}
}

func TestPrerequisiteFieldCatalogSidecarWritersStayGenericAcrossKnownCount(t *testing.T) {
	t.Parallel()

	render := func(count int) string {
		fields := make([]compiledKnownField, count)
		for index := range fields {
			fields[index] = compiledKnownField{
				name:               fmt.Sprintf("logical-secret-%d", index),
				presenceSQL:        quoteIdentifier(fmt.Sprintf("known_present_%d", index)),
				typeSQL:            quoteIdentifier(fmt.Sprintf("known_type_%d", index)),
				relativeNamesSQL:   quoteIdentifier(fmt.Sprintf("retained_names_%d", index)),
				relativeTypesSQL:   quoteIdentifier(fmt.Sprintf("retained_types_%d", index)),
				metadataVersionSQL: quoteIdentifier(fmt.Sprintf("retained_version_%d", index)),
			}
		}
		var sql strings.Builder
		writePrerequisiteKnownFieldRows(&sql, fields)
		sql.WriteString(", ")
		sql.WriteString(quoteIdentifier(fieldCatalogRowsCTE))
		sql.WriteString(" AS (SELECT toUInt8(")
		writePrerequisiteFieldCatalogMetadataPredicate(&sql, true)
		sql.WriteString(") AS invalid, ")
		sql.WriteString(quoteIdentifier(fieldCatalogPackedKnownCounts))
		sql.WriteString(", ")
		sql.WriteString(quoteIdentifier(fieldCatalogPackedKnownMasks))
		sql.WriteString(", ")
		writePrerequisiteFieldCatalogCandidates(&sql, true)
		sql.WriteString(" FROM ")
		sql.WriteString(quoteIdentifier(fieldCatalogKnownRowsCTE))
		sql.WriteString("), ")
		writePrerequisiteFieldCatalogHeaderArray(&sql, fields)
		return sql.String()
	}

	narrow := render(1)
	wide := render(64)
	for _, sql := range []string{narrow, wide} {
		for _, generic := range []string{
			"arrayExists((relative_names, relative_types, relative_version)",
			"arrayFlatten(arrayMap(field_state",
			"arrayMap(field_state -> toUInt16(",
			"arraySlice(arrayFlatten(arrayMap((field_name, relative_names, relative_types)",
			"arrayResize(arrayMap((field_name, field_index)",
		} {
			if got := strings.Count(sql, generic); got != 1 {
				t.Errorf("generic sidecar expression %q count = %d, want one\nSQL: %s", generic, got, sql)
			}
		}
		if got := strings.Count(sql, "CAST(? AS Array(String))"); got != 2 {
			t.Errorf("bound root/name arrays = %d, want two independent fixed binds\nSQL: %s", got, sql)
		}
		if strings.Contains(sql, "logical-secret-") {
			t.Errorf("logical known name was interpolated instead of carried by one bound array:\n%s", sql)
		}
		for _, packed := range []string{
			fieldCatalogPackedKnownCounts,
			fieldCatalogPackedKnownMasks,
			fieldCatalogSidecarInvalid,
			fieldCatalogSidecarCandidates,
		} {
			if got := strings.Count(sql, quoteIdentifier(packed)); got != 2 {
				t.Errorf("packed sidecar output %q references = %d, want definition plus one downstream read\nSQL: %s", packed, got, sql)
			}
		}
	}
	if growth := len(wide) - len(narrow); growth <= 0 || growth > 32<<10 {
		t.Fatalf("64-field generic sidecar source growth = %d bytes, want only bounded producer/binding growth", growth)
	}

	thirteen := render(13)
	boundaryEnd := strings.Index(thirteen, ", "+quoteIdentifier(fieldCatalogRowsCTE)+" AS (SELECT")
	if boundaryEnd < 0 {
		t.Fatalf("packed sidecar boundary is missing:\n%s", thirteen)
	}
	boundary, downstream := thirteen[:boundaryEnd], thirteen[boundaryEnd:]
	rawProducerReferences := 0
	for index := range 13 {
		for name, want := range map[string]int{
			fmt.Sprintf("known_present_%d", index):    1,
			fmt.Sprintf("known_type_%d", index):       1,
			fmt.Sprintf("retained_names_%d", index):   1,
			fmt.Sprintf("retained_types_%d", index):   1,
			fmt.Sprintf("retained_version_%d", index): 1,
		} {
			rawProducerReferences += strings.Count(boundary, quoteIdentifier(name))
			if got := strings.Count(boundary, quoteIdentifier(name)); got != want {
				t.Errorf("guarded-boundary source %q references = %d, want %d", name, got, want)
			}
			if got := strings.Count(downstream, quoteIdentifier(name)); got != 0 {
				t.Errorf("sidecar source %q leaked through packed boundary %d times", name, got)
			}
		}
	}
	legacyRawReferences := 2*13 + 5*13
	packedRawReferences := 2*13 + 3*13
	if legacyRawReferences != 91 || packedRawReferences != 65 ||
		rawProducerReferences != packedRawReferences {
		t.Fatalf(
			"guarded-boundary raw producer references = %d, want 2K+3S=%d after 2K+5S=%d",
			rawProducerReferences,
			packedRawReferences,
			legacyRawReferences,
		)
	}
	packedReads := strings.Count(downstream, quoteIdentifier(fieldCatalogPackedKnownCounts)) +
		strings.Count(downstream, quoteIdentifier(fieldCatalogPackedKnownMasks)) +
		strings.Count(downstream, quoteIdentifier(fieldCatalogSidecarInvalid)) +
		strings.Count(downstream, quoteIdentifier(fieldCatalogSidecarCandidates))
	if packedReads != 4 {
		t.Fatalf("downstream packed catalog reads = %d, want 4", packedReads)
	}
	rawInput := ", [" + quoteIdentifier(fieldCatalogRawRowBinding) + "]) AS " +
		quoteIdentifier(fieldCatalogPackedRows)
	if got := strings.Count(boundary, rawInput); got != 1 {
		t.Fatalf("singleton scoped raw input count = %d, want one:\n%s", got, thirteen)
	}
	packedJoin := "ARRAY JOIN " + quoteIdentifier(fieldCatalogPackedRows) + " AS " +
		quoteIdentifier(fieldCatalogPackedRow)
	if got := strings.Count(boundary, packedJoin); got != 1 {
		t.Fatalf("singleton packed row join count = %d, want one:\n%s", got, thirteen)
	}
	lambdaStart := strings.Index(
		boundary,
		"arrayMap("+quoteIdentifier(fieldCatalogRowBinding)+" -> tuple(",
	)
	lambdaEnd := strings.Index(boundary, rawInput)
	if lambdaStart < 0 || lambdaEnd <= lambdaStart ||
		strings.Contains(boundary[lambdaEnd:], quoteIdentifier(fieldCatalogRowBinding)) {
		t.Fatalf("derived expressions escaped the scoped row-binding lambda:\n%s", thirteen)
	}
	perSidecarLimit := uint64(eventfields.MaximumStoredFieldsPerEvent) + 1
	if got := strings.Count(thirteen, fmt.Sprintf("toUInt64(%d)", perSidecarLimit)); got < 4 {
		t.Fatalf("pre-aggregate candidate sentinel bounds = %d, want canonical and retained arrays bounded\nSQL: %s", got, thirteen)
	}
	if want := fmt.Sprintf("toUInt64(%d)", 13*perSidecarLimit); !strings.Contains(thirteen, want) {
		t.Fatalf("packed sidecar generated-field bound %q is missing:\n%s", want, thirteen)
	}
}

func TestPrerequisiteFieldCatalogRejectsSidecarsPastGeneratedFieldCeiling(t *testing.T) {
	t.Parallel()

	fields := make([]compiledKnownField, knowledgeprogram.MaximumGeneratedFields+1)
	for index := range fields {
		fields[index] = compiledKnownField{
			relativeNamesSQL:   quoteIdentifier(fmt.Sprintf("retained_names_%d", index)),
			relativeTypesSQL:   quoteIdentifier(fmt.Sprintf("retained_types_%d", index)),
			metadataVersionSQL: quoteIdentifier(fmt.Sprintf("retained_version_%d", index)),
		}
	}
	_, err := finalizePrerequisiteFieldCatalog(
		compiledRelation{},
		nil,
		fields,
		nil,
		nil,
		false,
		FieldCatalogSpec{MaximumFields: 1},
		spl.Range{},
		eventAnalysisFinalizationPolicy{},
	)
	if err == nil || err.Error() != "compile ClickHouse prerequisite field catalog: retained knowledge sidecars exceed the generated-field limit" {
		t.Fatalf("sidecar ceiling error = %v", err)
	}
}

func TestPrerequisiteFieldCatalogRejectsKnownFieldsPastOverflowBound(t *testing.T) {
	t.Parallel()

	fields := make(
		[]compiledKnownField,
		maximumPrerequisiteFieldCatalogKnownFields+1,
	)
	_, err := finalizePrerequisiteFieldCatalog(
		compiledRelation{},
		nil,
		fields,
		nil,
		nil,
		false,
		FieldCatalogSpec{MaximumFields: MaximumFieldCatalogFields},
		spl.Range{},
		eventAnalysisFinalizationPolicy{},
	)
	if err == nil || err.Error() != "compile ClickHouse prerequisite field catalog: known fields exceed the catalog overflow bound" {
		t.Fatalf("known-field overflow-bound error = %v", err)
	}
}

func TestPrerequisiteFieldCatalogDenseKnownStateMatchesCounterSemantics(t *testing.T) {
	t.Parallel()

	type observation struct {
		present bool
		code    uint8
	}
	observations := make([]observation, 0, 32)
	for code := uint8(eventfields.StoredValueTypeNull); code <= uint8(eventfields.StoredValueTypeDecimal); code++ {
		observations = append(observations, observation{present: true, code: code})
	}
	observations = append(observations,
		observation{present: true, code: uint8(eventfields.StoredValueTypeNull)},
		observation{present: true, code: uint8(eventfields.StoredValueTypeString)},
		observation{present: false, code: uint8(eventfields.StoredValueTypeDecimal)},
		observation{present: true, code: 0},
		observation{present: true, code: 255},
	)
	encodeMask := func(present bool, code uint8) uint16 {
		ordinal := int(code) - int(eventfields.StoredValueTypeNull)
		ordinal = min(max(ordinal, 0), 11)
		base := uint16(1) << ordinal
		if !present ||
			code < uint8(eventfields.StoredValueTypeNull) ||
			code > uint8(eventfields.StoredValueTypeDecimal) {
			return 0
		}
		return base
	}

	var prior [13]uint64
	var presentCount, nullCount uint64
	var typeMask uint16
	for _, observation := range observations {
		if !observation.present {
			continue
		}
		prior[0]++
		presentCount++
		if observation.code >= uint8(eventfields.StoredValueTypeNull) &&
			observation.code <= uint8(eventfields.StoredValueTypeDecimal) {
			prior[observation.code]++
		}
		typeMask |= encodeMask(observation.present, observation.code)
		if observation.code == uint8(eventfields.StoredValueTypeNull) {
			nullCount++
		}
	}
	if presentCount != prior[0] || nullCount != prior[uint8(eventfields.StoredValueTypeNull)] {
		t.Fatalf(
			"dense counts = present %d null %d, prior = present %d null %d",
			presentCount,
			nullCount,
			prior[0],
			prior[uint8(eventfields.StoredValueTypeNull)],
		)
	}
	var decoded []uint8
	for code := uint8(eventfields.StoredValueTypeNull); code <= uint8(eventfields.StoredValueTypeDecimal); code++ {
		bit := uint16(1) << (code - uint8(eventfields.StoredValueTypeNull))
		if typeMask&bit != 0 {
			decoded = append(decoded, code)
		}
		if got := typeMask&bit != 0; got != (prior[code] != 0) {
			t.Errorf("dense type bit for code %d = %t, prior count = %d", code, got, prior[code])
		}
	}
	wantCodes := make([]uint8, 0, 12)
	for code := uint8(eventfields.StoredValueTypeNull); code <= uint8(eventfields.StoredValueTypeDecimal); code++ {
		wantCodes = append(wantCodes, code)
	}
	if !slices.Equal(decoded, wantCodes) {
		t.Fatalf("decoded durable type codes = %v, want %v", decoded, wantCodes)
	}
	if typeMask != uint16(0x0fff) ||
		typeMask&(uint16(1)<<11) == 0 ||
		typeMask&uint16(1) == 0 {
		t.Fatalf("dense all-type mask = %#04x, want bits 0..11 including Null and Decimal", typeMask)
	}
	if encodeMask(true, 0) != 0 || encodeMask(true, 255) != 0 ||
		encodeMask(false, uint8(eventfields.StoredValueTypeDecimal)) != 0 ||
		encodeMask(true, uint8(eventfields.StoredValueTypeDecimal)) != uint16(1)<<11 {
		t.Fatal("dense mask corrupt/missing/bit11 gates are not exact")
	}

	var emptyPresent, emptyNull uint64
	var emptyMask uint16
	if emptyPresent != 0 || emptyNull != 0 || emptyMask != 0 {
		t.Fatalf("empty dense state = %d/%d/%#x", emptyPresent, emptyNull, emptyMask)
	}
}

func TestPrerequisiteFieldCatalogDenseKnownStateIsFixedAndBoundedAtMaximumK(t *testing.T) {
	t.Parallel()

	const producerSampleCount = knowledgeprogram.MaximumGeneratedFields
	fields := make([]compiledKnownField, producerSampleCount)
	for index := range fields {
		fields[index] = compiledKnownField{
			name:        fmt.Sprintf("known-%d", index),
			presenceSQL: quoteIdentifier(fmt.Sprintf("producer_present_%d", index)),
			typeSQL:     quoteIdentifier(fmt.Sprintf("producer_type_%d", index)),
		}
	}

	var knownRows strings.Builder
	writePrerequisiteKnownFieldRows(&knownRows, fields)
	knownSQL := knownRows.String()
	for index := range fields {
		for _, producer := range []string{
			fmt.Sprintf("producer_present_%d", index),
			fmt.Sprintf("producer_type_%d", index),
		} {
			if got := strings.Count(knownSQL, quoteIdentifier(producer)); got != 1 {
				t.Fatalf("maximum-K raw producer %q references = %d, want one", producer, got)
			}
		}
	}
	for _, fragment := range []string{
		"toUInt8(ifNull(",
		"arrayFlatten(arrayMap(field_state",
		"arrayMap(field_state -> toUInt16(",
		"least(greatest(toInt16(tupleElement(field_state, 2)) - toInt16(1), toInt16(0)), toInt16(11))",
		"toUInt16(tupleElement(field_state, 2) >= toUInt8(1))",
		"toUInt16(tupleElement(field_state, 2) <= toUInt8(12))",
	} {
		if !strings.Contains(knownSQL, fragment) {
			t.Errorf("maximum-K dense state is missing %q\nSQL: %s", fragment, knownSQL)
		}
	}

	var observations strings.Builder
	writePrerequisiteFieldCatalogObservations(
		&observations,
		maximumPrerequisiteFieldCatalogKnownFields,
	)
	observationSQL := observations.String()
	for _, fragment := range []string{
		fmt.Sprintf(
			"arrayResize(CAST([], 'Array(UInt64)'), toUInt64(%d), toUInt64(0))",
			maximumPrerequisiteFieldCatalogKnownFields*2,
		),
		fmt.Sprintf(
			"arrayResize(CAST([], 'Array(UInt16)'), toUInt64(%d), toUInt16(0))",
			maximumPrerequisiteFieldCatalogKnownFields,
		),
		"CAST([], 'Array(UInt64)'), CAST([], 'Array(UInt16)')",
	} {
		if !strings.Contains(observationSQL, fragment) {
			t.Errorf("maximum-K synthetic/dynamic state is missing %q\nSQL: %s", fragment, observationSQL)
		}
	}

	var groups strings.Builder
	writePrerequisiteFieldCatalogGroups(&groups)
	if got := strings.Count(groups.String(), "sumForEach("+quoteIdentifier(fieldCatalogKnownCounts)+")"); got != 1 {
		t.Errorf("dense count aggregate count = %d, want one", got)
	}
	if got := strings.Count(groups.String(), "groupBitOrForEach("+quoteIdentifier(fieldCatalogKnownTypeMasks)+")"); got != 1 {
		t.Errorf("dense mask aggregate count = %d, want one", got)
	}

	var header strings.Builder
	writePrerequisiteFieldCatalogHeaderArray(
		&header,
		make([]compiledKnownField, maximumPrerequisiteFieldCatalogKnownFields),
	)
	headerSQL := header.String()
	for _, fragment := range []string{
		"bitAnd(toUInt16(arrayElement(",
		"bitShiftLeft(toUInt16(1), toUInt8(type_code - toUInt8(1)))",
		"field_index * toUInt64(2) + toUInt64(1)",
		"field_index * toUInt64(2) + toUInt64(2)",
	} {
		if !strings.Contains(headerSQL, fragment) {
			t.Errorf("dense header decoder is missing %q\nSQL: %s", fragment, headerSQL)
		}
	}

	var codes strings.Builder
	writeStoredFieldTypeCodeArray(&codes)
	var wantCodes strings.Builder
	wantCodes.WriteByte('[')
	for code := uint8(eventfields.StoredValueTypeNull); code <= uint8(eventfields.StoredValueTypeDecimal); code++ {
		if code > uint8(eventfields.StoredValueTypeNull) {
			wantCodes.WriteString(", ")
		}
		fmt.Fprintf(&wantCodes, "toUInt8(%d)", code)
	}
	wantCodes.WriteByte(']')
	if codes.String() != wantCodes.String() || strings.Contains(codes.String(), "toUInt8(0)") {
		t.Fatalf("decoded durable code domain = %q, want %q without invalid code zero", codes.String(), wantCodes.String())
	}

	countLength, maskLength, ok := prerequisiteFieldCatalogKnownVectorLengths(
		maximumPrerequisiteFieldCatalogKnownFields,
	)
	if !ok || countLength != uint64(maximumPrerequisiteFieldCatalogKnownFields*2) ||
		maskLength != uint64(maximumPrerequisiteFieldCatalogKnownFields) {
		t.Fatalf(
			"maximum-K dense vector lengths = %d/%d, %t",
			countLength,
			maskLength,
			ok,
		)
	}
	if _, _, ok := prerequisiteFieldCatalogKnownVectorLengths(
		maximumPrerequisiteFieldCatalogKnownFields + 1,
	); ok {
		t.Fatal("maximum-K+1 dense vector length was accepted")
	}
}

func TestCompileFieldCatalogEmitsHeaderForEmptyKnownDomain(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t, `index=gradethis`)
	logical.Operators = append(logical.Operators, &plan.Project{Mode: plan.ProjectModeTable})
	compiled := compileFieldCatalog(t, logical, 5)
	if !strings.Contains(compiled.SQL, "UNION ALL") ||
		!strings.Contains(compiled.SQL, "toUInt8(0) AS "+quoteIdentifier(FieldCatalogRowKindColumn)) {
		t.Fatalf("empty domain cannot return its header:\n%s", compiled.SQL)
	}
	if strings.Contains(compiled.SQL, `CAST(? AS String) AS "__os_field_catalog_profile_name"`) {
		t.Fatalf("empty exact schema unexpectedly emitted a known profile:\n%s", compiled.SQL)
	}
}

func TestCompileFieldCatalogProjectionAndShadowSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		source          string
		wantKnown       []string
		wantAbsentKnown []string
		wantShadowed    []string
		wantPrefixes    []string
		wantDynamic     bool
		wantTypePath    string
	}{
		{
			name: "include closes schema", source: `index=gradethis | fields status`,
			wantKnown: []string{"_raw", "_time", "status"}, wantDynamic: false, wantTypePath: "status",
		},
		{
			name: "exclude blocks exact", source: `index=gradethis | fields - status`,
			wantAbsentKnown: []string{"status"}, wantShadowed: []string{"status"}, wantDynamic: true,
		},
		{
			name: "table closes schema", source: `index=gradethis | table status`,
			wantKnown: []string{"status"}, wantAbsentKnown: []string{"_raw", "_time"}, wantDynamic: false, wantTypePath: "status",
		},
		{
			name: "rename moves dynamic type", source: `index=gradethis | rename logger AS component | table component`,
			wantKnown: []string{"component"}, wantShadowed: []string{"component", "logger"},
			wantPrefixes: []string{"component", "logger"}, wantDynamic: false, wantTypePath: "logger",
		},
		{
			name: "eval shadows stored destination", source: `index=gradethis | eval status=logger | table status`,
			wantKnown: []string{"status"}, wantShadowed: []string{"status"}, wantDynamic: false, wantTypePath: "logger",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileFieldCatalog(t, buildPlan(t, test.source), 100)
			known := catalogStringArguments(compiled.Args)
			for _, name := range test.wantKnown {
				if !slices.Contains(known, name) {
					t.Errorf("known names = %v, missing %q", known, name)
				}
			}
			for _, name := range test.wantAbsentKnown {
				if slices.Contains(known, name) {
					t.Errorf("known names = %v, unexpectedly include %q", known, name)
				}
			}
			shadows, prefixes, dynamic, ok := catalogDynamicControlArguments(compiled.Args)
			if !ok {
				t.Fatalf("dynamic control arguments missing: %#v", compiled.Args)
			}
			if dynamic != test.wantDynamic {
				t.Errorf("allowDynamic = %t, want %t", dynamic, test.wantDynamic)
			}
			for _, name := range test.wantShadowed {
				if !slices.Contains(shadows, name) {
					t.Errorf("shadow set = %v, missing %q", shadows, name)
				}
			}
			for _, name := range test.wantPrefixes {
				if !slices.Contains(prefixes, name) {
					t.Errorf("blocked prefixes = %v, missing %q", prefixes, name)
				}
			}
			if test.wantTypePath != "" && !containsArgument(compiled.Args, test.wantTypePath) {
				t.Errorf("metadata type path %q not bound: %#v", test.wantTypePath, compiled.Args)
			}
		})
	}
}

func TestCompileFieldCatalogAnalyzesRexFinalRelationAndPresence(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(
		t,
		buildPlan(t, `index=gradethis | rex field=duration "^(?<duration_value>\d+)(?<duration_unit>ms|µs)$" | table duration_value, duration_unit`),
		20,
	)
	if strings.Count(compiled.SQL, "extractGroups(") != 1 ||
		!strings.Contains(compiled.SQL, `"__os_rex_exists_`) {
		t.Fatalf("field catalog lost rex value or presence semantics:\n%s", compiled.SQL)
	}
	known := catalogStringArguments(compiled.Args)
	for _, field := range []string{"duration_value", "duration_unit"} {
		if !slices.Contains(known, field) {
			t.Fatalf("known fields = %v, missing rex output %q", known, field)
		}
	}
	if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
		t.Fatalf("placeholder count = %d, args = %d\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func TestCompileFieldCatalogBindsEscapedLogicalAndPhysicalNames(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t, `index=gradethis | table labels.kubernetes\.io/app,foo?bar`), 10)
	for _, name := range []string{`labels.kubernetes\.io/app`, "foo?bar"} {
		if !containsArgument(compiled.Args, name) {
			t.Errorf("logical name %q is not bound: %#v", name, compiled.Args)
		}
		if strings.Contains(compiled.SQL, name) {
			t.Errorf("logical name %q was interpolated into SQL:\n%s", name, compiled.SQL)
		}
	}
	if !containsArgument(compiled.Args, `labels.kubernetes\.io/app`) || !containsArgument(compiled.Args, "foo?bar") {
		t.Fatalf("normalized metadata paths are not bound: %#v", compiled.Args)
	}
}

func TestCompileFieldCatalogMarksInvalidMetadataWithoutGuessing(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t, `index=gradethis | table status`), 10)
	for _, fragment := range []string{
		quoteIdentifier(internalFieldMetadataVersionColumn) + " != ?",
		"length(" + quoteIdentifier(internalFieldNamesColumn) + ") > ?",
		"length(" + quoteIdentifier(internalFieldTypesColumn) + ") > ?",
		"length(" + quoteIdentifier(internalFieldNamesColumn) + ") != length(" + quoteIdentifier(internalFieldTypesColumn) + ")",
		quoteIdentifier(internalFieldNamesColumn) + " != arraySort(arrayDistinct(" + quoteIdentifier(internalFieldNamesColumn) + "))",
		"arrayExists(field_name -> empty(field_name) OR NOT isValidUTF8(field_name) OR length(field_name) > ?, " + quoteIdentifier(internalFieldNamesColumn) + ")",
		"arrayExists(stored_type -> stored_type < ? OR stored_type > ?, " + quoteIdentifier(internalFieldTypesColumn) + ")",
		"indexOf(" + quoteIdentifier(internalFieldNamesColumn) + ", ?)",
		"arrayZip(arraySlice(" + quoteIdentifier(internalFieldNamesColumn),
	} {
		if !strings.Contains(compiled.SQL, fragment) {
			t.Fatalf("metadata guard is missing %q:\n%s", fragment, compiled.SQL)
		}
	}
	for _, want := range []any{
		eventfields.CurrentFieldMetadataVersion,
		uint64(eventfields.MaximumStoredFieldsPerEvent),
		uint64(eventfields.MaximumNormalizedFieldNameBytes),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeDecimal),
	} {
		if !containsArgument(compiled.Args, want) {
			t.Errorf("metadata guard arg %#v is missing: %#v", want, compiled.Args)
		}
	}
	if strings.Contains(compiled.SQL, "dynamicType("+quoteIdentifier("status")+")") {
		t.Fatalf("projected dynamic field guessed its semantic type:\n%s", compiled.SQL)
	}
}

func TestCompileFieldCatalogPreservesKnownScalarTypeCodes(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t,
		`index=gradethis | eval signed=-7,unsigned=18446744073709551615,ratio=1.25,ok=true,text="x",nil=null | table signed,unsigned,ratio,ok,text,nil,_time`), 20)
	for _, code := range []eventfields.StoredValueType{
		eventfields.StoredValueTypeNull,
		eventfields.StoredValueTypeString,
		eventfields.StoredValueTypeUint64,
		eventfields.StoredValueTypeDouble,
		eventfields.StoredValueTypeBool,
		eventfields.StoredValueTypeTimestamp,
	} {
		if !containsArgument(compiled.Args, uint8(code)) {
			t.Errorf("stored type code %d missing: %#v", code, compiled.Args)
		}
	}
}

func TestCompileFieldCatalogAnalyzesNumericBinFinalType(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(
		t,
		buildPlan(t, `index=gradethis | eval signed=-11 | bin signed span=10 AS band | table band`),
		10,
	)
	if !strings.Contains(compiled.SQL, UnsupportedNumericBinValueMarker) ||
		!containsArgument(compiled.Args, uint8(eventfields.StoredValueTypeDouble)) {
		t.Fatalf("numeric-bin catalog lost its guarded Float64 final type:\n%s\nargs: %#v", compiled.SQL, compiled.Args)
	}
	if known := catalogStringArguments(compiled.Args); !slices.Contains(known, "band") {
		t.Fatalf("known catalog fields = %v, want band", known)
	}
}

func TestCompileFieldCatalogAssignsNullTypeToMissingDynamicEvalInputs(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t, `index=gradethis | eval copied=status | table copied`), 10)
	for _, want := range []string{
		"multiIf(indexOf(" + quoteIdentifier(internalFieldNamesColumn) + ", ?) != 0, arrayElement(" + quoteIdentifier(internalFieldTypesColumn),
		"isNull(\"copied\"), CAST(? AS UInt8), CAST(? AS UInt8))",
	} {
		if strings.Contains(compiled.SQL, want) {
			continue
		}
		t.Fatalf("dynamic eval has no missing-to-null semantic type fallback:\n%s", compiled.SQL)
	}
	if got := countArgument(compiled.Args, "status"); got < 2 {
		t.Fatalf("dynamic eval metadata path occurrence count = %d, want at least 2: %#v", got, compiled.Args)
	}
}

func TestCompileFieldCatalogRecognizesProvenDynamicObjectParents(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		source string
		value  string
	}{
		{name: "table", source: `index=gradethis | table parent`, value: "parent"},
		{name: "eval copy", source: `index=gradethis | eval copied=parent | table copied`, value: "copied"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			compiled := compileFieldCatalog(t, buildPlan(t, test.source), 10)
			for _, fragment := range []string{
				`arrayExists(name -> startsWith(name, ?), "__os_field_names")`,
				`isNull("` + test.value + `"), CAST(? AS UInt8), CAST(? AS UInt8))`,
			} {
				if !strings.Contains(compiled.SQL, fragment) {
					t.Fatalf("object-parent analysis is missing %q:\n%s", fragment, compiled.SQL)
				}
			}
			if !containsArgument(compiled.Args, uint8(eventfields.StoredValueTypeObject)) {
				t.Fatalf("object semantic code is not bound: %#v", compiled.Args)
			}
		})
	}
}

func TestCompileFieldCatalogDistinguishesBinaryRawFromUTF8String(t *testing.T) {
	t.Parallel()

	compiled := compileFieldCatalog(t, buildPlan(t, `index=gradethis | table _raw`), 10)
	if !strings.Contains(compiled.SQL, `("__os_raw_encoding" = 1)`) ||
		!strings.Contains(compiled.SQL, `isValidUTF8("_raw")`) ||
		!containsArgument(compiled.Args, uint8(eventfields.StoredValueTypeBytes)) {
		t.Fatalf("_raw semantic type does not distinguish binary bytes:\n%s\nargs: %#v", compiled.SQL, compiled.Args)
	}
}

func TestCompileFieldCatalogRejectsTransformingAndForgedPlans(t *testing.T) {
	t.Parallel()

	tests := []*plan.Query{
		buildPlan(t, `index=gradethis | stats count by status`),
		{Operators: []plan.Operator{buildPlan(t, `index=gradethis`).Operators[0], &plan.Aggregate{}}},
		{Operators: []plan.Operator{buildPlan(t, `index=gradethis`).Operators[0], (*plan.Project)(nil)}},
	}
	for _, logical := range tests {
		_, err := (Compiler{}).CompileFieldCatalog(logical, FieldCatalogSpec{MaximumFields: 10})
		diagnostic := &plan.Diagnostic{}
		ok := errors.As(err, &diagnostic)
		if !ok || diagnostic.Code != "SPL_UNSUPPORTED_FIELD_ANALYSIS_PIPELINE" {
			t.Errorf("CompileFieldCatalog(%#v) error = %#v", logical.Operators, err)
		}
	}
}

func compileFieldCatalog(t *testing.T, logical *plan.Query, maximum uint32) CompiledFieldCatalog {
	t.Helper()
	compiled, err := (Compiler{}).CompileFieldCatalog(logical, FieldCatalogSpec{MaximumFields: maximum})
	if err != nil {
		t.Fatalf("CompileFieldCatalog: %v", err)
	}
	return compiled
}

func containsArgument(arguments []any, want any) bool {
	return slices.ContainsFunc(arguments, func(argument any) bool { return reflect.DeepEqual(argument, want) })
}

func countArgument(arguments []any, want any) int {
	count := 0
	for _, argument := range arguments {
		if reflect.DeepEqual(argument, want) {
			count++
		}
	}
	return count
}

func catalogStringArguments(arguments []any) []string {
	names := make([]string, 0)
	for _, argument := range arguments {
		name, ok := argument.(string)
		if !ok || argument == "" {
			continue
		}
		names = append(names, name)
	}
	return names
}

func catalogDynamicControlArguments(arguments []any) (shadows, prefixes []string, allow bool, ok bool) {
	for index := range arguments {
		if index+2 >= len(arguments) {
			continue
		}
		var shadowsOK, prefixesOK, allowOK bool
		shadows, shadowsOK = arguments[index].([]string)
		prefixes, prefixesOK = arguments[index+1].([]string)
		allow, allowOK = arguments[index+2].(bool)
		if shadowsOK && prefixesOK && allowOK {
			return shadows, prefixes, allow, true
		}
	}
	return nil, nil, false, false
}
