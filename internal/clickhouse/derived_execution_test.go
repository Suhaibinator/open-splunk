package clickhouse

import (
	"hash"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestDerivedExecutionSealsEveryPublicContractField(t *testing.T) {
	t.Parallel()

	compiler := Compiler{}
	logical := func() *plan.Query {
		return buildPlan(t, `index=gradethis level=error`)
	}
	timeline, err := compiler.CompileTimeline(logical(), validTimelineSpec())
	if err != nil {
		t.Fatalf("CompileTimeline: %v", err)
	}
	catalog, err := compiler.CompileFieldCatalog(logical(), FieldCatalogSpec{MaximumFields: 17})
	if err != nil {
		t.Fatalf("CompileFieldCatalog: %v", err)
	}
	summary, err := compiler.CompileFieldSummary(logical(), fieldSummaryTestSpec("level"))
	if err != nil {
		t.Fatalf("CompileFieldSummary: %v", err)
	}
	suggestions, err := compiler.CompileFieldSuggestions(
		logical(),
		FieldSuggestionSpec{Prefix: "lev", MaximumFields: 19},
	)
	if err != nil {
		t.Fatalf("CompileFieldSuggestions: %v", err)
	}

	if !timeline.HasValidExecutionSeal() || !catalog.HasValidExecutionSeal() ||
		!summary.HasValidExecutionSeal() || !suggestions.HasValidExecutionSeal() {
		t.Fatal("a compiler-produced derived execution is unsigned")
	}
	if count, ok := catalog.KnowledgeGeneratedFields(); !ok || count != 0 {
		t.Fatalf("legacy catalog generated-field evidence = (%d, %t), want (0, true)", count, ok)
	}

	assertTimelineMutationRejected := func(name string, mutate func(*CompiledTimeline)) {
		t.Helper()
		candidate := timeline
		mutate(&candidate)
		if candidate.HasValidExecutionSeal() {
			t.Errorf("timeline accepted %s mutation", name)
		}
		if _, ok := candidate.CloneForExecution(); ok {
			t.Errorf("timeline cloned %s mutation", name)
		}
	}
	assertTimelineMutationRejected("SQL", func(candidate *CompiledTimeline) {
		candidate.SQL += " /* mutation */"
	})
	assertTimelineMutationRejected("FirstBucket", func(candidate *CompiledTimeline) {
		candidate.Spec.FirstBucket = candidate.Spec.FirstBucket.Add(time.Second)
	})
	assertTimelineMutationRejected("SpanSeconds", func(candidate *CompiledTimeline) {
		candidate.Spec.SpanSeconds++
	})
	assertTimelineMutationRejected("BucketCount", func(candidate *CompiledTimeline) {
		candidate.Spec.BucketCount++
	})
	assertTimelineMutationRejected("Earliest", func(candidate *CompiledTimeline) {
		candidate.Spec.Earliest = candidate.Spec.Earliest.Add(time.Nanosecond)
	})
	assertTimelineMutationRejected("Latest", func(candidate *CompiledTimeline) {
		candidate.Spec.Latest = candidate.Spec.Latest.Add(time.Nanosecond)
	})

	assertCatalogMutationRejected := func(name string, mutate func(*CompiledFieldCatalog)) {
		t.Helper()
		candidate := catalog
		mutate(&candidate)
		if candidate.HasValidExecutionSeal() {
			t.Errorf("field catalog accepted %s mutation", name)
		}
		if _, ok := candidate.CloneForExecution(); ok {
			t.Errorf("field catalog cloned %s mutation", name)
		}
	}
	assertCatalogMutationRejected("SQL", func(candidate *CompiledFieldCatalog) {
		candidate.SQL += " /* mutation */"
	})
	assertCatalogMutationRejected("MaximumFields", func(candidate *CompiledFieldCatalog) {
		candidate.Spec.MaximumFields++
	})
	assertCatalogMutationRejected("knowledgeGeneratedFields", func(candidate *CompiledFieldCatalog) {
		candidate.knowledgeGeneratedFields++
	})
	tamperedCatalogCount := catalog
	tamperedCatalogCount.knowledgeGeneratedFields++
	if _, ok := tamperedCatalogCount.KnowledgeGeneratedFields(); ok {
		t.Fatal("field catalog accessor opened tampered generated-field evidence")
	}
	if _, ok := (CompiledFieldCatalog{}).KnowledgeGeneratedFields(); ok {
		t.Fatal("field catalog accessor opened hand-built generated-field evidence")
	}
	assertCatalogMutationRejected("scope argument", func(candidate *CompiledFieldCatalog) {
		candidate.Args = slices.Clone(candidate.Args)
		candidate.Args[candidate.readScope.argumentPositions[0]] = "other-tenant"
	})

	valueArgs, valuePosition := mutateFirstDerivedNonScopeArgument(
		t,
		catalog.Args,
		catalog.readScope,
		false,
	)
	valueMutation := catalog
	valueMutation.Args = valueArgs
	if _, _, ok := valueMutation.ReadScope(); !ok {
		t.Fatal("non-scope argument mutation invalidated the catalog read scope")
	}
	if valueMutation.HasValidExecutionSeal() {
		t.Fatalf("field catalog accepted non-scope argument %d value mutation", valuePosition)
	}

	typeArgs, typePosition := mutateFirstDerivedNonScopeArgument(
		t,
		catalog.Args,
		catalog.readScope,
		true,
	)
	typeMutation := catalog
	typeMutation.Args = typeArgs
	if _, _, ok := typeMutation.ReadScope(); !ok {
		t.Fatal("non-scope argument type mutation invalidated the catalog read scope")
	}
	if typeMutation.HasValidExecutionSeal() {
		t.Fatalf("field catalog accepted non-scope argument %d concrete-type mutation", typePosition)
	}

	assertSummaryMutationRejected := func(name string, mutate func(*CompiledFieldSummary)) {
		t.Helper()
		candidate := summary
		mutate(&candidate)
		if candidate.HasValidExecutionSeal() {
			t.Errorf("field summary accepted %s mutation", name)
		}
		if _, ok := candidate.CloneForExecution(); ok {
			t.Errorf("field summary cloned %s mutation", name)
		}
	}
	assertSummaryMutationRejected("FieldName", func(candidate *CompiledFieldSummary) {
		candidate.Spec.FieldName += "_other"
	})
	assertSummaryMutationRejected("MaximumValues", func(candidate *CompiledFieldSummary) {
		candidate.Spec.MaximumValues++
	})
	assertSummaryMutationRejected("MaximumDistinctValues", func(candidate *CompiledFieldSummary) {
		candidate.Spec.MaximumDistinctValues++
	})
	assertSummaryMutationRejected("MaximumValueBytes", func(candidate *CompiledFieldSummary) {
		candidate.Spec.MaximumValueBytes++
	})
	assertSummaryMutationRejected("FieldKnown", func(candidate *CompiledFieldSummary) {
		candidate.FieldKnown = !candidate.FieldKnown
	})

	assertSuggestionsMutationRejected := func(name string, mutate func(*CompiledFieldSuggestions)) {
		t.Helper()
		candidate := suggestions
		mutate(&candidate)
		if candidate.HasValidExecutionSeal() {
			t.Errorf("field suggestions accepted %s mutation", name)
		}
		if _, ok := candidate.CloneForExecution(); ok {
			t.Errorf("field suggestions cloned %s mutation", name)
		}
	}
	assertSuggestionsMutationRejected("Prefix", func(candidate *CompiledFieldSuggestions) {
		candidate.Spec.Prefix += "x"
	})
	assertSuggestionsMutationRejected("MaximumFields", func(candidate *CompiledFieldSuggestions) {
		candidate.Spec.MaximumFields++
	})
}

func TestDerivedExecutionPinsSourceKindAndTypedArgumentEncoding(t *testing.T) {
	t.Parallel()

	compiler := Compiler{}
	legacyPlan := buildPlan(t, `index=gradethis level=error`)
	legacy, err := compiler.CompileFieldCatalog(
		legacyPlan,
		FieldCatalogSpec{MaximumFields: 17},
	)
	if err != nil {
		t.Fatalf("CompileFieldCatalog(legacy): %v", err)
	}
	emptyProgram, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("Prepare(empty): %v", err)
	}
	presentEmptyPlan, err := plan.InjectKnowledgePrelude(
		buildPlan(t, `index=gradethis level=error`),
		emptyProgram,
	)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude(empty): %v", err)
	}
	presentEmpty, err := compiler.CompileFieldCatalog(
		presentEmptyPlan,
		FieldCatalogSpec{MaximumFields: 17},
	)
	if err != nil {
		t.Fatalf("CompileFieldCatalog(present empty): %v", err)
	}
	if legacy.SQL != presentEmpty.SQL || !reflect.DeepEqual(legacy.Args, presentEmpty.Args) ||
		legacy.Spec != presentEmpty.Spec {
		t.Fatal("test requires identical derived public surfaces")
	}
	if legacy.executionAuthority.sourceDigest == presentEmpty.executionAuthority.sourceDigest ||
		legacy.executionAuthority.seal == presentEmpty.executionAuthority.seal {
		t.Fatal("different source knowledge authorities produced equal derived authority")
	}

	sameKindSubstitution := legacy
	sameKindAuthority := *legacy.executionAuthority
	sameKindAuthority.seal = presentEmpty.executionAuthority.seal
	sameKindSubstitution.executionAuthority = &sameKindAuthority
	if sameKindSubstitution.HasValidExecutionSeal() {
		t.Fatal("field catalog accepted a seal from an identical surface with different source authority")
	}

	writeIdenticalSpec := func(writer hash.Hash) bool {
		writeTokenPart(writer, "identical-contract")
		return true
	}
	catalogKindAuthority, err := sealDerivedExecution(
		derivedExecutionFieldCatalog,
		sourceForDerivedKindTest(t, compiler),
		legacy.SQL,
		legacy.Args,
		legacy.readScope,
		writeIdenticalSpec,
	)
	if err != nil {
		t.Fatalf("seal catalog-kind authority: %v", err)
	}
	suggestionsKindAuthority, err := sealDerivedExecution(
		derivedExecutionFieldSuggestions,
		sourceForDerivedKindTest(t, compiler),
		legacy.SQL,
		legacy.Args,
		legacy.readScope,
		writeIdenticalSpec,
	)
	if err != nil {
		t.Fatalf("seal suggestions-kind authority: %v", err)
	}
	if catalogKindAuthority.sourceDigest != suggestionsKindAuthority.sourceDigest ||
		catalogKindAuthority.seal == suggestionsKindAuthority.seal {
		t.Fatal("derived kind was not an isolated authority discriminator")
	}
	if hasValidDerivedExecution(
		derivedExecutionFieldCatalog,
		suggestionsKindAuthority,
		legacy.SQL,
		legacy.Args,
		legacy.readScope,
		writeIdenticalSpec,
	) {
		t.Fatal("catalog validation accepted an otherwise-identical suggestions-kind seal")
	}

	source := sourceForDerivedKindTest(t, compiler)
	nilArgument := legacy
	nilArgument.Args = append(slices.Clone(legacy.Args), nil)
	nilArgument.executionAuthority, err = sealCompiledFieldCatalogExecution(source, nilArgument)
	if err != nil {
		t.Fatalf("seal nil-argument fixture: %v", err)
	}
	if !nilArgument.HasValidExecutionSeal() {
		t.Fatal("sealed nil-argument fixture is invalid")
	}
	emptySliceArgument := nilArgument
	emptySliceArgument.Args = slices.Clone(nilArgument.Args)
	emptySliceArgument.Args[len(emptySliceArgument.Args)-1] = []string{}
	if _, _, ok := emptySliceArgument.ReadScope(); !ok {
		t.Fatal("nil-to-empty mutation invalidated the read scope")
	}
	if emptySliceArgument.HasValidExecutionSeal() {
		t.Fatal("derived authority conflated a nil argument with an empty typed slice")
	}
}

func TestDerivedExecutionRejectsLoweredKnowledgeArgumentMutation(t *testing.T) {
	t.Parallel()

	program := deferredMixedKnowledgeProgramForTest(t)
	logical, err := plan.InjectKnowledgePrelude(
		buildPlan(t, `index=gradethis`),
		program,
	)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}
	capture, compiled, compileErr := compileCentralKnowledgeCapture(logical)
	requireCentralKnowledgeCompilerBoundary(t, compiled.HasValidExecutionSeal(), compileErr)
	if !capture.called {
		t.Fatal("knowledge compilation did not reach the captured finalizer")
	}

	preparation, err := prepareKnowledgeCompilation(logical)
	if err != nil {
		t.Fatalf("prepareKnowledgeCompilation: %v", err)
	}
	prelude, err := compileKnowledgePrelude(knowledgeExtractionStageState(), preparation)
	if err != nil {
		t.Fatalf("compileKnowledgePrelude: %v", err)
	}
	evidence, err := compileKnowledgeCompilationEvidence(
		preparation,
		prelude,
		uint64(len(capture.compiled.SQL)),
	)
	if err != nil {
		t.Fatalf("compileKnowledgeCompilationEvidence: %v", err)
	}
	scan := logical.Operators[0].(*plan.Scan)
	source, err := sealCompiledQueryReadScope(
		capture.compiled,
		scan.TenantID,
		scan.Indexes,
	)
	if err != nil {
		t.Fatalf("sealCompiledQueryReadScope: %v", err)
	}
	source.knowledgeEvidence = evidence
	source, err = sealCompiledQueryExecution(source)
	if err != nil {
		t.Fatalf("sealCompiledQueryExecution: %v", err)
	}
	if !source.HasValidExecutionSeal() {
		t.Fatal("test-only nonempty knowledge source is not sealed")
	}

	derived := CompiledFieldCatalog{
		SQL:                      source.SQL,
		Args:                     source.Args,
		Spec:                     FieldCatalogSpec{MaximumFields: 17},
		knowledgeGeneratedFields: evidence.prelude.charges.GeneratedFields,
		readScope:                source.readScope,
	}
	derived.executionAuthority, err = sealCompiledFieldCatalogExecution(source, derived)
	if err != nil {
		t.Fatalf("seal derived knowledge execution: %v", err)
	}
	if count, ok := derived.KnowledgeGeneratedFields(); !ok ||
		count != evidence.prelude.charges.GeneratedFields {
		t.Fatalf(
			"knowledge catalog generated-field evidence = (%d, %t), want (%d, true)",
			count,
			ok,
			evidence.prelude.charges.GeneratedFields,
		)
	}
	forgedCount := derived
	forgedCount.knowledgeGeneratedFields--
	if _, sealErr := sealCompiledFieldCatalogExecution(source, forgedCount); sealErr == nil {
		t.Fatal("source authority accepted forged generated-field evidence")
	}
	loweredPattern := program.RegexExtractions()[0].Pattern()
	mutated := derived
	mutated.Args = slices.Clone(derived.Args)
	patternPosition := -1
	for position, argument := range mutated.Args {
		if value, ok := argument.(string); ok && value == loweredPattern {
			mutated.Args[position] = `(?P<forged>[0-9]+)`
			patternPosition = position
			break
		}
	}
	if patternPosition < 0 {
		t.Fatalf("lowered knowledge pattern is absent from arguments: %#v", mutated.Args)
	}
	if slices.Contains(mutated.readScope.argumentPositions, patternPosition) {
		t.Fatal("knowledge pattern unexpectedly occupies a read-scope argument")
	}
	if _, _, ok := mutated.ReadScope(); !ok {
		t.Fatal("knowledge pattern mutation invalidated the older read-scope seal")
	}
	if mutated.HasValidExecutionSeal() {
		t.Fatal("derived authority accepted a lowered knowledge pattern mutation")
	}

	selectorMutation := derived
	selectorMutation.Args = slices.Clone(derived.Args)
	selectorPosition := -1
	for position, argument := range selectorMutation.Args {
		values, ok := argument.([]string)
		if !ok || len(values) == 0 {
			continue
		}
		selectorMutation.Args[position] = slices.Clone(values)
		selectorMutation.Args[position].([]string)[0] = "forged-selector"
		selectorPosition = position
		break
	}
	if selectorPosition < 0 {
		t.Fatalf("lowered knowledge selector is absent from arguments: %#v", derived.Args)
	}
	if slices.Contains(selectorMutation.readScope.argumentPositions, selectorPosition) {
		t.Fatal("knowledge selector unexpectedly occupies a read-scope argument")
	}
	if _, _, ok := selectorMutation.ReadScope(); !ok {
		t.Fatal("knowledge selector mutation invalidated the older read-scope seal")
	}
	if selectorMutation.HasValidExecutionSeal() {
		t.Fatal("derived authority accepted a lowered knowledge selector mutation")
	}

	cloned, ok := derived.CloneForExecution()
	if !ok {
		t.Fatal("clone rejected the sealed lowered knowledge execution")
	}
	clonedSelector := cloned.Args[selectorPosition].([]string)
	originalSelector := derived.Args[selectorPosition].([]string)
	clonedSelector[0] = "mutated-clone-selector"
	if originalSelector[0] == clonedSelector[0] || !derived.HasValidExecutionSeal() {
		t.Fatal("lowered selector slice aliases its detached clone")
	}
}

func TestDerivedExecutionCloneIsDetachedAndNeverRepairsUnsignedValues(t *testing.T) {
	t.Parallel()

	compiled, err := (Compiler{}).CompileFieldSummary(
		buildPlan(t, `index=gradethis level=error`),
		fieldSummaryTestSpec("level"),
	)
	if err != nil {
		t.Fatalf("CompileFieldSummary: %v", err)
	}
	cloned, ok := compiled.CloneForExecution()
	if !ok || !cloned.HasValidExecutionSeal() {
		t.Fatal("CloneForExecution rejected compiler-produced field summary")
	}
	if cloned.executionAuthority == compiled.executionAuthority {
		t.Fatal("derived authority pointer aliases its source")
	}
	if len(cloned.Args) != 0 && &cloned.Args[0] == &compiled.Args[0] {
		t.Fatal("argument slice aliases its source")
	}
	if len(cloned.readScope.indexNames) != 0 &&
		&cloned.readScope.indexNames[0] == &compiled.readScope.indexNames[0] {
		t.Fatal("read-scope index slice aliases its source")
	}
	if len(cloned.readScope.argumentPositions) != 0 &&
		&cloned.readScope.argumentPositions[0] == &compiled.readScope.argumentPositions[0] {
		t.Fatal("read-scope position slice aliases its source")
	}

	cloned.Spec.FieldName += "_mutated"
	cloned.readScope.indexNames[0] = "other-index"
	cloned.executionAuthority.seal[0] ^= 0xff
	if !compiled.HasValidExecutionSeal() {
		t.Fatal("mutating a detached clone invalidated the original")
	}

	if (CompiledTimeline{}).HasValidExecutionSeal() ||
		(CompiledFieldCatalog{}).HasValidExecutionSeal() ||
		(CompiledFieldSummary{}).HasValidExecutionSeal() ||
		(CompiledFieldSuggestions{}).HasValidExecutionSeal() {
		t.Fatal("a zero derived execution has valid authority")
	}
	if _, ok := (CompiledTimeline{}).CloneForExecution(); ok {
		t.Fatal("zero timeline was repaired")
	}
	if _, ok := (CompiledFieldCatalog{}).CloneForExecution(); ok {
		t.Fatal("zero field catalog was repaired")
	}
	if _, ok := (CompiledFieldSummary{}).CloneForExecution(); ok {
		t.Fatal("zero field summary was repaired")
	}
	if _, ok := (CompiledFieldSuggestions{}).CloneForExecution(); ok {
		t.Fatal("zero field suggestions were repaired")
	}
}

func mutateFirstDerivedNonScopeArgument(
	t *testing.T,
	args []any,
	readScope compiledReadScope,
	changeType bool,
) ([]any, int) {
	t.Helper()
	result := slices.Clone(args)
	for position, argument := range result {
		if slices.Contains(readScope.argumentPositions, position) {
			continue
		}
		if changeType {
			result[position] = []byte("different-concrete-type")
			return result, position
		}
		switch value := argument.(type) {
		case string:
			result[position] = value + "_mutation"
		case time.Time:
			result[position] = value.Add(time.Nanosecond)
		case int:
			result[position] = value + 1
		case int64:
			result[position] = value + 1
		case uint:
			result[position] = value + 1
		case uint32:
			result[position] = value + 1
		case uint64:
			result[position] = value + 1
		case bool:
			result[position] = !value
		default:
			continue
		}
		return result, position
	}
	t.Fatal("compiled derived query has no mutable non-scope argument")
	return nil, -1
}

func sourceForDerivedKindTest(t *testing.T, compiler Compiler) CompiledQuery {
	t.Helper()
	source, err := compiler.Compile(buildPlan(t, `index=gradethis level=error`))
	if err != nil {
		t.Fatalf("Compile(source): %v", err)
	}
	return source
}
