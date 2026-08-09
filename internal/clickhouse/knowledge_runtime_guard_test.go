package clickhouse

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileKnowledgeRuntimeGuard(t *testing.T) {
	state := knowledgeExtractionStageState()
	private := quoteIdentifier("__os_guard_test_private")
	state.visible["prior"] = fieldState{
		valueSQL:  quoteIdentifier("prior"),
		existsSQL: private,
		kind:      fieldKindString,
	}
	state.privateColumns = []string{private}
	state.order = []compiledSortKey{{valueSQL: quoteIdentifier(internalSortTimeColumn)}}
	state.tieBreakers = []compiledSortKey{{valueSQL: quoteIdentifier(internalSortIDColumn)}}
	program := knowledgePreludeProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		knowledgeRegexStageDefinition(
			"guard-regex",
			`(?P<guard_value>[a-z]+)`,
			[]string{"guard_value"},
			"west",
		),
	})
	preparation := knowledgePreludePreparationForTest(program)
	prelude, err := compileKnowledgePrelude(state, preparation)
	if err != nil {
		t.Fatalf("compile guarded prelude: %v", err)
	}
	relation := compiledRelation{sql: `SELECT ? AS "seed"`, depth: 94}
	guarded, err := compileKnowledgeRuntimeGuard(relation, prelude, preparation)
	if err != nil {
		t.Fatalf("compile runtime guard: %v", err)
	}
	if guarded.relation.depth != maximumCompiledRelationalDepth ||
		guarded.relation.ownerRange != relation.ownerRange {
		t.Fatalf("guarded relation = %#v", guarded.relation)
	}
	if len(guarded.suffixArgs) != 0 ||
		strings.Count(guarded.relation.sql, "?") != strings.Count(relation.sql, "?") {
		t.Fatalf("guard introduced arguments: %#v\n%s", guarded.suffixArgs, guarded.relation.sql)
	}
	if !reflect.DeepEqual(guarded.state, prelude.state) ||
		guarded.state.rexCapturedBytesSQL != prelude.capturedBytes ||
		!slices.Contains(guarded.state.privateColumns, private) {
		t.Fatalf("guard changed event state: %#v", guarded.state)
	}
	guarded.state.visible["detached"] = canonicalState("detached")
	if _, aliased := prelude.state.visible["detached"]; aliased {
		t.Fatal("guarded state aliases the prelude state")
	}

	inputBytes := prelude.selectorCharges.inputBytes
	queryUnits := prelude.selectorCharges.queryUnits
	capturedBytes := prelude.capturedBytes
	eventMaximum := "maxOrDefault(toUInt128(" + inputBytes + "))"
	rexMaximum := "maxOrDefault(toUInt128(" + capturedBytes + "))"
	queryTotal := "sum(toUInt128(" + queryUnits + "))"
	eventOver := eventMaximum + " > toUInt128(" +
		strconv.Itoa(knowledge.MaximumSelectorRuntimeEventBytes) + ")"
	rexOver := rexMaximum + " > toUInt128(" +
		strconv.FormatUint(MaximumRexCapturedBytesPerRow, 10) + ")"
	queryOver := queryTotal + " > toUInt128(" +
		strconv.Itoa(knowledge.MaximumSelectorRuntimeQueryUnits) + ")"
	violationRef := `"__os_ko_guard_total"."__os_ko_guard_violation"`
	eventViolation := violationRef + " = toUInt8(1)"
	rexViolation := violationRef + " = toUInt8(2)"
	queryViolation := violationRef + " = toUInt8(3)"
	wantExcept := "SELECT * EXCEPT (" + inputBytes + ", " + queryUnits +
		`, "__os_ko_guard_violation")`
	if strings.Contains(wantExcept, capturedBytes) {
		t.Fatalf("rex accounting entered EXCEPT list: %s", wantExcept)
	}
	for _, fragment := range []string{
		`WITH "__os_ko_guard_input" AS MATERIALIZED (` + relation.sql + `)`,
		eventMaximum,
		rexMaximum,
		queryTotal,
		"multiIf(" + eventOver + ", toUInt8(1), " + rexOver +
			", toUInt8(2), " + queryOver + ", toUInt8(3), toUInt8(0))",
		knowledgeRuntimeGuardThrow(eventViolation, KnowledgeSelectorEventLimitMarker),
		knowledgeRuntimeGuardThrow(rexViolation, RexCaptureLimitMarker),
		knowledgeRuntimeGuardThrow(queryViolation, KnowledgeSelectorQueryLimitMarker),
		wantExcept,
		`CROSS JOIN "__os_ko_guard_totals"`,
		materializedCTESettingsSQL,
	} {
		if !strings.Contains(guarded.relation.sql, fragment) {
			t.Fatalf("guard SQL omits %q:\n%s", fragment, guarded.relation.sql)
		}
	}
	if strings.Contains(guarded.relation.sql, ">=") ||
		strings.Contains(guarded.relation.sql, "throwIf(toUInt8(1)") ||
		strings.Contains(guarded.relation.sql, " = 0 AND ") ||
		strings.Count(guarded.relation.sql, relation.sql) != 1 ||
		strings.Count(guarded.relation.sql, " AS MATERIALIZED (") != 1 ||
		strings.Count(guarded.relation.sql, KnowledgeSelectorEventLimitMarker) != 1 ||
		strings.Count(guarded.relation.sql, RexCaptureLimitMarker) != 1 ||
		strings.Count(guarded.relation.sql, KnowledgeSelectorQueryLimitMarker) != 1 {
		t.Fatalf("guard comparison/materialization shape is invalid:\n%s", guarded.relation.sql)
	}
	eventMarker := strings.Index(guarded.relation.sql, KnowledgeSelectorEventLimitMarker)
	rexMarker := strings.Index(guarded.relation.sql, RexCaptureLimitMarker)
	queryMarker := strings.Index(guarded.relation.sql, KnowledgeSelectorQueryLimitMarker)
	if eventMarker < 0 || !(eventMarker < rexMarker && rexMarker < queryMarker) {
		t.Fatalf("guard marker precedence = %d, %d, %d", eventMarker, rexMarker, queryMarker)
	}
	if len(guarded.relation.sql) > maxCompiledKnowledgeRuntimeGuardSQLBytes {
		t.Fatalf("guard SQL = %d bytes", len(guarded.relation.sql))
	}

	t.Run("detached and deterministic", func(t *testing.T) {
		second, compileErr := compileKnowledgeRuntimeGuard(relation, prelude, preparation)
		if compileErr != nil {
			t.Fatalf("compile second runtime guard: %v", compileErr)
		}
		if second.relation != guarded.relation {
			t.Fatalf("second relation differs:\nfirst:  %#v\nsecond: %#v", guarded.relation, second.relation)
		}
		if len(second.suffixArgs) != 0 {
			t.Fatalf("second guard introduced arguments: %#v", second.suffixArgs)
		}
		if !reflect.DeepEqual(second.state, prelude.state) {
			t.Fatalf("second state differs from prelude:\nprelude: %#v\nsecond:  %#v", prelude.state, second.state)
		}
		second.state.visible["host"] = fieldState{}
		second.state.privateColumns[0] = quoteIdentifier("mutated")
		if prelude.state.visible["host"].valueSQL == "" ||
			prelude.state.privateColumns[0] != private {
			t.Fatal("guarded state aliases retained prelude authority")
		}
	})

	t.Run("identity", func(t *testing.T) {
		absentPrelude, compileErr := compileKnowledgePrelude(compileState{}, preparedKnowledgeCompilation{})
		if compileErr != nil {
			t.Fatalf("compile absent prelude: %v", compileErr)
		}
		identityInput := compiledRelation{sql: "SELECT 1", depth: 1}
		absent, compileErr := compileKnowledgeRuntimeGuard(
			identityInput,
			absentPrelude,
			preparedKnowledgeCompilation{},
		)
		if compileErr != nil || absent.relation != identityInput ||
			!reflect.DeepEqual(absent.state, absentPrelude.state) {
			t.Fatalf("absent identity = %#v, %v", absent, compileErr)
		}
		emptyProgram, prepareErr := knowledgeprogram.Prepare(knowledgeprogram.Input{})
		if prepareErr != nil {
			t.Fatalf("prepare empty program: %v", prepareErr)
		}
		emptyPreparation := knowledgePreludePreparationForTest(emptyProgram)
		emptyPrelude, compileErr := compileKnowledgePrelude(
			knowledgeExtractionStageState(),
			emptyPreparation,
		)
		if compileErr != nil {
			t.Fatalf("compile empty prelude: %v", compileErr)
		}
		empty, compileErr := compileKnowledgeRuntimeGuard(
			identityInput,
			emptyPrelude,
			emptyPreparation,
		)
		if compileErr != nil || empty.relation != identityInput ||
			!reflect.DeepEqual(empty.state, emptyPrelude.state) {
			t.Fatalf("empty identity = %#v, %v", empty, compileErr)
		}
	})

	t.Run("optional rex", func(t *testing.T) {
		jsonProgram := knowledgePreludeProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
			knowledgeJSONStageDefinition("guard-json", "json_value", "payload.value", "east"),
		})
		jsonPreparation := knowledgePreludePreparationForTest(jsonProgram)
		jsonPrelude, compileErr := compileKnowledgePrelude(
			knowledgeExtractionStageState(),
			jsonPreparation,
		)
		if compileErr != nil {
			t.Fatalf("compile JSON prelude: %v", compileErr)
		}
		jsonGuard, compileErr := compileKnowledgeRuntimeGuard(
			compiledRelation{sql: "SELECT 1", depth: 1},
			jsonPrelude,
			jsonPreparation,
		)
		if compileErr != nil || strings.Contains(jsonGuard.relation.sql, RexCaptureLimitMarker) ||
			strings.Contains(jsonGuard.relation.sql, "rex_capture") {
			t.Fatalf("capture-free guard = %v\n%s", compileErr, jsonGuard.relation.sql)
		}
	})

	t.Run("rejects forged authority and bounds", func(t *testing.T) {
		clone := func() compiledKnowledgePrelude {
			candidate := prelude
			candidate.state = cloneCompileState(prelude.state)
			candidate.stages = slices.Clone(prelude.stages)
			last := len(candidate.stages) - 1
			candidate.stages[last].projection = slices.Clone(candidate.stages[last].projection)
			return candidate
		}
		cases := []struct {
			name   string
			mutate func(*compiledKnowledgePrelude, *preparedKnowledgeCompilation, *compiledRelation)
		}{
			{"half charge pair", func(p *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, _ *compiledRelation) {
				p.selectorCharges.queryUnits = ""
			}},
			{"equal charge pair", func(p *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, _ *compiledRelation) {
				p.selectorCharges.queryUnits = p.selectorCharges.inputBytes
			}},
			{"mismatched proof", func(p *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, _ *compiledRelation) {
				p.proof.objectCount++
			}},
			{"capture published", func(p *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, _ *compiledRelation) {
				p.state.visible["leak"] = fieldState{valueSQL: p.capturedBytes, kind: fieldKindNumber}
			}},
			{"charge in order", func(p *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, _ *compiledRelation) {
				p.state.order = append(p.state.order, compiledSortKey{valueSQL: p.selectorCharges.inputBytes})
			}},
			{"charge in container sidecar", func(p *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, _ *compiledRelation) {
				p.state.visible["leak"] = fieldState{
					valueSQL:                quoteIdentifier("leak"),
					kind:                    fieldKindDynamic,
					relativeFieldNamesSQL:   p.selectorCharges.inputBytes,
					relativeFieldTypesSQL:   quoteIdentifier("leak_types"),
					fieldMetadataVersionSQL: quoteIdentifier("leak_version"),
				}
			}},
			{"capture in container metadata", func(p *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, _ *compiledRelation) {
				p.state.visible["leak"] = fieldState{
					valueSQL:                quoteIdentifier("leak"),
					kind:                    fieldKindDynamic,
					relativeFieldNamesSQL:   quoteIdentifier("leak_names"),
					relativeFieldTypesSQL:   quoteIdentifier("leak_types"),
					fieldMetadataVersionSQL: p.capturedBytes,
				}
			}},
			{"charge definition removed", func(p *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, _ *compiledRelation) {
				last := len(p.stages) - 1
				p.stages[last].projection = slices.DeleteFunc(p.stages[last].projection, func(value string) bool {
					return strings.HasSuffix(value, " AS "+p.selectorCharges.inputBytes)
				})
			}},
			{"prepared commitment", func(_ *compiledKnowledgePrelude, p *preparedKnowledgeCompilation, _ *compiledRelation) {
				p.programCommitment[0] ^= 1
			}},
			{"empty relation", func(_ *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, r *compiledRelation) { r.sql = "" }},
			{"zero depth", func(_ *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, r *compiledRelation) { r.depth = 0 }},
			{"depth overflow", func(_ *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, r *compiledRelation) { r.depth = 95 }},
			{"SQL overflow", func(_ *compiledKnowledgePrelude, _ *preparedKnowledgeCompilation, r *compiledRelation) {
				r.sql = strings.Repeat("x", maxCompiledKnowledgeRuntimeGuardSQLBytes+1)
			}},
		}
		for _, test := range cases {
			t.Run(test.name, func(t *testing.T) {
				candidate := clone()
				candidatePreparation := preparation
				candidateRelation := compiledRelation{sql: "SELECT 1", depth: 1}
				test.mutate(&candidate, &candidatePreparation, &candidateRelation)
				if _, compileErr := compileKnowledgeRuntimeGuard(
					candidateRelation,
					candidate,
					candidatePreparation,
				); compileErr == nil {
					t.Fatal("forged guard input compiled")
				}
			})
		}
		_, depthErr := compileKnowledgeRuntimeGuard(
			compiledRelation{sql: "SELECT 1", depth: 95},
			prelude,
			preparation,
		)
		var diagnostic *plan.Diagnostic
		if !errors.As(depthErr, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
			t.Fatalf("depth error = %#v", depthErr)
		}
	})
}

func TestCompileKnowledgeRuntimeGuardAccountsAliasCopies(t *testing.T) {
	program := knowledgePreludeProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{{
		AppId:        "app",
		Name:         "guard-alias",
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField:       "host",
				DestinationField:  "copied_host",
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			},
		},
	}})
	preparation := knowledgePreludePreparationForTest(program)
	prelude, err := compileKnowledgePrelude(
		knowledgeExtractionStageState(),
		preparation,
	)
	if err != nil {
		t.Fatalf("compile alias prelude: %v", err)
	}
	guarded, err := compileKnowledgeRuntimeGuard(
		compiledRelation{sql: "SELECT 1", depth: 1},
		prelude,
		preparation,
	)
	if err != nil {
		t.Fatalf("compile alias runtime guard: %v", err)
	}
	eventMaximum := "maxOrDefault(toUInt128(" + prelude.aliasCopyCharges.eventBytes + "))"
	queryTotal := "sum(toUInt128(" + prelude.aliasCopyCharges.queryUnits + "))"
	wantExcept := "* EXCEPT (" + strings.Join(
		knowledgeRuntimeGuardAccountingColumns(prelude),
		", ",
	) + `, "__os_ko_guard_violation")`
	for _, fragment := range []string{
		eventMaximum,
		queryTotal,
		fmt.Sprintf(
			"%s > toUInt128(%d)",
			eventMaximum,
			knowledge.MaximumAliasCopyRuntimeEventBytes,
		),
		fmt.Sprintf(
			"%s > toUInt128(%d)",
			queryTotal,
			knowledge.MaximumAliasCopyRuntimeQueryUnits,
		),
		` = toUInt8(4)`,
		` = toUInt8(5)`,
		KnowledgeAliasCopyEventLimitMarker,
		KnowledgeAliasCopyQueryLimitMarker,
		wantExcept,
	} {
		if !strings.Contains(guarded.relation.sql, fragment) {
			t.Fatalf("alias guard omits %q:\n%s", fragment, guarded.relation.sql)
		}
	}
	selectorEvent := strings.Index(guarded.relation.sql, KnowledgeSelectorEventLimitMarker)
	aliasEvent := strings.Index(guarded.relation.sql, KnowledgeAliasCopyEventLimitMarker)
	selectorQuery := strings.Index(guarded.relation.sql, KnowledgeSelectorQueryLimitMarker)
	aliasQuery := strings.Index(guarded.relation.sql, KnowledgeAliasCopyQueryLimitMarker)
	if !(selectorEvent >= 0 && selectorEvent < aliasEvent && aliasEvent < selectorQuery &&
		selectorQuery < aliasQuery) {
		t.Fatalf(
			"alias guard marker precedence = %d, %d, %d, %d\n%s",
			selectorEvent,
			aliasEvent,
			selectorQuery,
			aliasQuery,
			guarded.relation.sql,
		)
	}

	clonePrelude := func() compiledKnowledgePrelude {
		candidate := prelude
		candidate.state = cloneCompileState(prelude.state)
		candidate.stages = slices.Clone(prelude.stages)
		last := len(candidate.stages) - 1
		candidate.stages[last].projection = slices.Clone(candidate.stages[last].projection)
		return candidate
	}
	for _, test := range []struct {
		name   string
		mutate func(*compiledKnowledgePrelude)
	}{
		{"partial pair", func(candidate *compiledKnowledgePrelude) {
			candidate.aliasCopyCharges.queryUnits = ""
		}},
		{"overlap selector", func(candidate *compiledKnowledgePrelude) {
			candidate.aliasCopyCharges.eventBytes = candidate.selectorCharges.inputBytes
		}},
		{"published leak", func(candidate *compiledKnowledgePrelude) {
			candidate.state.visible["leak"] = fieldState{
				valueSQL: candidate.aliasCopyCharges.eventBytes,
				kind:     fieldKindNumber,
			}
		}},
		{"definition removed", func(candidate *compiledKnowledgePrelude) {
			last := len(candidate.stages) - 1
			candidate.stages[last].projection = slices.DeleteFunc(
				candidate.stages[last].projection,
				func(value string) bool {
					return strings.HasSuffix(
						value,
						" AS "+candidate.aliasCopyCharges.eventBytes,
					)
				},
			)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := clonePrelude()
			test.mutate(&candidate)
			if _, compileErr := compileKnowledgeRuntimeGuard(
				compiledRelation{sql: "SELECT 1", depth: 1},
				candidate,
				preparation,
			); compileErr == nil {
				t.Fatal("forged alias-copy accounting compiled")
			}
		})
	}
}
