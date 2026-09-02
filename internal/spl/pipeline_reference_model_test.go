package spl_test

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// The pipeline reference model is intentionally independent of the parser, planner
// transformations, ClickHouse SQL, and result transport. It shares only the
// public plan resource-ceiling constants. The operations below are a small,
// independently implemented statement of the command-level value/cardinality/
// order rules, so agreement among production layers cannot become its own
// oracle.
type pipelineReferenceKind uint8

const (
	pipelineReferenceMissing pipelineReferenceKind = iota
	pipelineReferenceNull
	pipelineReferenceString
	pipelineReferenceNumber
	pipelineReferenceBool
	pipelineReferenceTime
	pipelineReferenceList
)

type pipelineReferenceValue struct {
	kind    pipelineReferenceKind
	text    string
	number  float64
	boolean bool
	members []pipelineReferenceValue
}

type pipelineReferenceRow map[string]pipelineReferenceValue

var (
	errPipelineReferenceUnsupportedValue = errors.New("unsupported pipeline reference value")
	errPipelineReferenceResourceLimit    = errors.New("pipeline reference resource limit exceeded")
)

func TestPipelineIndependentReferenceModelCoversCommands(t *testing.T) {
	t.Parallel()

	rows := []pipelineReferenceRow{
		{
			"id": pipelineRefString("a"), "message": pipelineRefString("keep timeout"),
			"n": pipelineRefNumber(2), "host": pipelineRefString("api"),
			"route": pipelineRefString("v1"), "optional": pipelineRefNull(),
			"tags": pipelineRefString("red,,blue"), "prior": pipelineRefString("old"),
		},
		{
			"id": pipelineRefString("b"), "message": pipelineRefString("keep"),
			"n": pipelineRefNumber(3), "host": pipelineRefString("worker"),
			"route": pipelineRefMissing(), "optional": pipelineRefString("present"),
			"tags": pipelineRefNull(), "prior": pipelineRefString("stay"),
		},
		{
			"id": pipelineRefString("c"), "message": pipelineRefString("reject"),
			"n": pipelineRefNumber(5), "tags": pipelineRefString("green"),
		},
	}

	filtered, err := pipelineReferenceRegex(rows, "message", `reject`, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := pipelineReferenceIDs(filtered); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("regex ids = %v", got)
	}

	reversed := pipelineReferenceReverse(filtered)
	pipelineReferenceAccum(reversed, "n", "running")
	pipelineReferenceStrcat(reversed, []pipelineReferenceConcatPart{
		{field: "host"}, {literal: "/"}, {field: "route"},
	}, "endpoint", false)
	pipelineReferenceAddInfo(reversed, pipelineReferenceInfo{
		minimum: 1, maximum: 9, started: 5, sid: "search-pipeline-reference",
	})
	pipelineReferenceFillNull(reversed, []string{"optional"}, "unknown")
	pipelineReferenceAddTotals(reversed, []string{"n", "running"}, "total")
	pipelineReferenceDelta(reversed, "running", "step", 1)
	if err := pipelineReferenceMakeMV(reversed, "tags", ",", false); err != nil {
		t.Fatal(err)
	}
	expanded, err := pipelineReferenceMVExpand(reversed, "tags", 2)
	if err != nil {
		t.Fatal(err)
	}

	want := []pipelineReferenceRow{
		{
			"id": pipelineRefString("b"), "message": pipelineRefString("keep"),
			"n": pipelineRefNumber(3), "host": pipelineRefString("worker"),
			"route": pipelineRefMissing(), "optional": pipelineRefString("present"),
			"tags": pipelineRefNull(), "prior": pipelineRefString("stay"),
			"running": pipelineRefNumber(3), "endpoint": pipelineRefString("worker/"),
			"info_min_time": pipelineRefNumber(1), "info_max_time": pipelineRefNumber(9),
			"info_search_time": pipelineRefNumber(5), "info_sid": pipelineRefString("search-pipeline-reference"),
			"total": pipelineRefNumber(6), "step": pipelineRefNull(),
		},
		{
			"id": pipelineRefString("a"), "message": pipelineRefString("keep timeout"),
			"n": pipelineRefNumber(2), "host": pipelineRefString("api"),
			"route": pipelineRefString("v1"), "optional": pipelineRefString("unknown"),
			"tags": pipelineRefString("red"), "prior": pipelineRefString("old"),
			"running": pipelineRefNumber(5), "endpoint": pipelineRefString("api/v1"),
			"info_min_time": pipelineRefNumber(1), "info_max_time": pipelineRefNumber(9),
			"info_search_time": pipelineRefNumber(5), "info_sid": pipelineRefString("search-pipeline-reference"),
			"total": pipelineRefNumber(7), "step": pipelineRefNumber(2),
		},
		{
			"id": pipelineRefString("a"), "message": pipelineRefString("keep timeout"),
			"n": pipelineRefNumber(2), "host": pipelineRefString("api"),
			"route": pipelineRefString("v1"), "optional": pipelineRefString("unknown"),
			"tags": pipelineRefString("blue"), "prior": pipelineRefString("old"),
			"running": pipelineRefNumber(5), "endpoint": pipelineRefString("api/v1"),
			"info_min_time": pipelineRefNumber(1), "info_max_time": pipelineRefNumber(9),
			"info_search_time": pipelineRefNumber(5), "info_sid": pipelineRefString("search-pipeline-reference"),
			"total": pipelineRefNumber(7), "step": pipelineRefNumber(2),
		},
	}
	if !reflect.DeepEqual(expanded, want) {
		t.Fatalf("pipeline-command reference result = %#v, want %#v", expanded, want)
	}

	// The command-local hostile domains are also model-owned: makemv accepts
	// only a scalar String, while mvexpand admits scalar String/Number/Bool/time
	// and nullable String lists but rejects heterogeneous/container members.
	hostile := []pipelineReferenceRow{{"value": pipelineRefNumber(1)}}
	if err := pipelineReferenceMakeMV(hostile, "value", ",", false); !errors.Is(err, errPipelineReferenceUnsupportedValue) {
		t.Fatalf("makemv hostile error = %v", err)
	}
	nullable := []pipelineReferenceRow{
		{},
		{"value": pipelineRefNull()},
		{"value": pipelineRefString("")},
	}
	if err := pipelineReferenceMakeMV(nullable, "value", ",", false); err != nil {
		t.Fatalf("makemv nullable reference: %v", err)
	}
	if pipelineRefGet(nullable[0], "value").kind != pipelineReferenceNull ||
		pipelineRefGet(nullable[1], "value").kind != pipelineReferenceNull ||
		pipelineRefGet(nullable[2], "value").kind != pipelineReferenceList ||
		len(pipelineRefGet(nullable[2], "value").members) != 0 {
		t.Fatalf("makemv null/empty distinctions = %#v", nullable)
	}
	for _, scalar := range []pipelineReferenceValue{
		pipelineRefString("x"), pipelineRefNumber(1), {kind: pipelineReferenceBool, boolean: true},
		{kind: pipelineReferenceTime, number: 7}, pipelineRefNull(), pipelineRefMissing(),
	} {
		if _, err := pipelineReferenceMVExpand([]pipelineReferenceRow{{"value": scalar}}, "value", 0); err != nil {
			t.Fatalf("mvexpand scalar %#v: %v", scalar, err)
		}
	}
	badList := pipelineReferenceValue{kind: pipelineReferenceList, members: []pipelineReferenceValue{pipelineRefNumber(1)}}
	if _, err := pipelineReferenceMVExpand([]pipelineReferenceRow{{"value": badList}}, "value", 0); !errors.Is(err, errPipelineReferenceUnsupportedValue) {
		t.Fatalf("mvexpand hostile error = %v", err)
	}
	accumRows := []pipelineReferenceRow{
		{"value": pipelineRefNumber(2)},
		{"value": pipelineRefNumber(math.Inf(1))},
		{"value": pipelineRefNumber(3)},
	}
	pipelineReferenceAccum(accumRows, "value", "running")
	if got := pipelineRefGet(accumRows[1], "running"); !reflect.DeepEqual(got, pipelineRefNumber(2)) {
		t.Fatalf("accum admitted a stored non-finite input: %#v", got)
	}
	if got := pipelineRefGet(accumRows[2], "running"); !reflect.DeepEqual(got, pipelineRefNumber(5)) {
		t.Fatalf("accum finite row after ineligible infinity = %#v, want 5", got)
	}

	// dedup drops rows without a complete key, keeps the first count per tuple
	// globally, and per run of adjacent duplicates when consecutive; head keeps
	// the leading rows of whatever order dedup left behind.
	dedupRows := []pipelineReferenceRow{
		{"id": pipelineRefString("a"), "host": pipelineRefString("api"), "n": pipelineRefNumber(1)},
		{"id": pipelineRefString("b"), "host": pipelineRefString("api"), "n": pipelineRefNumber(1)},
		{"id": pipelineRefString("c"), "host": pipelineRefNull(), "n": pipelineRefNumber(1)},
		{"id": pipelineRefString("d"), "host": pipelineRefString("worker"), "n": pipelineRefString("1")},
		{"id": pipelineRefString("e"), "host": pipelineRefString("api"), "n": pipelineRefNumber(1)},
		{"id": pipelineRefString("f"), "n": pipelineRefNumber(1)},
		{"id": pipelineRefString("g"), "host": pipelineRefString("worker"), "n": pipelineRefNumber(1)},
	}
	global, err := pipelineReferenceDedup(dedupRows, []string{"host", "n"}, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := pipelineReferenceIDs(global); !reflect.DeepEqual(got, []string{"a", "d", "g"}) {
		t.Fatalf("global dedup ids = %v", got)
	}
	runs, err := pipelineReferenceDedup(dedupRows, []string{"host"}, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := pipelineReferenceIDs(runs); !reflect.DeepEqual(got, []string{"a", "d", "e", "g"}) {
		t.Fatalf("consecutive dedup ids = %v", got)
	}
	twoPerRun, err := pipelineReferenceDedup(dedupRows, []string{"host"}, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := pipelineReferenceIDs(twoPerRun); !reflect.DeepEqual(got, []string{"a", "b", "d", "e", "g"}) {
		t.Fatalf("consecutive dedup 2 ids = %v", got)
	}
	if got := pipelineReferenceIDs(pipelineReferenceHead(global, 2)); !reflect.DeepEqual(got, []string{"a", "d"}) {
		t.Fatalf("head ids = %v", got)
	}
	if got := pipelineReferenceHead(global, 9); len(got) != len(global) {
		t.Fatalf("head beyond the input kept %d rows, want %d", len(got), len(global))
	}
	listKey := []pipelineReferenceRow{{"host": {kind: pipelineReferenceList, members: []pipelineReferenceValue{pipelineRefString("x")}}}}
	if _, err := pipelineReferenceDedup(listKey, []string{"host"}, 1, false); !errors.Is(err, errPipelineReferenceUnsupportedValue) {
		t.Fatalf("dedup hostile error = %v", err)
	}
}

func TestPipelineIndependentReferenceStrcatUsesSharedConversionAndWritePolicy(t *testing.T) {
	t.Parallel()

	rows := []pipelineReferenceRow{
		{
			"left": pipelineRefString("a"), "unsupported": {kind: pipelineReferenceBool, boolean: true},
			"number": pipelineRefNumber(2), "right": pipelineRefString("z"),
		},
		{
			"left": pipelineRefString("a"), "missing": pipelineRefMissing(),
			"number": pipelineRefNumber(-0.25), "right": pipelineRefString("z"),
		},
		{
			"left": pipelineRefString("a"), "unsupported": pipelineRefString("u"),
			"missing": pipelineRefNull(), "number": pipelineRefNumber(3.5), "right": pipelineRefString("z"),
		},
	}
	parts := []pipelineReferenceConcatPart{
		{field: "left"}, {literal: "/"}, {field: "unsupported"},
		{literal: "/"}, {field: "missing"}, {literal: "/"},
		{field: "number"}, {literal: "/"}, {field: "right"},
	}
	pipelineReferenceStrcat(rows, parts, "joined", false)
	if got := pipelineRefGet(rows[0], "joined"); !reflect.DeepEqual(got, pipelineRefString("a///2/z")) {
		t.Fatalf("unsupported middle operand result = %#v, want a///2/z", got)
	}
	if got := pipelineRefGet(rows[1], "joined"); !reflect.DeepEqual(got, pipelineRefString("a///-0.25/z")) {
		t.Fatalf("missing middle operand and numeric conversion result = %#v, want a///-0.25/z", got)
	}
	if got := pipelineRefGet(rows[2], "joined"); !reflect.DeepEqual(got, pipelineRefString("a/u//3.5/z")) {
		t.Fatalf("null middle operand and numeric conversion result = %#v, want a/u//3.5/z", got)
	}

	required := []pipelineReferenceRow{
		{
			"left": pipelineRefString("a"), "missing": pipelineRefMissing(),
			"number": pipelineRefNumber(2), "joined": pipelineRefString("keep-string"),
		},
		{
			"left": pipelineRefString("a"), "unsupported": {kind: pipelineReferenceList},
			"number": pipelineRefNumber(2), "joined": pipelineRefNull(),
		},
		{
			"left": pipelineRefString("a"), "number": pipelineRefNumber(2),
		},
	}
	pipelineReferenceStrcat(required, []pipelineReferenceConcatPart{
		{field: "left"}, {literal: "/"}, {field: "missing"}, {field: "number"},
	}, "joined", true)
	if got := pipelineRefGet(required[0], "joined"); !reflect.DeepEqual(got, pipelineRefString("keep-string")) {
		t.Fatalf("allrequired missing operand replaced destination: %#v", got)
	}
	pipelineReferenceStrcat(required[1:2], []pipelineReferenceConcatPart{
		{field: "left"}, {literal: "/"}, {field: "unsupported"}, {field: "number"},
	}, "joined", true)
	if got := pipelineRefGet(required[1], "joined"); got.kind != pipelineReferenceNull {
		t.Fatalf("allrequired unsupported operand replaced null destination: %#v", got)
	}
	pipelineReferenceStrcat(required[2:], []pipelineReferenceConcatPart{
		{field: "left"}, {literal: "/"}, {field: "missing"}, {field: "number"},
	}, "joined", true)
	if got := pipelineRefGet(required[2], "joined"); got.kind != pipelineReferenceNull {
		t.Fatalf("allrequired missing operand did not retain a null destination: %#v", got)
	}

	pipelineReferenceStrcat(required[2:], []pipelineReferenceConcatPart{
		{field: "left"}, {literal: "/"}, {field: "number"},
	}, "joined", true)
	if got := pipelineRefGet(required[2], "joined"); !reflect.DeepEqual(got, pipelineRefString("a/2")) {
		t.Fatalf("allrequired numeric operand result = %#v, want a/2", got)
	}
}

func TestPipelineIndependentReferenceResourceModel(t *testing.T) {
	t.Parallel()

	// This invokes the semantic model rather than merely comparing constants:
	// a legal input String that constructs 1,001 members must fail atomically.
	overMembers := strings.Repeat("x,", int(plan.MaximumMakeMVMembersPerRow)) + "x"
	rows := []pipelineReferenceRow{{"value": pipelineRefString(overMembers)}}
	if err := pipelineReferenceMakeMV(rows, "value", ",", true); !errors.Is(err, errPipelineReferenceResourceLimit) {
		t.Fatalf("makemv 1,001-member error = %v", err)
	}
	if got := pipelineRefGet(rows[0], "value"); !reflect.DeepEqual(got, pipelineRefString(overMembers)) {
		t.Fatal("failed makemv published a partially converted value")
	}

	if err := pipelineReferenceValidateMakeMVCharge(pipelineReferenceMakeMVCharge{
		maximumRowMembers: plan.MaximumMakeMVMembersPerRow,
		totalMembers:      plan.MaximumMakeMVMembersPerResult,
		maximumRowBytes:   plan.MaximumMakeMVMemberBytesPerRow,
		totalMemberBytes:  plan.MaximumMakeMVMemberBytesPerResult,
		retainedBytes:     plan.MaximumMakeMVRetainedBytesPerResult,
	}); err != nil {
		t.Fatalf("makemv exact resource boundaries: %v", err)
	}
	if err := pipelineReferenceValidateMakeMVCharge(pipelineReferenceMakeMVCharge{
		retainedBytes: plan.MaximumMakeMVRetainedBytesPerResult + 1,
	}); !errors.Is(err, errPipelineReferenceResourceLimit) {
		t.Fatalf("makemv retained-byte overflow error = %v", err)
	}

	var stageLedger pipelineReferenceMVExpandLedger
	if err := stageLedger.charge(plan.MaximumMVExpandRowsPerStage+1, 0); !errors.Is(err, errPipelineReferenceResourceLimit) {
		t.Fatalf("mvexpand per-stage overflow error = %v", err)
	}
	if stageLedger != (pipelineReferenceMVExpandLedger{}) {
		t.Fatalf("failed per-stage charge mutated ledger: %#v", stageLedger)
	}

	var queryLedger pipelineReferenceMVExpandLedger
	if err := queryLedger.charge(8_000, 1); err != nil {
		t.Fatalf("first mvexpand query charge: %v", err)
	}
	beforeFailure := queryLedger
	if err := queryLedger.charge(8_000, 1); !errors.Is(err, errPipelineReferenceResourceLimit) {
		t.Fatalf("mvexpand cumulative overflow error = %v", err)
	}
	if queryLedger != beforeFailure {
		t.Fatalf("failed cumulative charge mutated ledger: got %#v, want %#v", queryLedger, beforeFailure)
	}

	var exactLedger pipelineReferenceMVExpandLedger
	if err := exactLedger.charge(plan.MaximumMVExpandRowsPerStage, plan.MaximumMVExpandRetainedBytesPerStage); err != nil {
		t.Fatalf("first exact mvexpand charge: %v", err)
	}
	if err := exactLedger.charge(
		plan.MaximumMVExpandRowsPerQuery-plan.MaximumMVExpandRowsPerStage,
		plan.MaximumMVExpandRetainedBytesPerStage,
	); err != nil {
		t.Fatalf("query-wide exact mvexpand charge: %v", err)
	}
	if err := new(pipelineReferenceMVExpandLedger).charge(
		1, plan.MaximumMVExpandRetainedBytesPerStage+1,
	); !errors.Is(err, errPipelineReferenceResourceLimit) {
		t.Fatalf("mvexpand retained-byte overflow error = %v", err)
	}
	overExpandMembers := make([]pipelineReferenceValue, plan.MaximumMakeMVMembersPerRow+1)
	for index := range overExpandMembers {
		overExpandMembers[index] = pipelineRefString("x")
	}
	if _, err := pipelineReferenceMVExpand(
		[]pipelineReferenceRow{{
			"value": {kind: pipelineReferenceList, members: overExpandMembers},
		}},
		"value",
		1,
	); !errors.Is(err, errPipelineReferenceResourceLimit) {
		t.Fatalf("mvexpand source-member overflow with limit=1 error = %v", err)
	}
}

func pipelineReferenceRegex(rows []pipelineReferenceRow, field, pattern string, negated bool) ([]pipelineReferenceRow, error) {
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	result := make([]pipelineReferenceRow, 0, len(rows))
	for _, row := range rows {
		value := pipelineRefGet(row, field)
		matched := value.kind == pipelineReferenceString && compiled.MatchString(value.text)
		keep := matched
		if negated {
			keep = value.kind == pipelineReferenceMissing || value.kind == pipelineReferenceNull || !matched
		}
		if keep {
			result = append(result, pipelineRefCloneRow(row))
		}
	}
	return result, nil
}

func pipelineReferenceReverse(rows []pipelineReferenceRow) []pipelineReferenceRow {
	result := make([]pipelineReferenceRow, len(rows))
	for index := range rows {
		result[len(rows)-1-index] = pipelineRefCloneRow(rows[index])
	}
	return result
}

// pipelineReferenceDedup keeps the first count rows of every complete key
// tuple in the established order. A row whose key is missing or null is not
// eligible and is dropped (Splunk's keepempty=false default). With consecutive,
// the count restarts whenever an eligible row's tuple differs from the
// immediately preceding eligible row, so each run of adjacent duplicates keeps
// its own first count rows.
func pipelineReferenceDedup(
	rows []pipelineReferenceRow,
	keys []string,
	count int,
	consecutive bool,
) ([]pipelineReferenceRow, error) {
	result := make([]pipelineReferenceRow, 0, len(rows))
	seen := make(map[string]int, len(rows))
	previous, runLength := "", 0
	for _, row := range rows {
		identity, eligible, err := pipelineReferenceDedupIdentity(row, keys)
		if err != nil {
			return nil, err
		}
		if !eligible {
			continue
		}
		var retained int
		if consecutive {
			if runLength == 0 || identity != previous {
				runLength = 0
			}
			runLength++
			previous = identity
			retained = runLength
		} else {
			seen[identity]++
			retained = seen[identity]
		}
		if retained <= count {
			result = append(result, pipelineRefCloneRow(row))
		}
	}
	return result, nil
}

// pipelineReferenceDedupIdentity renders the key tuple with each member's kind
// so a String "1" and a Number 1 stay distinct, matching the typed column path.
func pipelineReferenceDedupIdentity(row pipelineReferenceRow, keys []string) (string, bool, error) {
	var identity strings.Builder
	for _, key := range keys {
		value := pipelineRefGet(row, key)
		switch value.kind {
		case pipelineReferenceMissing, pipelineReferenceNull:
			return "", false, nil
		case pipelineReferenceString:
			identity.WriteString("s" + strconv.Quote(value.text))
		case pipelineReferenceNumber, pipelineReferenceTime:
			identity.WriteString("n" + strconv.FormatFloat(value.number, 'g', -1, 64))
		case pipelineReferenceBool:
			identity.WriteString("b" + strconv.FormatBool(value.boolean))
		default:
			return "", false, errPipelineReferenceUnsupportedValue
		}
		identity.WriteByte(0)
	}
	return identity.String(), true, nil
}

// pipelineReferenceHead keeps the first count rows of the established order.
func pipelineReferenceHead(rows []pipelineReferenceRow, count int) []pipelineReferenceRow {
	if count > len(rows) {
		count = len(rows)
	}
	result := make([]pipelineReferenceRow, count)
	for index := range result {
		result[index] = pipelineRefCloneRow(rows[index])
	}
	return result
}

func pipelineReferenceAccum(rows []pipelineReferenceRow, input, output string) {
	total := float64(0)
	eligible := false
	for _, row := range rows {
		value := pipelineRefGet(row, input)
		if value.kind == pipelineReferenceNumber && !math.IsNaN(value.number) && !math.IsInf(value.number, 0) {
			total += value.number
			eligible = true
		}
		if eligible {
			row[output] = pipelineRefNumber(total)
		} else {
			row[output] = pipelineRefNull()
		}
	}
}

type pipelineReferenceConcatPart struct {
	field   string
	literal string
}

func pipelineReferenceStrcat(rows []pipelineReferenceRow, parts []pipelineReferenceConcatPart, output string, allRequired bool) {
	for _, row := range rows {
		var result strings.Builder
		complete := true
		for _, part := range parts {
			if part.field == "" {
				result.WriteString(part.literal)
				continue
			}
			converted, ok := pipelineReferenceConcatOperand(pipelineRefGet(row, part.field))
			if !ok {
				complete = false
				if allRequired {
					break
				}
				// The strcat default changes the shared concatenation's null
				// result into this command's empty-operand policy. Continue so
				// later operands retain their source order.
				continue
			}
			result.WriteString(converted)
		}
		if complete || !allRequired {
			row[output] = pipelineRefString(result.String())
		} else if _, alreadyPresent := row[output]; !alreadyPresent {
			row[output] = pipelineRefNull()
		}
	}
}

func pipelineReferenceConcatOperand(value pipelineReferenceValue) (string, bool) {
	switch value.kind {
	case pipelineReferenceString:
		return value.text, true
	case pipelineReferenceNumber:
		return strconv.FormatFloat(value.number, 'g', -1, 64), true
	default:
		return "", false
	}
}

type pipelineReferenceInfo struct {
	minimum, maximum, started float64
	sid                       string
}

func pipelineReferenceAddInfo(rows []pipelineReferenceRow, info pipelineReferenceInfo) {
	for _, row := range rows {
		row["info_min_time"] = pipelineRefNumber(info.minimum)
		row["info_max_time"] = pipelineRefNumber(info.maximum)
		row["info_search_time"] = pipelineRefNumber(info.started)
		row["info_sid"] = pipelineRefString(info.sid)
	}
}

func pipelineReferenceFillNull(rows []pipelineReferenceRow, fields []string, value string) {
	for _, row := range rows {
		for _, field := range fields {
			current := pipelineRefGet(row, field)
			if current.kind == pipelineReferenceMissing || current.kind == pipelineReferenceNull {
				row[field] = pipelineRefString(value)
			}
		}
	}
}

func pipelineReferenceAddTotals(rows []pipelineReferenceRow, fields []string, output string) {
	for _, row := range rows {
		total := float64(0)
		for _, field := range fields {
			value := pipelineRefGet(row, field)
			if value.kind == pipelineReferenceNumber && !math.IsNaN(value.number) && !math.IsInf(value.number, 0) {
				total += value.number
			}
		}
		row[output] = pipelineRefNumber(total)
	}
}

func pipelineReferenceDelta(rows []pipelineReferenceRow, input, output string, period int) {
	for index, row := range rows {
		if period < 1 || index < period {
			row[output] = pipelineRefNull()
			continue
		}
		current, prior := pipelineRefGet(row, input), pipelineRefGet(rows[index-period], input)
		if current.kind != pipelineReferenceNumber || prior.kind != pipelineReferenceNumber {
			row[output] = pipelineRefNull()
			continue
		}
		row[output] = pipelineRefNumber(current.number - prior.number)
	}
}

type pipelineReferenceMakeMVCharge struct {
	maximumRowMembers uint64
	maximumRowBytes   uint64
	totalMembers      uint64
	totalMemberBytes  uint64
	retainedBytes     uint64
}

func pipelineReferenceValidateMakeMVCharge(charge pipelineReferenceMakeMVCharge) error {
	if charge.maximumRowMembers > plan.MaximumMakeMVMembersPerRow ||
		charge.maximumRowBytes > plan.MaximumMakeMVMemberBytesPerRow ||
		charge.totalMembers > plan.MaximumMakeMVMembersPerResult ||
		charge.totalMemberBytes > plan.MaximumMakeMVMemberBytesPerResult ||
		charge.retainedBytes > plan.MaximumMakeMVRetainedBytesPerResult {
		return errPipelineReferenceResourceLimit
	}
	return nil
}

func pipelineReferenceMakeMV(rows []pipelineReferenceRow, field, delimiter string, allowEmpty bool) error {
	if delimiter == "" {
		return errPipelineReferenceUnsupportedValue
	}
	convertedRows := make([]pipelineReferenceRow, len(rows))
	charge := pipelineReferenceMakeMVCharge{}
	for index, sourceRow := range rows {
		row := pipelineRefCloneRow(sourceRow)
		convertedRows[index] = row
		value := pipelineRefGet(row, field)
		switch value.kind {
		case pipelineReferenceMissing, pipelineReferenceNull:
			row[field] = pipelineRefNull()
		case pipelineReferenceString:
			parts := strings.Split(value.text, delimiter)
			members := make([]pipelineReferenceValue, 0, len(parts))
			for _, part := range parts {
				if part != "" || allowEmpty {
					members = append(members, pipelineRefString(part))
				}
			}
			rowMembers := uint64(len(members))
			rowBytes := uint64(0)
			for _, member := range members {
				rowBytes += uint64(len(member.text))
			}
			charge.maximumRowMembers = max(charge.maximumRowMembers, rowMembers)
			charge.maximumRowBytes = max(charge.maximumRowBytes, rowBytes)
			charge.totalMembers += rowMembers
			charge.totalMemberBytes += rowBytes
			if err := pipelineReferenceValidateMakeMVCharge(charge); err != nil {
				return err
			}
			row[field] = pipelineReferenceValue{kind: pipelineReferenceList, members: members}
		default:
			return errPipelineReferenceUnsupportedValue
		}
	}
	retainedBytes, err := pipelineReferenceRetainedBytes(convertedRows)
	if err != nil {
		return err
	}
	charge.retainedBytes = retainedBytes
	if err := pipelineReferenceValidateMakeMVCharge(charge); err != nil {
		return err
	}
	copy(rows, convertedRows)
	return nil
}

func pipelineReferenceMVExpand(rows []pipelineReferenceRow, field string, limit int) ([]pipelineReferenceRow, error) {
	return pipelineReferenceMVExpandWithLedger(rows, field, limit, new(pipelineReferenceMVExpandLedger))
}

type pipelineReferenceMVExpandLedger struct {
	stages uint64
	rows   uint64
}

func (ledger *pipelineReferenceMVExpandLedger) charge(stageRows, retainedBytes uint64) error {
	if ledger == nil || ledger.stages >= uint64(plan.MaximumMVExpandStages) ||
		ledger.rows > plan.MaximumMVExpandRowsPerQuery ||
		stageRows > plan.MaximumMVExpandRowsPerStage ||
		stageRows > plan.MaximumMVExpandRowsPerQuery-ledger.rows ||
		retainedBytes > plan.MaximumMVExpandRetainedBytesPerStage {
		return errPipelineReferenceResourceLimit
	}
	ledger.stages++
	ledger.rows += stageRows
	return nil
}

func pipelineReferenceMVExpandWithLedger(
	rows []pipelineReferenceRow,
	field string,
	limit int,
	ledger *pipelineReferenceMVExpandLedger,
) ([]pipelineReferenceRow, error) {
	if limit < 0 || uint64(limit) > plan.MaximumMakeMVMembersPerRow {
		return nil, errPipelineReferenceResourceLimit
	}
	result := make([]pipelineReferenceRow, 0, len(rows))
	for _, row := range rows {
		value := pipelineRefGet(row, field)
		members := []pipelineReferenceValue{value}
		switch value.kind {
		case pipelineReferenceMissing, pipelineReferenceNull:
			members = []pipelineReferenceValue{pipelineRefNull()}
		case pipelineReferenceString, pipelineReferenceNumber, pipelineReferenceBool, pipelineReferenceTime:
		case pipelineReferenceList:
			members = value.members
			if uint64(len(members)) > plan.MaximumMakeMVMembersPerRow {
				return nil, errPipelineReferenceResourceLimit
			}
			for _, member := range members {
				if member.kind != pipelineReferenceString && member.kind != pipelineReferenceNull {
					return nil, errPipelineReferenceUnsupportedValue
				}
			}
		default:
			return nil, errPipelineReferenceUnsupportedValue
		}
		if limit > 0 && len(members) > limit {
			members = members[:limit]
		}
		for _, member := range members {
			expandedRow := pipelineRefCloneRow(row)
			expandedRow[field] = member
			result = append(result, expandedRow)
		}
	}
	retainedBytes, err := pipelineReferenceRetainedBytes(result)
	if err != nil {
		return nil, err
	}
	if err := ledger.charge(uint64(len(result)), retainedBytes); err != nil {
		return nil, err
	}
	return result, nil
}

// pipelineReferenceRetainedBytes is deliberately standard-library-only. It sums
// deterministic JSON for each complete public row instead of importing the
// production SQL/transport encoders whose agreement this test is meant to
// check independently.
func pipelineReferenceRetainedBytes(rows []pipelineReferenceRow) (uint64, error) {
	retainedBytes := uint64(0)
	for _, row := range rows {
		publicRow := make(map[string]any, len(row))
		for field, value := range row {
			if value.kind == pipelineReferenceMissing {
				continue
			}
			encoded, err := pipelineReferenceJSONValue(value)
			if err != nil {
				return 0, err
			}
			publicRow[field] = encoded
		}
		encoded, err := json.Marshal(publicRow)
		if err != nil {
			return 0, err
		}
		retainedBytes += uint64(len(encoded))
	}
	return retainedBytes, nil
}

func pipelineReferenceJSONValue(value pipelineReferenceValue) (any, error) {
	switch value.kind {
	case pipelineReferenceMissing, pipelineReferenceNull:
		return nil, nil
	case pipelineReferenceString:
		return value.text, nil
	case pipelineReferenceNumber, pipelineReferenceTime:
		return value.number, nil
	case pipelineReferenceBool:
		return value.boolean, nil
	case pipelineReferenceList:
		members := make([]any, len(value.members))
		for index, member := range value.members {
			encoded, err := pipelineReferenceJSONValue(member)
			if err != nil {
				return nil, err
			}
			members[index] = encoded
		}
		return members, nil
	default:
		return nil, errPipelineReferenceUnsupportedValue
	}
}

func pipelineRefGet(row pipelineReferenceRow, field string) pipelineReferenceValue {
	if value, ok := row[field]; ok {
		return value
	}
	return pipelineRefMissing()
}

func pipelineRefCloneRow(row pipelineReferenceRow) pipelineReferenceRow {
	result := make(pipelineReferenceRow, len(row))
	for name, value := range row {
		value.members = append([]pipelineReferenceValue(nil), value.members...)
		result[name] = value
	}
	return result
}

func pipelineReferenceIDs(rows []pipelineReferenceRow) []string {
	result := make([]string, len(rows))
	for index, row := range rows {
		result[index] = pipelineRefGet(row, "id").text
	}
	return result
}

func pipelineRefMissing() pipelineReferenceValue {
	return pipelineReferenceValue{kind: pipelineReferenceMissing}
}
func pipelineRefNull() pipelineReferenceValue {
	return pipelineReferenceValue{kind: pipelineReferenceNull}
}
func pipelineRefString(value string) pipelineReferenceValue {
	return pipelineReferenceValue{kind: pipelineReferenceString, text: value}
}
func pipelineRefNumber(value float64) pipelineReferenceValue {
	return pipelineReferenceValue{kind: pipelineReferenceNumber, number: value}
}
