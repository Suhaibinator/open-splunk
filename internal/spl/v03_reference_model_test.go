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

// The v0.3 reference model is intentionally independent of the parser, planner
// transformations, ClickHouse SQL, and result transport. It shares only the
// public plan resource-ceiling constants. The operations below are a small,
// independently implemented statement of the command-level value/cardinality/
// order rules, so agreement among production layers cannot become its own
// oracle.
type v03ReferenceKind uint8

const (
	v03ReferenceMissing v03ReferenceKind = iota
	v03ReferenceNull
	v03ReferenceString
	v03ReferenceNumber
	v03ReferenceBool
	v03ReferenceTime
	v03ReferenceList
)

type v03ReferenceValue struct {
	kind    v03ReferenceKind
	text    string
	number  float64
	boolean bool
	members []v03ReferenceValue
}

type v03ReferenceRow map[string]v03ReferenceValue

var (
	errV03ReferenceUnsupportedValue = errors.New("unsupported v0.3 reference value")
	errV03ReferenceResourceLimit    = errors.New("v0.3 reference resource limit exceeded")
)

func TestV03IndependentReferenceModelCoversAllTenCommands(t *testing.T) {
	t.Parallel()

	rows := []v03ReferenceRow{
		{
			"id": v03RefString("a"), "message": v03RefString("keep timeout"),
			"n": v03RefNumber(2), "host": v03RefString("api"),
			"route": v03RefString("v1"), "optional": v03RefNull(),
			"tags": v03RefString("red,,blue"), "prior": v03RefString("old"),
		},
		{
			"id": v03RefString("b"), "message": v03RefString("keep"),
			"n": v03RefNumber(3), "host": v03RefString("worker"),
			"route": v03RefMissing(), "optional": v03RefString("present"),
			"tags": v03RefNull(), "prior": v03RefString("stay"),
		},
		{
			"id": v03RefString("c"), "message": v03RefString("reject"),
			"n": v03RefNumber(5), "tags": v03RefString("green"),
		},
	}

	filtered, err := v03ReferenceRegex(rows, "message", `reject`, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := v03ReferenceIDs(filtered); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("regex ids = %v", got)
	}

	reversed := v03ReferenceReverse(filtered)
	v03ReferenceAccum(reversed, "n", "running")
	v03ReferenceStrcat(reversed, []v03ReferenceConcatPart{
		{field: "host"}, {literal: "/"}, {field: "route"},
	}, "endpoint", false)
	v03ReferenceAddInfo(reversed, v03ReferenceInfo{
		minimum: 1, maximum: 9, started: 5, sid: "search-v03-reference",
	})
	v03ReferenceFillNull(reversed, []string{"optional"}, "unknown")
	v03ReferenceAddTotals(reversed, []string{"n", "running"}, "total")
	v03ReferenceDelta(reversed, "running", "step", 1)
	if err := v03ReferenceMakeMV(reversed, "tags", ",", false); err != nil {
		t.Fatal(err)
	}
	expanded, err := v03ReferenceMVExpand(reversed, "tags", 2)
	if err != nil {
		t.Fatal(err)
	}

	want := []v03ReferenceRow{
		{
			"id": v03RefString("b"), "message": v03RefString("keep"),
			"n": v03RefNumber(3), "host": v03RefString("worker"),
			"route": v03RefMissing(), "optional": v03RefString("present"),
			"tags": v03RefNull(), "prior": v03RefString("stay"),
			"running": v03RefNumber(3), "endpoint": v03RefString("worker/"),
			"info_min_time": v03RefNumber(1), "info_max_time": v03RefNumber(9),
			"info_search_time": v03RefNumber(5), "info_sid": v03RefString("search-v03-reference"),
			"total": v03RefNumber(6), "step": v03RefNull(),
		},
		{
			"id": v03RefString("a"), "message": v03RefString("keep timeout"),
			"n": v03RefNumber(2), "host": v03RefString("api"),
			"route": v03RefString("v1"), "optional": v03RefString("unknown"),
			"tags": v03RefString("red"), "prior": v03RefString("old"),
			"running": v03RefNumber(5), "endpoint": v03RefString("api/v1"),
			"info_min_time": v03RefNumber(1), "info_max_time": v03RefNumber(9),
			"info_search_time": v03RefNumber(5), "info_sid": v03RefString("search-v03-reference"),
			"total": v03RefNumber(7), "step": v03RefNumber(2),
		},
		{
			"id": v03RefString("a"), "message": v03RefString("keep timeout"),
			"n": v03RefNumber(2), "host": v03RefString("api"),
			"route": v03RefString("v1"), "optional": v03RefString("unknown"),
			"tags": v03RefString("blue"), "prior": v03RefString("old"),
			"running": v03RefNumber(5), "endpoint": v03RefString("api/v1"),
			"info_min_time": v03RefNumber(1), "info_max_time": v03RefNumber(9),
			"info_search_time": v03RefNumber(5), "info_sid": v03RefString("search-v03-reference"),
			"total": v03RefNumber(7), "step": v03RefNumber(2),
		},
	}
	if !reflect.DeepEqual(expanded, want) {
		t.Fatalf("all-ten reference result = %#v, want %#v", expanded, want)
	}

	// The command-local hostile domains are also model-owned: makemv accepts
	// only a scalar String, while mvexpand admits scalar String/Number/Bool/time
	// and nullable String lists but rejects heterogeneous/container members.
	hostile := []v03ReferenceRow{{"value": v03RefNumber(1)}}
	if err := v03ReferenceMakeMV(hostile, "value", ",", false); !errors.Is(err, errV03ReferenceUnsupportedValue) {
		t.Fatalf("makemv hostile error = %v", err)
	}
	nullable := []v03ReferenceRow{
		{},
		{"value": v03RefNull()},
		{"value": v03RefString("")},
	}
	if err := v03ReferenceMakeMV(nullable, "value", ",", false); err != nil {
		t.Fatalf("makemv nullable reference: %v", err)
	}
	if v03RefGet(nullable[0], "value").kind != v03ReferenceNull ||
		v03RefGet(nullable[1], "value").kind != v03ReferenceNull ||
		v03RefGet(nullable[2], "value").kind != v03ReferenceList ||
		len(v03RefGet(nullable[2], "value").members) != 0 {
		t.Fatalf("makemv null/empty distinctions = %#v", nullable)
	}
	for _, scalar := range []v03ReferenceValue{
		v03RefString("x"), v03RefNumber(1), {kind: v03ReferenceBool, boolean: true},
		{kind: v03ReferenceTime, number: 7}, v03RefNull(), v03RefMissing(),
	} {
		if _, err := v03ReferenceMVExpand([]v03ReferenceRow{{"value": scalar}}, "value", 0); err != nil {
			t.Fatalf("mvexpand scalar %#v: %v", scalar, err)
		}
	}
	badList := v03ReferenceValue{kind: v03ReferenceList, members: []v03ReferenceValue{v03RefNumber(1)}}
	if _, err := v03ReferenceMVExpand([]v03ReferenceRow{{"value": badList}}, "value", 0); !errors.Is(err, errV03ReferenceUnsupportedValue) {
		t.Fatalf("mvexpand hostile error = %v", err)
	}
	accumRows := []v03ReferenceRow{
		{"value": v03RefNumber(2)},
		{"value": v03RefNumber(math.Inf(1))},
		{"value": v03RefNumber(3)},
	}
	v03ReferenceAccum(accumRows, "value", "running")
	if got := v03RefGet(accumRows[1], "running"); !reflect.DeepEqual(got, v03RefNumber(2)) {
		t.Fatalf("accum admitted a stored non-finite input: %#v", got)
	}
	if got := v03RefGet(accumRows[2], "running"); !reflect.DeepEqual(got, v03RefNumber(5)) {
		t.Fatalf("accum finite row after ineligible infinity = %#v, want 5", got)
	}
}

func TestV03IndependentReferenceStrcatUsesSharedConversionAndWritePolicy(t *testing.T) {
	t.Parallel()

	rows := []v03ReferenceRow{
		{
			"left": v03RefString("a"), "unsupported": {kind: v03ReferenceBool, boolean: true},
			"number": v03RefNumber(2), "right": v03RefString("z"),
		},
		{
			"left": v03RefString("a"), "missing": v03RefMissing(),
			"number": v03RefNumber(-0.25), "right": v03RefString("z"),
		},
		{
			"left": v03RefString("a"), "unsupported": v03RefString("u"),
			"missing": v03RefNull(), "number": v03RefNumber(3.5), "right": v03RefString("z"),
		},
	}
	parts := []v03ReferenceConcatPart{
		{field: "left"}, {literal: "/"}, {field: "unsupported"},
		{literal: "/"}, {field: "missing"}, {literal: "/"},
		{field: "number"}, {literal: "/"}, {field: "right"},
	}
	v03ReferenceStrcat(rows, parts, "joined", false)
	if got := v03RefGet(rows[0], "joined"); !reflect.DeepEqual(got, v03RefString("a///2/z")) {
		t.Fatalf("unsupported middle operand result = %#v, want a///2/z", got)
	}
	if got := v03RefGet(rows[1], "joined"); !reflect.DeepEqual(got, v03RefString("a///-0.25/z")) {
		t.Fatalf("missing middle operand and numeric conversion result = %#v, want a///-0.25/z", got)
	}
	if got := v03RefGet(rows[2], "joined"); !reflect.DeepEqual(got, v03RefString("a/u//3.5/z")) {
		t.Fatalf("null middle operand and numeric conversion result = %#v, want a/u//3.5/z", got)
	}

	required := []v03ReferenceRow{
		{
			"left": v03RefString("a"), "missing": v03RefMissing(),
			"number": v03RefNumber(2), "joined": v03RefString("keep-string"),
		},
		{
			"left": v03RefString("a"), "unsupported": {kind: v03ReferenceList},
			"number": v03RefNumber(2), "joined": v03RefNull(),
		},
		{
			"left": v03RefString("a"), "number": v03RefNumber(2),
		},
	}
	v03ReferenceStrcat(required, []v03ReferenceConcatPart{
		{field: "left"}, {literal: "/"}, {field: "missing"}, {field: "number"},
	}, "joined", true)
	if got := v03RefGet(required[0], "joined"); !reflect.DeepEqual(got, v03RefString("keep-string")) {
		t.Fatalf("allrequired missing operand replaced destination: %#v", got)
	}
	v03ReferenceStrcat(required[1:2], []v03ReferenceConcatPart{
		{field: "left"}, {literal: "/"}, {field: "unsupported"}, {field: "number"},
	}, "joined", true)
	if got := v03RefGet(required[1], "joined"); got.kind != v03ReferenceNull {
		t.Fatalf("allrequired unsupported operand replaced null destination: %#v", got)
	}
	v03ReferenceStrcat(required[2:], []v03ReferenceConcatPart{
		{field: "left"}, {literal: "/"}, {field: "missing"}, {field: "number"},
	}, "joined", true)
	if got := v03RefGet(required[2], "joined"); got.kind != v03ReferenceNull {
		t.Fatalf("allrequired missing operand did not retain a null destination: %#v", got)
	}

	v03ReferenceStrcat(required[2:], []v03ReferenceConcatPart{
		{field: "left"}, {literal: "/"}, {field: "number"},
	}, "joined", true)
	if got := v03RefGet(required[2], "joined"); !reflect.DeepEqual(got, v03RefString("a/2")) {
		t.Fatalf("allrequired numeric operand result = %#v, want a/2", got)
	}
}

func TestV03IndependentReferenceResourceModel(t *testing.T) {
	t.Parallel()

	// This invokes the semantic model rather than merely comparing constants:
	// a legal input String that constructs 1,001 members must fail atomically.
	overMembers := strings.Repeat("x,", int(plan.MaximumMakeMVMembersPerRow)) + "x"
	rows := []v03ReferenceRow{{"value": v03RefString(overMembers)}}
	if err := v03ReferenceMakeMV(rows, "value", ",", true); !errors.Is(err, errV03ReferenceResourceLimit) {
		t.Fatalf("makemv 1,001-member error = %v", err)
	}
	if got := v03RefGet(rows[0], "value"); !reflect.DeepEqual(got, v03RefString(overMembers)) {
		t.Fatal("failed makemv published a partially converted value")
	}

	if err := v03ReferenceValidateMakeMVCharge(v03ReferenceMakeMVCharge{
		maximumRowMembers: plan.MaximumMakeMVMembersPerRow,
		totalMembers:      plan.MaximumMakeMVMembersPerResult,
		maximumRowBytes:   plan.MaximumMakeMVMemberBytesPerRow,
		totalMemberBytes:  plan.MaximumMakeMVMemberBytesPerResult,
		retainedBytes:     plan.MaximumMakeMVRetainedBytesPerResult,
	}); err != nil {
		t.Fatalf("makemv exact resource boundaries: %v", err)
	}
	if err := v03ReferenceValidateMakeMVCharge(v03ReferenceMakeMVCharge{
		retainedBytes: plan.MaximumMakeMVRetainedBytesPerResult + 1,
	}); !errors.Is(err, errV03ReferenceResourceLimit) {
		t.Fatalf("makemv retained-byte overflow error = %v", err)
	}

	var stageLedger v03ReferenceMVExpandLedger
	if err := stageLedger.charge(plan.MaximumMVExpandRowsPerStage+1, 0); !errors.Is(err, errV03ReferenceResourceLimit) {
		t.Fatalf("mvexpand per-stage overflow error = %v", err)
	}
	if stageLedger != (v03ReferenceMVExpandLedger{}) {
		t.Fatalf("failed per-stage charge mutated ledger: %#v", stageLedger)
	}

	var queryLedger v03ReferenceMVExpandLedger
	if err := queryLedger.charge(8_000, 1); err != nil {
		t.Fatalf("first mvexpand query charge: %v", err)
	}
	beforeFailure := queryLedger
	if err := queryLedger.charge(8_000, 1); !errors.Is(err, errV03ReferenceResourceLimit) {
		t.Fatalf("mvexpand cumulative overflow error = %v", err)
	}
	if queryLedger != beforeFailure {
		t.Fatalf("failed cumulative charge mutated ledger: got %#v, want %#v", queryLedger, beforeFailure)
	}

	var exactLedger v03ReferenceMVExpandLedger
	if err := exactLedger.charge(plan.MaximumMVExpandRowsPerStage, plan.MaximumMVExpandRetainedBytesPerStage); err != nil {
		t.Fatalf("first exact mvexpand charge: %v", err)
	}
	if err := exactLedger.charge(
		plan.MaximumMVExpandRowsPerQuery-plan.MaximumMVExpandRowsPerStage,
		plan.MaximumMVExpandRetainedBytesPerStage,
	); err != nil {
		t.Fatalf("query-wide exact mvexpand charge: %v", err)
	}
	if err := new(v03ReferenceMVExpandLedger).charge(
		1, plan.MaximumMVExpandRetainedBytesPerStage+1,
	); !errors.Is(err, errV03ReferenceResourceLimit) {
		t.Fatalf("mvexpand retained-byte overflow error = %v", err)
	}
	overExpandMembers := make([]v03ReferenceValue, plan.MaximumMakeMVMembersPerRow+1)
	for index := range overExpandMembers {
		overExpandMembers[index] = v03RefString("x")
	}
	if _, err := v03ReferenceMVExpand(
		[]v03ReferenceRow{{
			"value": {kind: v03ReferenceList, members: overExpandMembers},
		}},
		"value",
		1,
	); !errors.Is(err, errV03ReferenceResourceLimit) {
		t.Fatalf("mvexpand source-member overflow with limit=1 error = %v", err)
	}
}

func v03ReferenceRegex(rows []v03ReferenceRow, field, pattern string, negated bool) ([]v03ReferenceRow, error) {
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	result := make([]v03ReferenceRow, 0, len(rows))
	for _, row := range rows {
		value := v03RefGet(row, field)
		matched := value.kind == v03ReferenceString && compiled.MatchString(value.text)
		keep := matched
		if negated {
			keep = value.kind == v03ReferenceMissing || value.kind == v03ReferenceNull || !matched
		}
		if keep {
			result = append(result, v03RefCloneRow(row))
		}
	}
	return result, nil
}

func v03ReferenceReverse(rows []v03ReferenceRow) []v03ReferenceRow {
	result := make([]v03ReferenceRow, len(rows))
	for index := range rows {
		result[len(rows)-1-index] = v03RefCloneRow(rows[index])
	}
	return result
}

func v03ReferenceAccum(rows []v03ReferenceRow, input, output string) {
	total := float64(0)
	eligible := false
	for _, row := range rows {
		value := v03RefGet(row, input)
		if value.kind == v03ReferenceNumber && !math.IsNaN(value.number) && !math.IsInf(value.number, 0) {
			total += value.number
			eligible = true
		}
		if eligible {
			row[output] = v03RefNumber(total)
		} else {
			row[output] = v03RefNull()
		}
	}
}

type v03ReferenceConcatPart struct {
	field   string
	literal string
}

func v03ReferenceStrcat(rows []v03ReferenceRow, parts []v03ReferenceConcatPart, output string, allRequired bool) {
	for _, row := range rows {
		var result strings.Builder
		complete := true
		for _, part := range parts {
			if part.field == "" {
				result.WriteString(part.literal)
				continue
			}
			converted, ok := v03ReferenceConcatOperand(v03RefGet(row, part.field))
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
			row[output] = v03RefString(result.String())
		} else if _, alreadyPresent := row[output]; !alreadyPresent {
			row[output] = v03RefNull()
		}
	}
}

func v03ReferenceConcatOperand(value v03ReferenceValue) (string, bool) {
	switch value.kind {
	case v03ReferenceString:
		return value.text, true
	case v03ReferenceNumber:
		return strconv.FormatFloat(value.number, 'g', -1, 64), true
	default:
		return "", false
	}
}

type v03ReferenceInfo struct {
	minimum, maximum, started float64
	sid                       string
}

func v03ReferenceAddInfo(rows []v03ReferenceRow, info v03ReferenceInfo) {
	for _, row := range rows {
		row["info_min_time"] = v03RefNumber(info.minimum)
		row["info_max_time"] = v03RefNumber(info.maximum)
		row["info_search_time"] = v03RefNumber(info.started)
		row["info_sid"] = v03RefString(info.sid)
	}
}

func v03ReferenceFillNull(rows []v03ReferenceRow, fields []string, value string) {
	for _, row := range rows {
		for _, field := range fields {
			current := v03RefGet(row, field)
			if current.kind == v03ReferenceMissing || current.kind == v03ReferenceNull {
				row[field] = v03RefString(value)
			}
		}
	}
}

func v03ReferenceAddTotals(rows []v03ReferenceRow, fields []string, output string) {
	for _, row := range rows {
		total := float64(0)
		for _, field := range fields {
			value := v03RefGet(row, field)
			if value.kind == v03ReferenceNumber && !math.IsNaN(value.number) && !math.IsInf(value.number, 0) {
				total += value.number
			}
		}
		row[output] = v03RefNumber(total)
	}
}

func v03ReferenceDelta(rows []v03ReferenceRow, input, output string, period int) {
	for index, row := range rows {
		if period < 1 || index < period {
			row[output] = v03RefNull()
			continue
		}
		current, prior := v03RefGet(row, input), v03RefGet(rows[index-period], input)
		if current.kind != v03ReferenceNumber || prior.kind != v03ReferenceNumber {
			row[output] = v03RefNull()
			continue
		}
		row[output] = v03RefNumber(current.number - prior.number)
	}
}

type v03ReferenceMakeMVCharge struct {
	maximumRowMembers uint64
	maximumRowBytes   uint64
	totalMembers      uint64
	totalMemberBytes  uint64
	retainedBytes     uint64
}

func v03ReferenceValidateMakeMVCharge(charge v03ReferenceMakeMVCharge) error {
	if charge.maximumRowMembers > plan.MaximumMakeMVMembersPerRow ||
		charge.maximumRowBytes > plan.MaximumMakeMVMemberBytesPerRow ||
		charge.totalMembers > plan.MaximumMakeMVMembersPerResult ||
		charge.totalMemberBytes > plan.MaximumMakeMVMemberBytesPerResult ||
		charge.retainedBytes > plan.MaximumMakeMVRetainedBytesPerResult {
		return errV03ReferenceResourceLimit
	}
	return nil
}

func v03ReferenceMakeMV(rows []v03ReferenceRow, field, delimiter string, allowEmpty bool) error {
	if delimiter == "" {
		return errV03ReferenceUnsupportedValue
	}
	convertedRows := make([]v03ReferenceRow, len(rows))
	charge := v03ReferenceMakeMVCharge{}
	for index, sourceRow := range rows {
		row := v03RefCloneRow(sourceRow)
		convertedRows[index] = row
		value := v03RefGet(row, field)
		switch value.kind {
		case v03ReferenceMissing, v03ReferenceNull:
			row[field] = v03RefNull()
		case v03ReferenceString:
			parts := strings.Split(value.text, delimiter)
			members := make([]v03ReferenceValue, 0, len(parts))
			for _, part := range parts {
				if part != "" || allowEmpty {
					members = append(members, v03RefString(part))
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
			if err := v03ReferenceValidateMakeMVCharge(charge); err != nil {
				return err
			}
			row[field] = v03ReferenceValue{kind: v03ReferenceList, members: members}
		default:
			return errV03ReferenceUnsupportedValue
		}
	}
	retainedBytes, err := v03ReferenceRetainedBytes(convertedRows)
	if err != nil {
		return err
	}
	charge.retainedBytes = retainedBytes
	if err := v03ReferenceValidateMakeMVCharge(charge); err != nil {
		return err
	}
	copy(rows, convertedRows)
	return nil
}

func v03ReferenceMVExpand(rows []v03ReferenceRow, field string, limit int) ([]v03ReferenceRow, error) {
	return v03ReferenceMVExpandWithLedger(rows, field, limit, new(v03ReferenceMVExpandLedger))
}

type v03ReferenceMVExpandLedger struct {
	stages uint64
	rows   uint64
}

func (ledger *v03ReferenceMVExpandLedger) charge(stageRows, retainedBytes uint64) error {
	if ledger == nil || ledger.stages >= uint64(plan.MaximumMVExpandStages) ||
		ledger.rows > plan.MaximumMVExpandRowsPerQuery ||
		stageRows > plan.MaximumMVExpandRowsPerStage ||
		stageRows > plan.MaximumMVExpandRowsPerQuery-ledger.rows ||
		retainedBytes > plan.MaximumMVExpandRetainedBytesPerStage {
		return errV03ReferenceResourceLimit
	}
	ledger.stages++
	ledger.rows += stageRows
	return nil
}

func v03ReferenceMVExpandWithLedger(
	rows []v03ReferenceRow,
	field string,
	limit int,
	ledger *v03ReferenceMVExpandLedger,
) ([]v03ReferenceRow, error) {
	if limit < 0 || uint64(limit) > plan.MaximumMakeMVMembersPerRow {
		return nil, errV03ReferenceResourceLimit
	}
	result := make([]v03ReferenceRow, 0, len(rows))
	for _, row := range rows {
		value := v03RefGet(row, field)
		members := []v03ReferenceValue{value}
		switch value.kind {
		case v03ReferenceMissing, v03ReferenceNull:
			members = []v03ReferenceValue{v03RefNull()}
		case v03ReferenceString, v03ReferenceNumber, v03ReferenceBool, v03ReferenceTime:
		case v03ReferenceList:
			members = value.members
			if uint64(len(members)) > plan.MaximumMakeMVMembersPerRow {
				return nil, errV03ReferenceResourceLimit
			}
			for _, member := range members {
				if member.kind != v03ReferenceString && member.kind != v03ReferenceNull {
					return nil, errV03ReferenceUnsupportedValue
				}
			}
		default:
			return nil, errV03ReferenceUnsupportedValue
		}
		if limit > 0 && len(members) > limit {
			members = members[:limit]
		}
		for _, member := range members {
			expandedRow := v03RefCloneRow(row)
			expandedRow[field] = member
			result = append(result, expandedRow)
		}
	}
	retainedBytes, err := v03ReferenceRetainedBytes(result)
	if err != nil {
		return nil, err
	}
	if err := ledger.charge(uint64(len(result)), retainedBytes); err != nil {
		return nil, err
	}
	return result, nil
}

// v03ReferenceRetainedBytes is deliberately standard-library-only. It sums
// deterministic JSON for each complete public row instead of importing the
// production SQL/transport encoders whose agreement this test is meant to
// check independently.
func v03ReferenceRetainedBytes(rows []v03ReferenceRow) (uint64, error) {
	retainedBytes := uint64(0)
	for _, row := range rows {
		publicRow := make(map[string]any, len(row))
		for field, value := range row {
			if value.kind == v03ReferenceMissing {
				continue
			}
			encoded, err := v03ReferenceJSONValue(value)
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

func v03ReferenceJSONValue(value v03ReferenceValue) (any, error) {
	switch value.kind {
	case v03ReferenceMissing, v03ReferenceNull:
		return nil, nil
	case v03ReferenceString:
		return value.text, nil
	case v03ReferenceNumber, v03ReferenceTime:
		return value.number, nil
	case v03ReferenceBool:
		return value.boolean, nil
	case v03ReferenceList:
		members := make([]any, len(value.members))
		for index, member := range value.members {
			encoded, err := v03ReferenceJSONValue(member)
			if err != nil {
				return nil, err
			}
			members[index] = encoded
		}
		return members, nil
	default:
		return nil, errV03ReferenceUnsupportedValue
	}
}

func v03RefGet(row v03ReferenceRow, field string) v03ReferenceValue {
	if value, ok := row[field]; ok {
		return value
	}
	return v03RefMissing()
}

func v03RefCloneRow(row v03ReferenceRow) v03ReferenceRow {
	result := make(v03ReferenceRow, len(row))
	for name, value := range row {
		value.members = append([]v03ReferenceValue(nil), value.members...)
		result[name] = value
	}
	return result
}

func v03ReferenceIDs(rows []v03ReferenceRow) []string {
	result := make([]string, len(rows))
	for index, row := range rows {
		result[index] = v03RefGet(row, "id").text
	}
	return result
}

func v03RefMissing() v03ReferenceValue { return v03ReferenceValue{kind: v03ReferenceMissing} }
func v03RefNull() v03ReferenceValue    { return v03ReferenceValue{kind: v03ReferenceNull} }
func v03RefString(value string) v03ReferenceValue {
	return v03ReferenceValue{kind: v03ReferenceString, text: value}
}
func v03RefNumber(value float64) v03ReferenceValue {
	return v03ReferenceValue{kind: v03ReferenceNumber, number: value}
}
