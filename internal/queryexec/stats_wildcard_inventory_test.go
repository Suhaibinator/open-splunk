package queryexec

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestExecutorExecuteStatsWildcardInventoryReturnsOpaqueExpansion(t *testing.T) {
	t.Parallel()

	compiled := validCompiledStatsWildcardInventory(t, `index=gradethis | stats avg(pay*)`)
	rows := statsWildcardFakeRows(0,
		plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "pay.bytes"},
		plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "payload"},
	)
	connection := &fakeQueryConnection{rows: rows}
	expansion, err := mustExecutor(t, connection).ExecuteStatsWildcardInventory(
		context.Background(), compiled,
	)
	if err != nil {
		t.Fatal(err)
	}
	if expansion.IsZero() || !rows.closed || connection.query != compiled.SQL ||
		!reflect.DeepEqual(connection.args, compiled.Args) {
		t.Fatalf("ExecuteStatsWildcardInventory = zero:%t closed:%t query:%q args:%#v", expansion.IsZero(), rows.closed, connection.query, connection.args)
	}
	parsed, err := spl.Parse(`index=gradethis | stats avg(pay*)`)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := plan.BuildWithStatsWildcardExpansion(parsed, statsWildcardTestScope(), expansion)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(logical.OutputFields, []string{"avg(pay.bytes)", "avg(payload)"}) {
		t.Fatalf("expanded outputs = %#v", logical.OutputFields)
	}
}

func TestExecutorExecuteStatsWildcardInventoryRejectsRowsAtomically(t *testing.T) {
	t.Parallel()

	compiled := validCompiledStatsWildcardInventory(t, `index=gradethis | stats avg(*)`)
	maximumPairs := compiled.Request().MaximumPairs()
	overflow := make([]plan.StatsWildcardInventoryMatch, maximumPairs)
	for index := range overflow {
		overflow[index] = plan.StatsWildcardInventoryMatch{
			Ordinal: 0,
			Field:   "f" + strings.Repeat("0", 3-len(strconv.Itoa(index))) + strconv.Itoa(index),
		}
	}
	tests := []struct {
		name    string
		rows    *fakeRows
		wantErr error
	}{
		{name: "metadata poison", rows: statsWildcardFakeRows(1, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "payload"}), wantErr: ErrFieldMetadataUnavailable},
		{name: "non-header invalid", rows: statsWildcardRows([][]any{{uint8(0), "", uint8(0)}, {uint8(0), "payload", uint8(1)}}), wantErr: searchjobs.ErrInvalidResult},
		{name: "duplicate", rows: statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "payload"}, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "payload"}), wantErr: searchjobs.ErrInvalidResult},
		{name: "out of order", rows: statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "z"}, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "a"}), wantErr: searchjobs.ErrInvalidResult},
		{name: "overflow sentinel", rows: statsWildcardFakeRows(0, overflow...), wantErr: planDiagnosticSentinel{}},
		{name: "zero matches", rows: statsWildcardFakeRows(0), wantErr: planDiagnosticSentinel{}},
		{name: "private", rows: statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "__os_secret"}), wantErr: searchjobs.ErrInvalidResult},
		{name: "raw container", rows: statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "fields"}), wantErr: searchjobs.ErrInvalidResult},
		{name: "control", rows: statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "bad\nname"}), wantErr: searchjobs.ErrInvalidResult},
		{name: "invalid utf8", rows: statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: string([]byte{0xff})}), wantErr: searchjobs.ErrInvalidResult},
		{name: "physical reserved root", rows: statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "tenant_id"}), wantErr: searchjobs.ErrInvalidResult},
		{name: "physical reserved descendant", rows: statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "body.foo"}), wantErr: searchjobs.ErrInvalidResult},
		{name: "canonical reserved descendant", rows: statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "host.child"}), wantErr: searchjobs.ErrInvalidResult},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := mustExecutor(t, &fakeQueryConnection{rows: test.rows}).
				ExecuteStatsWildcardInventory(context.Background(), compiled)
			if !got.IsZero() {
				t.Fatalf("result is nonzero on error: %#v", got)
			}
			var diagnosticSentinel planDiagnosticSentinel
			if errors.As(test.wantErr, &diagnosticSentinel) {
				var planErr *plan.Diagnostic
				if !errors.As(err, &planErr) {
					t.Fatalf("error = %v, want plan diagnostic", err)
				}
			} else if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if !test.rows.closed {
				t.Fatal("rows were not closed after atomic rejection")
			}
		})
	}
}

type planDiagnosticSentinel struct{}

func (planDiagnosticSentinel) Error() string { return "plan diagnostic" }

func TestExecutorExecuteStatsWildcardInventoryRejectsMalformedSchemaAndStream(t *testing.T) {
	t.Parallel()

	compiled := validCompiledStatsWildcardInventory(t, `index=gradethis | stats avg(pay*)`)
	tests := []struct {
		name   string
		mutate func(*fakeRows)
	}{
		{name: "wrong column", mutate: func(rows *fakeRows) { rows.columns[1] = "wrong" }},
		{name: "missing type", mutate: func(rows *fakeRows) { rows.types = rows.types[:2] }},
		{name: "nullable", mutate: func(rows *fakeRows) {
			rows.types[2] = fakeColumnType{name: clickhouse.StatsWildcardInventoryInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeOf(uint8(0)), nullable: true}
		}},
		{name: "wrong scan type", mutate: func(rows *fakeRows) {
			rows.types[1] = fakeColumnType{name: clickhouse.StatsWildcardInventoryFieldColumn, databaseType: "String", scanType: reflect.TypeOf([]byte{})}
		}},
		{name: "partial stream", mutate: func(rows *fakeRows) { rows.err = errors.New("stream failed") }},
		{name: "close failure", mutate: func(rows *fakeRows) { rows.closeErr = errors.New("close failed") }},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			rows := statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "payload"})
			test.mutate(rows)
			got, err := mustExecutor(t, &fakeQueryConnection{rows: rows}).
				ExecuteStatsWildcardInventory(context.Background(), compiled)
			if err == nil || !got.IsZero() || !rows.closed {
				t.Fatalf("result/error/closed = (%#v, %v, %t)", got, err, rows.closed)
			}
		})
	}
}

func TestStatsWildcardInventorySettingsPreservePrefixGroupsAndMaximumName(t *testing.T) {
	t.Parallel()

	base, err := querySettings(Config{ReadAdmission: indexread.UnfencedAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	settings, err := settingsForStatsWildcardInventory(base, 17)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := settings["max_rows_to_group_by"], base["max_rows_to_group_by"]; got != want {
		t.Fatalf("max_rows_to_group_by = %#v, want preserved %#v", got, want)
	}
	minimumBytes := uint64(18) * uint64(eventfields.MaximumNormalizedFieldNameBytes+64)
	if got := settings["max_result_bytes"].(uint64); got < minimumBytes {
		t.Fatalf("max_result_bytes = %d, want at least %d", got, minimumBytes)
	}
}

func TestStatsWildcardInventoryReadAdmissionUsesSealedScopeAndRejectsTampering(t *testing.T) {
	t.Parallel()

	compiled := validCompiledStatsWildcardInventory(t, `index=gradethis | stats avg(pay*)`)
	admission := &recordingReadAdmission{}
	rows := statsWildcardFakeRows(0, plan.StatsWildcardInventoryMatch{Ordinal: 0, Field: "payload"})
	executor := mustExecutor(t, &fakeQueryConnection{rows: rows})
	executor.readAdmission = admission
	if _, err := executor.ExecuteStatsWildcardInventory(context.Background(), compiled); err != nil {
		t.Fatal(err)
	}
	admission.mu.Lock()
	tenant, indexes := admission.tenantID, slices.Clone(admission.indexNames)
	admission.mu.Unlock()
	if admission.acquireCalls.Load() != 1 || admission.releaseCalls.Load() != 1 ||
		tenant != "tenant-1" || !slices.Equal(indexes, []string{"gradethis"}) {
		t.Fatalf("admission = calls %d/%d scope %q/%#v", admission.acquireCalls.Load(), admission.releaseCalls.Load(), tenant, indexes)
	}

	tampered := compiled
	tampered.Args = slices.Clone(compiled.Args)
	tampered.Args[0] = "other-tenant"
	blockedAdmission := &recordingReadAdmission{}
	connection := &fakeQueryConnection{err: errors.New("must not query")}
	executor = mustExecutor(t, connection)
	executor.readAdmission = blockedAdmission
	got, err := executor.ExecuteStatsWildcardInventory(context.Background(), tampered)
	if !errors.Is(err, searchjobs.ErrInvalidResult) || !got.IsZero() ||
		blockedAdmission.acquireCalls.Load() != 0 || connection.query != "" {
		t.Fatalf("tampered execution = (%#v, %v), admission=%d query=%q", got, err, blockedAdmission.acquireCalls.Load(), connection.query)
	}
}

func validCompiledStatsWildcardInventory(
	t *testing.T,
	source string,
) clickhouse.CompiledStatsWildcardInventory {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := plan.PrepareStatsWildcard(parsed, statsWildcardTestScope())
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).CompileStatsWildcardInventory(
		preparation.Prefix(), preparation.Request(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func statsWildcardTestScope() plan.Scope {
	return plan.Scope{
		TenantID:          "tenant-1",
		AuthorizedIndexes: []string{"gradethis"},
		Earliest:          time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC),
		SearchStart:       time.Date(2026, 7, 22, 0, 0, 0, 500_000_000, time.UTC),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   time.Date(2026, 7, 22, 0, 0, 1, 0, time.UTC),
		VisibilityCutoff:  func() *uint64 { value := uint64(73); return &value }(),
	}
}

func statsWildcardFakeRows(
	metadataInvalid uint8,
	matches ...plan.StatsWildcardInventoryMatch,
) *fakeRows {
	data := make([][]any, 0, len(matches)+1)
	data = append(data, []any{uint8(0), "", metadataInvalid})
	for _, match := range matches {
		data = append(data, []any{match.Ordinal, match.Field, uint8(0)})
	}
	return statsWildcardRows(data)
}

func statsWildcardRows(data [][]any) *fakeRows {
	columns := []string{
		clickhouse.StatsWildcardInventoryOrdinalColumn,
		clickhouse.StatsWildcardInventoryFieldColumn,
		clickhouse.StatsWildcardInventoryInvalidColumn,
	}
	databaseTypes := []string{"UInt8", "String", "UInt8"}
	scanTypes := []reflect.Type{
		reflect.TypeOf(uint8(0)), reflect.TypeOf(""), reflect.TypeOf(uint8(0)),
	}
	types := make([]driver.ColumnType, len(columns))
	for index := range columns {
		types[index] = fakeColumnType{
			name: columns[index], databaseType: databaseTypes[index], scanType: scanTypes[index],
		}
	}
	return &fakeRows{columns: columns, types: types, data: data}
}

func TestStatsWildcardInventoryMaximumFieldValidationBoundary(t *testing.T) {
	t.Parallel()
	maximum := strings.Repeat("x", eventfields.MaximumDynamicPathSegmentBytes)
	if len(maximum) > eventfields.MaximumNormalizedFieldNameBytes || !utf8.ValidString(maximum) {
		t.Fatal("maximum fixture is invalid")
	}
	if err := validateStatsWildcardInventoryField(maximum); err != nil {
		t.Fatalf("maximum legal field rejected: %v", err)
	}
}

func TestStatsWildcardInventoryFieldValidationAcceptsPlannerLiteralReferences(t *testing.T) {
	t.Parallel()
	for _, field := range []string{".com", "Product Name"} {
		if err := validateStatsWildcardInventoryField(field); err != nil {
			t.Fatalf("literal field %q rejected: %v", field, err)
		}
	}
}
