package queryexec

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestStatsByFixedMultivaluePublishesStringOrBytesCells(t *testing.T) {
	t.Parallel()

	query := compileFixedMultivalueStatsByFixture(t)
	descriptors, valid := query.ValidatedResultStringOrBytesOutputs()
	if !valid || len(descriptors) != 1 || descriptors[0].OutputIndex != 0 {
		t.Fatalf("String-or-Bytes descriptors = %#v, valid=%t", descriptors, valid)
	}
	invalid := string([]byte{0xff, 0, 'b', 'y', 't', 'e', 's'})
	rows := &fakeRows{
		columns: append(
			slices.Clone(query.OutputFields),
			descriptors[0].SemanticBytesColumn(),
		),
		types: []driver.ColumnType{
			fakeColumnType{name: "members", databaseType: "String", scanType: reflect.TypeFor[string]()},
			fakeColumnType{name: "count", databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			fakeColumnType{name: descriptors[0].SemanticBytesColumn(), databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
		},
		data: [][]any{
			{"valid", uint64(1), uint8(0)},
			{invalid, uint64(2), uint8(1)},
		},
	}
	sink := &fakeSink{}
	if err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		sink,
	); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !rows.closed || sink.setCalls != 1 || len(sink.rows) != 2 {
		t.Fatalf(
			"publication closed=%t schemas=%d rows=%d",
			rows.closed,
			sink.setCalls,
			len(sink.rows),
		)
	}
	wantColumn := searchjobs.Column{
		Name: "members", Kind: searchjobs.ValueKindMixed, Nullable: true,
	}
	if sink.schema.Columns[0] != wantColumn {
		t.Fatalf("members schema = %#v, want %#v", sink.schema.Columns[0], wantColumn)
	}
	if value, ok := sink.rows[0][0].String(); !ok || value != "valid" {
		t.Fatalf("valid member = %#v", sink.rows[0][0])
	}
	if value, ok := sink.rows[1][0].Bytes(); !ok || !slices.Equal(value, []byte(invalid)) {
		t.Fatalf("invalid member = %#v", sink.rows[1][0])
	}
}

func TestStatsByStringOrBytesTransportRejectsPhysicalTypeDrift(t *testing.T) {
	t.Parallel()

	query := compileFixedMultivalueStatsByFixture(t)
	descriptors, valid := query.ValidatedResultStringOrBytesOutputs()
	if !valid || len(descriptors) != 1 {
		t.Fatalf("String-or-Bytes descriptors = %#v, valid=%t", descriptors, valid)
	}
	rows := &fakeRows{
		columns: append(
			slices.Clone(query.OutputFields),
			descriptors[0].SemanticBytesColumn(),
		),
		types: []driver.ColumnType{
			fakeColumnType{name: "members", databaseType: "Nullable(String)", scanType: reflect.TypeFor[*string](), nullable: true},
			fakeColumnType{name: "count", databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			fakeColumnType{name: descriptors[0].SemanticBytesColumn(), databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
		},
	}
	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(
		context.Background(),
		query,
		&fakeSink{},
	)
	if !errors.Is(err, searchjobs.ErrInvalidResult) {
		t.Fatalf("Execute() error = %v, want ErrInvalidResult", err)
	}
}

func compileFixedMultivalueStatsByFixture(t *testing.T) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(
		`index=main | stats values(host) AS members | stats count BY members`,
	)
	if err != nil {
		t.Fatalf("parse fixed multivalue stats BY fixture: %v", err)
	}
	searchStart := time.Date(2026, time.August, 12, 20, 0, 0, 0, time.UTC)
	visibility := uint64(23)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		Earliest:          searchStart.Add(-time.Hour),
		Latest:            searchStart,
		SearchStart:       searchStart,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   searchStart,
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatalf("plan fixed multivalue stats BY fixture: %v", err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile fixed multivalue stats BY fixture: %v", err)
	}
	return compiled
}
