package searchjobs

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestPrepareNearbyContextPreservesNanosecondsAndExactScalars(t *testing.T) {
	decimal, err := DecimalValue("9007199254740993.000001")
	if err != nil {
		t.Fatal(err)
	}
	anchor := time.Date(2026, time.September, 12, 8, 9, 10, 123456789, time.FixedZone("source", 2*60*60))
	schema := Schema{Columns: []Column{
		{Name: "_raw", Kind: ValueKindString},
		{Name: "host", Kind: ValueKindString},
		{Name: "trace_id", Kind: ValueKindString},
		{Name: "_time", Kind: ValueKindTime},
		{Name: "index", Kind: ValueKindString},
		{Name: "source", Kind: ValueKindString},
		{Name: "large", Kind: ValueKindUnsigned},
		{Name: "decimal", Kind: ValueKindDecimal},
		{Name: "signed", Kind: ValueKindSigned},
		{Name: "enabled", Kind: ValueKindBool},
		{Name: "ratio", Kind: ValueKindDouble},
		{Name: "infinite", Kind: ValueKindDouble},
		{Name: "nested", Kind: ValueKindList},
		{Name: "absent", Kind: ValueKindMissing},
	}}
	row := ResultRow{Ordinal: 17, Values: []Value{
		StringValue("wild*card 'quote' \\ slash\nnewline"),
		StringValue("api-host"), StringValue("trace-value"), TimeValue(anchor),
		StringValue("main"), StringValue("/var/log/app"),
		UnsignedValue(math.MaxUint64), decimal, SignedValue(math.MinInt64),
		BoolValue(true), DoubleValue(1.25), DoubleValue(math.Inf(1)),
		ListValue(StringValue("nested")), MissingValue(),
	}}
	provenance := &NearbyEventProvenance{
		Version:   NearbyEventProvenanceVersion,
		TimeIndex: 3, IndexIndex: 4, HostIndex: 1, SourceIndex: 5,
	}

	nearby, err := PrepareNearbyContext(provenance, schema, row)
	if err != nil {
		t.Fatal(err)
	}
	wantAnchor := anchor.UTC()
	if !nearby.AnchorTime.Equal(wantAnchor) ||
		!nearby.Earliest.Equal(wantAnchor.Add(-5*time.Minute)) ||
		!nearby.Latest.Equal(wantAnchor.Add(5*time.Minute)) || nearby.Clipped {
		t.Fatalf("nearby interval = %+v, want anchor %v with exact five-minute radius", nearby, wantAnchor)
	}
	if nearby.Index != "main" || nearby.Host != "api-host" || nearby.Source != "/var/log/app" {
		t.Fatalf("nearby source identity = (%q, %q, %q)", nearby.Index, nearby.Host, nearby.Source)
	}
	if len(nearby.Fields) != 10 {
		t.Fatalf("comparable fields = %d, want 10: %+v", len(nearby.Fields), nearby.Fields)
	}
	if !nearby.Fields[2].Suggested || nearby.Fields[0].Suggested {
		t.Fatalf("suggested fields = %+v", nearby.Fields)
	}
	large, ok := nearby.Fields[5].Value.Unsigned()
	if !ok || large != math.MaxUint64 {
		t.Fatalf("large unsigned = %d, %t", large, ok)
	}
	exact, ok := nearby.Fields[6].Value.Decimal()
	if !ok || exact != "9007199254740993.000001" {
		t.Fatalf("exact decimal = %q, %t", exact, ok)
	}
}

func TestPrepareNearbyContextClipsToGlobalSearchBounds(t *testing.T) {
	schema := nearbyTestSchema()
	provenance := nearbyTestProvenance()
	for _, test := range []struct {
		name     string
		anchor   time.Time
		earliest time.Time
		latest   time.Time
	}{
		{
			name: "minimum", anchor: time.Date(1900, 1, 1, 0, 1, 0, 1, time.UTC),
			earliest: time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC),
			latest:   time.Date(1900, 1, 1, 0, 6, 0, 1, time.UTC),
		},
		{
			name: "maximum", anchor: time.Date(2261, 12, 31, 23, 59, 0, 999999999, time.UTC),
			earliest: time.Date(2261, 12, 31, 23, 54, 0, 999999999, time.UTC),
			latest:   time.Date(2262, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			nearby, err := PrepareNearbyContext(provenance, schema, nearbyTestRow(test.anchor))
			if err != nil {
				t.Fatal(err)
			}
			if !nearby.Earliest.Equal(test.earliest) || !nearby.Latest.Equal(test.latest) || !nearby.Clipped {
				t.Fatalf("clipped interval = [%v, %v), clipped %t", nearby.Earliest, nearby.Latest, nearby.Clipped)
			}
		})
	}
}

func TestPrepareNearbyContextFailsClosedWithoutExactProvenance(t *testing.T) {
	schema := nearbyTestSchema()
	row := nearbyTestRow(time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC))
	for _, test := range []struct {
		name       string
		provenance *NearbyEventProvenance
		schema     Schema
		row        ResultRow
	}{
		{name: "legacy", schema: schema, row: row},
		{name: "unknown version", provenance: &NearbyEventProvenance{Version: 2}, schema: schema, row: row},
		{name: "duplicate ordinal", provenance: &NearbyEventProvenance{Version: 1, TimeIndex: 0, IndexIndex: 1, HostIndex: 1, SourceIndex: 3}, schema: schema, row: row},
		{name: "schema renamed", provenance: nearbyTestProvenance(), schema: Schema{Columns: []Column{{Name: "time", Kind: ValueKindTime}, schema.Columns[1], schema.Columns[2], schema.Columns[3]}}, row: row},
		{name: "required null", provenance: nearbyTestProvenance(), schema: schema, row: ResultRow{Values: []Value{row.Values[0], row.Values[1], NullValue(), row.Values[3]}}},
		{name: "outside bounds", provenance: nearbyTestProvenance(), schema: schema, row: nearbyTestRow(time.Date(2262, 1, 1, 0, 0, 0, 1, time.UTC))},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := PrepareNearbyContext(test.provenance, test.schema, test.row); !errors.Is(err, ErrNearbyContextUnavailable) {
				t.Fatalf("PrepareNearbyContext() error = %v", err)
			}
		})
	}
}

func TestManagerPersistsCompilerProvenanceAndBindsGeneration(t *testing.T) {
	anchor := time.Date(2026, 9, 12, 10, 11, 12, 987654321, time.UTC)
	clock := &fakeClock{now: anchor.Add(time.Minute)}
	executor := executorFunc(func(_ context.Context, query clickhouse.CompiledQuery, sink ResultSink) error {
		columns := make([]Column, len(query.OutputFields))
		values := make([]Value, len(query.OutputFields))
		for index, name := range query.OutputFields {
			columns[index] = Column{Name: name, Kind: ValueKindString}
			values[index] = StringValue(name + "-value")
			switch name {
			case "_time":
				columns[index].Kind = ValueKindTime
				values[index] = TimeValue(anchor)
			case "index":
				values[index] = StringValue("main")
			case "host":
				values[index] = StringValue("api")
			case "source":
				values[index] = StringValue("events.log")
			}
		}
		if err := sink.SetSchema(Schema{Columns: columns}); err != nil {
			return err
		}
		return sink.AddRow(values)
	})
	manager := newTestManager(t, Config{
		Executor: executor, NewID: sequenceIDs("nearby"), Now: clock.Now,
		RetentionTTL: time.Minute,
	})
	created, err := manager.Create(context.Background(), withSPL(validRequest(), "index=main"))
	if err != nil {
		t.Fatal(err)
	}
	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.NearbyEventProvenance == nil {
		t.Fatal("completed event job did not persist compiler provenance")
	}
	page, err := manager.ResultsFor(AccessScope{TenantID: "tenant", OwnerID: "owner"}, created.ID, PageRequest{})
	if err != nil || page.Generation == 0 {
		t.Fatalf("result generation = %d, error %v", page.Generation, err)
	}
	nearby, err := manager.NearbyContextFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, created.ID, page.Generation, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !nearby.AnchorTime.Equal(anchor) || nearby.Index != "main" {
		t.Fatalf("nearby context = %+v", nearby)
	}
	if _, err := manager.NearbyContextFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "other"}, created.ID, page.Generation, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-owner prepare error = %v", err)
	}
	if _, err := manager.NearbyContextFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, created.ID, page.Generation+1, 0); !errors.Is(err, ErrNearbyContextUnavailable) {
		t.Fatalf("wrong-generation prepare error = %v", err)
	}
	cloned, err := manager.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	cloned.NearbyEventProvenance.TimeIndex++
	again, err := manager.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if again.NearbyEventProvenance.TimeIndex == cloned.NearbyEventProvenance.TimeIndex {
		t.Fatal("mutating detached job provenance changed retained job")
	}
	clock.Add(2 * time.Minute)
	if _, err := manager.NearbyContextFor(context.Background(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, created.ID, page.Generation, 0); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired prepare error = %v", err)
	}
}

func nearbyTestSchema() Schema {
	return Schema{Columns: []Column{
		{Name: "_time", Kind: ValueKindTime},
		{Name: "index", Kind: ValueKindString},
		{Name: "host", Kind: ValueKindString},
		{Name: "source", Kind: ValueKindString},
	}}
}

func nearbyTestProvenance() *NearbyEventProvenance {
	return &NearbyEventProvenance{
		Version:   NearbyEventProvenanceVersion,
		TimeIndex: 0, IndexIndex: 1, HostIndex: 2, SourceIndex: 3,
	}
}

func nearbyTestRow(anchor time.Time) ResultRow {
	return ResultRow{Values: []Value{
		TimeValue(anchor), StringValue("main"), StringValue("host"), StringValue("source"),
	}}
}
