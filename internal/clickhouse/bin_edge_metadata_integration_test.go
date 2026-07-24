package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

var (
	// The fixtures below use the shared stored-event helper, whose event time is
	// 2026-07-21T03:04:05+05:00, i.e. 2026-07-20T22:04:05Z. The search snapshot
	// must therefore open on the twentieth.
	binEdgeMetadataEarliest = time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC)
	binEdgeMetadataLatest   = time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC)
)

// TestBinEdgeMetadataAgainstClickHouse exercises the runtime Dynamic bin
// contract that only the database can settle: which events keep a destination
// they already had, which semantic type that destination reports afterwards,
// and how the bucketed value is described to field catalogs, field summaries,
// and timelines. It starts its own pinned container so it can also manufacture
// aligned-metadata states the store writer intentionally cannot produce.
func TestBinEdgeMetadataAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	connection, store := binEdgeMetadataCluster(t, ctx)

	indexTime := time.Date(2026, time.July, 21, 3, 4, 6, 987654321, time.UTC)
	extendedTimestamp := time.Date(2026, time.July, 21, 3, 4, 5, 123_456_789, time.UTC)
	extendedDuration := 3*time.Second + 4*time.Nanosecond

	// The fixture matrix deliberately mixes presence states for one source and
	// one destination so a single query separates "missing" from "explicitly
	// null" and from "present with a bucketable value".
	events := []*ingest.StoredEvent{
		binEdgeMetadataEvent("m-int", "compiler", "edge-batch", "value=47 alpha", indexTime,
			typedField("edge_src", typedSint(-11)),
			typedField("edge_dst", typedSint(7)),
			typedField("edge_decimal", typedDecimal("123.4500")),
			typedField("edge_flag", typedBool(true)),
			typedField("edge_obj", typedObject(
				typedField("child", typedSint(25)),
				typedField("grand", typedObject(typedField("leaf", typedSint(31)))),
			)),
			typedField("edge_ts", typedTimestamp(extendedTimestamp)),
			typedField("edge_dur", typedDuration(extendedDuration)),
			typedField("edge_bytes", typedBytes([]byte{0, 1, 2, 255})),
		),
		// No source at all: every destination this event already holds must
		// survive the bin untouched, including a tagged decimal that would be a
		// runtime error if bin had actually read it.
		binEdgeMetadataEvent("m-miss", "compiler", "edge-batch", "no digits here", indexTime,
			typedField("edge_dst", typedSint(7)),
			typedField("edge_cap", typedSint(55)),
			typedField("edge_decimal", typedDecimal("123.4500")),
			typedField("edge_flag", typedBool(true)),
			typedField("edge_ts", typedTimestamp(extendedTimestamp)),
			typedField("edge_dur", typedDuration(extendedDuration)),
			typedField("edge_bytes", typedBytes([]byte{0, 1, 2, 255})),
		),
		// An explicit null source is present, so it is binned and the bucket is
		// null: the destination's earlier value does not survive.
		binEdgeMetadataEvent("m-null", "compiler", "edge-batch", "value=8 beta", indexTime,
			typedField("edge_src", typedNull()),
			typedField("edge_dst", typedSint(3)),
			typedField("edge_cap", typedNull()),
		),
		// Raw text without a lowercase letter leaves a rex-made destination
		// absent, which is the only way to observe a rex destination that never
		// existed at all.
		binEdgeMetadataEvent("m-obj", "compiler", "edge-batch", "0000", indexTime,
			typedField("edge_obj", typedObject(typedField("child", typedSint(9)))),
			typedField("edge_container", typedList(typedSint(1), typedSint(2))),
		),
		binEdgeMetadataEvent("m-text", "compiler", "edge-batch", "plain text", indexTime,
			typedField("edge_src", typedString("25")),
			typedField("edge_dst", typedString("prior")),
			typedField("edge_cap", typedString("not-numeric")),
		),
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "edge-batch", BatchSequence: 1,
		SourceBatchSHA256: testSourceBatchDigest("edge-batch"),
		ReceivedAt:        indexTime,
		Events:            events,
	}); err != nil {
		t.Fatalf("store bin edge fixtures: %v", err)
	}

	// A third index isolates the runtime scalar type mix so its row count cannot
	// disturb the presence arithmetic asserted for the fixtures above.
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "mixed-batch", BatchSequence: 3,
		SourceBatchSHA256: testSourceBatchDigest("mixed-batch"),
		ReceivedAt:        indexTime,
		Events: []*ingest.StoredEvent{
			binEdgeMetadataEvent("x-double", "mixed", "mixed-batch", "double", indexTime,
				typedField("edge_num", typedDouble(25))),
			binEdgeMetadataEvent("x-negzero", "mixed", "mixed-batch", "negative zero", indexTime,
				typedField("edge_num", typedDouble(math.Copysign(0, -1)))),
			binEdgeMetadataEvent("x-signed", "mixed", "mixed-batch", "small signed", indexTime,
				typedField("edge_num", typedSint(25))),
			binEdgeMetadataEvent("x-negative", "mixed", "mixed-batch", "negative signed", indexTime,
				typedField("edge_num", typedSint(-25))),
			// A small unsigned value is the one runtime scalar whose stored
			// semantic type and physical Dynamic type could plausibly disagree
			// without any metadata damage.
			binEdgeMetadataEvent("x-unsigned", "mixed", "mixed-batch", "small unsigned", indexTime,
				typedField("edge_num", typedUint(25))),
			binEdgeMetadataEvent("x-unsigned-max", "mixed", "mixed-batch", "maximum unsigned", indexTime,
				typedField("edge_num", typedUint(^uint64(0)))),
		},
	}); err != nil {
		t.Fatalf("store runtime scalar mix fixtures: %v", err)
	}

	// A wide event pushes its last leaves out of the bounded dynamic-path budget
	// and into ClickHouse's shared-data encoding, where the runtime type must
	// still be readable for classification.
	numericPads := make([]*opensplunkv1.TypedObjectField, 0, 300)
	textPads := make([]*opensplunkv1.TypedObjectField, 0, 300)
	for index := 0; index < 300; index++ {
		// Every leaf spells the same value, so the assertion below does not
		// depend on which leaves ClickHouse chose to keep as subcolumns.
		numericPads = append(numericPads, typedField(fmt.Sprintf("pad_%03d", index), typedSint(25)))
		textPads = append(textPads, typedField(fmt.Sprintf("ptx_%03d", index), typedString("25")))
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "wide-batch", BatchSequence: 4,
		SourceBatchSHA256: testSourceBatchDigest("wide-batch"),
		ReceivedAt:        indexTime,
		Events: []*ingest.StoredEvent{
			binEdgeMetadataEvent("w-num", "wide", "wide-batch", "wide numeric event", indexTime, numericPads...),
			binEdgeMetadataEvent("w-txt", "wide", "wide-batch", "wide text event", indexTime, textPads...),
		},
	}); err != nil {
		t.Fatalf("store wide fixture: %v", err)
	}

	// A separate index holds the rows whose aligned metadata is rewritten below
	// into states the store writer can never produce, so those rows cannot make
	// every other assertion in this test depend on damaged metadata.
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "legacy-batch", BatchSequence: 2,
		SourceBatchSHA256: testSourceBatchDigest("legacy-batch"),
		ReceivedAt:        indexTime,
		Events: []*ingest.StoredEvent{
			binEdgeMetadataEvent("l-int", "legacy", "legacy-batch", "legacy signed", indexTime,
				typedField("edge_src", typedSint(-11)),
				typedField("edge_dst", typedSint(7)),
			),
			binEdgeMetadataEvent("l-text", "legacy", "legacy-batch", "legacy text", indexTime,
				typedField("edge_src", typedString("25")),
			),
			binEdgeMetadataEvent("l-skew", "legacy", "legacy-batch", "skewed metadata", indexTime,
				typedField("edge_src", typedSint(-11)),
			),
		},
	}); err != nil {
		t.Fatalf("store legacy metadata fixtures: %v", err)
	}
	// Version zero is exactly what a row written before migration 0003 looks
	// like: the sorted name array survives, the aligned type array does not.
	if err := connection.Exec(ctx,
		"ALTER TABLE open_splunk.events UPDATE `field_types` = CAST([], 'Array(UInt8)'), "+
			"`field_metadata_version` = toUInt8(0) WHERE event_id IN ('l-int', 'l-text') "+
			"SETTINGS mutations_sync = 2",
	); err != nil {
		t.Fatalf("rewrite legacy field metadata: %v", err)
	}
	// A row whose aligned semantic type disagrees with the physical Dynamic type
	// is corrupt rather than legacy, so it keeps the current metadata version.
	if err := connection.Exec(ctx,
		"ALTER TABLE open_splunk.events UPDATE `field_types` = arrayMap((name, code) -> "+
			"if(name = 'edge_src', toUInt8(2), code), `field_names`, `field_types`) "+
			"WHERE event_id = 'l-skew' SETTINGS mutations_sync = 2",
	); err != nil {
		t.Fatalf("skew legacy field metadata: %v", err)
	}
	var legacyRows, skewedRows uint64
	if err := connection.QueryRow(ctx,
		"SELECT countIf(`field_metadata_version` = 0 AND empty(`field_types`) AND has(`field_names`, 'edge_src')), "+
			"countIf(event_id = 'l-skew' AND `field_metadata_version` = 1 AND has(`field_types`, toUInt8(2))) "+
			"FROM open_splunk.events WHERE index_name = 'legacy'",
	).Scan(&legacyRows, &skewedRows); err != nil {
		t.Fatalf("verify manufactured metadata: %v", err)
	}
	if legacyRows != 2 || skewedRows != 1 {
		t.Fatalf("manufactured metadata = legacy:%d skewed:%d, want 2/1", legacyRows, skewedRows)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture visibility cutoff: %v", err)
	}
	cutoff := indexTime.Add(10 * time.Second)
	compile := func(t *testing.T, source string) CompiledQuery {
		t.Helper()
		return binEdgeMetadataCompile(t, source, cutoff, visibilityCutoff)
	}
	build := func(t *testing.T, source string) *plan.Query {
		t.Helper()
		return binEdgeMetadataPlan(t, source, cutoff, visibilityCutoff)
	}

	t.Run("missing source preserves a stored destination", func(t *testing.T) {
		compiled := compile(t, `index=compiler | bin edge_src span=10 AS edge_dst | table event_id edge_dst`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "edge_dst")
		want := []binEdgeMetadataRow{
			{"m-int", "Int64", "-20"},
			// The source is absent, so bin never writes and the destination's
			// exact prior value and signed type survive.
			{"m-miss", "Int64", "7"},
			// An explicit null is present and therefore binned: the
			// destination's earlier value 3 is correctly replaced by null.
			{"m-null", "None", "<none>"},
			{"m-obj", "None", "<none>"},
			// Numeric text becomes the number it spells.
			{"m-text", "Int64", "20"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("preserved destination values = %#v, want %#v", got, want)
		}

		// The catalog must describe presence, nullness, and semantic type from
		// the bin's own calculated metadata, not from the immutable document.
		profile := binEdgeMetadataCatalogProfile(t, ctx, connection,
			build(t, `index=compiler | bin edge_src span=10 AS edge_dst`), "edge_dst")
		wantProfile := binEdgeMetadataProfile{
			rows: 1, total: 5, events: 4, nulls: 1, missing: 1,
			types: []uint8{
				uint8(eventfields.StoredValueTypeNull),
				uint8(eventfields.StoredValueTypeSint64),
			},
		}
		if !reflect.DeepEqual(profile, wantProfile) {
			t.Fatalf("preserved destination catalog profile = %#v, want %#v", profile, wantProfile)
		}
	})

	t.Run("missing source preserves a rex-made destination", func(t *testing.T) {
		compiled := compile(t,
			`index=compiler | rex field=_raw "(?<edge_rex>[a-z]+)" `+
				`| bin edge_src span=10 AS edge_rex | table event_id edge_rex`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "edge_rex")
		want := []binEdgeMetadataRow{
			{"m-int", "Int64", "-20"},
			// rex captured "no" and the bin has no source, so the capture and
			// its String semantic type survive untouched.
			{"m-miss", "String", "no"},
			{"m-null", "None", "<none>"},
			// rex did not match and no prior value existed, so the destination
			// stays absent rather than becoming an unsupported-value error.
			{"m-obj", "None", "<none>"},
			{"m-text", "Int64", "20"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("preserved rex destination values = %#v, want %#v", got, want)
		}

		profile := binEdgeMetadataCatalogProfile(t, ctx, connection,
			build(t, `index=compiler | rex field=_raw "(?<edge_rex>[a-z]+)" | bin edge_src span=10 AS edge_rex`),
			"edge_rex")
		wantProfile := binEdgeMetadataProfile{
			rows: 1, total: 5, events: 4, nulls: 1, missing: 1,
			types: []uint8{
				uint8(eventfields.StoredValueTypeNull),
				uint8(eventfields.StoredValueTypeString),
				uint8(eventfields.StoredValueTypeSint64),
			},
		}
		if !reflect.DeepEqual(profile, wantProfile) {
			t.Fatalf("preserved rex destination catalog profile = %#v, want %#v", profile, wantProfile)
		}
	})

	t.Run("missing source preserves an eval-made destination", func(t *testing.T) {
		compiled := compile(t,
			`index=compiler | eval edge_eval="keep" | bin edge_src span=10 AS edge_eval | table event_id edge_eval`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "edge_eval")
		want := []binEdgeMetadataRow{
			{"m-int", "Int64", "-20"},
			{"m-miss", "String", "keep"},
			{"m-null", "None", "<none>"},
			{"m-obj", "String", "keep"},
			{"m-text", "Int64", "20"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("preserved eval destination values = %#v, want %#v", got, want)
		}
	})

	t.Run("bin classifies a rex output from that stage's metadata", func(t *testing.T) {
		compiled := compile(t,
			`index=compiler | rex field=_raw "(?<edge_cap>[0-9]+)" | bin edge_cap span=10 | table event_id edge_cap`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "edge_cap")
		want := []binEdgeMetadataRow{
			// Captured text spells an integer, so it buckets as a number.
			{"m-int", "Int64", "40"},
			// rex did not match, so the stored signed value survives into bin
			// and is bucketed through the private stage type, not through a
			// String reclassification.
			{"m-miss", "Int64", "50"},
			{"m-null", "Int64", "0"},
			{"m-obj", "Int64", "0"},
			// rex did not match and the prior value is non-numeric text, which
			// keeps its exact spelling instead of failing the search.
			{"m-text", "String", "not-numeric"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("bin after rex values = %#v, want %#v", got, want)
		}
	})

	t.Run("bin classifies an eval copy from the copied leaf", func(t *testing.T) {
		compiled := compile(t,
			`index=compiler | eval edge_copy=edge_src | bin edge_copy span=10 | table event_id edge_copy`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "edge_copy")
		want := []binEdgeMetadataRow{
			{"m-int", "Int64", "-20"},
			// eval materializes an output for every row, so a copied absent
			// leaf becomes an explicit null rather than an unclassified value.
			{"m-miss", "None", "<none>"},
			{"m-null", "None", "<none>"},
			{"m-obj", "None", "<none>"},
			{"m-text", "Int64", "20"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("bin after eval copy values = %#v, want %#v", got, want)
		}
	})

	t.Run("flattened object parents and dotted descendants", func(t *testing.T) {
		compiled := compile(t, `index=compiler | bin edge_obj.child span=10 AS band | table event_id band`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "band")
		want := []binEdgeMetadataRow{
			{"m-int", "Int64", "20"},
			{"m-miss", "None", "<none>"},
			{"m-null", "None", "<none>"},
			{"m-obj", "Int64", "0"},
			{"m-text", "None", "<none>"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("dotted descendant bin values = %#v, want %#v", got, want)
		}

		nested := compile(t, `index=compiler event_id=m-int | bin edge_obj.grand.leaf span=10 AS band | table event_id band`)
		if deep := binEdgeMetadataScan(t, ctx, connection, nested, "band"); !reflect.DeepEqual(
			deep, []binEdgeMetadataRow{{"m-int", "Int64", "30"}},
		) {
			t.Fatalf("nested descendant bin values = %#v, want m-int Int64 30", deep)
		}

		// Both the shallow and the intermediate flattened parent are containers
		// and carry the sanitized unsupported-value marker.
		for _, field := range []string{"edge_obj", "edge_obj.grand"} {
			field := field
			t.Run("parent "+field, func(t *testing.T) {
				parent := compile(t, `index=compiler event_id=m-int | bin `+field+` span=10 | table `+field)
				binEdgeMetadataRequireMarker(t, ctx, connection, parent, "flattened parent "+field)
			})
		}
	})

	t.Run("tagged and boolean scalars pass through with their exact value", func(t *testing.T) {
		for _, extended := range []struct {
			field       string
			physical    string
			storedType  eventfields.StoredValueType
			expectEqual bool
		}{
			{"edge_ts", "Map(String, String)", eventfields.StoredValueTypeTimestamp, true},
			{"edge_dur", "Map(String, String)", eventfields.StoredValueTypeDuration, true},
			{"edge_bytes", "Map(String, String)", eventfields.StoredValueTypeBytes, true},
			{"edge_flag", "Bool", eventfields.StoredValueTypeBool, true},
		} {
			extended := extended
			t.Run(extended.field, func(t *testing.T) {
				compiled := compile(t,
					`index=compiler | bin `+extended.field+` span=10 AS band | table event_id `+extended.field+` band`)
				var rows, identical uint64
				if err := connection.QueryRow(ctx,
					`SELECT countIf(dynamicType(band) = `+quoteStringLiteralForBinEdge(extended.physical)+`),
						countIf(dynamicType(band) != 'None'
							AND toString(band) = toString(`+quoteIdentifier(extended.field)+`)
							AND dynamicType(band) = dynamicType(`+quoteIdentifier(extended.field)+`))
					FROM (`+compiled.SQL+`)`,
					compiled.Args...,
				).Scan(&rows, &identical); err != nil {
					t.Fatalf("execute extended pass-through: %v\nSQL: %s", err, compiled.SQL)
				}
				if rows != 2 || identical != 2 {
					t.Fatalf("extended pass-through = typed:%d identical:%d, want 2/2", rows, identical)
				}

				profile := binEdgeMetadataCatalogProfile(t, ctx, connection,
					build(t, `index=compiler | bin `+extended.field+` span=10 AS band`), "band")
				wantProfile := binEdgeMetadataProfile{
					rows: 1, total: 5, events: 2, nulls: 0, missing: 3,
					types: []uint8{uint8(extended.storedType)},
				}
				if !reflect.DeepEqual(profile, wantProfile) {
					t.Fatalf("extended pass-through catalog profile = %#v, want %#v", profile, wantProfile)
				}
			})
		}
	})

	t.Run("a tagged decimal destination is never read by a missing source", func(t *testing.T) {
		compiled := compile(t, `index=compiler | bin edge_src span=10 AS edge_decimal | table event_id edge_decimal`)
		var preserved, replaced uint64
		if err := connection.QueryRow(ctx,
			`SELECT countIf(dynamicType(edge_decimal) = 'Map(String, String)' AND event_id = 'm-miss'),
				countIf(dynamicType(edge_decimal) = 'Int64' AND toString(edge_decimal) = '-20')
			FROM (`+compiled.SQL+`)`,
			compiled.Args...,
		).Scan(&preserved, &replaced); err != nil {
			t.Fatalf("execute decimal destination bin: %v\nSQL: %s", err, compiled.SQL)
		}
		if preserved != 1 || replaced != 1 {
			t.Fatalf("decimal destination = preserved:%d replaced:%d, want 1/1", preserved, replaced)
		}

		// The same value is an explicit runtime error the moment bin reads it.
		asSource := compile(t, `index=compiler event_id=m-miss | bin edge_decimal span=10 | table edge_decimal`)
		binEdgeMetadataRequireMarker(t, ctx, connection, asSource, "tagged decimal source")
	})

	t.Run("containers abort with the classified marker", func(t *testing.T) {
		container := compile(t, `index=compiler event_id=m-obj | bin edge_container span=10 | table edge_container`)
		binEdgeMetadataRequireMarker(t, ctx, connection, container, "multivalue source")
	})

	t.Run("consecutive bins reuse one destination", func(t *testing.T) {
		compiled := compile(t,
			`index=compiler | bin edge_src span=10 AS band | bin edge_obj.child span=7 AS band | table event_id band`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "band")
		want := []binEdgeMetadataRow{
			// Both sources exist, so the second bin overwrites the first.
			{"m-int", "Int64", "21"},
			// Neither source exists, so the destination stays absent.
			{"m-miss", "None", "<none>"},
			// The first bin wrote an explicit null; the second bin has no source
			// and must keep that null rather than resurrect anything.
			{"m-null", "None", "<none>"},
			// Only the second bin has a source: 9 floors onto the span-7 grid.
			{"m-obj", "Int64", "7"},
			// Only the first bin has a source, so its bucket survives.
			{"m-text", "Int64", "20"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("chained destination values = %#v, want %#v", got, want)
		}
	})

	t.Run("rows written before the aligned metadata pass through unbinned", func(t *testing.T) {
		compiled := compile(t, `index=legacy event_id!=l-skew | bin edge_src span=10 | table event_id edge_src`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "edge_src")
		want := []binEdgeMetadataRow{
			// Neither value is interpreted heuristically, and neither fails the
			// search: both keep exactly what the row already held.
			{"l-int", "Int64", "-11"},
			{"l-text", "String", "25"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("legacy metadata bin values = %#v, want %#v", got, want)
		}

		// A legacy row's missing source still preserves its destination.
		aliased := compile(t, `index=legacy event_id=l-int | bin edge_missing span=10 AS edge_dst | table event_id edge_dst`)
		if kept := binEdgeMetadataScan(t, ctx, connection, aliased, "edge_dst"); !reflect.DeepEqual(
			kept, []binEdgeMetadataRow{{"l-int", "Int64", "7"}},
		) {
			t.Fatalf("legacy destination preservation = %#v, want l-int Int64 7", kept)
		}
	})

	t.Run("a stored type that disagrees with the runtime type is never guessed", func(t *testing.T) {
		compiled := compile(t, `index=legacy event_id=l-skew | bin edge_src span=10 | table edge_src`)
		binEdgeMetadataRequireMarker(t, ctx, connection, compiled, "skewed stored metadata")
	})

	t.Run("bin output feeds field summaries and timelines", func(t *testing.T) {
		logical := build(t, `index=compiler | bin edge_src span=10 AS edge_dst`)
		summary, err := (Compiler{}).CompileFieldSummary(logical, FieldSummarySpec{
			FieldName:             "edge_dst",
			MaximumValues:         10,
			MaximumDistinctValues: 100,
			MaximumValueBytes:     4_096,
		})
		if err != nil {
			t.Fatalf("CompileFieldSummary: %v", err)
		}
		control := `SELECT count(),
				max(` + quoteIdentifier(FieldSummaryMetadataInvalidColumn) + `),
				max(` + quoteIdentifier(FieldSummaryUnsupportedColumn) + `),
				arraySort(groupArrayIf(concat(toString(` + quoteIdentifier(FieldSummaryValueTypeColumn) + `), ':',
					` + quoteIdentifier(FieldSummaryEncodedValueColumn) + `, ':',
					toString(` + quoteIdentifier(FieldSummaryValueCountColumn) + `)),
					` + quoteIdentifier(FieldSummaryRowKindColumn) + ` = 1))
			FROM (` + summary.SQL + `)`
		var summaryRows uint64
		var summaryInvalid, summaryUnsupported uint8
		var encoded []string
		if err := connection.QueryRow(ctx, control, summary.Args...).Scan(
			&summaryRows, &summaryInvalid, &summaryUnsupported, &encoded,
		); err != nil {
			t.Fatalf("execute Dynamic bin field summary: %v\nSQL: %s", err, summary.SQL)
		}
		wantEncoded := []string{
			fmt.Sprintf("%d:-20:1", uint8(eventfields.StoredValueTypeSint64)),
			fmt.Sprintf("%d:20:1", uint8(eventfields.StoredValueTypeSint64)),
			fmt.Sprintf("%d:7:1", uint8(eventfields.StoredValueTypeSint64)),
		}
		if summaryInvalid != 0 || summaryUnsupported != 0 || !reflect.DeepEqual(encoded, wantEncoded) {
			t.Fatalf(
				"Dynamic bin field summary = rows:%d invalid:%d unsupported:%d values:%#v, want values %#v",
				summaryRows, summaryInvalid, summaryUnsupported, encoded, wantEncoded,
			)
		}

		timeline, err := (Compiler{}).CompileTimeline(
			build(t, `index=compiler | bin edge_src span=10 AS edge_dst | where edge_dst>=7`),
			TimelineSpec{
				FirstBucket: binEdgeMetadataEarliest,
				SpanSeconds: 3_600,
				BucketCount: 48,
				Earliest:    binEdgeMetadataEarliest,
				Latest:      binEdgeMetadataLatest,
			},
		)
		if err != nil {
			t.Fatalf("CompileTimeline: %v", err)
		}
		var buckets, counted uint64
		if err := connection.QueryRow(ctx,
			`SELECT count(), sum(`+quoteIdentifier(TimelineCountColumn)+`) FROM (`+timeline.SQL+`)`,
			timeline.Args...,
		).Scan(&buckets, &counted); err != nil {
			t.Fatalf("execute Dynamic bin timeline: %v\nSQL: %s", err, timeline.SQL)
		}
		// Only the preserved 7 and the bucketed numeric text 20 clear the
		// downstream numeric predicate.
		if buckets != 48 || counted != 2 {
			t.Fatalf("Dynamic bin timeline = buckets:%d counted:%d, want 48/2", buckets, counted)
		}
	})

	t.Run("every runtime scalar the writer produces is classified", func(t *testing.T) {
		// Prove first that the fixtures really do cover distinct physical
		// Dynamic types; otherwise the bin assertions below would be vacuous.
		physical, err := connection.Query(ctx,
			"SELECT event_id, dynamicType(fields.edge_num) FROM open_splunk.events "+
				"WHERE index_name = 'mixed' ORDER BY event_id")
		if err != nil {
			t.Fatalf("query runtime scalar physical types: %v", err)
		}
		observed := make(map[string]string, 6)
		for physical.Next() {
			var eventID, physicalType string
			if err := physical.Scan(&eventID, &physicalType); err != nil {
				_ = physical.Close()
				t.Fatalf("scan runtime scalar physical types: %v", err)
			}
			observed[eventID] = physicalType
		}
		if err := physical.Close(); err != nil {
			t.Fatalf("close runtime scalar physical types: %v", err)
		}
		wantPhysical := map[string]string{
			"x-double": "Float64", "x-negzero": "Float64",
			"x-signed": "Int64", "x-negative": "Int64",
			"x-unsigned": "UInt64", "x-unsigned-max": "UInt64",
		}
		if !reflect.DeepEqual(observed, wantPhysical) {
			t.Fatalf("stored runtime physical types = %#v, want %#v", observed, wantPhysical)
		}

		compiled := compile(t, `index=mixed | bin edge_num span=10 | table event_id edge_num`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "edge_num")
		want := []binEdgeMetadataRow{
			{"x-double", "Float64", "20"},
			{"x-negative", "Int64", "-30"},
			// A negative zero bucket is normalized so it groups with zero.
			{"x-negzero", "Float64", "0"},
			{"x-signed", "Int64", "20"},
			{"x-unsigned", "UInt64", "20"},
			{"x-unsigned-max", "UInt64", "18446744073709551610"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("runtime scalar mix values = %#v, want %#v", got, want)
		}

		profile := binEdgeMetadataCatalogProfile(t, ctx, connection,
			build(t, `index=mixed | bin edge_num span=10`), "edge_num")
		wantProfile := binEdgeMetadataProfile{
			rows: 1, total: 6, events: 6, nulls: 0, missing: 0,
			types: []uint8{
				uint8(eventfields.StoredValueTypeSint64),
				uint8(eventfields.StoredValueTypeSint64),
				uint8(eventfields.StoredValueTypeUint64),
				uint8(eventfields.StoredValueTypeUint64),
				uint8(eventfields.StoredValueTypeDouble),
				uint8(eventfields.StoredValueTypeDouble),
			},
		}
		// groupUniqArray collapses duplicates, so compare the distinct set.
		wantProfile.types = []uint8{
			uint8(eventfields.StoredValueTypeSint64),
			uint8(eventfields.StoredValueTypeUint64),
			uint8(eventfields.StoredValueTypeDouble),
		}
		if !reflect.DeepEqual(profile, wantProfile) {
			t.Fatalf("runtime scalar mix catalog profile = %#v, want %#v", profile, wantProfile)
		}
	})

	t.Run("a renamed source keeps its stored classification", func(t *testing.T) {
		compiled := compile(t, `index=mixed | rename edge_num AS moved | bin moved span=10 | table event_id moved`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "moved")
		want := []binEdgeMetadataRow{
			{"x-double", "Float64", "20"},
			{"x-negative", "Int64", "-30"},
			{"x-negzero", "Float64", "0"},
			{"x-signed", "Int64", "20"},
			{"x-unsigned", "UInt64", "20"},
			{"x-unsigned-max", "UInt64", "18446744073709551610"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("renamed source bin values = %#v, want %#v", got, want)
		}
	})

	t.Run("a type-mixed bucket converges under a lexical group key", func(t *testing.T) {
		compiled := compile(t, `index=mixed | bin edge_num span=10 | stats count BY edge_num`)
		rows, err := connection.Query(ctx, compiled.SQL, compiled.Args...)
		if err != nil {
			t.Fatalf("execute type-mixed bin grouping: %v\nSQL: %s", err, compiled.SQL)
		}
		defer func() { _ = rows.Close() }()
		grouped := make(map[string]uint64, 4)
		for rows.Next() {
			var key string
			var count uint64
			if err := rows.Scan(&key, &count); err != nil {
				t.Fatalf("scan type-mixed bin grouping: %v", err)
			}
			grouped[key] = count
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate type-mixed bin grouping: %v", err)
		}
		// The signed, unsigned, and double buckets that all spell twenty
		// converge; nothing else collides.
		want := map[string]uint64{"-30": 1, "0": 1, "20": 3, "18446744073709551610": 1}
		if !reflect.DeepEqual(grouped, want) {
			t.Fatalf("type-mixed bin grouping = %#v, want %#v", grouped, want)
		}
	})

	t.Run("a type-mixed bucket keeps its per-value semantic type in a summary", func(t *testing.T) {
		summary, err := (Compiler{}).CompileFieldSummary(
			build(t, `index=mixed | bin edge_num span=10`),
			FieldSummarySpec{
				FieldName:             "edge_num",
				MaximumValues:         20,
				MaximumDistinctValues: 100,
				MaximumValueBytes:     4_096,
			},
		)
		if err != nil {
			t.Fatalf("CompileFieldSummary: %v", err)
		}
		q := quoteIdentifier
		control := `SELECT max(` + q(FieldSummaryMetadataInvalidColumn) + `),
				max(` + q(FieldSummaryUnsupportedColumn) + `),
				arraySort(groupArrayIf(concat(toString(` + q(FieldSummaryValueTypeColumn) + `), ':',
					` + q(FieldSummaryEncodedValueColumn) + `, ':',
					toString(` + q(FieldSummaryValueCountColumn) + `)),
					` + q(FieldSummaryRowKindColumn) + ` = 1))
			FROM (` + summary.SQL + `)`
		var invalid, unsupported uint8
		var encoded []string
		if err := connection.QueryRow(ctx, control, summary.Args...).Scan(&invalid, &unsupported, &encoded); err != nil {
			t.Fatalf("execute type-mixed bin summary: %v\nSQL: %s", err, summary.SQL)
		}
		signed := uint8(eventfields.StoredValueTypeSint64)
		unsigned := uint8(eventfields.StoredValueTypeUint64)
		double := uint8(eventfields.StoredValueTypeDouble)
		want := []string{
			fmt.Sprintf("%d:-30:1", signed),
			fmt.Sprintf("%d:20:1", signed),
			fmt.Sprintf("%d:18446744073709551610:1", unsigned),
			fmt.Sprintf("%d:20:1", unsigned),
			fmt.Sprintf("%d:0:1", double),
			fmt.Sprintf("%d:20:1", double),
		}
		wantSorted := append([]string(nil), want...)
		slices.Sort(wantSorted)
		if invalid != 0 || unsupported != 0 || !reflect.DeepEqual(encoded, wantSorted) {
			t.Fatalf("type-mixed bin summary = invalid:%d unsupported:%d values:%#v, want %#v",
				invalid, unsupported, encoded, wantSorted)
		}
	})

	t.Run("a flattened object parent as an AS destination", func(t *testing.T) {
		// bin never reads the destination, so writing to a parent name cannot
		// raise the container error the parent would raise as a source.
		preserved := compile(t, `index=compiler | bin edge_absent span=10 AS edge_obj | table event_id edge_obj`)
		got := binEdgeMetadataScan(t, ctx, connection, preserved, "edge_obj")
		want := []binEdgeMetadataRow{
			{"m-int", "None", "<none>"},
			{"m-miss", "None", "<none>"},
			{"m-null", "None", "<none>"},
			{"m-obj", "None", "<none>"},
			{"m-text", "None", "<none>"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("flattened parent destination values = %#v, want %#v", got, want)
		}
		profile := binEdgeMetadataCatalogProfile(t, ctx, connection,
			build(t, `index=compiler | bin edge_absent span=10 AS edge_obj`), "edge_obj")
		wantProfile := binEdgeMetadataProfile{
			rows: 1, total: 5, events: 2, nulls: 0, missing: 3,
			types: []uint8{uint8(eventfields.StoredValueTypeObject)},
		}
		if !reflect.DeepEqual(profile, wantProfile) {
			t.Fatalf("flattened parent destination catalog profile = %#v, want %#v", profile, wantProfile)
		}

		// A descendant may also overwrite the parent name it was flattened
		// under; the descendant itself stays addressable and unchanged.
		shadowed := compile(t,
			`index=compiler | bin edge_obj.child span=10 AS edge_obj | table event_id edge_obj edge_obj.child`)
		if values := binEdgeMetadataScan(t, ctx, connection, shadowed, "edge_obj"); !reflect.DeepEqual(
			values,
			[]binEdgeMetadataRow{
				{"m-int", "Int64", "20"},
				{"m-miss", "None", "<none>"},
				{"m-null", "None", "<none>"},
				{"m-obj", "Int64", "0"},
				{"m-text", "None", "<none>"},
			},
		) {
			t.Fatalf("descendant shadowing its parent = %#v", values)
		}
		if descendants := binEdgeMetadataScan(t, ctx, connection, shadowed, "edge_obj.child"); !reflect.DeepEqual(
			descendants,
			[]binEdgeMetadataRow{
				{"m-int", "Int64", "25"},
				{"m-miss", "None", "<none>"},
				{"m-null", "None", "<none>"},
				{"m-obj", "Int64", "9"},
				{"m-text", "None", "<none>"},
			},
		) {
			t.Fatalf("retained descendant source = %#v", descendants)
		}
	})

	t.Run("an explicitly projected schema keeps the same presence contract", func(t *testing.T) {
		// A closed schema resolves both the source and the destination from the
		// projection rather than from the open event schema, which is a
		// different metadata path to the same contract.
		compiled := compile(t,
			`index=compiler | table event_id edge_src edge_dst `+
				`| bin edge_src span=10 AS edge_dst | table event_id edge_dst`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "edge_dst")
		want := []binEdgeMetadataRow{
			{"m-int", "Int64", "-20"},
			{"m-miss", "Int64", "7"},
			{"m-null", "None", "<none>"},
			{"m-obj", "None", "<none>"},
			{"m-text", "Int64", "20"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("projected-schema destination values = %#v, want %#v", got, want)
		}

		profile := binEdgeMetadataCatalogProfile(t, ctx, connection,
			build(t, `index=compiler | table event_id edge_src edge_dst | bin edge_src span=10 AS edge_dst`),
			"edge_dst")
		wantProfile := binEdgeMetadataProfile{
			rows: 1, total: 5, events: 4, nulls: 1, missing: 1,
			types: []uint8{
				uint8(eventfields.StoredValueTypeNull),
				uint8(eventfields.StoredValueTypeSint64),
			},
		}
		if !reflect.DeepEqual(profile, wantProfile) {
			t.Fatalf("projected-schema catalog profile = %#v, want %#v", profile, wantProfile)
		}
	})

	t.Run("naming the source as its own destination stays sparse", func(t *testing.T) {
		// AS is explicit here, but the destination is the source, so there is no
		// earlier value to preserve and a missing source must stay missing.
		compiled := compile(t, `index=compiler | bin edge_src span=10 AS edge_src | table event_id edge_src`)
		got := binEdgeMetadataScan(t, ctx, connection, compiled, "edge_src")
		want := []binEdgeMetadataRow{
			{"m-int", "Int64", "-20"},
			{"m-miss", "None", "<none>"},
			{"m-null", "None", "<none>"},
			{"m-obj", "None", "<none>"},
			{"m-text", "Int64", "20"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("self-destination bin values = %#v, want %#v", got, want)
		}
		profile := binEdgeMetadataCatalogProfile(t, ctx, connection,
			build(t, `index=compiler | bin edge_src span=10 AS edge_src`), "edge_src")
		wantProfile := binEdgeMetadataProfile{
			rows: 1, total: 5, events: 3, nulls: 1, missing: 2,
			types: []uint8{
				uint8(eventfields.StoredValueTypeNull),
				uint8(eventfields.StoredValueTypeSint64),
			},
		}
		if !reflect.DeepEqual(profile, wantProfile) {
			t.Fatalf("self-destination catalog profile = %#v, want %#v", profile, wantProfile)
		}
	})

	t.Run("leaves beyond the dynamic-path budget stay classifiable", func(t *testing.T) {
		// A leaf that overflowed the bounded dynamic-path budget lives in
		// ClickHouse's shared-data encoding. Its runtime type must still be
		// readable, or the classification would fall through to the sanitized
		// unsupported-value error for an ordinary wide event.
		for _, wide := range []struct{ eventID, want string }{
			{"w-num", "Int64"},
			{"w-txt", "Int64"},
		} {
			wide := wide
			t.Run(wide.eventID, func(t *testing.T) {
				var leaf string
				if err := connection.QueryRow(ctx,
					"SELECT arraySort(JSONSharedDataPaths(`fields`))[1] FROM open_splunk.events WHERE event_id = '"+
						wide.eventID+"'",
				).Scan(&leaf); err != nil {
					t.Fatalf("query shared-data leaves: %v", err)
				}
				if leaf == "" {
					t.Fatalf("wide fixture %s kept every leaf as a dynamic subcolumn", wide.eventID)
				}
				compiled := compile(t,
					`index=wide event_id=`+wide.eventID+` | bin `+leaf+` span=10 AS band | table event_id band`)
				if got := binEdgeMetadataScan(t, ctx, connection, compiled, "band"); !reflect.DeepEqual(
					got, []binEdgeMetadataRow{{wide.eventID, wide.want, "20"}},
				) {
					t.Fatalf("shared-data leaf %q bin = %#v, want %s %s 20", leaf, got, wide.eventID, wide.want)
				}
			})
		}
	})

	t.Run("one container aborts the whole scope rather than dropping rows", func(t *testing.T) {
		// Only m-obj holds a container, and the classification is per row, so
		// the sanitized marker must surface for the entire search.
		compiled := compile(t, `index=compiler | bin edge_container span=10 | table edge_container`)
		binEdgeMetadataRequireMarker(t, ctx, connection, compiled, "scope-wide container")
	})
}

type binEdgeMetadataRow struct {
	eventID   string
	valueType string
	value     string
}

type binEdgeMetadataProfile struct {
	rows    uint64
	types   []uint8
	events  uint64
	nulls   uint64
	missing uint64
	total   uint64
	invalid uint8
}

func binEdgeMetadataCluster(t *testing.T, ctx context.Context) (clickhousedriver.Conn, *Store) {
	t.Helper()
	container := "open-splunk-bin-edge-metadata-" + integrationRandomHex(t, 6)
	password := integrationRandomHex(t, 24)
	image := os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE")
	if image == "" {
		image = storeIntegrationImage
	}
	integrationDocker(t, ctx, nil,
		"run", "--detach", "--rm", "--name", container,
		"--publish", "127.0.0.1::9000",
		"--env", "CLICKHOUSE_DB=open_splunk",
		"--env", "CLICKHOUSE_USER=open_splunk",
		"--env", "CLICKHOUSE_PASSWORD="+password,
		"--env", "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
		image,
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_ = exec.CommandContext(cleanupCtx, "docker", "rm", "--force", container).Run()
	})
	integrationWaitForClickHouse(t, ctx, container, password)

	migrationPaths, err := filepath.Glob(filepath.Join("..", "..", "migrations", "clickhouse", "[0-9][0-9][0-9][0-9]_*.sql"))
	if err != nil || len(migrationPaths) == 0 {
		t.Fatalf("discover migrations: paths=%v err=%v", migrationPaths, err)
	}
	var migrations bytes.Buffer
	for _, migrationPath := range migrationPaths {
		migration, readErr := os.ReadFile(migrationPath)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", migrationPath, readErr)
		}
		migrations.Write(migration)
		migrations.WriteByte('\n')
	}
	integrationDocker(t, ctx, bytes.NewReader(migrations.Bytes()),
		"exec", "--interactive", container, "clickhouse-client",
		"--user", "open_splunk", "--password", password, "--multiquery",
	)

	config := DefaultConfig()
	config.Addresses = []string{integrationNativeAddress(t, ctx, container)}
	config.Username = "open_splunk"
	config.Password = password
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open visibility control database: %v", err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatalf("create visibility sequencer: %v", err)
	}
	store, err := Open(config, fixedRetention(30*24*time.Hour), sequencer)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	options, _, err := config.clickHouseOptions()
	if err != nil {
		t.Fatal(err)
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, store
}

func binEdgeMetadataEvent(
	id, index, batch, raw string,
	indexTime time.Time,
	fields ...*opensplunkv1.TypedObjectField,
) *ingest.StoredEvent {
	event := testStoredEvent(id, index, indexTime)
	event.BatchID = batch
	event.Event.Host = "edge"
	event.Event.Raw = []byte(raw)
	event.Event.Message = stringPointer("bin edge metadata")
	event.Event.Fields = typedObjectValue(fields...)
	return event
}

func binEdgeMetadataPlan(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) *plan.Query {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse bin edge SPL %q: %v", source, err)
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"compiler", "legacy", "mixed", "wide"},
		Earliest:          binEdgeMetadataEarliest,
		Latest:            binEdgeMetadataLatest,
		IndexTimeCutoff:   cutoff,
		VisibilityCutoff:  uint64PointerForIntegration(visibilityCutoff),
	})
	if err != nil {
		t.Fatalf("build bin edge SPL %q: %v", source, err)
	}
	return logical
}

func binEdgeMetadataCompile(t *testing.T, source string, cutoff time.Time, visibilityCutoff uint64) CompiledQuery {
	t.Helper()
	compiled, err := (Compiler{}).Compile(binEdgeMetadataPlan(t, source, cutoff, visibilityCutoff))
	if err != nil {
		t.Fatalf("compile bin edge SPL %q: %v", source, err)
	}
	return compiled
}

// binEdgeMetadataScan reports the runtime type and exact rendered value of one
// Dynamic output column per event, which is the only way to separate a
// preserved value from a rewritten one that merely renders the same.
func binEdgeMetadataScan(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
	column string,
) []binEdgeMetadataRow {
	t.Helper()
	name := quoteIdentifier(column)
	query := `SELECT event_id, dynamicType(` + name + `), ` +
		`if(dynamicType(` + name + `) = 'None', '<none>', toString(` + name + `)) ` +
		`FROM (` + compiled.SQL + `) ORDER BY event_id`
	rows, err := connection.Query(ctx, query, compiled.Args...)
	if err != nil {
		t.Fatalf("execute bin edge query: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
	}
	defer func() { _ = rows.Close() }()
	result := make([]binEdgeMetadataRow, 0, 8)
	for rows.Next() {
		var row binEdgeMetadataRow
		if err := rows.Scan(&row.eventID, &row.valueType, &row.value); err != nil {
			t.Fatalf("scan bin edge row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate bin edge rows: %v", err)
	}
	return result
}

// binEdgeMetadataCatalogProfile reads exactly one field's catalog profile so an
// assertion can prove presence, nullness, and observed semantic types together.
func binEdgeMetadataCatalogProfile(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	logical *plan.Query,
	field string,
) binEdgeMetadataProfile {
	t.Helper()
	catalog, err := (Compiler{}).CompileFieldCatalog(logical, FieldCatalogSpec{MaximumFields: 64})
	if err != nil {
		t.Fatalf("CompileFieldCatalog: %v", err)
	}
	q := quoteIdentifier
	match := q(FieldCatalogRowKindColumn) + " = 1 AND " + q(FieldCatalogNameColumn) + " = " +
		quoteStringLiteralForBinEdge(field)
	query := `SELECT toUInt64(countIf(` + match + `)),
			arraySort(arrayFlatten(groupArrayIf(` + q(FieldCatalogObservedTypesColumn) + `, ` + match + `))),
			toUInt64(sumIf(` + q(FieldCatalogEventCountColumn) + `, ` + match + `)),
			toUInt64(sumIf(` + q(FieldCatalogNullCountColumn) + `, ` + match + `)),
			toUInt64(sumIf(` + q(FieldCatalogMissingCountColumn) + `, ` + match + `)),
			toUInt64(max(` + q(FieldCatalogTotalEventsColumn) + `)),
			toUInt8(max(` + q(FieldCatalogInvalidColumn) + `))
		FROM (` + catalog.SQL + `)`
	var profile binEdgeMetadataProfile
	if err := connection.QueryRow(ctx, query, catalog.Args...).Scan(
		&profile.rows,
		&profile.types,
		&profile.events,
		&profile.nulls,
		&profile.missing,
		&profile.total,
		&profile.invalid,
	); err != nil {
		t.Fatalf("execute bin edge field catalog: %v\nSQL: %s", err, catalog.SQL)
	}
	return profile
}

func binEdgeMetadataRequireMarker(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
	label string,
) {
	t.Helper()
	queryErr := executeCompiledExpectingNoRows(ctx, connection, compiled)
	var exception *clickhousedriver.Exception
	if !errors.As(queryErr, &exception) ||
		exception.Code != 395 ||
		!strings.Contains(exception.Message, UnsupportedNumericBinValueMarker) {
		t.Fatalf("%s error = %v, want the sanitized unsupported-value marker", label, queryErr)
	}
}

// quoteStringLiteralForBinEdge renders a test-controlled constant as a SQL
// literal. Control assertions must not add placeholders, because ClickHouse
// binds positionally and the compiled query's own arguments follow.
func quoteStringLiteralForBinEdge(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`) + "'"
}
