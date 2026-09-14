package queryexec

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestContainerMetadataCachePreservesEveryRowValidation(t *testing.T) {
	t.Parallel()

	names := []string{"nested.value"}
	types := []uint8{uint8(eventfields.StoredValueTypeString)}
	version := eventfields.CurrentFieldMetadataVersion
	var cache resultMetadataCache
	var budget resultMetadataCacheBudget
	valid := map[string]any{"nested": map[string]any{"value": "first"}}
	warmContainerMetadataCache(t, valid, names, types, &cache, &budget)
	root := cache.root
	for _, test := range []struct {
		name string
		raw  any
	}{
		{name: "changed value", raw: map[string]any{"nested": map[string]any{"value": "second"}}},
		{name: "wrong leaf type", raw: map[string]any{"nested": map[string]any{"value": int64(2)}}},
		{name: "missing nonnull leaf", raw: map[string]any{}},
		{name: "extra nonnull leaf", raw: map[string]any{"nested": map[string]any{"value": "ok", "extra": true}}},
		{name: "extra null leaf", raw: map[string]any{"nested": map[string]any{"value": "ok", "extra": nil}}},
		{name: "wrong container type", raw: "scalar"},
		{name: "invalid physical path", raw: map[string]any{"nested": map[string]any{"value": "ok", "bad%2X": true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			want, wantErr := convertContainerOutput(test.raw, names, types, version)
			got, gotErr := convertContainerOutputWithCache(test.raw, names, types, version, &cache, &budget)
			if !reflect.DeepEqual(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("cached = %#v, %v; uncached = %#v, %v", got, gotErr, want, wantErr)
			}
			if cache.root != root || budget.bytes != cache.bytes {
				t.Fatal("a cache hit changed the retained schema")
			}
		})
	}
}

func TestContainerMetadataCacheDetachesKeysAndRevalidatesShapeChanges(t *testing.T) {
	t.Parallel()

	driverName := []byte("first")
	names := []string{unsafe.String(unsafe.SliceData(driverName), len(driverName))}
	types := []uint8{uint8(eventfields.StoredValueTypeString)}
	version := eventfields.CurrentFieldMetadataVersion
	var cache resultMetadataCache
	var budget resultMetadataCacheBudget
	first := warmContainerMetadataCache(t, map[string]any{"first": "original"}, names, types, &cache, &budget)
	// A native driver may recycle metadata text as well as its arrays.
	copy(driverName, "other")
	if !cache.matches([]string{"first"}, []uint8{uint8(eventfields.StoredValueTypeString)}, version) {
		t.Fatal("cache retained driver-owned text storage")
	}
	names[0], types[0] = "second", uint8(eventfields.StoredValueTypeSint64)
	if !cache.matches([]string{"first"}, []uint8{uint8(eventfields.StoredValueTypeString)}, version) {
		t.Fatal("cache retained driver-owned metadata")
	}
	second := warmContainerMetadataCache(t, map[string]any{"second": int64(2)}, names, types, &cache, &budget)
	if got, ok := containerOutputObject(t, first)["first"].String(); !ok || got != "original" {
		t.Fatal("later metadata changed an earlier result")
	}
	if got, ok := containerOutputObject(t, second)["second"].Signed(); !ok || got != 2 {
		t.Fatal("new metadata did not replace the cached schema")
	}
	root := cache.root
	for _, test := range []struct {
		name    string
		names   []string
		types   []uint8
		version uint8
	}{
		{name: "version", names: names, types: types, version: version + 1},
		{name: "unaligned types", names: names, version: version},
		{name: "duplicate path", names: []string{"second", "second"}, types: []uint8{3, 3}, version: version},
		{name: "ancestor collision", names: []string{"second", "second.child"}, types: []uint8{3, 3}, version: version},
		{name: "semantic type change", names: names, types: []uint8{uint8(eventfields.StoredValueTypeString)}, version: version},
		{name: "invalid scalar sidecar", names: names, types: types},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := convertContainerOutputWithCache(map[string]any{"second": int64(2)}, test.names, test.types, test.version, &cache, &budget)
			if err == nil || cache.root != root || budget.bytes != cache.bytes {
				t.Fatalf("invalid row changed cache: err=%v", err)
			}
		})
	}
	if value, err := convertContainerOutputWithCache("scalar", nil, nil, 0, &cache, &budget); err != nil || value.Kind() != searchjobs.ValueKindString {
		t.Fatalf("scalar sentinel = %#v, %v", value, err)
	}
	// A valid type change with the same names must replace the previous tree.
	warmContainerMetadataCache(t, map[string]any{"second": "now text"}, names, []uint8{2}, &cache, &budget)
	if cache.root == root {
		t.Fatal("valid semantic type change did not replace cached tree")
	}
}

func TestContainerMetadataCachePreservesDecodedObjectOrder(t *testing.T) {
	t.Parallel()

	names, types := containerOutputMetadata(
		containerOutputMetadataField{name: "a0", storedType: eventfields.StoredValueTypeNull},
		containerOutputMetadataField{name: `a\.b`, storedType: eventfields.StoredValueTypeNull},
		containerOutputMetadataField{name: "nested.a0", storedType: eventfields.StoredValueTypeNull},
		containerOutputMetadataField{name: `nested.a\.b`, storedType: eventfields.StoredValueTypeNull},
	)
	var cache resultMetadataCache
	var budget resultMetadataCacheBudget
	for range 3 {
		value, err := convertContainerOutputWithCache(map[string]any{}, names, types, eventfields.CurrentFieldMetadataVersion, &cache, &budget)
		if err != nil {
			t.Fatal(err)
		}
		fields, ok := value.Object()
		if !ok || len(fields) != 3 || fields[0].Name != "a.b" || fields[1].Name != "a0" || fields[2].Name != "nested" {
			t.Fatalf("public field order = %#v", fields)
		}
		nested, ok := fields[2].Value.Object()
		if !ok || len(nested) != 2 || nested[0].Name != "a.b" || nested[1].Name != "a0" {
			t.Fatalf("nested field order = %#v", nested)
		}
	}
}

func TestResultMetadataCacheDoesNotRetainChangingShapes(t *testing.T) {
	t.Parallel()

	var cache resultMetadataCache
	var budget resultMetadataCacheBudget
	for index := range int(maximumMetadataPromotionMisses) {
		name := fmt.Sprintf("shape_%d", index%2)
		if _, err := convertContainerOutputWithCache(map[string]any{}, []string{name}, []uint8{1}, eventfields.CurrentFieldMetadataVersion, &cache, &budget); err != nil {
			t.Fatal(err)
		}
		if cache.bytes != 0 || budget.bytes != 0 || len(cache.names) != 0 || cache.root != nil {
			t.Fatal("changing shapes retained metadata without reuse")
		}
	}
	if cache.cooldownRows != metadataPromotionCooldownRows || cache.candidatePresent {
		t.Fatal("diverse metadata did not pause cache promotion")
	}
	for range int(metadataPromotionCooldownRows) {
		if _, err := convertContainerOutputWithCache(map[string]any{}, []string{"stable"}, []uint8{1}, eventfields.CurrentFieldMetadataVersion, &cache, &budget); err != nil {
			t.Fatal(err)
		}
		if cache.bytes != 0 || cache.candidatePresent {
			t.Fatal("promotion pause retained metadata or hashed a candidate")
		}
	}
	warmContainerMetadataCache(t, map[string]any{}, []string{"stable"}, []uint8{1}, &cache, &budget)
	if !cache.matches([]string{"stable"}, []uint8{1}, eventfields.CurrentFieldMetadataVersion) {
		t.Fatal("a stable shape was not admitted after a heterogeneous prefix")
	}
}

func TestResultMetadataCooldownPreservesValidationAndExactHits(t *testing.T) {
	t.Parallel()

	var cache resultMetadataCache
	var budget resultMetadataCacheBudget
	warmContainerMetadataCache(t, map[string]any{"original": "value"}, []string{"original"}, []uint8{2}, &cache, &budget)
	root := cache.root
	for index := range int(maximumMetadataPromotionMisses) {
		name := fmt.Sprintf("shape_%d", index)
		if _, err := convertContainerOutputWithCache(map[string]any{}, []string{name}, []uint8{1}, eventfields.CurrentFieldMetadataVersion, &cache, &budget); err != nil {
			t.Fatal(err)
		}
	}
	if cache.cooldownRows != metadataPromotionCooldownRows {
		t.Fatal("promotion did not enter cooldown")
	}
	for _, test := range []struct {
		name  string
		raw   any
		names []string
		types []uint8
	}{
		{name: "cached shape invalid value", raw: map[string]any{"original": int64(1)}, names: []string{"original"}, types: []uint8{2}},
		{name: "new shape invalid value", raw: map[string]any{"new": int64(1)}, names: []string{"new"}, types: []uint8{2}},
		{name: "invalid metadata", raw: map[string]any{}, names: []string{"duplicate", "duplicate"}, types: []uint8{1, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := convertContainerOutputWithCache(test.raw, test.names, test.types, eventfields.CurrentFieldMetadataVersion, &cache, &budget); err == nil {
				t.Fatal("cooldown skipped row validation")
			}
			if cache.root != root || cache.cooldownRows != metadataPromotionCooldownRows {
				t.Fatal("invalid row changed cache policy or retained metadata")
			}
		})
	}
	if _, err := convertContainerOutputWithCache(map[string]any{"original": "new value"}, []string{"original"}, []uint8{2}, eventfields.CurrentFieldMetadataVersion, &cache, &budget); err != nil {
		t.Fatal(err)
	}
	if cache.cooldownRows != 0 || cache.promotionMisses != 0 || cache.root != root {
		t.Fatal("an exact cache hit did not reset the promotion policy")
	}
}

func TestSparseMetadataCooldownStillValidatesAndResumes(t *testing.T) {
	t.Parallel()

	var cache resultMetadataCache
	var budget resultMetadataCacheBudget
	for index := range int(maximumMetadataPromotionMisses) {
		if _, err := convertSparseEventFieldsWithCache(chcol.NewJSON(), []string{fmt.Sprintf("shape_%d", index)}, false, &cache, &budget); err != nil {
			t.Fatal(err)
		}
	}
	if cache.cooldownRows != metadataPromotionCooldownRows {
		t.Fatal("sparse metadata did not enter cooldown")
	}
	poisoned := chcol.NewJSON()
	poisoned.SetValueAtPath("extra", "not in presence metadata")
	if _, err := convertSparseEventFieldsWithCache(poisoned, []string{"stable"}, false, &cache, &budget); err == nil {
		t.Fatal("sparse cooldown skipped payload validation")
	}
	for range int(metadataPromotionCooldownRows) + 2 {
		if _, err := convertSparseEventFieldsWithCache(chcol.NewJSON(), []string{"stable"}, false, &cache, &budget); err != nil {
			t.Fatal(err)
		}
	}
	if !cache.matches([]string{"stable"}, nil, 0) || cache.cooldownRows != 0 {
		t.Fatal("sparse cache did not recover for a stable shape")
	}
}

func TestResultMetadataAdmissionFingerprintNeverSubstitutesForExactMetadata(t *testing.T) {
	t.Parallel()

	var cache resultMetadataCache
	var budget resultMetadataCacheBudget
	warmContainerMetadataCache(t, map[string]any{}, []string{"original"}, []uint8{1}, &cache, &budget)
	// Simulate a promotion fingerprint collision. The current shape must still
	// be parsed and reconstructed independently of the existing cached tree.
	candidate := resultMetadataCache{seed: cache.seed}
	var candidateBudget resultMetadataCacheBudget
	if _, err := convertContainerOutputWithCache(map[string]any{"replacement": int64(7)}, []string{"replacement"}, []uint8{3}, eventfields.CurrentFieldMetadataVersion, &candidate, &candidateBudget); err != nil {
		t.Fatal(err)
	}
	cache.candidate, cache.candidatePresent = candidate.candidate, true
	value, err := convertContainerOutputWithCache(map[string]any{"replacement": int64(7)}, []string{"replacement"}, []uint8{3}, eventfields.CurrentFieldMetadataVersion, &cache, &budget)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := containerOutputObject(t, value)["replacement"].Signed(); !ok || got != 7 {
		t.Fatal("promotion used the old shape instead of parsing the current metadata")
	}
	if _, err := convertContainerOutputWithCache(map[string]any{"replacement": "wrong"}, []string{"replacement"}, []uint8{3}, eventfields.CurrentFieldMetadataVersion, &cache, &budget); err == nil {
		t.Fatal("promoted shape skipped per-row type validation")
	}
}

func TestSparseMetadataCacheStillChecksPhysicalValuesAndSubsetContract(t *testing.T) {
	t.Parallel()

	names := []string{"nested.value", "nothing"}
	var cache resultMetadataCache
	var budget resultMetadataCacheBudget
	for _, test := range []struct {
		name        string
		extra       string
		allowSubset bool
	}{
		{name: "first row"},
		{name: "repeated shape"},
		{name: "extra field rejected", extra: "secret"},
		{name: "extra field allowed by subset", extra: "secret", allowSubset: true},
		{name: "malformed physical path", extra: "bad%2X"},
	} {
		t.Run(test.name, func(t *testing.T) {
			document := chcol.NewJSON()
			document.SetValueAtPath("nested.value", test.name)
			if test.extra != "" {
				document.SetValueAtPath(test.extra, "extra")
			}
			want, wantErr := convertSparseEventFields(document, names, test.allowSubset)
			got, gotErr := convertSparseEventFieldsWithCache(document, names, test.allowSubset, &cache, &budget)
			if !reflect.DeepEqual(got, want) || fmt.Sprint(gotErr) != fmt.Sprint(wantErr) {
				t.Fatalf("cached = %#v, %v; uncached = %#v, %v", got, gotErr, want, wantErr)
			}
		})
	}
	names[0] = "nothing"
	if !cache.matches([]string{"nested.value", "nothing"}, nil, 0) {
		t.Fatal("sparse cache retained driver-owned names")
	}
	if _, err := convertSparseEventFieldsWithCache(chcol.NewJSON(), names, false, &cache, &budget); err == nil {
		t.Fatal("duplicate paths passed after a cache hit")
	}
	for index, path := range cache.paths {
		if cap(path) != len(path) || len(path) == 0 || cache.names[index] == "" {
			t.Fatal("cached parsed paths retained parser buffer capacity")
		}
	}
}

func TestResultMetadataCacheBudgetIsSharedAndOversizedShapesStillConvert(t *testing.T) {
	t.Parallel()

	names, types := largeResultMetadataShape(512)
	var first, second resultMetadataCache
	var budget resultMetadataCacheBudget
	for _, cache := range []*resultMetadataCache{&first, &second} {
		warmContainerMetadataCache(t, map[string]any{}, names, types, cache, &budget)
	}
	if first.bytes == 0 || second.bytes != 0 || budget.bytes != first.bytes || budget.bytes > maximumResultMetadataCacheBytes {
		t.Fatalf("cache budget = %d, entries = %d/%d", budget.bytes, first.bytes, second.bytes)
	}
	// Replacing a large shape releases its reservation for another output.
	warmContainerMetadataCache(t, map[string]any{}, []string{"small"}, []uint8{1}, &first, &budget)
	warmContainerMetadataCache(t, map[string]any{}, names, types, &second, &budget)
	if second.bytes == 0 || budget.bytes != first.bytes+second.bytes || budget.bytes > maximumResultMetadataCacheBytes {
		t.Fatal("cache replacement lost the shared budget")
	}
	// The sparse-path cache uses the same allowance as the container cache.
	var sparse resultMetadataCache
	for range 2 {
		if _, err := convertSparseEventFieldsWithCache(chcol.NewJSON(), names, false, &sparse, &budget); err != nil {
			t.Fatal(err)
		}
	}
	if sparse.bytes != 0 || budget.bytes != first.bytes+second.bytes {
		t.Fatal("sparse metadata bypassed the shared budget")
	}
	oversizedNames, oversizedTypes := largeResultMetadataShape(1024)
	warmContainerMetadataCache(t, map[string]any{}, oversizedNames, oversizedTypes, &second, &budget)
	if second.bytes != 0 || budget.bytes != first.bytes {
		t.Fatal("oversized metadata was cached or retained a superseded entry")
	}
}

func TestResultMetadataCacheAccountsExactBoundary(t *testing.T) {
	t.Parallel()

	names := []string{"value"}
	types := []uint8{uint8(eventfields.StoredValueTypeNull)}
	var measured resultMetadataCache
	var measuredBudget resultMetadataCacheBudget
	warmContainerMetadataCache(t, map[string]any{}, names, types, &measured, &measuredBudget)
	wantBytes := uint64(unsafe.Sizeof(resultMetadataCache{})) + uint64(unsafe.Sizeof("")) +
		uint64(len("value")*2+1) + 2*uint64(unsafe.Sizeof(resultContainerNode{}))
	if measured.bytes != wantBytes {
		t.Fatalf("cached size = %d, want %d", measured.bytes, wantBytes)
	}
	for _, remaining := range []uint64{wantBytes - 1, wantBytes} {
		var cache resultMetadataCache
		budget := resultMetadataCacheBudget{bytes: maximumResultMetadataCacheBytes - remaining}
		warmContainerMetadataCache(t, map[string]any{}, names, types, &cache, &budget)
		if (cache.bytes != 0) != (remaining == wantBytes) || budget.bytes > maximumResultMetadataCacheBytes {
			t.Fatalf("remaining=%d, retained=%d, total=%d", remaining, cache.bytes, budget.bytes)
		}
	}
}

func TestExecuteMetadataCachesAreIndependentAndRejectInvalidLaterRow(t *testing.T) {
	t.Parallel()

	descriptor := testResultContainerOutput(1)
	secondDescriptor := testResultContainerOutput(2)
	names, types := []string{"value"}, []uint8{uint8(eventfields.StoredValueTypeString)}
	secondTypes := []uint8{uint8(eventfields.StoredValueTypeSint64)}
	columnTypes := containerOutputColumnTypes(descriptor)
	secondColumnTypes := containerOutputColumnTypes(secondDescriptor)
	secondColumnTypes[1] = fakeColumnType{name: "other", databaseType: "Dynamic", scanType: reflect.TypeFor[any]()}
	rows := &fakeRows{
		columns: []string{
			"event_id", "payload", "other",
			descriptor.NamesColumn(), descriptor.TypesColumn(), descriptor.MetadataVersionColumn(),
			secondDescriptor.NamesColumn(), secondDescriptor.TypesColumn(), secondDescriptor.MetadataVersionColumn(),
		},
		data: [][]any{
			{"one", chcol.NewDynamic(map[string]any{"value": "first"}), chcol.NewDynamic(map[string]any{"value": int64(1)}), names, types, uint8(1), names, secondTypes, uint8(1)},
			{"two", chcol.NewDynamic(map[string]any{"value": "second"}), chcol.NewDynamic(map[string]any{"value": int64(2)}), names, types, uint8(1), names, secondTypes, uint8(1)},
			{"three", chcol.NewDynamic(map[string]any{"value": "third"}), chcol.NewDynamic(map[string]any{"value": int64(3)}), names, types, uint8(1), names, secondTypes, uint8(1)},
			{"four", chcol.NewDynamic(map[string]any{"value": "fourth"}), chcol.NewDynamic(map[string]any{"value": "invalid"}), names, types, uint8(1), names, secondTypes, uint8(1)},
		},
	}
	rows.types = append(rows.types, columnTypes[:2]...)
	rows.types = append(rows.types, secondColumnTypes[1])
	rows.types = append(rows.types, columnTypes[2:]...)
	rows.types = append(rows.types, secondColumnTypes[2:]...)
	query := clickhouse.CompiledQuery{
		SQL: "SELECT container output", OutputFields: []string{"event_id", "payload", "other"},
		ContainerOutputs: []clickhouse.ResultContainerOutput{descriptor, secondDescriptor},
	}
	sink := &fakeSink{}
	err := mustExecutor(t, &fakeQueryConnection{rows: rows}).Execute(context.Background(), query, sink)
	if !errors.Is(err, searchjobs.ErrInvalidResult) || len(sink.rows) != 3 {
		t.Fatalf("Execute = %v, published rows = %d", err, len(sink.rows))
	}
	for index, want := range []string{"first", "second", "third"} {
		if got, ok := containerOutputObject(t, sink.rows[index][1])["value"].String(); !ok || got != want {
			t.Fatalf("row %d = %q, want %q", index, got, want)
		}
		if got, ok := containerOutputObject(t, sink.rows[index][2])["value"].Signed(); !ok || got != int64(index+1) {
			t.Fatalf("second container row %d = %d", index, got)
		}
	}
}

func warmContainerMetadataCache(
	t *testing.T,
	raw any,
	names []string,
	types []uint8,
	cache *resultMetadataCache,
	budget *resultMetadataCacheBudget,
) searchjobs.Value {
	t.Helper()
	var value searchjobs.Value
	for range 2 {
		var err error
		value, err = convertContainerOutputWithCache(raw, names, types, eventfields.CurrentFieldMetadataVersion, cache, budget)
		if err != nil {
			t.Fatal(err)
		}
	}
	return value
}

func largeResultMetadataShape(count int) ([]string, []uint8) {
	names, types := make([]string, count), make([]uint8, count)
	suffix := strings.Repeat("."+strings.Repeat("y", 128), 3)
	for index := range names {
		names[index] = fmt.Sprintf("f%04d%s%s", index, strings.Repeat("x", 123), suffix)
		types[index] = uint8(eventfields.StoredValueTypeNull)
	}
	return names, types
}

// BenchmarkResultMetadataConversion pairs the same fully validating converter
// with and without metadata reuse. The 64-shape stream exercises mostly misses
// as well as the repeated-schema and alternating-schema cases.
func BenchmarkResultMetadataConversion(b *testing.B) {
	for _, count := range []int{8, 64} {
		shapes := make([]resultMetadataBenchmarkShape, 64)
		for index := range shapes {
			shapes[index] = newResultMetadataBenchmarkShape(count, fmt.Sprintf("group_%02d", index))
		}
		for _, transport := range []string{"container", "sparse"} {
			for _, shapeCount := range []int{1, 2, 64} {
				for _, cached := range []bool{false, true} {
					b.Run(fmt.Sprintf("%s/%d_fields/%d_shapes/cached_%t", transport, count, shapeCount, cached), func(b *testing.B) {
						var cache *resultMetadataCache
						var budget resultMetadataCacheBudget
						if cached {
							cache = new(resultMetadataCache)
						}
						convert := func(index int) (searchjobs.Value, error) {
							shape := shapes[index%shapeCount]
							if transport == "container" {
								return convertContainerOutputWithCache(shape.raw, shape.names, shape.types, eventfields.CurrentFieldMetadataVersion, cache, &budget)
							}
							return convertSparseEventFieldsWithCache(shape.document, shape.names, false, cache, &budget)
						}
						for range 2 {
							if _, err := convert(0); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportAllocs()
						b.ResetTimer()
						for index := range b.N {
							if value, err := convert(index); err != nil || value.Kind() != searchjobs.ValueKindObject {
								b.Fatalf("convert = %#v, %v", value, err)
							}
						}
					})
				}
			}
		}
	}
}

type resultMetadataBenchmarkShape struct {
	names    []string
	types    []uint8
	raw      map[string]any
	document *chcol.JSON
}

func newResultMetadataBenchmarkShape(count int, prefix string) resultMetadataBenchmarkShape {
	shape := resultMetadataBenchmarkShape{
		names: make([]string, count), types: make([]uint8, count),
		raw: make(map[string]any), document: chcol.NewJSON(),
	}
	for index := range shape.names {
		group := fmt.Sprintf("%s_%02d", prefix, index/8)
		field := fmt.Sprintf("field_%02d", index)
		shape.names[index] = group + "." + field
		shape.types[index] = uint8(eventfields.StoredValueTypeString)
		if shape.raw[group] == nil {
			shape.raw[group] = make(map[string]any)
		}
		shape.raw[group].(map[string]any)[field] = "representative event field value"
		shape.document.SetValueAtPath(shape.names[index], "representative event field value")
	}
	return shape
}
