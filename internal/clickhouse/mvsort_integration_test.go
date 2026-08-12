package clickhouse

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestMVSortPrimitivesAgainstClickHouse is a table-free opt-in contract test.
// It keeps lexical and resource-bound SQL independently runnable from Store,
// migration, and broader compiled-corpus behavior.
func TestMVSortPrimitivesAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatalf("start pinned ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cleanupCancel := context.WithTimeout(
			context.Background(),
			8*time.Second,
		)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close mvsort ClickHouse fixture: %v", closeErr)
		}
	})
	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("open mvsort ClickHouse connection: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("close mvsort ClickHouse connection: %v", closeErr)
		}
	})
	queryContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithSettings(clickhousedriver.Settings{
			"short_circuit_function_evaluation": "enable",
		}),
	)
	if err := connection.Ping(queryContext); err != nil {
		t.Fatalf("ping mvsort ClickHouse fixture: %v", err)
	}

	emptyFixedArray := "CAST([], 'Array(String)')"
	lexicalSQL := boundedMVSortStringArraySQL(
		"['9', '10', '70', '100', 'a', 'B', 'A', 'b', '10', '', ' ', '!', '_', '~']",
		emptyFixedArray,
		"Array(String)",
		false,
	)
	unicodeSQL := boundedMVSortStringArraySQL(
		"['é', 'e', 'e\u0301', '東京', 'Ä', 'Z']",
		emptyFixedArray,
		"Array(String)",
		false,
	)
	var lexical, unicode []string
	if err := connection.QueryRow(
		queryContext,
		"SELECT "+lexicalSQL+", "+unicodeSQL,
	).Scan(&lexical, &unicode); err != nil {
		t.Fatalf("execute mvsort lexical primitives: %v", err)
	}
	if !slices.Equal(
		lexical,
		[]string{"", " ", "!", "10", "10", "100", "70", "9", "A", "B", "_", "a", "b", "~"},
	) || !slices.Equal(unicode, []string{"Z", "e", "e\u0301", "Ä", "é", "東京"}) {
		t.Fatalf("mvsort lexical primitives = %#v / %#v", lexical, unicode)
	}

	testMVSortPrimitiveBoundariesAgainstClickHouse(t, queryContext, connection)
}

func testMVSortPrimitiveBoundariesAgainstClickHouse(
	t *testing.T,
	queryContext context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()

	maximumValues := strconv.FormatUint(uint64(MaximumMVSortValues), 10)
	overMaximumValues := strconv.FormatUint(uint64(MaximumMVSortValues)+1, 10)
	halfMaximumBytes := strconv.FormatUint(uint64(MaximumMVSortBytes)/2, 10)
	exactPayload := "concat(repeat('a', toUInt64(" + halfMaximumBytes +
		")), repeat('a', toUInt64(" + halfMaximumBytes + ")))"
	overPayload := "concat(" + exactPayload + ", 'a')"
	emptyFixedArray := "CAST([], 'Array(String)')"
	fixedMembers := func(count string) string {
		return "arrayMap(number -> toString(number), range(toUInt64(" + count + ")))"
	}
	fixedBoundarySQL := "SELECT " +
		"length(" + boundedMVSortStringArraySQL(
		fixedMembers(maximumValues),
		emptyFixedArray,
		"Array(String)",
		false,
	) + "), " +
		"length(" + boundedMVSortStringArraySQL(
		fixedMembers(overMaximumValues),
		emptyFixedArray,
		"Array(String)",
		false,
	) + "), " +
		"length(arrayElement(" + boundedMVSortStringArraySQL(
		"["+exactPayload+"]",
		emptyFixedArray,
		"Array(String)",
		false,
	) + ", 1)), " +
		"length(" + boundedMVSortStringArraySQL(
		"["+overPayload+"]",
		emptyFixedArray,
		"Array(String)",
		false,
	) + "), " +
		"length(" + boundedMVSortStringArraySQL(
		"[unhex('ff')]",
		emptyFixedArray,
		"Array(String)",
		false,
	) + "), " +
		"length(" + boundedMVSortStringArraySQL(
		emptyFixedArray,
		emptyFixedArray,
		"Array(String)",
		false,
	) + ")"
	var fixedExactMembers, fixedOverMembers uint64
	var fixedExactBytes, fixedOverBytes uint64
	var fixedInvalidUTF8, fixedPhysicalEmpty uint64
	if err := connection.QueryRow(queryContext, fixedBoundarySQL).Scan(
		&fixedExactMembers,
		&fixedOverMembers,
		&fixedExactBytes,
		&fixedOverBytes,
		&fixedInvalidUTF8,
		&fixedPhysicalEmpty,
	); err != nil {
		t.Fatalf("execute fixed mvsort boundaries: %v\nSQL: %s", err, fixedBoundarySQL)
	}
	if fixedExactMembers != MaximumMVSortValues || fixedOverMembers != 0 ||
		fixedExactBytes != MaximumMVSortBytes || fixedOverBytes != 0 ||
		fixedInvalidUTF8 != 0 || fixedPhysicalEmpty != 0 {
		t.Fatalf(
			"fixed mvsort boundaries = members:%d/%d bytes:%d/%d invalid:%d empty:%d",
			fixedExactMembers,
			fixedOverMembers,
			fixedExactBytes,
			fixedOverBytes,
			fixedInvalidUTF8,
			fixedPhysicalEmpty,
		)
	}

	nullDynamic := "CAST(NULL AS Dynamic)"
	dynamicMembers := func(count string) string {
		return "arrayMap(number -> CAST(toString(number) AS Dynamic), " +
			"range(toUInt64(" + count + ")))"
	}
	dynamicExactMembers := boundedMVSortDynamicArraySQL(
		dynamicMembers(maximumValues),
		nullDynamic,
	)
	dynamicOverMembers := boundedMVSortDynamicArraySQL(
		dynamicMembers(overMaximumValues),
		nullDynamic,
	)
	dynamicExactBytes := boundedMVSortDynamicArraySQL(
		"[CAST("+exactPayload+" AS Dynamic)]",
		nullDynamic,
	)
	dynamicOverBytes := boundedMVSortDynamicArraySQL(
		"[CAST("+overPayload+" AS Dynamic)]",
		nullDynamic,
	)
	dynamicInvalidUTF8 := boundedMVSortDynamicArraySQL(
		"[CAST(unhex('ff') AS Dynamic)]",
		nullDynamic,
	)
	dynamicBoundarySQL := "SELECT " +
		"length(dynamicElement(" + dynamicExactMembers + ", 'Array(String)')), " +
		"dynamicType(" + dynamicOverMembers + "), " +
		"length(arrayElement(dynamicElement(" + dynamicExactBytes +
		", 'Array(String)'), 1)), " +
		"dynamicType(" + dynamicOverBytes + "), " +
		"dynamicType(" + dynamicInvalidUTF8 + ")"
	var dynamicExactMemberCount, dynamicExactByteCount *uint64
	var dynamicOverMemberType, dynamicOverByteType, dynamicInvalidType string
	if err := connection.QueryRow(queryContext, dynamicBoundarySQL).Scan(
		&dynamicExactMemberCount,
		&dynamicOverMemberType,
		&dynamicExactByteCount,
		&dynamicOverByteType,
		&dynamicInvalidType,
	); err != nil {
		t.Fatalf("execute Dynamic mvsort boundaries: %v\nSQL: %s", err, dynamicBoundarySQL)
	}
	if dynamicExactMemberCount == nil ||
		*dynamicExactMemberCount != MaximumMVSortValues ||
		dynamicOverMemberType != "None" ||
		dynamicExactByteCount == nil ||
		*dynamicExactByteCount != MaximumMVSortBytes ||
		dynamicOverByteType != "None" || dynamicInvalidType != "None" {
		t.Fatalf(
			"Dynamic mvsort boundaries = members:%v/%q bytes:%v/%q invalid:%q",
			dynamicExactMemberCount,
			dynamicOverMemberType,
			dynamicExactByteCount,
			dynamicOverByteType,
			dynamicInvalidType,
		)
	}
}

func testMVSortAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	primary := testStoredEvent("mvsort-primary", "mvsort", indexTime)
	primary.Event.Fields = typedObjectValue(
		typedField("single", typedString("beta")),
		typedField(
			"ordered",
			typedList(
				typedString("9"),
				typedString("10"),
				typedString("70"),
				typedString("100"),
				typedString("a"),
				typedString("B"),
				typedString("A"),
				typedString("b"),
				typedString("10"),
			),
		),
		typedField(
			"unicode_values",
			typedList(
				typedString("é"),
				typedString("e"),
				typedString("e\u0301"),
				typedString("東京"),
				typedString("Ä"),
				typedString("Z"),
			),
		),
		typedField(
			"case_values",
			typedList(typedString("B"), typedString("a"), typedString("A")),
		),
		typedField(
			"symbol_values",
			typedList(
				typedString("~"),
				typedString("a"),
				typedString("_"),
				typedString("A"),
				typedString("0"),
				typedString("!"),
				typedString(" "),
				typedString(""),
			),
		),
		typedField("empty", typedList()),
		typedField(
			"mixed",
			typedList(typedString("b"), typedSint(2), typedString("a")),
		),
		typedField(
			"null_member",
			typedList(typedString("b"), typedNull(), typedString("a")),
		),
		typedField(
			"nested",
			typedList(typedString("b"), typedList(typedString("a"))),
		),
		typedField("number", typedSint(7)),
		typedField("flag", typedBool(true)),
		typedField(
			"object",
			typedObject(typedField("member", typedString("value"))),
		),
		typedField("nothing", typedNull()),
	)
	secondary := testStoredEvent("mvsort-secondary", "mvsort", indexTime)
	secondary.Event.Fields = typedObjectValue(
		typedField("single", typedString("alpha")),
	)
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"mvsort",
		"mvsort-batch",
		97,
		primary,
		secondary,
	)

	dynamic := compile(
		`index=mvsort event_id="mvsort-primary"` +
			` | eval sorted=mvsort(ordered), unicode_sorted=mvsort(unicode_values), symbol_sorted=mvsort(symbol_values), empty_sorted=mvsort(empty), mixed_sorted=mvsort(mixed), null_member_sorted=mvsort(null_member), nested_sorted=mvsort(nested), scalar_sorted=mvsort(single), number_sorted=mvsort(number), bool_sorted=mvsort(flag), object_sorted=mvsort(object), null_sorted=mvsort(nothing), missing_sorted=mvsort(absent)` +
			` | table sorted,unicode_sorted,symbol_sorted,empty_sorted,mixed_sorted,null_member_sorted,nested_sorted,scalar_sorted,number_sorted,bool_sorted,object_sorted,null_sorted,missing_sorted`,
	)
	control := "SELECT " +
		"dynamicType(sorted), dynamicElement(sorted, 'Array(String)'), " +
		"dynamicType(unicode_sorted), dynamicElement(unicode_sorted, 'Array(String)'), " +
		"dynamicType(symbol_sorted), dynamicElement(symbol_sorted, 'Array(String)'), " +
		"dynamicType(empty_sorted), dynamicType(mixed_sorted), " +
		"dynamicType(null_member_sorted), dynamicType(nested_sorted), " +
		"dynamicType(scalar_sorted), dynamicType(number_sorted), " +
		"dynamicType(bool_sorted), dynamicType(object_sorted), dynamicType(null_sorted), " +
		"dynamicType(missing_sorted) FROM (" + dynamic.SQL + ")"
	var (
		sortedType, unicodeType, symbolType                      string
		sorted, unicodeSorted, symbolSorted                      []string
		emptyType, mixedType, nullMemberType                     string
		nestedType, scalarType, numberType, boolType, objectType string
		nullType, missingType                                    string
	)
	if err := connection.QueryRow(
		queryContext,
		control,
		dynamic.Args...,
	).Scan(
		&sortedType,
		&sorted,
		&unicodeType,
		&unicodeSorted,
		&symbolType,
		&symbolSorted,
		&emptyType,
		&mixedType,
		&nullMemberType,
		&nestedType,
		&scalarType,
		&numberType,
		&boolType,
		&objectType,
		&nullType,
		&missingType,
	); err != nil {
		t.Fatalf(
			"execute Dynamic mvsort matrix: %v\nSQL: %s\nargs: %#v",
			err,
			control,
			dynamic.Args,
		)
	}
	if sortedType != "Array(String)" || !slices.Equal(
		sorted,
		[]string{"10", "10", "100", "70", "9", "A", "B", "a", "b"},
	) {
		t.Fatalf("Dynamic lexical mvsort = %q/%#v", sortedType, sorted)
	}
	if unicodeType != "Array(String)" || !slices.Equal(
		unicodeSorted,
		[]string{"Z", "e", "e\u0301", "Ä", "é", "東京"},
	) {
		t.Fatalf("Dynamic Unicode mvsort = %q/%#v", unicodeType, unicodeSorted)
	}
	if symbolType != "Array(String)" || !slices.Equal(
		symbolSorted,
		[]string{"", " ", "!", "0", "A", "_", "a", "~"},
	) {
		t.Fatalf("Dynamic symbol mvsort = %q/%#v", symbolType, symbolSorted)
	}
	for name, got := range map[string]string{
		"empty":       emptyType,
		"mixed":       mixedType,
		"null member": nullMemberType,
		"nested":      nestedType,
		"scalar":      scalarType,
		"number":      numberType,
		"Boolean":     boolType,
		"object":      objectType,
		"null":        nullType,
		"missing":     missingType,
	} {
		if got != "None" {
			t.Fatalf("Dynamic unsupported %s mvsort type = %q, want None", name, got)
		}
	}

	composed := compile(
		`index=mvsort event_id="mvsort-primary"` +
			` | eval sorted=mvsort(lower(case_values)), count=mvcount(mvsort(ordered))` +
			` | table sorted,count`,
	)
	composedControl := "SELECT dynamicType(sorted), " +
		"dynamicElement(sorted, 'Array(String)'), count FROM (" + composed.SQL + ")"
	var composedType string
	var composedValues []string
	var count uint64
	if err := connection.QueryRow(
		queryContext,
		composedControl,
		composed.Args...,
	).Scan(&composedType, &composedValues, &count); err != nil {
		t.Fatalf(
			"execute composed mvsort: %v\nSQL: %s\nargs: %#v",
			err,
			composedControl,
			composed.Args,
		)
	}
	if composedType != "Array(String)" ||
		!slices.Equal(composedValues, []string{"a", "a", "b"}) || count != 9 {
		t.Fatalf(
			"composed mvsort = %q/%#v count=%d, want Array(String)/[a a b]/9",
			composedType,
			composedValues,
			count,
		)
	}

	fixed := compile(
		`index=mvsort | stats list(single) AS collected` +
			` | eval sorted=mvsort(collected) | table sorted`,
	)
	var fixedValues []string
	if err := connection.QueryRow(
		queryContext,
		fixed.SQL,
		fixed.Args...,
	).Scan(&fixedValues); err != nil {
		t.Fatalf(
			"execute fixed mvsort: %v\nSQL: %s\nargs: %#v",
			err,
			fixed.SQL,
			fixed.Args,
		)
	}
	if !slices.Equal(fixedValues, []string{"alpha", "beta"}) {
		t.Fatalf("fixed mvsort = %#v, want [alpha beta]", fixedValues)
	}

	nullBehavior := compile(
		`index=mvsort event_id="mvsort-primary"` +
			` | eval direct_missing=if(isnull(mvsort(null)), 1, 0), direct_present=if(isnotnull(mvsort(null)), 1, 0), sorted=mvsort(null), projected_missing=if(isnull(sorted), 1, 0), count=mvcount(sorted)` +
			` | table direct_missing,direct_present,projected_missing,count`,
	)
	var directMissing, directPresent, projectedMissing int64
	var nullCount *uint64
	if err := connection.QueryRow(
		queryContext,
		nullBehavior.SQL,
		nullBehavior.Args...,
	).Scan(&directMissing, &directPresent, &projectedMissing, &nullCount); err != nil {
		t.Fatalf(
			"execute null mvsort: %v\nSQL: %s\nargs: %#v",
			err,
			nullBehavior.SQL,
			nullBehavior.Args,
		)
	}
	if directMissing != 1 || directPresent != 0 || projectedMissing != 1 || nullCount != nil {
		t.Fatalf(
			"null mvsort = direct_missing:%d direct_present:%d projected_missing:%d count:%v",
			directMissing,
			directPresent,
			projectedMissing,
			nullCount,
		)
	}

	fixedEmpty := compile(
		`index=mvsort | stats list(absent) AS collected` +
			` | eval direct_missing=if(isnull(mvsort(collected)), 1, 0), sorted=mvsort(collected), projected_missing=if(isnull(sorted), 1, 0), count=mvcount(sorted)` +
			` | table direct_missing,sorted,projected_missing,count`,
	)
	var fixedDirectMissing, fixedProjectedMissing int64
	var fixedEmptyValues []string
	var fixedEmptyCount *uint64
	if err := connection.QueryRow(
		queryContext,
		fixedEmpty.SQL,
		fixedEmpty.Args...,
	).Scan(
		&fixedDirectMissing,
		&fixedEmptyValues,
		&fixedProjectedMissing,
		&fixedEmptyCount,
	); err != nil {
		t.Fatalf(
			"execute fixed-empty mvsort: %v\nSQL: %s\nargs: %#v",
			err,
			fixedEmpty.SQL,
			fixedEmpty.Args,
		)
	}
	if fixedDirectMissing != 1 || len(fixedEmptyValues) != 0 ||
		fixedProjectedMissing != 1 || fixedEmptyCount != nil {
		t.Fatalf(
			"fixed-empty mvsort = direct_missing:%d values:%#v projected_missing:%d count:%v",
			fixedDirectMissing,
			fixedEmptyValues,
			fixedProjectedMissing,
			fixedEmptyCount,
		)
	}

	filtered := compile(
		`index=mvsort event_id="mvsort-primary"` +
			` | where mvcount(mvsort(ordered))=9 | stats count`,
	)
	var filteredCount uint64
	if err := connection.QueryRow(
		queryContext,
		filtered.SQL,
		filtered.Args...,
	).Scan(&filteredCount); err != nil {
		t.Fatalf(
			"execute where mvsort: %v\nSQL: %s\nargs: %#v",
			err,
			filtered.SQL,
			filtered.Args,
		)
	}
	if filteredCount != 1 {
		t.Fatalf("where mvsort count = %d, want 1", filteredCount)
	}

	testMVSortPrimitiveBoundariesAgainstClickHouse(t, queryContext, connection)

	actions := explainCompiledQuery(
		t,
		queryContext,
		connection,
		"EXPLAIN actions=1 ",
		dynamic,
	)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("mvsort lowering expands event rows:\n%s", actions)
	}
}
