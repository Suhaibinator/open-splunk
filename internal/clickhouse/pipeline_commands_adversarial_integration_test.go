package clickhouse

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestPipelineAdversarialAgainstClickHouse exercises the complete pipeline command
// surface against a digest-pinned production ClickHouse image. It is kept in
// one lifecycle because the interesting assertions compose order, null/type
// transport, and multivalue expansion over the same deliberately hostile rows.
func TestPipelineAdversarialAgainstClickHouse(t *testing.T) {
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
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	eventTimeAnchor := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)

	newEvent := func(
		id, source, raw string,
		ordinal int,
		fields ...*opensplunk.TypedObjectField,
	) *ingest.StoredEvent {
		event := testStoredEvent(id, "spl-v03", indexTime)
		event.Event.Source = source
		event.Event.Raw = []byte(raw)
		event.Event.Message = new(raw)
		eventTime := eventTimeAnchor.Add(time.Duration(ordinal) * time.Second)
		event.Event.EventTime = timestamppb.New(eventTime)
		event.Event.CollectedAt = timestamppb.New(eventTime.Add(-time.Second))
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}

	core := []*ingest.StoredEvent{
		newEvent("v03-01", "v03-core", "TIMEOUT 拒否", 1,
			typedField("tie_key", typedString("same")),
			typedField("dedup_key", typedString("alpha")),
			typedField("bucket", typedString("A")),
			typedField("regex_text", typedString("blocked")),
			typedField("n", typedSint(2)),
			typedField("delta_n", typedSint(10)),
			typedField("join_host", typedString("api")),
			typedField("route", typedString("/α")),
			typedField("a", typedSint(2)),
			typedField("b", typedDouble(3.5)),
			typedField("Total", typedString("attacker-total")),
			typedField("tags_csv", typedString("a,,β")),
			typedField("unicode_csv", typedString("α💥界💥界ω")),
			typedField("mv_list", typedList(typedString("x"), typedNull(), typedString("y"))),
			typedField("optional_mv", typedList(typedString("keep"), typedString("界"))),
			typedField("info_min_time", typedString("attacker-min")),
			typedField("info_max_time", typedString("attacker-max")),
			typedField("info_search_time", typedString("attacker-search")),
			typedField("info_sid", typedString("attacker-sid")),
		),
		newEvent("v03-02", "v03-core", "ordinary", 2,
			typedField("tie_key", typedString("same")),
			typedField("dedup_key", typedString("alpha")),
			typedField("bucket", typedString("A")),
			typedField("regex_text", typedNull()),
			typedField("n", typedString("3.5")),
			typedField("delta_n", typedSint(7)),
			typedField("optional", typedNull()),
			typedField("join_host", typedNull()),
			typedField("route", typedString("/b")),
			typedField("joined", typedString("preexisting")),
			typedField("a", typedString("4")),
			typedField("b", typedBool(true)),
			typedField("tags_csv", typedString(",x,")),
			typedField("mv_list", typedList()),
			typedField("optional_mv", typedNull()),
		),
		newEvent("v03-03", "v03-core", "ordinary", 3,
			typedField("tie_key", typedString("same")),
			typedField("dedup_key", typedString("beta")),
			typedField("bucket", typedString("B")),
			typedField("regex_text", typedString("")),
			typedField("n", typedBool(true)),
			typedField("delta_n", typedSint(5)),
			typedField("optional", typedString("")),
			typedField("join_host", typedString("")),
			typedField("route", typedString("/c")),
			typedField("b", typedNull()),
			typedField("tags_csv", typedString("")),
			typedField("mv_list", typedString("scalar")),
		),
		newEvent("v03-04", "v03-core", "ordinary", 4,
			typedField("tie_key", typedString("same")),
			typedField("dedup_key", typedString("gamma")),
			typedField("bucket", typedString("B")),
			typedField("route", typedString("/d")),
			typedField("a", typedString("not-a-number")),
			typedField("b", typedList(typedSint(1))),
		),
		newEvent("v03-05", "v03-core", "ordinary", 5,
			typedField("tie_key", typedString("same")),
			typedField("dedup_key", typedString("delta")),
			typedField("bucket", typedString("C")),
			typedField("regex_text", typedString("open")),
			typedField("n", typedNull()),
			typedField("delta_n", typedSint(2)),
			typedField("optional", typedSint(0)),
			typedField("join_host", typedString("worker")),
			typedField("route", typedNull()),
			typedField("tags_csv", typedNull()),
			typedField("mv_list", typedNull()),
		),
		newEvent("v03-06", "v03-core", "ordinary", 6,
			typedField("tie_key", typedString("same")),
			typedField("dedup_key", typedString("epsilon")),
			typedField("bucket", typedString("C")),
			typedField("regex_text", typedString("BLOCKED")),
			typedField("n", typedSint(-1)),
			typedField("delta_n", typedSint(-1)),
			typedField("optional", typedBool(false)),
			typedField("join_host", typedString("worker")),
			typedField("route", typedString("/f")),
			typedField("a", typedDecimal("1.25")),
			typedField("b", typedDouble(-0.25)),
			typedField("tags_csv", typedString("dup,,dup,界")),
			typedField("mv_list", typedList(typedString("dup"), typedString("dup"), typedString("界"))),
		),
		newEvent("v03-fillnull-container-present", "v03-fillnull-container", "container", 7,
			typedField("parent", typedObject(
				typedField("child", typedNull()),
				typedField("sibling", typedString("keep-界")),
				typedField("nested", typedObject(
					typedField("count", typedSint(7)),
				)),
			)),
			typedField("literal.dot", typedString("literal-dot-keep")),
			typedField("literal", typedObject(
				typedField("dot", typedString("nested-dot-keep")),
			)),
		),
		newEvent("v03-fillnull-container-missing", "v03-fillnull-container", "missing", 8,
			typedField("literal.dot", typedNull()),
		),
	}
	for _, fixture := range []struct {
		id      string
		raw     string
		ordinal int
	}{
		{id: "v03-fixed-binary-present", raw: "binary,valid-utf8", ordinal: 9},
		{id: "v03-fixed-binary-null-fill", raw: "ignored,binary-utf8", ordinal: 10},
	} {
		event := newEvent(
			fixture.id,
			"v03-fixed-binary-provenance",
			fixture.raw,
			fixture.ordinal,
		)
		// The payload is deliberately valid UTF-8. Rejection must therefore
		// come from semantic Bytes provenance, not the physical byte validator.
		event.Event.RawEncoding = opensplunk.RawEncoding_RAW_ENCODING_BINARY
		core = append(core, event)
	}

	stringsList := func(prefix string, count int) *opensplunk.TypedValue {
		members := make([]*opensplunk.TypedValue, count)
		for index := range members {
			members[index] = typedString(fmt.Sprintf("%s-%04d", prefix, index))
		}
		return typedList(members...)
	}
	commaList := func(count int) string {
		members := make([]string, count)
		for index := range members {
			members[index] = fmt.Sprintf("m%04d", index)
		}
		return strings.Join(members, ",")
	}

	resources := []*ingest.StoredEvent{
		newEvent("v03-makemv-bomb", "v03-makemv-bomb", "bomb", 20,
			typedField("bomb_csv", typedString(commaList(1001))),
		),
		newEvent("v03-makemv-separator-bomb", "v03-makemv-separator-bomb", "separator bomb", 30,
			typedField("bomb_csv", typedString(strings.Repeat(",", (1<<20)-1))),
		),
		newEvent("v03-makemv-number", "v03-makemv-number", "number", 21,
			typedField("bomb_csv", typedSint(7)),
		),
		newEvent("v03-makemv-array", "v03-makemv-array", "array", 22,
			typedField("bomb_csv", typedList(typedString("a"), typedString("b"))),
		),
		newEvent("v03-mvexpand-bomb", "v03-mvexpand-bomb", "bomb", 23,
			typedField("bomb_mv", stringsList("member", 1001)),
		),
		newEvent("v03-mvexpand-boundary", "v03-mvexpand-boundary", "boundary", 29,
			typedField("boundary_mv", stringsList("member", 1000)),
		),
		newEvent("v03-mvexpand-repeat", "v03-mvexpand-repeat", "repeat", 24,
			typedField("tags", stringsList("tag", 101)),
			typedField("zones", stringsList("zone", 101)),
		),
		newEvent("v03-mvexpand-retained", "v03-mvexpand-retained", strings.Repeat("r", 70<<10), 25,
			typedField("retained_tags", stringsList("tag", 1000)),
		),
		newEvent("v03-mvexpand-object", "v03-mvexpand-object", "object", 26,
			typedField("bad_mv", typedObject(typedField("child", typedString("x")))),
		),
		newEvent("v03-mvexpand-nested", "v03-mvexpand-nested", "nested", 27,
			typedField("bad_mv", typedList(typedList(typedString("x")))),
		),
		newEvent("v03-mvexpand-mixed-number", "v03-mvexpand-mixed-number", "mixed number", 30,
			typedField("bad_mv", typedList(typedString("x"), typedSint(7))),
		),
		newEvent("v03-mvexpand-mixed-bool", "v03-mvexpand-mixed-bool", "mixed bool", 31,
			typedField("bad_mv", typedList(typedString("x"), typedBool(true))),
		),
		newEvent("v03-mvexpand-timestamp", "v03-mvexpand-timestamp", "timestamp", 32,
			typedField("mv_scalar", typedTimestamp(time.Date(2026, time.July, 21, 12, 34, 56, 789, time.UTC))),
		),
		newEvent("v03-mvexpand-decimal", "v03-mvexpand-decimal", "decimal", 33,
			typedField("mv_scalar", typedDecimal("123.450")),
		),
		newEvent("v03-mvexpand-bytes", "v03-mvexpand-bytes", "bytes", 34,
			typedField("mv_scalar", typedBytes([]byte{0x00, 0xff, 'A'})),
		),
		newEvent("v03-mvexpand-duration", "v03-mvexpand-duration", "duration", 35,
			typedField("mv_scalar", typedDuration(5*time.Second+7*time.Nanosecond)),
		),
		newEvent("v03-mvexpand-cancel", "v03-mvexpand-cancel", "cancel", 28,
			typedField("tags", stringsList("tag", 100)),
			typedField("zones", stringsList("zone", 100)),
		),
	}
	// Exercise whole-result makemv ceilings independently of the per-row
	// member/byte bounds. These rows are individually ordinary and only become
	// hostile when the command tries to publish the complete stage result.
	for index := range 101 {
		resources = append(resources, newEvent(
			fmt.Sprintf("v03-makemv-result-members-%03d", index),
			"v03-makemv-result-members",
			"members",
			100+index,
			typedField("bomb_csv", typedString(commaList(1000))),
		))
	}
	for index := range 100 {
		resources = append(resources, newEvent(
			fmt.Sprintf("v03-makemv-result-bytes-%03d", index),
			"v03-makemv-result-bytes",
			"bytes",
			300+index,
			typedField("bomb_csv", typedString(strings.Repeat("界", 30_000))),
		))
	}
	for index := range 100 {
		resources = append(resources, newEvent(
			fmt.Sprintf("v03-makemv-retained-%03d", index),
			"v03-makemv-retained",
			strings.Repeat("r", 700<<10),
			500+index,
			typedField("bomb_csv", typedString("a,b")),
		))
	}
	// Each expansion stage emits exactly its independent 10,000-row maximum:
	// ten source events fan out to 1,000 tag rows, then each tag row retains one
	// zone. The 20,000-row cumulative charge must therefore select the stricter
	// query-wide marker without being masked by a row- or stage-local failure.
	for index := range 10 {
		resources = append(resources, newEvent(
			fmt.Sprintf("v03-mvexpand-query-rows-%02d", index),
			"v03-mvexpand-query-rows",
			"query rows",
			700+index,
			typedField("tags", stringsList(fmt.Sprintf("tag-%02d", index), 1000)),
			typedField("zones", stringsList(fmt.Sprintf("zone-%02d", index), 1)),
		))
	}
	// Pin the cumulative query boundary itself, independently of either 10,000
	// row stage ceiling. Ten 1,000-member inputs make stage one exactly 10,000;
	// only five retain one zone, so stage two emits 5,000 and the cumulative
	// charge is exactly the admitted 15,000.
	for index := range 10 {
		zones := typedList()
		if index < 5 {
			zones = stringsList(fmt.Sprintf("exact-zone-%02d", index), 1)
		}
		resources = append(resources, newEvent(
			fmt.Sprintf("v03-mvexpand-query-exact-%02d", index),
			"v03-mvexpand-query-exact",
			"query exact",
			800+index,
			typedField("tags", stringsList(fmt.Sprintf("exact-tag-%02d", index), 1000)),
			typedField("zones", zones),
		))
	}
	// The overflow fixture keeps both stages independently legal while adding
	// exactly one second-stage row: 5*1000 + 1 emitted rows after an exact
	// 10,000-row first stage. This must select the 15,001 cumulative marker.
	for index := range 11 {
		tagCount := 1000
		zones := typedList()
		switch {
		case index < 5:
			zones = stringsList(fmt.Sprintf("overflow-zone-%02d", index), 1)
		case index == 5:
			tagCount = 1
			zones = stringsList("overflow-zone-05", 1)
		case index == 10:
			tagCount = 999
		}
		resources = append(resources, newEvent(
			fmt.Sprintf("v03-mvexpand-query-overflow-%02d", index),
			"v03-mvexpand-query-overflow",
			"query overflow",
			900+index,
			typedField("tags", stringsList(fmt.Sprintf("overflow-tag-%02d", index), tagCount)),
			typedField("zones", zones),
		))
	}

	// Store hostile retained-byte fixtures in deliberately small durable
	// batches. The test is meant to cross a search-stage ceiling, not the
	// independent ingest outbox admission limit.
	pipelineStoreIntegrationFixtureBatches(ctx, t, store, indexTime, resources)

	// Delta shares the bounded ordered-window implementation with streamstats.
	// One row beyond that ceiling must remain an atomic failure even when a
	// later command would otherwise discard every result row.
	const deltaBoundaryRows = int(MaximumStreamStatsInputRows + 1)
	deltaBoundary := make([]*ingest.StoredEvent, deltaBoundaryRows)
	for index := range deltaBoundary {
		deltaBoundary[index] = newEvent(
			fmt.Sprintf("v03-delta-boundary-%05d", index),
			"v03-delta-boundary",
			"delta boundary",
			1_000+index,
			typedField("delta_n", typedSint(int64(index))),
		)
	}
	pipelineStoreDeltaBoundaryFixtureBatches(ctx, t, store, indexTime, deltaBoundary)

	// Normal ingest cannot manufacture a malformed semantic envelope. Insert
	// one raw poison value with honest Decimal metadata so mvexpand must reject
	// the malformed payload itself, not an unrelated storage/type mismatch.
	malformedVisibility, malformedVisibilityErr := store.VisibilityCutoff(ctx)
	if malformedVisibilityErr != nil {
		t.Fatalf("capture malformed pipeline envelope visibility: %v", malformedVisibilityErr)
	}
	binEdgeInsertRawDecimalEnvelopes(t, ctx, connection, "spl-v03-mvexpand-malformed-envelope", []binEdgeRawDecimalEnvelope{
		{
			eventID:       "v03-mvexpand-malformed-envelope",
			tenantID:      "tenant",
			indexName:     "spl-v03",
			eventTime:     eventTimeAnchor.Add(36 * time.Second),
			indexTime:     indexTime,
			visibilitySeq: malformedVisibility,
			fieldName:     "mv_scalar",
			fieldType:     eventfields.StoredValueTypeDecimal,
			envelope: map[string]string{
				"\x00open_splunk_type":  "decimal/v1",
				"\x00open_splunk_value": "malformed-secret-1e",
			},
		},
		{
			eventID:       "v03-addtotals-default-malformed-envelope",
			tenantID:      "tenant",
			indexName:     "spl-v03",
			eventTime:     eventTimeAnchor.Add(37 * time.Second),
			indexTime:     indexTime,
			visibilitySeq: malformedVisibility,
			fieldName:     "Total",
			fieldType:     eventfields.StoredValueTypeDecimal,
			envelope: map[string]string{
				"\x00open_splunk_type":  "decimal/v1",
				"\x00open_splunk_value": "malformed-secret-default",
			},
		},
	})
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"spl-v03",
		"spl-v03-adversarial-core",
		2_000,
		core...,
	)
	visibilityCutoff, visibilityErr := store.VisibilityCutoff(ctx)
	if visibilityErr != nil {
		t.Fatalf("capture pipeline visibility cutoff: %v", visibilityErr)
	}
	base := `index=spl-v03 source="v03-core"`

	t.Run("regex missing-null negation and Unicode RE2", func(t *testing.T) {
		pipelineAssertIDs(t, queryContext, connection, compile(
			base+` | regex "(?i)timeout|拒否" | sort 0 +event_id | table event_id`,
		), "v03-01")
		pipelineAssertIDs(t, queryContext, connection, compile(
			base+` | regex regex_text!="(?i)^blocked$" | sort 0 +event_id | table event_id`,
		), "v03-02", "v03-03", "v03-04", "v03-05")
	})

	t.Run("reverse uses established private order through repeats head tail dedup aggregation and ties", func(t *testing.T) {
		pipelineAssertIDs(t, queryContext, connection, compile(
			base+` | sort 0 +event_id | reverse | table event_id`,
		), "v03-06", "v03-05", "v03-04", "v03-03", "v03-02", "v03-01")
		pipelineAssertIDs(t, queryContext, connection, compile(
			base+` | sort 0 +event_id | reverse | reverse | table event_id`,
		), "v03-01", "v03-02", "v03-03", "v03-04", "v03-05", "v03-06")
		pipelineAssertIDs(t, queryContext, connection, compile(
			base+` | sort 0 +event_id | reverse | head 2 | reverse | table event_id`,
		), "v03-05", "v03-06")
		pipelineAssertIDs(t, queryContext, connection, compile(
			base+` | sort 0 +event_id | tail 2 | reverse | table event_id`,
		// The authored expression tail contract itself returns its selected suffix in
		// reverse established order (06,05); pipeline reverse must invert that
		// current order rather than reinterpret tail as a forward suffix.
		), "v03-05", "v03-06")
		pipelineAssertIDs(t, queryContext, connection, compile(
			base+` | sort 0 +event_id | dedup dedup_key | reverse | table event_id`,
		), "v03-06", "v03-05", "v03-04", "v03-03", "v03-01")
		pipelineAssertJSONRows(t, queryContext, connection, compile(
			base+` | stats count BY bucket | sort 0 +bucket | reverse | table bucket count`,
		), []string{"bucket", "count"}, [][]string{{"C", "2"}, {"B", "2"}, {"A", "2"}})
		// All public sort keys are equal. Only the compiler-private source
		// ordinal can make this repeatable; reverse must invert that ordinal.
		pipelineAssertIDs(t, queryContext, connection, compile(
			base+` | sort 0 +tie_key | reverse | table event_id`,
		), "v03-01", "v03-02", "v03-03", "v03-04", "v03-05", "v03-06")

		// A reverse is not merely a terminal presentation. The next ordered
		// window must consume the reversed private relation lineage.
		pipelineAssertJSONRows(t, queryContext, connection, compile(
			base+` | sort 0 +event_id | reverse | accum n AS running | table event_id running`,
		), []string{"event_id", "running"}, [][]string{
			{"v03-06", "-1"}, {"v03-05", "-1"}, {"v03-04", "-1"},
			{"v03-03", "-1"}, {"v03-02", "2.5"}, {"v03-01", "4.5"},
		})
	})

	t.Run("accum and delta share numeric eligibility and ordered windows", func(t *testing.T) {
		compiled := compile(base +
			` | sort 0 +event_id | accum n AS running | delta running AS step` +
			` | table event_id running step`)
		pipelineAssertJSONRows(t, queryContext, connection, compiled,
			[]string{"event_id", "running", "step"},
			[][]string{
				{"v03-01", "2", "<null>"},
				{"v03-02", "5.5", "3.5"},
				{"v03-03", "5.5", "0"},
				{"v03-04", "5.5", "0"},
				{"v03-05", "5.5", "0"},
				{"v03-06", "4.5", "-1"},
			},
		)
		lagged := compile(base +
			` | sort 0 +event_id | delta delta_n AS lag2 p=2 | table event_id lag2`)
		pipelineAssertJSONRows(t, queryContext, connection, lagged,
			[]string{"event_id", "lag2"},
			[][]string{
				{"v03-01", "<null>"}, {"v03-02", "<null>"},
				{"v03-03", "-5"}, {"v03-04", "<null>"},
				{"v03-05", "-3"}, {"v03-06", "<null>"},
			},
		)

		nonFiniteAccumInput := compile(base + ` event_id="v03-01"` +
			` | eval overflow=1e308*1e308 | accum overflow AS running | table running`)
		pipelineAssertJSONRows(t, queryContext, connection, nonFiniteAccumInput,
			[]string{"running"}, [][]string{{"<null>"}})
		overflowAccumResult := compile(base + ` (event_id="v03-01" OR event_id="v03-02")` +
			` | sort 0 +event_id | eval finite=1e308 | accum finite AS running` +
			` | tail 1 | table running`)
		pipelineAssertJSONRows(t, queryContext, connection, overflowAccumResult,
			[]string{"running"}, [][]string{{"+Inf"}})
		nonFiniteDeltaInput := compile(base + ` (event_id="v03-01" OR event_id="v03-02")` +
			` | sort 0 +event_id` +
			` | eval overflow=if(event_id="v03-01",1e308*1e308,0.0)` +
			` | delta overflow AS step | table event_id step`)
		pipelineAssertJSONRows(t, queryContext, connection, nonFiniteDeltaInput,
			[]string{"event_id", "step"},
			[][]string{{"v03-01", "<null>"}, {"v03-02", "-Inf"}})
		overflowDeltaResult := compile(base + ` (event_id="v03-01" OR event_id="v03-02")` +
			` | sort 0 +event_id` +
			` | eval finite=if(event_id="v03-01",-1e308,1e308)` +
			` | delta finite AS step | tail 1 | table step`)
		pipelineAssertJSONRows(t, queryContext, connection, overflowDeltaResult,
			[]string{"step"}, [][]string{{"+Inf"}})
	})

	t.Run("strcat missing policy fillnull type distinctions and row totals", func(t *testing.T) {
		allRequired := compile(base +
			` | sort 0 +event_id | strcat allrequired=true join_host ":" route joined` +
			` | table event_id joined`)
		pipelineAssertJSONRows(t, queryContext, connection, allRequired,
			[]string{"event_id", "joined"},
			[][]string{
				{"v03-01", "api:/α"}, {"v03-02", "preexisting"},
				{"v03-03", ":/c"}, {"v03-04", "<null>"},
				{"v03-05", "<null>"}, {"v03-06", "worker:/f"},
			},
		)
		defaultRequired := compile(base +
			` | sort 0 +event_id | strcat join_host ":" route joined | table event_id joined`)
		pipelineAssertJSONRows(t, queryContext, connection, defaultRequired,
			[]string{"event_id", "joined"},
			[][]string{
				{"v03-01", "api:/α"}, {"v03-02", ":/b"},
				{"v03-03", ":/c"}, {"v03-04", ":/d"},
				{"v03-05", "worker:"}, {"v03-06", "worker:/f"},
			},
		)

		filled := compile(base +
			` | sort 0 +event_id | fillnull value="unknown" optional | table event_id optional`)
		pipelineAssertJSONRows(t, queryContext, connection, filled,
			[]string{"event_id", "optional"},
			[][]string{
				{"v03-01", "unknown"}, {"v03-02", "unknown"},
				{"v03-03", ""}, {"v03-04", "unknown"},
				{"v03-05", "0"}, {"v03-06", "false"},
			},
		)

		totals := compile(base +
			` | sort 0 +event_id | addtotals a b | table event_id Total`)
		pipelineAssertJSONRows(t, queryContext, connection, totals,
			[]string{"event_id", "Total"},
			[][]string{
				{"v03-01", "5.5"}, {"v03-02", "4"},
				{"v03-03", "0"}, {"v03-04", "0"},
				{"v03-05", "0"}, {"v03-06", "1"},
			},
		)
		nonFiniteTotals := compile(base + ` event_id="v03-01"` +
			` | eval positive_inf=1e308*1e308, negative_inf=0-(1e308*1e308), nan=(1e308*1e308)-(1e308*1e308)` +
			` | addtotals fieldname=finite_total positive_inf negative_inf nan` +
			` | table finite_total`)
		pipelineAssertJSONRows(t, queryContext, connection, nonFiniteTotals,
			[]string{"finite_total"}, [][]string{{"0"}})

		fillMultivalue := compile(base +
			` | sort 0 +event_id | fillnull value="unknown" optional_mv | table event_id optional_mv`)
		pipelineAssertJSONRows(t, queryContext, connection, fillMultivalue,
			[]string{"event_id", "optional_mv"},
			[][]string{
				{"v03-01", "[keep,界]"}, {"v03-02", "unknown"},
				{"v03-03", "unknown"}, {"v03-04", "unknown"},
				{"v03-05", "unknown"}, {"v03-06", "unknown"},
			},
		)

		converted := compile(base +
			` | sort 0 +event_id | strcat n ":" b converted | table event_id converted`)
		pipelineAssertJSONRows(t, queryContext, connection, converted,
			[]string{"event_id", "converted"},
			[][]string{
				{"v03-01", "2:3.5"}, {"v03-02", "3.5:"},
				{"v03-03", ":"}, {"v03-04", ":"},
				{"v03-05", ":"}, {"v03-06", "-1:-0.25"},
			},
		)

		maximumFields := make([]string, spl.MaximumExplicitProjectionFields)
		for index := range maximumFields {
			maximumFields[index] = fmt.Sprintf("v03_limit_%02d", index)
		}
		filledMaximum := compile(base + ` event_id="v03-01" | fillnull value="x" ` +
			strings.Join(maximumFields, " ") + ` | table v03_limit_00 v03_limit_63`)
		pipelineAssertJSONRows(t, queryContext, connection, filledMaximum,
			[]string{"v03_limit_00", "v03_limit_63"}, [][]string{{"x", "x"}})
		totaledMaximum := compile(base + ` event_id="v03-01" | addtotals fieldname=total ` +
			strings.Join(maximumFields, " ") + ` | table total`)
		pipelineAssertJSONRows(t, queryContext, connection, totaledMaximum,
			[]string{"total"}, [][]string{{"0"}})
	})

	t.Run("strcat allrequired preserves optional list nullness and flattened parents", func(t *testing.T) {
		optionalNull := compile(base + ` event_id="v03-05"` +
			` | makemv delim="," tags_csv` +
			` | strcat allrequired=true missing ":" tags_csv` +
			` | table tags_csv`)
		pipelineAssertJSONRows(t, queryContext, connection, optionalNull,
			[]string{"tags_csv"}, [][]string{{"<null>"}})

		container := compile(`index=spl-v03 source="v03-fillnull-container"` +
			` | sort 0 +event_id` +
			` | strcat allrequired=true missing ":" parent parent` +
			` | table event_id parent`)
		if outputs, ok := container.ValidatedResultContainerOutputs(); !ok || !reflect.DeepEqual(outputs, []ResultContainerOutput{
			canonicalResultContainerOutput(1),
		}) {
			t.Fatalf("strcat container descriptors = %#v / valid %t", outputs, ok)
		}
		rows := pipelineJSONRows(t, queryContext, connection, container, []string{"event_id", "parent"})
		if len(rows) != 2 || len(rows[0]) != 2 || len(rows[1]) != 2 ||
			pipelineJSONText(rows[0][0]) != "v03-fillnull-container-missing" ||
			pipelineJSONText(rows[0][1]) != "<null>" ||
			pipelineJSONText(rows[1][0]) != "v03-fillnull-container-present" {
			t.Fatalf("strcat preserved container rows = %#v", rows)
		}
		parent, ok := pipelineDynamicStringMap(rows[1][1])
		if !ok || pipelineJSONText(parent["child"]) != "<null>" ||
			pipelineJSONText(parent["sibling"]) != "keep-界" {
			t.Fatalf("strcat preserved parent = %#v", rows[1][1])
		}
	})

	t.Run("fillnull preserves flattened parents and exact descendant siblings", func(t *testing.T) {
		const containerBase = `index=spl-v03 source="v03-fillnull-container"`
		for _, source := range []string{
			containerBase + ` | sort 0 +event_id | fillnull value="fallback-界" parent | table event_id parent`,
			containerBase + ` | sort 0 +event_id | fillnull value="fallback-界" parent.child | table event_id parent.child parent.sibling`,
			containerBase + ` | sort 0 +event_id | fillnull value="fallback-界" parent parent.child | table event_id parent parent.child parent.sibling`,
			containerBase + ` | sort 0 +event_id | fillnull value="fallback-界" parent.child parent | table event_id parent parent.child parent.sibling`,
		} {
			compiled := compile(source)
			if !compiled.HasValidExecutionSeal() {
				t.Fatalf("fillnull container query lacks valid execution seal: %s", source)
			}
			if outputs, ok := compiled.ValidatedResultContainerOutputs(); !ok || len(outputs) == 0 {
				t.Fatalf("fillnull container descriptors = %#v / valid %t for %s", outputs, ok, source)
			}
		}

		parent := compile(containerBase +
			` | sort 0 +event_id | fillnull value="fallback-界" parent | table event_id parent`)
		parentRows := pipelineJSONRows(t, queryContext, connection, parent, []string{"event_id", "parent"})
		if len(parentRows) != 2 || len(parentRows[0]) != 2 || len(parentRows[1]) != 2 ||
			pipelineJSONText(parentRows[0][0]) != "v03-fillnull-container-missing" ||
			pipelineJSONText(parentRows[0][1]) != "fallback-界" ||
			pipelineJSONText(parentRows[1][0]) != "v03-fillnull-container-present" {
			t.Fatalf("fillnull parent rows = %#v", parentRows)
		}
		parentMap, ok := pipelineDynamicStringMap(parentRows[1][1])
		if !ok || pipelineJSONText(parentMap["child"]) != "<null>" ||
			pipelineJSONText(parentMap["sibling"]) != "keep-界" {
			t.Fatalf("preserved fillnull parent = %#v, want null child and unchanged sibling", parentRows[1][1])
		}

		for _, test := range []struct {
			fields string
			table  string
		}{
			{fields: "parent.child", table: "event_id parent.child parent.sibling"},
			{fields: "parent parent.child", table: "event_id parent parent.child parent.sibling"},
			{fields: "parent.child parent", table: "event_id parent parent.child parent.sibling"},
		} {
			compiled := compile(containerBase + ` | sort 0 +event_id | fillnull value="fallback-界" ` +
				test.fields + ` | table ` + test.table)
			pipelineAssertJSONRows(t, queryContext, connection, compiled,
				[]string{"event_id", "parent.child", "parent.sibling"},
				[][]string{
					{"v03-fillnull-container-missing", "fallback-界", "<null>"},
					{"v03-fillnull-container-present", "fallback-界", "keep-界"},
				},
			)
		}

		// The descendant-first form used to publish the literal dotted column and
		// then lose it as soon as the same projection introduced its top-level
		// Dynamic ancestor. Keep every important downstream resolver on that exact
		// same-command boundary. The two hostile rows cover both an absent parent
		// and a flattened parent with an explicit-null child plus an untouched
		// sibling.
		const descendantBeforeParent = containerBase +
			` | sort 0 +event_id | fillnull value="fallback-界" parent.child parent`
		wantExpanded := [][]string{
			{"v03-fillnull-container-missing", "fallback-界"},
			{"v03-fillnull-container-present", "fallback-界"},
		}
		t.Run("descendant before parent composes with mvexpand", func(t *testing.T) {
			compiled := compile(descendantBeforeParent +
				` | mvexpand parent.child | table event_id parent.child`)
			pipelineAssertJSONRows(t, queryContext, connection, compiled,
				[]string{"event_id", "parent.child"}, wantExpanded)
		})
		t.Run("descendant before parent composes with stats", func(t *testing.T) {
			compiled := compile(descendantBeforeParent +
				` | stats count BY parent.child | table parent.child count`)
			pipelineAssertJSONRows(t, queryContext, connection, compiled,
				[]string{"parent.child", "count"}, [][]string{{"fallback-界", "2"}})
		})
		t.Run("descendant before parent composes with search", func(t *testing.T) {
			compiled := compile(descendantBeforeParent +
				` | search parent.child="fallback-界" | table event_id parent.child`)
			pipelineAssertJSONRows(t, queryContext, connection, compiled,
				[]string{"event_id", "parent.child"}, wantExpanded)
		})
		t.Run("descendant before parent composes with chart", func(t *testing.T) {
			compiled := compile(descendantBeforeParent +
				` | fillnull value="missing-sibling" parent.sibling` +
				` | chart count OVER parent.child BY parent.sibling`)
			want := []chartEdgeTransportRow{{
				ordinal: 0,
				row:     "fallback-界",
				names:   "0:keep-界|0:missing-sibling",
				counts:  "1|1",
			}}
			if got := chartEdgeTransport(t, queryContext, connection, compiled); !reflect.DeepEqual(got, want) {
				t.Fatalf("descendant-first fillnull chart = %#v, want %#v", got, want)
			}
		})

		// Escaping the dot makes literal\.dot one top-level authored key, not a
		// descendant of literal. It must therefore remain independent when the
		// adjacent ancestor-looking field is filled in the same command.
		t.Run("escaped literal dot is not an ancestor collision", func(t *testing.T) {
			compiled := compile(containerBase +
				` | sort 0 +event_id` +
				` | fillnull value="literal-fill" literal\.dot literal` +
				` | table event_id literal\.dot literal.dot`)
			pipelineAssertJSONRows(t, queryContext, connection, compiled,
				[]string{"event_id", `literal\.dot`, "literal.dot"},
				[][]string{
					{"v03-fillnull-container-missing", "literal-fill", "<null>"},
					{"v03-fillnull-container-present", "literal-dot-keep", "nested-dot-keep"},
				},
			)
		})

		// Stats aliases are opaque public names, so parent..child is admitted as
		// an output even though it is not a canonical path. Publishing the real
		// top-level parent afterwards must preserve that opaque aggregate column.
		// pipelineJSONRows scans every physical driver column before selecting these
		// two names, catching both omission and duplicate-name regressions.
		t.Run("opaque stats alias survives new parent publication", func(t *testing.T) {
			compiled := compile(containerBase +
				` | stats count AS "parent..child"` +
				` | fillnull value="parent-fallback" parent`)
			if got, want := compiled.OutputFields, []string{"parent..child", "parent"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("opaque stats/fillnull output fields = %#v, want %#v", got, want)
			}
			pipelineAssertJSONRows(t, queryContext, connection, compiled,
				[]string{"parent..child", "parent"},
				[][]string{{"2", "parent-fallback"}})
		})

		// A leading-dot literal alias is the intentionally referenceable opaque
		// spelling. Keep it as the positive downstream projection control.
		t.Run("leading dot stats alias remains downstream referenceable", func(t *testing.T) {
			compiled := compile(containerBase +
				` | stats count AS ".com"` +
				` | fillnull value="parent-fallback" parent` +
				` | table '.com' parent`)
			pipelineAssertJSONRows(t, queryContext, connection, compiled,
				[]string{".com", "parent"},
				[][]string{{"2", "parent-fallback"}})
		})

		t.Run("escaped physical identifier does not overlap new parent", func(t *testing.T) {
			compiled := compile(containerBase +
				` | sort 0 +event_id` +
				` | eval parent\.child="kept"` +
				` | fillnull value="fallback" parent\.child parent` +
				` | table event_id parent\.child`)
			pipelineAssertJSONRows(t, queryContext, connection, compiled,
				[]string{"event_id", `parent\.child`},
				[][]string{
					{"v03-fillnull-container-missing", "kept"},
					{"v03-fillnull-container-present", "kept"},
				},
			)
		})
	})

	t.Run("addinfo overwrites authored shadows from immutable scope", func(t *testing.T) {
		const publicJobID = "pipeline-live-integration-job"
		compiled := pipelineCompileIntegrationSPLWithJobID(
			t,
			base+` event_id="v03-01" | addinfo | table info_min_time info_max_time info_search_time info_sid`,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
			publicJobID,
		)
		rows := pipelineJSONRows(t, queryContext, connection, compiled,
			[]string{"info_min_time", "info_max_time", "info_search_time", "info_sid"})
		if len(rows) != 1 || len(rows[0]) != 4 {
			t.Fatalf("addinfo rows = %#v, want one four-field row", rows)
		}
		wantMin := time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC).Unix()
		wantMax := time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC).Unix()
		wantSearch := indexTime.Add(9 * time.Second).Unix()
		if got := pipelineJSONText(rows[0][0]); got != fmt.Sprint(wantMin) {
			t.Fatalf("info_min_time = %s, want %d", got, wantMin)
		}
		if got := pipelineJSONText(rows[0][1]); got != fmt.Sprint(wantMax) {
			t.Fatalf("info_max_time = %s, want %d", got, wantMax)
		}
		if got := pipelineJSONText(rows[0][2]); got != fmt.Sprint(wantSearch) {
			t.Fatalf("info_search_time = %s, want %d", got, wantSearch)
		}
		if sid := pipelineJSONText(rows[0][3]); sid != publicJobID {
			t.Fatalf("info_sid = %q, want immutable admitted public job id %q", sid, publicJobID)
		}
	})

	t.Run("pipeline commands commands execute together over hostile live data", func(t *testing.T) {
		const publicJobID = "pipeline-live-command"
		compiled := pipelineCompileIntegrationSPLWithJobID(
			t,
			base+` event_id="v03-01"`+
				` | regex regex_text="(?i)^blocked$"`+
				` | sort 0 +tie_key`+
				` | accum n AS running`+
				` | strcat join_host ":" route endpoint`+
				` | addinfo`+
				` | fillnull value="fallback" optional`+
				` | addtotals fieldname=total a b running`+
				` | delta running AS step`+
				` | makemv delim="," allowempty=true tags_csv`+
				` | mvexpand tags_csv limit=3`+
				` | reverse`+
				` | table event_id tags_csv running endpoint optional total step info_sid`,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
			publicJobID,
		)
		pipelineAssertJSONRows(t, queryContext, connection, compiled,
			[]string{"event_id", "tags_csv", "running", "endpoint", "optional", "total", "step", "info_sid"},
			[][]string{
				{"v03-01", "β", "2", "api:/α", "fallback", "7.5", "<null>", publicJobID},
				{"v03-01", "", "2", "api:/α", "fallback", "7.5", "<null>", publicJobID},
				{"v03-01", "a", "2", "api:/α", "fallback", "7.5", "<null>", publicJobID},
			},
		)
	})

	t.Run("makemv Unicode empty-member and typed null boundaries", func(t *testing.T) {
		withoutEmpty := compile(base +
			` | sort 0 +event_id | makemv delim="," tags_csv | table event_id tags_csv`)
		descriptors, valid := withoutEmpty.ValidatedResultOptionalMultivalueOutputs()
		if !valid || len(descriptors) != 1 || descriptors[0].OutputIndex != 1 {
			t.Fatalf("live makemv optional transport = %#v, valid %t", descriptors, valid)
		}
		presentColumn := descriptors[0].PresentColumn()
		// ClickHouse cannot represent Nullable(Array(String)). The physical
		// transport therefore carries a non-null array plus a private tri-state:
		// 0 is missing, 1 is a present list (including empty), and 2 is explicit
		// null. The queryexec adversarial test separately verifies the public
		// values produced from all three states.
		pipelineAssertJSONRows(t, queryContext, connection, withoutEmpty,
			[]string{"event_id", "tags_csv", presentColumn},
			[][]string{
				{"v03-01", "[a,β]", "1"}, {"v03-02", "[x]", "1"},
				{"v03-03", "[]", "1"}, {"v03-04", "[]", "0"},
				{"v03-05", "[]", "2"}, {"v03-06", "[dup,dup,界]", "1"},
			},
		)
		withEmpty := compile(base + ` event_id="v03-01"` +
			` | makemv delim="," allowempty=true tags_csv | table tags_csv`)
		pipelineAssertJSONRows(t, queryContext, connection, withEmpty,
			[]string{"tags_csv"}, [][]string{{"[a,,β]"}})
		unicode := compile(base + ` event_id="v03-01"` +
			` | makemv delim="💥界" unicode_csv | table unicode_csv`)
		pipelineAssertJSONRows(t, queryContext, connection, unicode,
			[]string{"unicode_csv"}, [][]string{{"[α,ω]"}})

		// Filtering empty members must collapse a hostile delimiter run before
		// constructing the physical array. This remains a present empty list,
		// rather than a missing/null value or a resource failure.
		separatorBomb := compile(`index=spl-v03 source="v03-makemv-separator-bomb"` +
			` | makemv delim="," bomb_csv | table bomb_csv`)
		separatorDescriptors, separatorValid := separatorBomb.ValidatedResultOptionalMultivalueOutputs()
		if !separatorValid || len(separatorDescriptors) != 1 ||
			separatorDescriptors[0].OutputIndex != 0 {
			t.Fatalf(
				"separator-bomb optional transport = %#v, valid %t",
				separatorDescriptors,
				separatorValid,
			)
		}
		pipelineAssertJSONRows(t, queryContext, connection, separatorBomb,
			[]string{"bomb_csv", separatorDescriptors[0].PresentColumn()},
			[][]string{{"[]", "1"}})
	})

	t.Run("mvexpand preserves member order scalar missing null and null members", func(t *testing.T) {
		expanded := compile(base +
			` | sort 0 +event_id | mvexpand mv_list | table event_id mv_list`)
		pipelineAssertJSONRows(t, queryContext, connection, expanded,
			[]string{"event_id", "mv_list"},
			[][]string{
				{"v03-01", "x"}, {"v03-01", "<null>"}, {"v03-01", "y"},
				{"v03-03", "scalar"}, {"v03-04", "<null>"}, {"v03-05", "<null>"},
				{"v03-06", "dup"}, {"v03-06", "dup"}, {"v03-06", "界"},
			},
		)
		limited := compile(base + ` event_id="v03-01" | mvexpand mv_list limit=2 | table mv_list`)
		pipelineAssertJSONRows(t, queryContext, connection, limited,
			[]string{"mv_list"}, [][]string{{"x"}, {"<null>"}})
		omittedLimit := compile(base + ` event_id="v03-01" | mvexpand mv_list | table mv_list`)
		pipelineAssertJSONRows(t, queryContext, connection, omittedLimit,
			[]string{"mv_list"}, [][]string{{"x"}, {"<null>"}, {"y"}})
		zeroLimit := compile(base + ` event_id="v03-01" | mvexpand mv_list limit=0 | table mv_list`)
		pipelineAssertJSONRows(t, queryContext, connection, zeroLimit,
			[]string{"mv_list"}, [][]string{{"x"}, {"<null>"}, {"y"}})
		absentFixedArray := compile(base +
			` | stats values(definitely_missing) AS tags | mvexpand tags | table tags`)
		pipelineAssertJSONRows(t, queryContext, connection, absentFixedArray,
			[]string{"tags"}, [][]string{{"<null>"}})
		// A supported scalar never becomes a String multivalue implicitly. Number
		// and Boolean values each preserve one row and their exact runtime type.
		numericScalar := compile(base + ` event_id="v03-01" | mvexpand n | table event_id n`)
		pipelineAssertJSONRows(t, queryContext, connection, numericScalar,
			[]string{"event_id", "n"}, [][]string{{"v03-01", "2"}})
		booleanScalar := compile(base + ` event_id="v03-03" | mvexpand n | table event_id n`)
		pipelineAssertJSONRows(t, queryContext, connection, booleanScalar,
			[]string{"event_id", "n"}, [][]string{{"v03-03", "true"}})
		pipelineAssertTaggedScalar(t, queryContext, connection, compile(
			`index=spl-v03 event_id="v03-mvexpand-timestamp"`+
				` | mvexpand mv_scalar | table event_id mv_scalar`,
		), "mv_scalar", "timestamp/v1", "2026-07-21T12:34:56.000000789Z")
		pipelineAssertTaggedScalar(t, queryContext, connection, compile(
			`index=spl-v03 event_id="v03-mvexpand-decimal"`+
				` | mvexpand mv_scalar | table event_id mv_scalar`,
		), "mv_scalar", "decimal/v1", "123.450")

		repeated := compile(`index=spl-v03 source="v03-mvexpand-cancel"` +
			` | mvexpand tags limit=2 | mvexpand zones limit=2 | table tags zones`)
		pipelineAssertJSONRows(t, queryContext, connection, repeated,
			[]string{"tags", "zones"},
			[][]string{
				{"tag-0000", "zone-0000"}, {"tag-0000", "zone-0001"},
				{"tag-0001", "zone-0000"}, {"tag-0001", "zone-0001"},
			},
		)
		reversed := compile(base + ` event_id="v03-01"` +
			` | makemv delim="," allowempty=true tags_csv | mvexpand tags_csv | reverse | table tags_csv`)
		pipelineAssertJSONRows(t, queryContext, connection, reversed,
			[]string{"tags_csv"}, [][]string{{"β"}, {""}, {"a"}})
	})

	t.Run("fixed String provenance survives fillnull makemv and mvexpand", func(t *testing.T) {
		for _, test := range []struct {
			source string
			marker string
		}{
			{
				source: `index=spl-v03 event_id="v03-fixed-binary-present"` +
					` | eval copied=_raw | makemv delim="," copied | table event_id`,
				marker: UnsupportedMakeMVValueMarker,
			},
			{
				source: `index=spl-v03 event_id="v03-fixed-binary-present"` +
					` | eval copied=_raw | mvexpand copied | table event_id`,
				marker: UnsupportedMVExpandValueMarker,
			},
			{
				source: `index=spl-v03 event_id="v03-fixed-binary-present"` +
					` | eval copied=_raw | fillnull value="safe" copied` +
					` | makemv delim="," copied | table event_id`,
				marker: UnsupportedMakeMVValueMarker,
			},
		} {
			pipelineAssertAtomicExecutionFailure(
				t,
				queryContext,
				connection,
				compile(test.source),
				test.marker,
			)
		}

		binary := []byte("binary,valid-utf8")
		for _, test := range []struct {
			source  string
			payload []byte
		}{
			{
				source: `index=spl-v03 event_id="v03-fixed-binary-present"` +
					` | eval copied=_raw | fillnull value="safe" copied | table event_id copied`,
				payload: binary,
			},
			{
				source: `index=spl-v03 event_id="v03-fixed-binary-present"` +
					` | strcat _raw ":" copied | table event_id copied`,
				payload: append(append([]byte(nil), binary...), ':'),
			},
			{
				source: `index=spl-v03 event_id="v03-fixed-binary-present"` +
					` | strcat allrequired=true _raw ":" copied | table event_id copied`,
				payload: append(append([]byte(nil), binary...), ':'),
			},
			{
				source: `index=spl-v03 event_id="v03-fixed-binary-present"` +
					` | eval copied=_raw` +
					` | strcat allrequired=true missing ":" copied | table event_id copied`,
				payload: binary,
			},
		} {
			pipelineAssertTaggedScalar(
				t,
				queryContext,
				connection,
				compile(test.source),
				"copied",
				"bytes/v1",
				base64.RawStdEncoding.EncodeToString(test.payload),
			)
		}
		textFromNullBinary := compile(
			`index=spl-v03 event_id="v03-fixed-binary-present"` +
				` | eval maybe=if(event_id="never",_raw,null)` +
				` | strcat maybe ":" copied | table event_id copied`,
		)
		pipelineAssertJSONRows(
			t,
			queryContext,
			connection,
			textFromNullBinary,
			[]string{"event_id", "copied"},
			[][]string{{"v03-fixed-binary-present", ":"}},
		)

		filled := compile(
			`index=spl-v03 event_id="v03-fixed-binary-null-fill"` +
				` | eval copied=if(event_id="v03-fixed-binary-present",_raw,null)` +
				` | fillnull value="safe" copied | makemv delim="," copied` +
				` | mvexpand copied | table event_id copied`,
		)
		pipelineAssertJSONRows(
			t,
			queryContext,
			connection,
			filled,
			[]string{"event_id", "copied"},
			[][]string{{"v03-fixed-binary-null-fill", "safe"}},
		)
	})

	t.Run("multivalue hard ceilings fail atomically before downstream head", func(t *testing.T) {
		for _, test := range []struct {
			source string
			marker string
		}{
			{source: `index=spl-v03 source="v03-makemv-bomb" | makemv delim="," bomb_csv | head 1 | table event_id`, marker: MakeMVRowMembersLimitMarker},
			{source: `index=spl-v03 source="v03-makemv-separator-bomb" | makemv delim="," allowempty=true bomb_csv | head 1 | table event_id`, marker: MakeMVRowMembersLimitMarker},
			{source: `index=spl-v03 source="v03-makemv-retained" | strcat _raw _raw bomb_csv | makemv delim="," bomb_csv | head 1 | table event_id`, marker: MakeMVRowBytesLimitMarker},
			{source: `index=spl-v03 source="v03-makemv-number" | makemv delim="," bomb_csv | head 1 | table event_id`, marker: UnsupportedMakeMVValueMarker},
			{source: `index=spl-v03 source="v03-makemv-array" | makemv delim="," bomb_csv | head 1 | table event_id`, marker: UnsupportedMakeMVValueMarker},
			{source: `index=spl-v03 source="v03-makemv-result-members" | makemv delim="," bomb_csv | head 1 | table event_id`, marker: MakeMVResultMembersLimitMarker},
			{source: `index=spl-v03 source="v03-makemv-result-bytes" | makemv delim="," bomb_csv | head 1 | table event_id`, marker: MakeMVResultBytesLimitMarker},
			{source: `index=spl-v03 source="v03-makemv-retained" | table event_id _raw bomb_csv | makemv delim="," bomb_csv | head 1`, marker: MakeMVRetainedBytesLimitMarker},
			{source: `index=spl-v03 source="v03-mvexpand-bomb" | table event_id bomb_mv | mvexpand bomb_mv | head 1`, marker: MVExpandRowMembersLimitMarker},
			{source: `index=spl-v03 source="v03-mvexpand-bomb" | table event_id bomb_mv | mvexpand bomb_mv limit=0 | head 1`, marker: MVExpandRowMembersLimitMarker},
			{source: `index=spl-v03 source="v03-mvexpand-bomb" | table event_id bomb_mv | mvexpand bomb_mv limit=1 | head 1`, marker: MVExpandRowMembersLimitMarker},
			{source: `index=spl-v03 source="v03-mvexpand-repeat" | table event_id tags zones | mvexpand tags | mvexpand zones | head 1`, marker: MVExpandStageRowsLimitMarker},
			{source: `index=spl-v03 source="v03-mvexpand-query-rows" | table event_id tags zones | mvexpand tags | mvexpand zones | head 1`, marker: MVExpandQueryRowsLimitMarker},
			{source: `index=spl-v03 source="v03-mvexpand-retained" | table event_id _raw retained_tags | mvexpand retained_tags | head 1`, marker: MVExpandRetainedBytesLimitMarker},
			{source: `index=spl-v03 source="v03-mvexpand-object" | mvexpand bad_mv | head 1 | table event_id`, marker: UnsupportedMVExpandValueMarker},
			{source: `index=spl-v03 source="v03-mvexpand-nested" | mvexpand bad_mv | head 1 | table event_id`, marker: UnsupportedMVExpandValueMarker},
			{source: `index=spl-v03 source="v03-mvexpand-bytes" | mvexpand mv_scalar | head 1 | table event_id`, marker: UnsupportedMVExpandValueMarker},
			{source: `index=spl-v03 source="v03-mvexpand-duration" | mvexpand mv_scalar | head 1 | table event_id`, marker: UnsupportedMVExpandValueMarker},
			{source: `index=spl-v03 event_id="v03-mvexpand-malformed-envelope" | mvexpand mv_scalar | head 1 | table event_id`, marker: UnsupportedMVExpandValueMarker},
		} {
			t.Run(test.source, func(t *testing.T) {
				pipelineAssertAtomicExecutionFailure(t, queryContext, connection, compile(test.source), test.marker)
			})
		}

		for _, test := range []struct {
			source string
			want   [][]string
		}{
			{
				source: `v03-mvexpand-mixed-number`,
				want: [][]string{
					{"v03-mvexpand-mixed-number", "x"},
					{"v03-mvexpand-mixed-number", "7"},
				},
			},
			{
				source: `v03-mvexpand-mixed-bool`,
				want: [][]string{
					{"v03-mvexpand-mixed-bool", "x"},
					{"v03-mvexpand-mixed-bool", "true"},
				},
			},
		} {
			nativeMixed := compile(`index=spl-v03 source="` + test.source +
				`" | mvexpand bad_mv | table event_id bad_mv`)
			pipelineAssertJSONRows(
				t,
				queryContext,
				connection,
				nativeMixed,
				[]string{"event_id", "bad_mv"},
				test.want,
			)
		}

		limited := compile(`index=spl-v03 source="v03-mvexpand-boundary"` +
			` | mvexpand boundary_mv limit=1 | table event_id boundary_mv`)
		pipelineAssertJSONRows(t, queryContext, connection, limited,
			[]string{"event_id", "boundary_mv"}, [][]string{{"v03-mvexpand-boundary", "member-0000"}})

		exactQueryRows := compile(`index=spl-v03 source="v03-mvexpand-query-exact"` +
			` | sort 0 +event_id | mvexpand tags | mvexpand zones | head 1` +
			` | table event_id tags zones`)
		pipelineAssertJSONRows(t, queryContext, connection, exactQueryRows,
			[]string{"event_id", "tags", "zones"},
			[][]string{{
				"v03-mvexpand-query-exact-00",
				"exact-tag-00-0000",
				"exact-zone-00-0000",
			}})
		pipelineAssertAtomicExecutionFailure(
			t,
			queryContext,
			connection,
			compile(`index=spl-v03 source="v03-mvexpand-query-overflow"`+
				` | sort 0 +event_id | mvexpand tags | mvexpand zones | head 1`+
				` | table event_id`),
			MVExpandQueryRowsLimitMarker,
		)
	})

	t.Run("delta input ceiling survives every downstream consumer", func(t *testing.T) {
		// The inclusive boundary must remain usable through a transforming
		// consumer. Exclude the single sentinel row before delta so its complete
		// ordered input is exactly the normative 10,000-row maximum.
		exactBoundary := compile(
			`index=spl-v03 source="v03-delta-boundary" ` +
				`event_id!="v03-delta-boundary-10000"` +
				` | delta delta_n AS step | stats count`,
		)
		pipelineAssertJSONRows(
			t,
			queryContext,
			connection,
			exactBoundary,
			[]string{"count"},
			[][]string{{"10000"}},
		)

		for _, source := range []string{
			`index=spl-v03 source="v03-delta-boundary" | delta delta_n AS step | where event_id="never"`,
			`index=spl-v03 source="v03-delta-boundary" | delta delta_n AS step | fields event_id`,
			`index=spl-v03 source="v03-delta-boundary" | delta delta_n AS step | table event_id`,
			`index=spl-v03 source="v03-delta-boundary" | delta delta_n AS step | head 1`,
			`index=spl-v03 source="v03-delta-boundary" | delta delta_n AS step | stats count`,
		} {
			t.Run(source, func(t *testing.T) {
				pipelineAssertAtomicExecutionFailure(
					t, queryContext, connection, compile(source), StreamStatsInputLimitMarker,
				)
			})
		}
		pipelineAssertAtomicExecutionFailure(
			t,
			queryContext,
			connection,
			compile(`index=spl-v03 event_id="v03-mvexpand-malformed-envelope"`+
				` | delta mv_scalar AS step | fields event_id`),
			UnsupportedExpressionValueMarker,
		)
		// A transforming aggregate must not erase delta's deferred malformed-value
		// validation merely because neither the source nor output stays public.
		pipelineAssertAtomicExecutionFailure(
			t,
			queryContext,
			connection,
			compile(`index=spl-v03 event_id="v03-mvexpand-malformed-envelope"`+
				` | delta mv_scalar AS step | stats count`),
			UnsupportedExpressionValueMarker,
		)
	})

	t.Run("addtotals malformed-value guard survives downstream consumers", func(t *testing.T) {
		for _, source := range []string{
			`index=spl-v03 event_id="v03-mvexpand-malformed-envelope" | addtotals fieldname=total mv_scalar | where event_id="never"`,
			`index=spl-v03 event_id="v03-mvexpand-malformed-envelope" | addtotals fieldname=total mv_scalar | fields event_id`,
			`index=spl-v03 event_id="v03-mvexpand-malformed-envelope" | addtotals fieldname=total mv_scalar | table event_id`,
			`index=spl-v03 event_id="v03-mvexpand-malformed-envelope" | addtotals fieldname=total mv_scalar | head 1`,
			`index=spl-v03 event_id="v03-mvexpand-malformed-envelope" | addtotals fieldname=total mv_scalar | stats count`,
			`index=spl-v03 event_id="v03-mvexpand-malformed-envelope" | addtotals fieldname=mv_scalar mv_scalar | fields event_id`,
			`index=spl-v03 event_id="v03-mvexpand-malformed-envelope" | addtotals fieldname=mv_scalar mv_scalar | table event_id`,
			`index=spl-v03 event_id="v03-mvexpand-malformed-envelope" | addtotals fieldname=mv_scalar mv_scalar | where event_id="never"`,
			`index=spl-v03 event_id="v03-addtotals-default-malformed-envelope" | addtotals Total | fields event_id`,
		} {
			t.Run(source, func(t *testing.T) {
				pipelineAssertAtomicExecutionFailure(
					t, queryContext, connection, compile(source), UnsupportedExpressionValueMarker,
				)
			})
		}
	})

	t.Run("downstream filtering and projection cannot hide an earlier expansion breach", func(t *testing.T) {
		for _, source := range []string{
			`index=spl-v03 source="v03-mvexpand-bomb" | mvexpand bomb_mv | where event_id="never"`,
			`index=spl-v03 source="v03-mvexpand-bomb" | mvexpand bomb_mv | fields event_id`,
			`index=spl-v03 source="v03-mvexpand-bomb" | mvexpand bomb_mv | table event_id`,
			`index=spl-v03 source="v03-mvexpand-bomb" | mvexpand bomb_mv | head 1`,
		} {
			t.Run(source, func(t *testing.T) {
				pipelineAssertAtomicExecutionFailure(
					t, queryContext, connection, compile(source), MVExpandRowMembersLimitMarker,
				)
			})
		}
	})

	t.Run("cancellation reaches an admitted repeated expansion", func(t *testing.T) {
		compiled := compile(`index=spl-v03 source="v03-mvexpand-cancel"` +
			` | mvexpand tags | mvexpand zones | table event_id tags zones`)
		queryCtx, queryCancel := context.WithCancel(queryContext)
		rows, queryErr := connection.Query(queryCtx, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			queryCancel()
			t.Fatalf("start cancelable expansion: %v", queryErr)
		}
		queryCancel()
		for rows.Next() {
			t.Fatal("canceled expansion published a row")
		}
		iterationErr := rows.Err()
		_ = rows.Close()
		if iterationErr == nil ||
			(!errors.Is(iterationErr, context.Canceled) && !strings.Contains(strings.ToLower(iterationErr.Error()), "cancel")) {
			t.Fatalf("canceled expansion error = %v, want context cancellation", iterationErr)
		}
	})
}

func pipelineStoreIntegrationFixtureBatches(
	ctx context.Context,
	t *testing.T,
	store *Store,
	indexTime time.Time,
	events []*ingest.StoredEvent,
) {
	t.Helper()
	const batchEvents = 4
	for start, batchNumber := 0, uint64(0); start < len(events); start, batchNumber = start+batchEvents, batchNumber+1 {
		end := min(start+batchEvents, len(events))
		batch := events[start:end]
		batchID := fmt.Sprintf("spl-v03-adversarial-resource-%03d", batchNumber)
		for _, event := range batch {
			event.BatchID = batchID
		}
		if _, err := store.Store(ctx, ingest.StoreBatch{
			TenantID:           "tenant",
			CollectorID:        "collector",
			BatchID:            batchID,
			BatchSequence:      1_000 + batchNumber,
			OriginalEventCount: uint32(len(batch)),
			SourceBatchSHA256:  testSourceBatchDigest(batchID),
			ReceivedAt:         indexTime,
			Events:             batch,
		}); err != nil {
			t.Fatalf("store pipeline resource batch %s: %v", batchID, err)
		}
	}
}

func pipelineStoreDeltaBoundaryFixtureBatches(
	ctx context.Context,
	t *testing.T,
	store *Store,
	indexTime time.Time,
	events []*ingest.StoredEvent,
) {
	t.Helper()
	const batchEvents = 1_000
	for start := 0; start < len(events); start += batchEvents {
		end := min(start+batchEvents, len(events))
		batch := events[start:end]
		batchID := fmt.Sprintf("spl-v03-delta-boundary-%03d", start/batchEvents)
		for _, event := range batch {
			event.BatchID = batchID
		}
		if _, err := store.Store(ctx, ingest.StoreBatch{
			TenantID:           "tenant",
			CollectorID:        "collector",
			BatchID:            batchID,
			BatchSequence:      10_000 + uint64(start/batchEvents),
			OriginalEventCount: uint32(len(batch)),
			SourceBatchSHA256:  testSourceBatchDigest(batchID),
			ReceivedAt:         indexTime,
			Events:             batch,
		}); err != nil {
			t.Fatalf("store pipeline delta boundary batch %s: %v", batchID, err)
		}
	}
}

func pipelineCompileIntegrationSPLWithJobID(
	t *testing.T,
	source string,
	cutoff time.Time,
	visibilityCutoff uint64,
	jobID string,
) CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse addinfo integration SPL %q: %v", source, err)
	}
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"spl-v03"},
		SearchJobID:       jobID,
		Earliest:          time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, time.July, 22, 0, 0, 0, 0, time.UTC),
		SearchStart:       cutoff.Add(-time.Second),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   cutoff,
		VisibilityCutoff:  &visibilityCutoff,
	})
	if err != nil {
		t.Fatalf("build addinfo integration SPL %q: %v", source, err)
	}
	compiled, err := (Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("compile addinfo integration SPL %q: %v", source, err)
	}
	return compiled
}

func pipelineAssertIDs(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
	want ...string,
) {
	t.Helper()
	rows, err := connection.Query(ctx, compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatalf("execute IDs: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			t.Fatalf("scan ID: %v", scanErr)
		}
		got = append(got, id)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		t.Fatalf("iterate IDs: %v", rowsErr)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
}

func pipelineAssertJSONRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
	fields []string,
	want [][]string,
) {
	t.Helper()
	decoded := pipelineJSONRows(t, ctx, connection, compiled, fields)
	got := make([][]string, len(decoded))
	for rowIndex, row := range decoded {
		got[rowIndex] = make([]string, len(row))
		for fieldIndex, value := range row {
			got[rowIndex][fieldIndex] = pipelineJSONText(value)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JSON rows = %#v, want %#v\nSQL: %s\nargs: %#v", got, want, compiled.SQL, compiled.Args)
	}
}

func pipelineAssertTaggedScalar(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
	field, wantTag, wantPayload string,
) {
	t.Helper()
	rows := pipelineJSONRows(t, ctx, connection, compiled, []string{"event_id", field})
	if len(rows) != 1 || len(rows[0]) != 2 {
		t.Fatalf("tagged scalar rows = %#v, want one two-column row", rows)
	}
	dynamic, ok := rows[0][1].(chcol.Dynamic)
	if !ok {
		t.Fatalf("tagged scalar native value = %T, want chcol.Dynamic", rows[0][1])
	}
	if dynamic.Type() != "Map(String, String)" {
		t.Fatalf("tagged scalar physical type = %q, want Map(String, String)", dynamic.Type())
	}
	envelope, ok := dynamic.Any().(map[string]string)
	if !ok {
		t.Fatalf("tagged scalar payload = %T, want map[string]string", dynamic.Any())
	}
	want := map[string]string{
		"\x00open_splunk_type":  wantTag,
		"\x00open_splunk_value": wantPayload,
	}
	if !reflect.DeepEqual(envelope, want) {
		t.Fatalf("tagged scalar envelope = %#v, want %#v", envelope, want)
	}
}

func pipelineJSONRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
	fields []string,
) [][]any {
	t.Helper()
	// Execute the compiled statement itself. Wrapping an ordered statement in
	// an outer SELECT without its own ORDER BY makes row order an optimizer
	// accident, which is unusable as an oracle for reverse/accum/delta/mvexpand.
	rows, err := connection.Query(ctx, compiled.SQL, compiled.Args...)
	if err != nil {
		t.Fatalf("execute native rows: %v\nSQL: %s\nargs: %#v", err, compiled.SQL, compiled.Args)
	}
	defer rows.Close()
	columns := rows.Columns()
	columnTypes := rows.ColumnTypes()
	if len(columns) != len(columnTypes) {
		t.Fatalf("native result columns/types = %d/%d", len(columns), len(columnTypes))
	}
	requested := make([]int, len(fields))
	for fieldIndex, field := range fields {
		requested[fieldIndex] = -1
		for columnIndex, column := range columns {
			if column != field {
				continue
			}
			if requested[fieldIndex] >= 0 {
				t.Fatalf("native result duplicates requested field %q: %v", field, columns)
			}
			requested[fieldIndex] = columnIndex
		}
		if requested[fieldIndex] < 0 {
			t.Fatalf("native result omits requested field %q: %v", field, columns)
		}
	}
	var result [][]any
	for rows.Next() {
		destinations := make([]any, len(columnTypes))
		for index, columnType := range columnTypes {
			baseType := strings.TrimSpace(columnType.DatabaseTypeName())
			if strings.HasPrefix(baseType, "Dynamic") || strings.HasPrefix(baseType, "Variant") {
				destinations[index] = new(chcol.Dynamic)
				continue
			}
			if columnType.ScanType() == nil {
				t.Fatalf("native column %q has no scan type", columns[index])
			}
			destinations[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if scanErr := rows.Scan(destinations...); scanErr != nil {
			t.Fatalf("scan native row: %v", scanErr)
		}
		decoded := make([]any, len(requested))
		for fieldIndex, columnIndex := range requested {
			decoded[fieldIndex] = pipelineScannedNativeValue(destinations[columnIndex])
		}
		result = append(result, decoded)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		t.Fatalf("iterate native rows: %v", rowsErr)
	}
	return result
}

func pipelineScannedNativeValue(destination any) any {
	value := reflect.ValueOf(destination)
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return nil
	}
	return value.Interface()
}

func pipelineDynamicStringMap(value any) (map[string]any, bool) {
	switch value := value.(type) {
	case chcol.Dynamic:
		if value.Nil() {
			return nil, false
		}
		return pipelineDynamicStringMap(value.Any())
	case *chcol.Dynamic:
		if value == nil || value.Nil() {
			return nil, false
		}
		return pipelineDynamicStringMap(value.Any())
	case map[string]any:
		return value, true
	case map[string]chcol.Dynamic:
		result := make(map[string]any, len(value))
		for name, member := range value {
			result[name] = member
		}
		return result, true
	default:
		return nil, false
	}
}

func pipelineJSONText(value any) string {
	switch value := value.(type) {
	case nil:
		return "<null>"
	case chcol.Dynamic:
		if value.Nil() {
			return "<null>"
		}
		return pipelineJSONText(value.Any())
	case *chcol.Dynamic:
		if value == nil || value.Nil() {
			return "<null>"
		}
		return pipelineJSONText(value.Any())
	case string:
		return value
	case json.Number:
		return value.String()
	case bool:
		if value {
			return "true"
		}
		return "false"
	case float32:
		return strconv.FormatFloat(float64(value), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	case []any:
		parts := make([]string, len(value))
		for index, member := range value {
			parts[index] = pipelineJSONText(member)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case []string:
		return "[" + strings.Join(value, ",") + "]"
	case []chcol.Dynamic:
		parts := make([]string, len(value))
		for index, member := range value {
			parts[index] = pipelineJSONText(member)
		}
		return "[" + strings.Join(parts, ",") + "]"
	default:
		reflected := reflect.ValueOf(value)
		if reflected.IsValid() && (reflected.Kind() == reflect.Array || reflected.Kind() == reflect.Slice) {
			parts := make([]string, reflected.Len())
			for index := 0; index < reflected.Len(); index++ {
				parts[index] = pipelineJSONText(reflected.Index(index).Interface())
			}
			return "[" + strings.Join(parts, ",") + "]"
		}
		return fmt.Sprint(value)
	}
}

func pipelineAssertAtomicExecutionFailure(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	compiled CompiledQuery,
	expectedMarker string,
) {
	t.Helper()
	if !compiled.RequiresAtomicResult() {
		t.Fatal("resource-sensitive pipeline query did not retain atomic-result evidence")
	}
	queryErr := executeCompiledExpectingNoRows(ctx, connection, compiled)
	var exception *clickhousedriver.Exception
	if !errors.As(queryErr, &exception) || exception.Code != 395 ||
		!strings.Contains(exception.Message, expectedMarker) {
		t.Fatalf("atomic query error = %v, want ClickHouse code 395 containing %q\nSQL: %s", queryErr, expectedMarker, compiled.SQL)
	}
}
