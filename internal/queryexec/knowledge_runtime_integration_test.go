package queryexec

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

const (
	knowledgeRuntimeExpectedClickHouseVersion = "26.3.17.4"
	knowledgeRuntimeOverflowAliasCount        = 5
)

func TestKnowledgeRuntimeIntegrationProgramsAreCanonical(t *testing.T) {
	program := knowledgeRuntimeProgram(t)
	if program.ObjectCount() != 13 || len(program.Aliases()) != 10 ||
		len(program.OperatorKinds()) != 4 {
		t.Fatalf(
			"runtime program authority = objects %d aliases %d operators %v",
			program.ObjectCount(),
			len(program.Aliases()),
			program.OperatorKinds(),
		)
	}
	overflow := knowledgeRuntimeOverflowProgram(t)
	if overflow.ObjectCount() != knowledgeRuntimeOverflowAliasCount ||
		len(overflow.Aliases()) != knowledgeRuntimeOverflowAliasCount ||
		len(overflow.OperatorKinds()) != 1 {
		t.Fatalf(
			"overflow program authority = objects %d aliases %d operators %v",
			overflow.ObjectCount(),
			len(overflow.Aliases()),
			overflow.OperatorKinds(),
		)
	}
	for index, alias := range overflow.Aliases() {
		if !alias.Selector().IsUnrestricted() {
			t.Fatalf("overflow alias %d selector is restricted", index)
		}
	}
	_ = knowledgeRuntimeOverflowSourceBytes(t)

	fixtures := knowledgeRuntimeFixtures(
		"knowledge-tenant",
		"knowledge-runtime",
		"selector-runtime",
		time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC),
		time.Date(2026, time.August, 8, 12, 10, 0, 0, time.UTC),
	)
	roles := make(map[string]int)
	missingPreserveDestinations := 0
	for _, fixture := range fixtures {
		roles[fixture.role]++
		if !fixture.preserveDestination {
			missingPreserveDestinations++
		}
	}
	if len(fixtures) != 9 || roles[knowledgeRuntimeRoleMatrix] != 4 ||
		roles[knowledgeRuntimeRoleIndexControl] != 1 ||
		roles[knowledgeRuntimeRoleHostControl] != 1 ||
		roles[knowledgeRuntimeRoleSourceControl] != 1 ||
		roles[knowledgeRuntimeRoleSourcetypeControl] != 1 ||
		roles[knowledgeRuntimeRoleTenantDecoy] != 1 || missingPreserveDestinations != 1 {
		t.Fatalf(
			"runtime fixture roles = %#v, missing preserve destinations = %d",
			roles,
			missingPreserveDestinations,
		)
	}

	t.Run("private limit markers are classified and redacted", func(t *testing.T) {
		for _, marker := range knowledgeRuntimePrivateLimitMarkers() {
			classified := classifyQueryError(context.Background(), &clickhousedriver.Exception{
				Code:    395,
				Name:    "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
				Message: "private-prefix " + marker + " private-suffix",
			})
			if !errors.Is(classified, searchjobs.ErrExecutionLimit) ||
				strings.Contains(classified.Error(), marker) ||
				strings.Contains(classified.Error(), "private-") {
				t.Fatalf("private marker %q classification = %v", marker, classified)
			}
		}
	})
}

// TestKnowledgeCompilerAndExecutorMatrixAgainstClickHouse is the production-
// path KO-1C matrix. Unlike the table-free relation test in clickhouse, this
// test crosses the public Compiler surface, the migrated event table, every
// derived execution seal, and the query executor's result decoders. It keeps
// one digest-pinned, cache-required container for the complete bounded matrix.
// The matrix directly covers String, signed, unsigned, floating, Bytes, null,
// list, and object alias values. Bool, decimal, timestamp, and duration alias
// byte-size boundaries remain separate gate evidence and are not claimed here.
//
// Run only this test with:
//
//	OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 go test \
//	  -tags=open_splunk_knowledge_runtime_acceptance -v ./internal/queryexec \
//	  -run '^TestKnowledgeCompilerAndExecutorMatrixAgainstClickHouse$' \
//	  -count=1 -timeout=90s
func TestKnowledgeCompilerAndExecutorMatrixAgainstClickHouse(t *testing.T) {
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

	// Compile before starting the container so an invalid matrix fails without
	// consuming Docker startup time. The explicit acceptance build tag crosses
	// the compiler-only boundary inside go test; snapshot finalization remains
	// independently closed.
	const (
		indexName         = "knowledge-runtime"
		selectorIndexName = "selector-runtime"
		tenantID          = "knowledge-tenant"
	)
	base := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	indexTime := base.Add(10 * time.Minute)
	earliest := base
	latest := base.Add(2 * time.Minute)
	program := knowledgeRuntimeProgram(t)
	matrix := compileKnowledgeRuntimeMatrix(
		t,
		program,
		tenantID,
		indexName,
		selectorIndexName,
		base,
		indexTime,
		earliest,
		latest,
	)

	overallContext, cancelOverall := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancelOverall()
	startupContext, cancelStartup := context.WithTimeout(overallContext, 20*time.Second)
	defer cancelStartup()
	if err := exec.CommandContext(startupContext, "docker", "image", "inspect", image).Run(); err != nil {
		t.Fatalf("digest-pinned ClickHouse image must be cached before this bounded test runs: %v", err)
	}
	container, err := testsupport.StartClickHouse(startupContext, image)
	if err != nil {
		t.Fatalf("start pinned ClickHouse fixture: %v", err)
	}
	t.Cleanup(func() {
		cleanupContext, cancelCleanup := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancelCleanup()
		if closeErr := container.Close(cleanupContext); closeErr != nil {
			t.Errorf("close knowledge runtime ClickHouse fixture: %v", closeErr)
		}
	})
	if container.Image != image {
		t.Fatalf("started ClickHouse image = %q, want %q", container.Image, image)
	}
	queryIntegrationMigrate(t, overallContext, container.Name, container.Password)

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Protocol: clickhousedriver.Native,
		Addr:     []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("open knowledge runtime ClickHouse connection: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("close knowledge runtime ClickHouse connection: %v", closeErr)
		}
	})
	if err := connection.Ping(overallContext); err != nil {
		t.Fatalf("ping knowledge runtime ClickHouse fixture: %v", err)
	}
	var exactVersion uint8
	if err := connection.QueryRow(
		overallContext,
		"SELECT version() = '"+knowledgeRuntimeExpectedClickHouseVersion+"'",
	).Scan(&exactVersion); err != nil {
		t.Fatalf("verify exact ClickHouse version: %v", err)
	}
	if exactVersion != 1 {
		t.Fatalf("ClickHouse version is not exactly %s", knowledgeRuntimeExpectedClickHouseVersion)
	}
	insertKnowledgeRuntimeEvents(
		t,
		overallContext,
		connection,
		knowledgeRuntimeFixtures(
			tenantID,
			indexName,
			selectorIndexName,
			base,
			indexTime,
		),
	)
	insertKnowledgeRuntimeOverflowEvent(
		t,
		overallContext,
		connection,
		tenantID,
		indexName+"-overflow",
		base,
		indexTime,
	)

	executor, err := New(connection, Config{
		ReadAdmission:    indexread.UnfencedAdmission{},
		MaxExecutionTime: 5 * time.Second,
		MaxMemoryBytes:   256 << 20,
		MaxRowsToRead:    10_000,
		MaxBytesToRead:   64 << 20,
		MaxResultRows:    1_000,
		MaxResultBytes:   8 << 20,
		MaxRowsToGroupBy: 1_000,
		MaxThreads:       1,
	})
	if err != nil {
		t.Fatalf("create knowledge runtime query executor: %v", err)
	}

	t.Run("ordinary authored suffix and container decoding", func(t *testing.T) {
		sink := &fakeSink{}
		if err := executor.Execute(overallContext, matrix.ordinary, sink); err != nil {
			t.Fatalf("execute ordinary knowledge query: %v", err)
		}
		knowledgeRuntimeAssertOrdinary(t, sink)
	})

	t.Run("selector dimensions independently reject controls", func(t *testing.T) {
		sink := &fakeSink{}
		if err := executor.Execute(overallContext, matrix.controls, sink); err != nil {
			t.Fatalf("execute knowledge selector controls: %v", err)
		}
		knowledgeRuntimeAssertSelectorControls(t, sink)
	})

	t.Run("chart", func(t *testing.T) {
		sink := &fakeSink{}
		if err := executor.Execute(overallContext, matrix.chart, sink); err != nil {
			t.Fatalf("execute knowledge chart: %v", err)
		}
		knowledgeRuntimeAssertChart(t, sink)
	})

	t.Run("timechart", func(t *testing.T) {
		sink := &fakeSink{}
		if err := executor.Execute(overallContext, matrix.timechart, sink); err != nil {
			t.Fatalf("execute knowledge timechart: %v", err)
		}
		knowledgeRuntimeAssertTimechart(t, sink, base)
	})

	t.Run("stats", func(t *testing.T) {
		sink := &fakeSink{}
		if err := executor.Execute(overallContext, matrix.stats, sink); err != nil {
			t.Fatalf("execute knowledge stats: %v", err)
		}
		knowledgeRuntimeAssertStats(t, sink)
	})

	t.Run("timeline", func(t *testing.T) {
		buckets, err := executor.ExecuteTimeline(overallContext, matrix.timeline)
		if err != nil {
			t.Fatalf("execute knowledge timeline: %v", err)
		}
		if len(buckets) != 2 || !buckets[0].AlignedStart.Equal(base) || buckets[0].Count != 2 ||
			!buckets[1].AlignedStart.Equal(base.Add(time.Minute)) || buckets[1].Count != 2 {
			t.Fatalf("knowledge timeline buckets = %#v", buckets)
		}
	})

	t.Run("field catalog", func(t *testing.T) {
		catalog, err := executor.ExecuteFieldCatalog(overallContext, matrix.catalog)
		if err != nil {
			t.Fatalf("execute knowledge field catalog: %v", err)
		}
		knowledgeRuntimeAssertCatalog(t, catalog)
	})

	t.Run("field summary", func(t *testing.T) {
		summary, err := executor.ExecuteFieldSummary(overallContext, matrix.summary)
		if err != nil {
			t.Fatalf("execute knowledge field summary: %v", err)
		}
		knowledgeRuntimeAssertSummary(t, summary)
	})

	t.Run("field suggestions", func(t *testing.T) {
		suggestions, err := executor.ExecuteFieldSuggestions(overallContext, matrix.suggestions)
		if err != nil {
			t.Fatalf("execute knowledge field suggestions: %v", err)
		}
		if suggestions.Truncated || !slices.Equal(suggestions.FieldNames, []string{
			"payload_copy",
			"payload_copy.child",
			"payload_copy.nothing",
		}) {
			t.Fatalf("knowledge field suggestions = %#v", suggestions)
		}
	})

	t.Run("runtime guard failure is atomic", func(t *testing.T) {
		sink := &fakeSink{}
		err := executor.Execute(overallContext, matrix.overflow, sink)
		wantError := searchjobs.ErrExecutionLimit.Error() +
			": knowledge alias copy bytes exceeded the per-event limit"
		if !errors.Is(err, searchjobs.ErrExecutionLimit) || err.Error() != wantError ||
			sink.setCalls != 0 || len(sink.rows) != 0 {
			t.Fatalf("knowledge guard atomicity = error %v schema calls %d rows %#v", err, sink.setCalls, sink.rows)
		}
		for _, marker := range knowledgeRuntimePrivateLimitMarkers() {
			if strings.Contains(err.Error(), marker) {
				t.Fatalf("knowledge guard leaked private marker: %v", err)
			}
		}
	})
}

type compiledKnowledgeRuntimeMatrix struct {
	ordinary    clickhouse.CompiledQuery
	controls    clickhouse.CompiledQuery
	chart       clickhouse.CompiledQuery
	timechart   clickhouse.CompiledQuery
	stats       clickhouse.CompiledQuery
	timeline    clickhouse.CompiledTimeline
	catalog     clickhouse.CompiledFieldCatalog
	summary     clickhouse.CompiledFieldSummary
	suggestions clickhouse.CompiledFieldSuggestions
	overflow    clickhouse.CompiledQuery
}

type knowledgeRuntimeMatrixPlans struct {
	ordinary       *plan.Query
	controls       *plan.Query
	chart          *plan.Query
	timechart      *plan.Query
	stats          *plan.Query
	timeline       *plan.Query
	analysis       *plan.Query
	overflow       *plan.Query
	timelineSpec   clickhouse.TimelineSpec
	catalogSpec    clickhouse.FieldCatalogSpec
	summarySpec    clickhouse.FieldSummarySpec
	suggestionSpec clickhouse.FieldSuggestionSpec
}

func buildKnowledgeRuntimeMatrixPlans(
	t *testing.T,
	program knowledgeprogram.Program,
	tenantID string,
	indexName string,
	selectorIndexName string,
	base time.Time,
	indexTime time.Time,
	earliest time.Time,
	latest time.Time,
) knowledgeRuntimeMatrixPlans {
	t.Helper()
	build := func(
		source string,
		admitted knowledgeprogram.Program,
		indexes ...string,
	) *plan.Query {
		t.Helper()
		return knowledgeRuntimePlan(
			t,
			source,
			admitted,
			tenantID,
			indexes,
			indexTime,
			earliest,
			latest,
		)
	}

	overflowIndex := indexName + "-overflow"
	return knowledgeRuntimeMatrixPlans{
		ordinary: build(
			`index=`+indexName+` service=matrix`+
				` | where isnotnull(regex_value)`+
				` | rex field=_raw "(?<authored_rex>alpha|beta)"`+
				` | spath input=_raw output=authored_spath path=nested.value`+
				` | eval authored_value=upper(calculated_value)`+
				` | eventstats count AS cohort | sort 0 -event_id`+
				` | streamstats count AS ordinal | head 3`+
				` | table event_id regex_value json_value calculated_value payload_copy numbers_copy`+
				` signed_copy unsigned_copy float_copy bytes_copy null_copy empty_list_copy`+
				` preserved_value replaced_value authored_rex authored_spath authored_value cohort ordinal`,
			program,
			indexName,
		),
		controls: build(
			`index=`+indexName+` OR index=`+selectorIndexName+
				` | search service=selector-control | sort 0 +event_id`+
				` | table event_id regex_value json_value calculated_value signed_copy`,
			program,
			indexName,
			selectorIndexName,
		),
		chart: build(
			`index=`+indexName+` service=matrix | eventstats count AS cohort`+
				` | chart count OVER calculated_value BY regex_value`,
			program,
			indexName,
		),
		timechart: build(
			`index=`+indexName+` service=matrix | timechart span=1m count BY regex_value`,
			program,
			indexName,
		),
		stats: build(
			`index=`+indexName+` service=matrix`+
				` | stats count AS total BY regex_value | sort 0 +regex_value`,
			program,
			indexName,
		),
		timeline: build(
			`index=`+indexName+` service=matrix | where isnotnull(regex_value)`,
			program,
			indexName,
		),
		analysis: build(
			`index=`+indexName+` service=matrix | eventstats count AS cohort`,
			program,
			indexName,
		),
		overflow: build(
			`index=`+overflowIndex+` | table event_id`,
			knowledgeRuntimeOverflowProgram(t),
			overflowIndex,
		),
		timelineSpec: clickhouse.TimelineSpec{
			FirstBucket: base,
			SpanSeconds: 60,
			BucketCount: 2,
			Earliest:    earliest,
			Latest:      latest,
		},
		catalogSpec: clickhouse.FieldCatalogSpec{MaximumFields: 64},
		summarySpec: clickhouse.FieldSummarySpec{
			FieldName:             "calculated_value",
			MaximumValues:         8,
			MaximumDistinctValues: 32,
			MaximumValueBytes:     4 << 10,
		},
		suggestionSpec: clickhouse.FieldSuggestionSpec{
			Prefix:        "payload_copy",
			MaximumFields: 16,
		},
	}
}

func compileKnowledgeRuntimeMatrix(
	t *testing.T,
	program knowledgeprogram.Program,
	tenantID string,
	indexName string,
	selectorIndexName string,
	base time.Time,
	indexTime time.Time,
	earliest time.Time,
	latest time.Time,
) compiledKnowledgeRuntimeMatrix {
	t.Helper()
	compiler := clickhouse.Compiler{}
	plans := buildKnowledgeRuntimeMatrixPlans(
		t,
		program,
		tenantID,
		indexName,
		selectorIndexName,
		base,
		indexTime,
		earliest,
		latest,
	)
	compile := func(name string, logical *plan.Query) clickhouse.CompiledQuery {
		t.Helper()
		compiled, err := compiler.Compile(logical)
		if err != nil {
			t.Fatalf("compile production knowledge query %q: %v", name, err)
		}
		if !compiled.HasValidExecutionSeal() {
			t.Fatalf("production knowledge query has no valid execution seal: %q", name)
		}
		return compiled
	}

	ordinary := compile("ordinary", plans.ordinary)
	wantOutputFields := []string{
		"event_id",
		"regex_value",
		"json_value",
		"calculated_value",
		"payload_copy",
		"numbers_copy",
		"signed_copy",
		"unsigned_copy",
		"float_copy",
		"bytes_copy",
		"null_copy",
		"empty_list_copy",
		"preserved_value",
		"replaced_value",
		"authored_rex",
		"authored_spath",
		"authored_value",
		"cohort",
		"ordinal",
	}
	if !slices.Equal(ordinary.OutputFields, wantOutputFields) {
		t.Fatalf(
			"ordinary public outputs = %#v, want %#v",
			ordinary.OutputFields,
			wantOutputFields,
		)
	}
	wantContainerFields := []string{
		"regex_value",
		"json_value",
		"calculated_value",
		"payload_copy",
		"numbers_copy",
		"signed_copy",
		"unsigned_copy",
		"float_copy",
		"bytes_copy",
		"null_copy",
		"empty_list_copy",
		"preserved_value",
		"replaced_value",
	}
	if len(ordinary.ContainerOutputs) != len(wantContainerFields) {
		t.Fatalf(
			"ordinary container transport = %#v outputs %#v",
			ordinary.ContainerOutputs,
			ordinary.OutputFields,
		)
	}
	for index, output := range ordinary.ContainerOutputs {
		wantOutputIndex := uint16(index + 1)
		if output.OutputIndex != wantOutputIndex ||
			int(output.OutputIndex) >= len(ordinary.OutputFields) ||
			ordinary.OutputFields[int(output.OutputIndex)] != wantContainerFields[index] {
			t.Fatalf(
				"ordinary container transport %d = %#v outputs %#v, want index %d field %q",
				index,
				output,
				ordinary.OutputFields,
				wantOutputIndex,
				wantContainerFields[index],
			)
		}
	}
	validatedContainers, validContainers := ordinary.ValidatedResultContainerOutputs()
	if !validContainers || !slices.Equal(validatedContainers, ordinary.ContainerOutputs) {
		t.Fatalf(
			"ordinary validated container transport = (%#v, %t), want %#v",
			validatedContainers,
			validContainers,
			ordinary.ContainerOutputs,
		)
	}
	if len(validatedContainers) == 0 {
		t.Fatal("ordinary validated container transport is empty")
	}
	validatedContainers[0].OutputIndex = 0
	if ordinary.ContainerOutputs[0].OutputIndex != 1 {
		t.Fatal("validated container transport aliases compiled authority")
	}
	hiddenColumns := make(map[string]struct{}, len(ordinary.ContainerOutputs)*3)
	for _, output := range ordinary.ContainerOutputs {
		for _, hidden := range []string{
			output.NamesColumn(),
			output.TypesColumn(),
			output.MetadataVersionColumn(),
		} {
			quotedHidden := `"` + hidden + `"`
			if slices.Contains(ordinary.OutputFields, hidden) ||
				!strings.Contains(ordinary.SQL, quotedHidden) {
				t.Fatalf(
					"ordinary hidden container column %q has invalid visibility",
					hidden,
				)
			}
			if _, duplicate := hiddenColumns[hidden]; duplicate {
				t.Fatalf("ordinary hidden container column %q is duplicated", hidden)
			}
			hiddenColumns[hidden] = struct{}{}
		}
	}
	if len(hiddenColumns) != 39 {
		t.Fatalf("ordinary hidden container columns = %d, want 39", len(hiddenColumns))
	}
	knowledgeRuntimeRequireKnowledgeArguments(t, ordinary, program)

	controls := compile("selector controls", plans.controls)
	chart := compile("chart", plans.chart)
	timechart := compile("timechart", plans.timechart)
	stats := compile("stats", plans.stats)

	timeline, err := compiler.CompileTimeline(plans.timeline, plans.timelineSpec)
	if err != nil {
		t.Fatalf("compile production knowledge timeline: %v", err)
	}
	if !timeline.HasValidExecutionSeal() {
		t.Fatal("production knowledge timeline has no valid execution seal")
	}

	catalog, err := compiler.CompileFieldCatalog(
		plans.analysis,
		plans.catalogSpec,
	)
	if err != nil {
		t.Fatalf("compile production knowledge field catalog: %v", err)
	}
	summary, err := compiler.CompileFieldSummary(plans.analysis, plans.summarySpec)
	if err != nil {
		t.Fatalf("compile production knowledge field summary: %v", err)
	}
	suggestions, err := compiler.CompileFieldSuggestions(
		plans.analysis,
		plans.suggestionSpec,
	)
	if err != nil {
		t.Fatalf("compile production knowledge field suggestions: %v", err)
	}
	if !catalog.HasValidExecutionSeal() || !summary.HasValidExecutionSeal() ||
		!suggestions.HasValidExecutionSeal() {
		t.Fatal("production knowledge field analysis has no valid execution seal")
	}

	overflow := compile("alias event overflow", plans.overflow)
	if !strings.Contains(overflow.SQL, clickhouse.KnowledgeAliasCopyEventLimitMarker) {
		t.Fatal("compiled overflow query omits the alias-copy event guard")
	}

	return compiledKnowledgeRuntimeMatrix{
		ordinary:    ordinary,
		controls:    controls,
		chart:       chart,
		timechart:   timechart,
		stats:       stats,
		timeline:    timeline,
		catalog:     catalog,
		summary:     summary,
		suggestions: suggestions,
		overflow:    overflow,
	}
}

func knowledgeRuntimePlan(
	t *testing.T,
	source string,
	program knowledgeprogram.Program,
	tenantID string,
	indexNames []string,
	indexTime time.Time,
	earliest time.Time,
	latest time.Time,
) *plan.Query {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("parse production knowledge query %q: %v", source, err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          tenantID,
		AuthorizedIndexes: slices.Clone(indexNames),
		RequestedIndexes:  slices.Clone(indexNames),
		Earliest:          earliest,
		Latest:            latest,
		SearchStart:       indexTime,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   indexTime.Add(time.Millisecond),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatalf("plan production knowledge query %q: %v", source, err)
	}
	logical, err = plan.InjectKnowledgePrelude(logical, program)
	if err != nil {
		t.Fatalf("inject production knowledge query %q: %v", source, err)
	}
	return logical
}

func knowledgeRuntimeProgram(t *testing.T) knowledgeprogram.Program {
	t.Helper()
	replace := opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING
	selector := func(dimension string, value string) *opensplunkv1.KnowledgeSelector {
		pattern := []*opensplunkv1.KnowledgeSelectorPattern{{Value: value}}
		result := &opensplunkv1.KnowledgeSelector{}
		switch dimension {
		case "index":
			result.IndexPatterns = pattern
		case "host":
			result.HostPatterns = pattern
		case "source":
			result.SourcePatterns = pattern
		case "sourcetype":
			result.SourcetypePatterns = pattern
		default:
			t.Fatalf("unknown knowledge selector dimension %q", dimension)
		}
		return result
	}
	definitions := []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "knowledge-app", Name: "a-extract-kind",
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector:     selector("index", "knowledge-*"),
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
					InputField: "_raw", OverwriteBehavior: replace,
					Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
						Regex: &opensplunkv1.RegexFieldExtractionDefinition{
							Pattern:      `"kind":"(?P<regex_value>[a-z]+)"`,
							OutputFields: []string{"regex_value"},
						},
					},
				},
			},
		},
		{
			AppId: "knowledge-app", Name: "b-extract-json",
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector:     selector("sourcetype", "knowledge:*"),
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
				FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
					InputField: "_raw", OverwriteBehavior: replace,
					Extraction: &opensplunkv1.FieldExtractionDefinition_Json{
						Json: &opensplunkv1.JsonFieldExtractionDefinition{
							Path: "nested.value", OutputField: "json_value",
						},
					},
				},
			},
		},
		knowledgeRuntimeAliasDefinition("a-copy-bytes", "bytes_source", "bytes_copy", selector("source", "*Source")),
		knowledgeRuntimeAliasDefinition("b-copy-empty-list", "empty_list_source", "empty_list_copy", selector("source", "*Source")),
		knowledgeRuntimeAliasDefinition("c-copy-float", "float_source", "float_copy", selector("source", "*Source")),
		knowledgeRuntimeAliasDefinition("d-copy-null", "null_source", "null_copy", selector("source", "*Source")),
		knowledgeRuntimeAliasDefinition("e-copy-numbers", "numbers", "numbers_copy", selector("source", "*Source")),
		knowledgeRuntimeAliasDefinition("f-copy-payload", "payload", "payload_copy", selector("source", "*Source")),
		knowledgeRuntimeAliasDefinition("g-copy-signed", "signed_source", "signed_copy", selector("source", "*Source")),
		knowledgeRuntimeAliasDefinition("h-copy-unsigned", "unsigned_source", "unsigned_copy", selector("source", "*Source")),
		knowledgeRuntimeAliasDefinitionWithOverwrite(
			"i-preserve-value",
			"overwrite_source",
			"preserved_value",
			selector("source", "*Source"),
			opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
		),
		knowledgeRuntimeAliasDefinition("j-replace-value", "overwrite_source", "replaced_value", selector("source", "*Source")),
		{
			AppId: "knowledge-app", Name: "a-calculated-source",
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Selector:     selector("host", "fixture-*"),
			Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
				CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
					DestinationField:  "calculated_value",
					Expression:        "lower(source)",
					OverwriteBehavior: replace,
				},
			},
		},
	}
	return knowledgeRuntimePrepareProgram(t, definitions)
}

func knowledgeRuntimeOverflowProgram(t *testing.T) knowledgeprogram.Program {
	t.Helper()
	definitions := make([]*opensplunkv1.KnowledgeObjectDefinition, knowledgeRuntimeOverflowAliasCount)
	for index := range definitions {
		definitions[index] = knowledgeRuntimeAliasDefinition(
			string(rune('a'+index))+"-overflow-copy",
			"source",
			"overflow_copy_"+string(rune('a'+index)),
			&opensplunkv1.KnowledgeSelector{},
		)
	}
	return knowledgeRuntimePrepareProgram(t, definitions)
}

func knowledgeRuntimeAliasDefinition(
	name string,
	source string,
	destination string,
	selector *opensplunkv1.KnowledgeSelector,
) *opensplunkv1.KnowledgeObjectDefinition {
	return knowledgeRuntimeAliasDefinitionWithOverwrite(
		name,
		source,
		destination,
		selector,
		opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
	)
}

func knowledgeRuntimeAliasDefinitionWithOverwrite(
	name string,
	source string,
	destination string,
	selector *opensplunkv1.KnowledgeSelector,
	overwrite opensplunkv1.KnowledgeOverwriteBehavior,
) *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "knowledge-app", Name: name,
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Selector:     selector,
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: source, DestinationField: destination,
				OverwriteBehavior: overwrite,
			},
		},
	}
}

func knowledgeRuntimePrepareProgram(
	t *testing.T,
	definitions []*opensplunkv1.KnowledgeObjectDefinition,
) knowledgeprogram.Program {
	t.Helper()
	objects := make([]*opensplunkv1.KnowledgeSnapshotObject, len(definitions))
	stageOrdinals := map[opensplunkv1.KnowledgeSearchStage]uint32{}
	for index, definition := range definitions {
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatalf("normalize runtime knowledge definition %d: %v", index, err)
		}
		stage := knowledgeRuntimeStage(t, normalized.ObjectType)
		objects[index] = &opensplunkv1.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index),
			Stage:             stage,
			StageOrdinal:      stageOrdinals[stage],
			KnowledgeObjectId: "runtime-object-" + normalized.Name,
			Version:           1,
			ObjectType:        normalized.ObjectType,
			Name:              normalized.Name,
			AppId:             normalized.AppID,
			OwnerId:           "knowledge-owner",
			SharingScope:      normalized.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSha256:  slices.Clone(normalized.Digest[:]),
		}
		stageOrdinals[stage]++
	}
	program, err := knowledgeprogram.Prepare(knowledgeprogram.Input{Objects: objects})
	if err != nil {
		t.Fatalf("prepare runtime knowledge program: %v", err)
	}
	return program
}

func knowledgeRuntimeStage(
	t *testing.T,
	objectType opensplunkv1.KnowledgeObjectType,
) opensplunkv1.KnowledgeSearchStage {
	t.Helper()
	switch objectType {
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS
	case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
	default:
		t.Fatalf("unsupported runtime knowledge object type %v", objectType)
		return opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_UNSPECIFIED
	}
}

func knowledgeRuntimeRequireKnowledgeArguments(
	t *testing.T,
	compiled clickhouse.CompiledQuery,
	program knowledgeprogram.Program,
) {
	t.Helper()
	regex := program.RegexExtractions()
	if len(regex) != 1 {
		t.Fatalf("runtime knowledge regex operations = %d, want 1", len(regex))
	}
	jsonExtractions := program.JSONExtractions()
	aliases := program.Aliases()
	calculated := program.CalculatedFields()
	if len(jsonExtractions) != 1 || len(aliases) == 0 || len(calculated) != 1 {
		t.Fatalf(
			"runtime knowledge operations = JSON %d aliases %d calculated %d",
			len(jsonExtractions),
			len(aliases),
			len(calculated),
		)
	}
	wants := []string{regex[0].Pattern()}
	for _, selected := range []struct {
		selector  knowledgeprogram.Selector
		dimension knowledge.Dimension
		label     string
	}{
		{selector: regex[0].Selector(), dimension: knowledge.DimensionIndex, label: "index"},
		{selector: jsonExtractions[0].Selector(), dimension: knowledge.DimensionSourcetype, label: "sourcetype"},
		{selector: aliases[0].Selector(), dimension: knowledge.DimensionSource, label: "source"},
		{selector: calculated[0].Selector(), dimension: knowledge.DimensionHost, label: "host"},
	} {
		dimension, ok := selected.selector.RuntimeProgram(selected.dimension)
		if !ok || dimension.WildcardRE2 == "" {
			t.Fatalf("runtime knowledge selector has no %s wildcard program", selected.label)
		}
		wants = append(wants, dimension.WildcardRE2)
	}
	for _, want := range wants {
		if !slices.Contains(compiled.Args, any(want)) {
			t.Fatalf("compiled knowledge arguments omit %q: %#v", want, compiled.Args)
		}
	}
}

const (
	knowledgeRuntimeRoleMatrix            = "matrix"
	knowledgeRuntimeRoleIndexControl      = "index-control"
	knowledgeRuntimeRoleHostControl       = "host-control"
	knowledgeRuntimeRoleSourceControl     = "source-control"
	knowledgeRuntimeRoleSourcetypeControl = "sourcetype-control"
	knowledgeRuntimeRoleTenantDecoy       = "tenant-decoy"
)

type knowledgeRuntimeFixture struct {
	id                  string
	role                string
	tenantID            string
	indexName           string
	eventTime           time.Time
	indexTime           time.Time
	host                string
	source              string
	sourcetype          string
	service             string
	kind                string
	jsonValue           string
	payload             string
	numbers             []string
	preserveDestination bool
}

func knowledgeRuntimeFixtures(
	tenantID string,
	indexName string,
	selectorIndexName string,
	base time.Time,
	indexTime time.Time,
) []knowledgeRuntimeFixture {
	fixture := func(
		id string,
		role string,
		tenant string,
		index string,
		offset time.Duration,
		host string,
		source string,
		sourcetype string,
		service string,
		kind string,
	) knowledgeRuntimeFixture {
		return knowledgeRuntimeFixture{
			id:                  id,
			role:                role,
			tenantID:            tenant,
			indexName:           index,
			eventTime:           base.Add(offset),
			indexTime:           indexTime,
			host:                host,
			source:              source,
			sourcetype:          sourcetype,
			service:             service,
			kind:                kind,
			jsonValue:           "json-" + kind,
			payload:             "payload-" + kind,
			numbers:             []string{kind, "fixed"},
			preserveDestination: id != "knowledge-event-d",
		}
	}
	return []knowledgeRuntimeFixture{
		fixture("knowledge-event-a", knowledgeRuntimeRoleMatrix, tenantID, indexName, 10*time.Second, "fixture-a", "AlphaSource", "knowledge:fixture", "matrix", "alpha"),
		fixture("knowledge-event-b", knowledgeRuntimeRoleMatrix, tenantID, indexName, 40*time.Second, "fixture-b", "BetaSource", "knowledge:fixture", "matrix", "beta"),
		fixture("knowledge-event-c", knowledgeRuntimeRoleMatrix, tenantID, indexName, 70*time.Second, "fixture-c", "AlphaSource", "knowledge:fixture", "matrix", "alpha"),
		fixture("knowledge-event-d", knowledgeRuntimeRoleMatrix, tenantID, indexName, 100*time.Second, "fixture-d", "BetaSource", "knowledge:fixture", "matrix", "beta"),
		fixture("knowledge-control-index", knowledgeRuntimeRoleIndexControl, tenantID, selectorIndexName, 15*time.Second, "fixture-index", "IndexSource", "knowledge:fixture", "selector-control", "index"),
		fixture("knowledge-control-host", knowledgeRuntimeRoleHostControl, tenantID, indexName, 25*time.Second, "control-host", "HostSource", "knowledge:fixture", "selector-control", "host"),
		fixture("knowledge-control-source", knowledgeRuntimeRoleSourceControl, tenantID, indexName, 35*time.Second, "fixture-source", "control-source", "knowledge:fixture", "selector-control", "source"),
		fixture("knowledge-control-sourcetype", knowledgeRuntimeRoleSourcetypeControl, tenantID, indexName, 45*time.Second, "fixture-sourcetype", "SourcetypeSource", "control:fixture", "selector-control", "sourcetype"),
		fixture("knowledge-cross-tenant-decoy", knowledgeRuntimeRoleTenantDecoy, tenantID+"-decoy", indexName, 50*time.Second, "fixture-decoy", "DecoySource", "knowledge:fixture", "matrix", "decoy"),
	}
}

func insertKnowledgeRuntimeEvents(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	fixtures []knowledgeRuntimeFixture,
) {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, field_types, field_metadata_version, " +
		"collector_id, batch_id, batch_sequence, expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatalf("prepare knowledge runtime fixture: %v", err)
	}
	for index, fixture := range fixtures {
		document := clickhousedriver.NewJSON()
		document.SetValueAtPath("bytes_source", clickhousedriver.NewDynamicWithType(map[string]string{
			extendedTypeKey:  "bytes/v1",
			extendedValueKey: "AP8",
		}, "Map(String, String)"))
		document.SetValueAtPath("empty_list_source", clickhousedriver.NewDynamic([]string{}))
		document.SetValueAtPath("float_source", clickhousedriver.NewDynamic(float64(1.25)))
		document.SetValueAtPath("null_source", clickhousedriver.NewDynamic(nil))
		document.SetValueAtPath("numbers", clickhousedriver.NewDynamic(fixture.numbers))
		document.SetValueAtPath("overwrite_source", clickhousedriver.NewDynamic(int64(-11)))
		document.SetValueAtPath("payload.child", clickhousedriver.NewDynamic(fixture.payload))
		document.SetValueAtPath("payload.nothing", clickhousedriver.NewDynamic(nil))
		if fixture.preserveDestination {
			document.SetValueAtPath("preserved_value", clickhousedriver.NewDynamic("preserved-"+fixture.kind))
		}
		document.SetValueAtPath("replaced_value", clickhousedriver.NewDynamic("existing-"+fixture.kind))
		document.SetValueAtPath("signed_source", clickhousedriver.NewDynamic(int64(-7)))
		document.SetValueAtPath("unsigned_source", clickhousedriver.NewDynamic(uint64(9)))
		fieldNames := []string{
			"bytes_source",
			"empty_list_source",
			"float_source",
			"null_source",
			"numbers",
			"overwrite_source",
			"payload.child",
			"payload.nothing",
			"replaced_value",
			"signed_source",
			"unsigned_source",
		}
		fieldTypes := []uint8{
			uint8(eventfields.StoredValueTypeBytes),
			uint8(eventfields.StoredValueTypeList),
			uint8(eventfields.StoredValueTypeDouble),
			uint8(eventfields.StoredValueTypeNull),
			uint8(eventfields.StoredValueTypeList),
			uint8(eventfields.StoredValueTypeSint64),
			uint8(eventfields.StoredValueTypeString),
			uint8(eventfields.StoredValueTypeNull),
			uint8(eventfields.StoredValueTypeString),
			uint8(eventfields.StoredValueTypeSint64),
			uint8(eventfields.StoredValueTypeUint64),
		}
		if fixture.preserveDestination {
			fieldNames = slices.Insert(fieldNames, 8, "preserved_value")
			fieldTypes = slices.Insert(
				fieldTypes,
				8,
				uint8(eventfields.StoredValueTypeString),
			)
		}
		raw := []byte(`{"kind":"` + fixture.kind + `","nested":{"value":"` +
			fixture.jsonValue + `"}}`)
		service := fixture.service
		if err := batch.Append(
			fixture.id,
			fixture.tenantID,
			fixture.indexName,
			fixture.eventTime,
			fixture.indexTime,
			nil,
			uint8(1),
			fixture.host,
			fixture.source,
			fixture.sourcetype,
			&service,
			uint8(1),
			nil,
			nil,
			raw,
			uint8(1),
			nil,
			nil,
			document,
			fieldNames,
			fieldTypes,
			eventfields.CurrentFieldMetadataVersion,
			"knowledge-collector",
			"knowledge-batch",
			uint64(index+1),
			fixture.indexTime.Add(24*time.Hour),
			uint64(1),
		); err != nil {
			t.Fatalf("append knowledge runtime event %q: %v", fixture.id, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send knowledge runtime fixture: %v", err)
	}
}

func insertKnowledgeRuntimeOverflowEvent(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	tenantID string,
	indexName string,
	base time.Time,
	indexTime time.Time,
) {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"event_time_source, host, source, sourcetype, severity, raw, raw_encoding, fields, field_names, " +
		"field_types, field_metadata_version, collector_id, batch_id, batch_sequence, expires_at, visibility_seq) VALUES"
	if err := connection.Exec(
		ctx,
		query,
		"knowledge-overflow-event",
		tenantID,
		indexName,
		base.Add(20*time.Second),
		indexTime,
		uint8(1),
		"fixture-overflow",
		strings.Repeat("x", knowledgeRuntimeOverflowSourceBytes(t)),
		"knowledge:overflow",
		uint8(1),
		[]byte("kind=overflow"),
		uint8(1),
		clickhousedriver.NewJSON(),
		[]string{},
		[]uint8{},
		eventfields.CurrentFieldMetadataVersion,
		"knowledge-collector",
		"knowledge-overflow-batch",
		uint64(1),
		indexTime.Add(24*time.Hour),
		uint64(1),
	); err != nil {
		t.Fatalf("insert knowledge runtime overflow event: %v", err)
	}
}

func knowledgeRuntimeOverflowSourceBytes(t *testing.T) int {
	t.Helper()
	target := knowledge.MaximumAliasCopyRuntimeEventBytes + 1
	if target%knowledgeRuntimeOverflowAliasCount != 0 {
		t.Fatalf(
			"alias event limit sentinel %d is not divisible by %d",
			target,
			knowledgeRuntimeOverflowAliasCount,
		)
	}
	// A promoted scalar alias has no relative descendant metadata. Each winning
	// root copy charges byteSize(source) plus the mandatory metadata-version
	// byte, so five equal writes land exactly one byte beyond the event ceiling.
	valueBytes := target/knowledgeRuntimeOverflowAliasCount - 1
	charge, ok := knowledge.CheckedAliasCopyCharge(knowledge.AliasCopyWrite{
		ValueBytes: valueBytes,
	})
	if !ok {
		t.Fatal("construct exact alias-copy overflow charge")
	}
	charges := make([]knowledge.AliasCopyCharge, knowledgeRuntimeOverflowAliasCount)
	for index := range charges {
		charges[index] = charge
	}
	row := knowledge.SaturatingAliasCopyRow(charges)
	if row.PayloadBytes != target ||
		row.WorkUnits > knowledge.MaximumAliasCopyRuntimeQueryUnits {
		t.Fatalf("exact alias-copy overflow charge = %#v, want event %d only", row, target)
	}
	return int(valueBytes)
}

func knowledgeRuntimePrivateLimitMarkers() []string {
	return []string{
		clickhouse.KnowledgeSelectorValueLimitMarker,
		clickhouse.KnowledgeSelectorEventLimitMarker,
		clickhouse.KnowledgeSelectorQueryLimitMarker,
		clickhouse.KnowledgeAliasCopyEventLimitMarker,
		clickhouse.KnowledgeAliasCopyQueryLimitMarker,
	}
}

func knowledgeRuntimeAssertOrdinary(t *testing.T, sink *fakeSink) {
	t.Helper()
	wantSchema := []searchjobs.Column{{Name: "event_id", Kind: searchjobs.ValueKindString}}
	for _, name := range []string{
		"regex_value",
		"json_value",
		"calculated_value",
		"payload_copy",
		"numbers_copy",
		"signed_copy",
		"unsigned_copy",
		"float_copy",
		"bytes_copy",
		"null_copy",
		"empty_list_copy",
		"preserved_value",
		"replaced_value",
		"authored_rex",
		"authored_spath",
		"authored_value",
	} {
		wantSchema = append(wantSchema, searchjobs.Column{
			Name: name, Kind: searchjobs.ValueKindMixed, Nullable: true,
		})
	}
	wantSchema = append(
		wantSchema,
		searchjobs.Column{Name: "cohort", Kind: searchjobs.ValueKindUnsigned},
		searchjobs.Column{Name: "ordinal", Kind: searchjobs.ValueKindUnsigned},
	)
	if sink.setCalls != 1 || !slices.Equal(sink.schema.Columns, wantSchema) ||
		len(sink.rows) != 3 {
		t.Fatalf(
			"ordinary result = schema %#v calls %d rows %#v, want schema %#v",
			sink.schema,
			sink.setCalls,
			sink.rows,
			wantSchema,
		)
	}
	eventIDs := []string{"knowledge-event-d", "knowledge-event-c", "knowledge-event-b"}
	kinds := []string{"beta", "alpha", "beta"}
	sources := []string{"betasource", "alphasource", "betasource"}
	for rowIndex, row := range sink.rows {
		kind := kinds[rowIndex]
		knowledgeRuntimeRequireStringValue(t, row[0], eventIDs[rowIndex], "event_id")
		knowledgeRuntimeRequireStringValue(t, row[1], kind, "regex_value")
		knowledgeRuntimeRequireStringValue(t, row[2], "json-"+kind, "json_value")
		knowledgeRuntimeRequireStringValue(t, row[3], sources[rowIndex], "calculated_value")
		payload := knowledgeRuntimeObject(t, row[4])
		if len(payload) != 2 {
			t.Fatalf("ordinary row %d payload = %#v", rowIndex, payload)
		}
		knowledgeRuntimeRequireStringValue(t, payload["child"], "payload-"+kind, "payload child")
		if !payload["nothing"].IsNull() {
			t.Fatalf("ordinary row %d explicit object null = %#v", rowIndex, payload["nothing"])
		}
		knowledgeRuntimeRequireStringList(t, row[5], []string{kind, "fixed"}, "numbers_copy")
		if value, ok := row[6].Signed(); !ok || value != -7 {
			t.Fatalf("ordinary row %d signed_copy = %#v", rowIndex, row[6])
		}
		knowledgeRuntimeRequireUnsignedValue(t, row[7], 9, "unsigned_copy")
		if value, ok := row[8].Double(); !ok || value != 1.25 {
			t.Fatalf("ordinary row %d float_copy = %#v", rowIndex, row[8])
		}
		if value, ok := row[9].Bytes(); !ok || !bytes.Equal(value, []byte{0x00, 0xff}) {
			t.Fatalf("ordinary row %d bytes_copy = %#v (%x/%v)", rowIndex, row[9], value, ok)
		}
		if !row[10].IsNull() {
			t.Fatalf("ordinary row %d null_copy = %#v", rowIndex, row[10])
		}
		knowledgeRuntimeRequireStringList(t, row[11], []string{}, "empty_list_copy")
		if rowIndex == 0 {
			if value, ok := row[12].Signed(); !ok || value != -11 {
				t.Fatalf("ordinary missing-destination preserve = %#v", row[12])
			}
		} else {
			knowledgeRuntimeRequireStringValue(t, row[12], "preserved-"+kind, "preserved_value")
		}
		if value, ok := row[13].Signed(); !ok || value != -11 {
			t.Fatalf("ordinary replace stale destination type = %#v", row[13])
		}
		knowledgeRuntimeRequireStringValue(t, row[14], kind, "authored_rex")
		knowledgeRuntimeRequireStringValue(t, row[15], "json-"+kind, "authored_spath")
		knowledgeRuntimeRequireStringValue(t, row[16], strings.ToUpper(sources[rowIndex]), "authored_value")
		knowledgeRuntimeRequireUnsignedValue(t, row[17], 4, "cohort")
		knowledgeRuntimeRequireUnsignedValue(t, row[18], uint64(rowIndex+1), "ordinal")
	}
}

func knowledgeRuntimeAssertSelectorControls(t *testing.T, sink *fakeSink) {
	t.Helper()
	wantSchema := []searchjobs.Column{
		{Name: "event_id", Kind: searchjobs.ValueKindString},
		{Name: "regex_value", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "json_value", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "calculated_value", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "signed_copy", Kind: searchjobs.ValueKindMixed, Nullable: true},
	}
	if sink.setCalls != 1 || !slices.Equal(sink.schema.Columns, wantSchema) ||
		len(sink.rows) != 4 {
		t.Fatalf("selector controls = schema %#v calls %d rows %#v", sink.schema, sink.setCalls, sink.rows)
	}
	type wantControl struct {
		kind          string
		source        string
		missingColumn int
	}
	wants := map[string]wantControl{
		"knowledge-control-index":      {kind: "index", source: "IndexSource", missingColumn: 1},
		"knowledge-control-host":       {kind: "host", source: "HostSource", missingColumn: 3},
		"knowledge-control-source":     {kind: "source", source: "control-source", missingColumn: 4},
		"knowledge-control-sourcetype": {kind: "sourcetype", source: "SourcetypeSource", missingColumn: 2},
	}
	seen := make(map[string]bool, len(wants))
	for _, row := range sink.rows {
		eventID, ok := row[0].String()
		want, exists := wants[eventID]
		if !ok || !exists || seen[eventID] {
			t.Fatalf("selector control identity = %#v", row[0])
		}
		seen[eventID] = true
		if !row[want.missingColumn].IsNull() {
			t.Fatalf("selector control %q column %d = %#v, want null", eventID, want.missingColumn, row[want.missingColumn])
		}
		if want.missingColumn != 1 {
			knowledgeRuntimeRequireStringValue(t, row[1], want.kind, eventID+" regex")
		}
		if want.missingColumn != 2 {
			knowledgeRuntimeRequireStringValue(t, row[2], "json-"+want.kind, eventID+" JSON")
		}
		if want.missingColumn != 3 {
			knowledgeRuntimeRequireStringValue(t, row[3], strings.ToLower(want.source), eventID+" calculated")
		}
		if want.missingColumn != 4 {
			if value, signed := row[4].Signed(); !signed || value != -7 {
				t.Fatalf("selector control %q signed_copy = %#v", eventID, row[4])
			}
		}
	}
	if len(seen) != len(wants) {
		t.Fatalf("selector controls seen = %#v", seen)
	}
}

func knowledgeRuntimeAssertChart(t *testing.T, sink *fakeSink) {
	t.Helper()
	wantSchema := []searchjobs.Column{
		{Name: "calculated_value", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "alpha", Kind: searchjobs.ValueKindUnsigned},
		{Name: "beta", Kind: searchjobs.ValueKindUnsigned},
	}
	if sink.setCalls != 1 || !slices.Equal(sink.schema.Columns, wantSchema) ||
		len(sink.rows) != 2 {
		t.Fatalf("knowledge chart = schema %#v calls %d rows %#v", sink.schema, sink.setCalls, sink.rows)
	}
	for index, want := range []struct {
		row   string
		alpha uint64
		beta  uint64
	}{
		{row: "alphasource", alpha: 2},
		{row: "betasource", beta: 2},
	} {
		knowledgeRuntimeRequireStringValue(t, sink.rows[index][0], want.row, "chart row")
		knowledgeRuntimeRequireUnsignedValue(t, sink.rows[index][1], want.alpha, "chart alpha")
		knowledgeRuntimeRequireUnsignedValue(t, sink.rows[index][2], want.beta, "chart beta")
	}
}

func knowledgeRuntimeAssertTimechart(t *testing.T, sink *fakeSink, base time.Time) {
	t.Helper()
	wantSchema := []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "alpha", Kind: searchjobs.ValueKindUnsigned},
		{Name: "beta", Kind: searchjobs.ValueKindUnsigned},
	}
	if sink.setCalls != 1 || !slices.Equal(sink.schema.Columns, wantSchema) ||
		len(sink.rows) != 2 {
		t.Fatalf("knowledge timechart = schema %#v calls %d rows %#v", sink.schema, sink.setCalls, sink.rows)
	}
	for index, want := range []struct {
		at    time.Time
		alpha uint64
		beta  uint64
	}{
		{at: base, alpha: 1, beta: 1},
		{at: base.Add(time.Minute), alpha: 1, beta: 1},
	} {
		at, ok := sink.rows[index][0].Time()
		if !ok || !at.Equal(want.at) {
			t.Fatalf("timechart row %d time = %#v", index, sink.rows[index][0])
		}
		knowledgeRuntimeRequireUnsignedValue(t, sink.rows[index][1], want.alpha, "timechart alpha")
		knowledgeRuntimeRequireUnsignedValue(t, sink.rows[index][2], want.beta, "timechart beta")
	}
}

func knowledgeRuntimeAssertStats(t *testing.T, sink *fakeSink) {
	t.Helper()
	wantSchema := []searchjobs.Column{
		{Name: "regex_value", Kind: searchjobs.ValueKindMixed, Nullable: true},
		{Name: "total", Kind: searchjobs.ValueKindUnsigned},
	}
	if sink.setCalls != 1 || !slices.Equal(sink.schema.Columns, wantSchema) ||
		len(sink.rows) != 2 {
		t.Fatalf("knowledge stats = schema %#v calls %d rows %#v", sink.schema, sink.setCalls, sink.rows)
	}
	for index, want := range []struct {
		kind  string
		count uint64
	}{{kind: "alpha", count: 2}, {kind: "beta", count: 2}} {
		knowledgeRuntimeRequireStringValue(t, sink.rows[index][0], want.kind, "stats group")
		knowledgeRuntimeRequireUnsignedValue(t, sink.rows[index][1], want.count, "stats total")
	}
}

func knowledgeRuntimeAssertCatalog(t *testing.T, catalog FieldCatalogResult) {
	t.Helper()
	if catalog.TotalEvents != 4 {
		t.Fatalf("knowledge catalog total events = %d, want 4", catalog.TotalEvents)
	}
	wants := []FieldProfileRow{
		{FieldName: "bytes_copy", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeBytes}, EventCount: 4},
		{FieldName: "calculated_value", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 4},
		{FieldName: "empty_list_copy", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeList}, EventCount: 4},
		{FieldName: "float_copy", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeDouble}, EventCount: 4},
		{FieldName: "json_value", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 4},
		{FieldName: "null_copy", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeNull}, EventCount: 4, NullCount: 4},
		{FieldName: "numbers_copy", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeList}, EventCount: 4},
		{FieldName: "payload_copy", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeObject}, EventCount: 4},
		{FieldName: "payload_copy.child", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 4},
		{FieldName: "payload_copy.nothing", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeNull}, EventCount: 4, NullCount: 4},
		{FieldName: "preserved_value", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString, eventfields.StoredValueTypeSint64}, EventCount: 4},
		{FieldName: "regex_value", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeString}, EventCount: 4},
		{FieldName: "replaced_value", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeSint64}, EventCount: 4},
		{FieldName: "signed_copy", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeSint64}, EventCount: 4},
		{FieldName: "unsigned_copy", ObservedTypes: []eventfields.StoredValueType{eventfields.StoredValueTypeUint64}, EventCount: 4},
	}
	for _, want := range wants {
		got, ok := knowledgeRuntimeCatalogProfile(catalog, want.FieldName)
		if !ok || !slices.Equal(got.ObservedTypes, want.ObservedTypes) ||
			got.EventCount != want.EventCount || got.NullCount != want.NullCount ||
			got.MissingCount != want.MissingCount {
			t.Fatalf("knowledge catalog profile %q = %#v, want %#v", want.FieldName, got, want)
		}
	}
}

func knowledgeRuntimeAssertSummary(t *testing.T, summary FieldSummaryResult) {
	t.Helper()
	if summary.FieldName != "calculated_value" ||
		!slices.Equal(summary.ObservedTypes, []eventfields.StoredValueType{eventfields.StoredValueTypeString}) ||
		summary.EventCount != 4 || summary.NullCount != 0 || summary.MissingCount != 0 ||
		summary.DistinctCount != 2 || len(summary.TopValues) != 2 {
		t.Fatalf("knowledge field summary = %#v", summary)
	}
	knowledgeRuntimeRequireStringValue(t, summary.TopValues[0].Value, "alphasource", "summary first value")
	if summary.TopValues[0].Count != 2 {
		t.Fatalf("knowledge summary first count = %d, want 2", summary.TopValues[0].Count)
	}
	knowledgeRuntimeRequireStringValue(t, summary.TopValues[1].Value, "betasource", "summary second value")
	if summary.TopValues[1].Count != 2 {
		t.Fatalf("knowledge summary second count = %d, want 2", summary.TopValues[1].Count)
	}
}

func knowledgeRuntimeRequireStringValue(
	t *testing.T,
	value searchjobs.Value,
	want string,
	label string,
) {
	t.Helper()
	got, ok := value.String()
	if !ok || got != want {
		t.Fatalf("%s = %#v, want String(%q)", label, value, want)
	}
}

func knowledgeRuntimeRequireUnsignedValue(
	t *testing.T,
	value searchjobs.Value,
	want uint64,
	label string,
) {
	t.Helper()
	got, ok := value.Unsigned()
	if !ok || got != want {
		t.Fatalf("%s = %#v, want Unsigned(%d)", label, value, want)
	}
}

func knowledgeRuntimeRequireStringList(
	t *testing.T,
	value searchjobs.Value,
	want []string,
	label string,
) {
	t.Helper()
	got, ok := value.List()
	if !ok || len(got) != len(want) {
		t.Fatalf("%s = %#v, want string list %#v", label, value, want)
	}
	for index := range want {
		knowledgeRuntimeRequireStringValue(t, got[index], want[index], label)
	}
}

func knowledgeRuntimeObject(t *testing.T, value searchjobs.Value) map[string]searchjobs.Value {
	t.Helper()
	fields, ok := value.Object()
	if !ok {
		t.Fatalf("knowledge runtime value is not an object: %#v", value)
	}
	result := make(map[string]searchjobs.Value, len(fields))
	for _, field := range fields {
		result[field.Name] = field.Value
	}
	return result
}

func knowledgeRuntimeCatalogProfile(
	result FieldCatalogResult,
	name string,
) (FieldProfileRow, bool) {
	for _, field := range result.Fields {
		if field.FieldName == name {
			return field, true
		}
	}
	return FieldProfileRow{}, false
}
