package main

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/lookupservice"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchinspection"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/migrations"
)

// TestLookupRuntimeManagerAgainstClickHouse is the production-shaped lookup
// qualification for the boundary that unit tests cannot cover. It publishes a
// lookup through the runtime management service, resolves and seals it through
// Manager admission, attaches its immutable rows as a native external table,
// and consumes the real ClickHouse result through the public result page.
func TestLookupRuntimeManagerAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}

	ctx, connection, clickhouseOptions := startLookupClickHouse(t)
	runtime, database := newRuntimeKnowledgeTestRuntime(t)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close lookup control database: %v", err)
		}
	})
	createRuntimeKnowledgeTestApp(t, database)
	createRuntimeKnowledgeTestIndex(t, database)

	created, err := runtime.lookupManagement.Create(
		ctx,
		lookupserviceScope(),
		&opensplunk.CreateLookupRequest{
			Definition: &opensplunk.LookupDefinition{
				AppId:        runtimeKnowledgeTestApp,
				Name:         "service_owners",
				SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
				KeyMappings: []*opensplunk.LookupFieldMapping{{
					LookupField: "service_id",
					EventField:  "service_key",
				}},
				OutputMappings: []*opensplunk.LookupFieldMapping{{
					LookupField: "owner",
					EventField:  "service_owner",
				}},
				OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
			},
			CsvData: []byte("service_id,owner\napi,platform\n,empty-match\n7,numeric-match\n"),
		},
	)
	if err != nil {
		t.Fatalf("publish lookup: %v", err)
	}
	if created.GetLookup() == nil || created.GetLookup().GetVersion() != 1 {
		t.Fatalf("published lookup = %#v", created)
	}
	insertLookupEvents(t, ctx, connection)
	executor, err := queryexec.New(connection, queryexec.Config{
		ReadAdmission: indexread.UnfencedAdmission{},
	})
	if err != nil {
		t.Fatalf("create lookup query executor: %v", err)
	}
	counters := &runtimeKnowledgeAdmissionCounters{}
	config := runtimeKnowledgeAdmissionManagerConfig(runtime.resolver, counters)
	config.Executor = executor
	config.LookupResolver = runtime.lookupResolver
	manager, err := searchjobs.New(config)
	if err != nil {
		t.Fatalf("create lookup search manager: %v", err)
	}
	if !manager.LookupAdmissionEnabled() {
		t.Fatal("lookup manager did not report lookup admission")
	}
	t.Cleanup(func() {
		if closeErr := manager.Close(); closeErr != nil {
			t.Errorf("close lookup search manager: %v", closeErr)
		}
	})
	explainer, err := queryexec.NewExplainer(clickhouseOptions, queryexec.Config{})
	if err != nil {
		t.Fatalf("create lookup Explainer: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := explainer.Close(); closeErr != nil {
			t.Errorf("close lookup Explainer: %v", closeErr)
		}
	})
	inspection, err := searchinspection.New(searchinspection.Config{
		Searches:  manager,
		Compiler:  config.Compiler,
		Explainer: explainer,
	})
	if err != nil {
		t.Fatalf("create lookup inspection service: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if closeErr := inspection.Close(closeCtx); closeErr != nil {
			t.Errorf("close lookup inspection service: %v", closeErr)
		}
	})

	request := runtimeKnowledgeSearchRequest(t)
	request.SPL = "index=main | lookup service_owners service_id AS service_key " +
		"OUTPUTNEW owner AS service_owner | table event_id service_owner"
	job, err := manager.Create(ctx, request)
	if err != nil {
		t.Fatalf("admit lookup search: %v", err)
	}
	completed := waitForLookupTerminal(t, manager, job.ID)
	if completed.State != searchjobs.StateCompleted || completed.Failure != nil {
		t.Fatalf("lookup search = %#v", completed)
	}
	provenance := completed.KnowledgeSnapshot.GetLookupAssets()
	if len(provenance) != 1 ||
		provenance[0].GetLookupId() != created.GetLookup().GetLookupId() ||
		provenance[0].GetLookupVersion() != 1 ||
		provenance[0].GetAsset().GetVersion() != 1 {
		t.Fatalf("lookup provenance = %#v", provenance)
	}

	page, err := manager.Results(job.ID, searchjobs.PageRequest{Limit: 16})
	if err != nil {
		t.Fatalf("read lookup results: %v", err)
	}
	requireLookupResults(t, page)
	requireLookupInspection(t, ctx, inspection, job.ID, "Lookup", true)

	// Keep the inspection proof shaped like the release backend vertical: the
	// explicit lookup is followed by the complete authored suffix used there.
	// This catches plan/result bounds that a lookup-plus-table smoke query does
	// not exercise, while the first job above continues to assert returned rows.
	verticalRequest := runtimeKnowledgeSearchRequest(t)
	verticalRequest.SPL = "index=main | lookup service_owners service_id AS service_key " +
		"OUTPUTNEW owner AS service_owner | eval adjusted_duration=duration_ms+1 " +
		"| where status IN (status) | dedup event_id | table _time message status " +
		"duration_ms adjusted_duration service_owner api_key customer_credential " +
		"customer_pin _raw"
	verticalJob, err := manager.Create(ctx, verticalRequest)
	if err != nil {
		t.Fatalf("admit backend-shaped lookup search: %v", err)
	}
	verticalCompleted := waitForLookupTerminal(t, manager, verticalJob.ID)
	if verticalCompleted.State != searchjobs.StateCompleted ||
		verticalCompleted.Failure != nil {
		t.Fatalf("backend-shaped lookup search = %#v", verticalCompleted)
	}
	requireLookupInspection(
		t,
		ctx,
		inspection,
		verticalJob.ID,
		"Lookup",
		true,
	)

	// Publish automatic authority only after the explicit job completed so its
	// provenance remains an isolated one-asset assertion.
	automatic, err := runtime.lookupManagement.Create(
		ctx,
		lookupserviceScope(),
		&opensplunk.CreateLookupRequest{
			Definition: &opensplunk.LookupDefinition{
				AppId:        runtimeKnowledgeTestApp,
				Name:         "automatic_service_owners",
				SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
				Selector: &opensplunk.KnowledgeSelector{
					IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{
						Value: "main",
					}},
				},
				Automatic: true,
				KeyMappings: []*opensplunk.LookupFieldMapping{{
					LookupField: "service_id",
					EventField:  "service_key",
				}},
				OutputMappings: []*opensplunk.LookupFieldMapping{{
					LookupField: "owner",
					EventField:  "automatic_owner",
				}},
				OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
			},
			CsvData: []byte("service_id,owner\napi,automatic-platform\n,automatic-empty\n"),
		},
	)
	if err != nil {
		t.Fatalf("publish automatic lookup: %v", err)
	}
	if automatic.GetLookup() == nil || automatic.GetLookup().GetVersion() != 1 ||
		!automatic.GetLookup().GetDefinition().GetAutomatic() {
		t.Fatalf("published automatic lookup = %#v", automatic)
	}

	automaticRequest := runtimeKnowledgeSearchRequest(t)
	automaticRequest.SPL = "index=main | table event_id automatic_owner"
	automaticJob, err := manager.Create(ctx, automaticRequest)
	if err != nil {
		t.Fatalf("admit automatic lookup search: %v", err)
	}
	automaticCompleted := waitForLookupTerminal(t, manager, automaticJob.ID)
	if automaticCompleted.State != searchjobs.StateCompleted ||
		automaticCompleted.Failure != nil {
		t.Fatalf("automatic lookup search = %#v", automaticCompleted)
	}
	automaticProvenance := automaticCompleted.KnowledgeSnapshot.GetLookupAssets()
	if len(automaticProvenance) != 1 ||
		automaticProvenance[0].GetLookupId() != automatic.GetLookup().GetLookupId() ||
		automaticProvenance[0].GetLookupVersion() != 1 ||
		automaticProvenance[0].GetAsset().GetVersion() != 1 {
		t.Fatalf("automatic lookup provenance = %#v", automaticProvenance)
	}
	automaticPage, err := manager.Results(
		automaticJob.ID,
		searchjobs.PageRequest{Limit: 16},
	)
	if err != nil {
		t.Fatalf("read automatic lookup results: %v", err)
	}
	requireLookupAutomaticResults(t, automaticPage)
	requireLookupInspection(
		t,
		ctx,
		inspection,
		automaticJob.ID,
		"AutomaticLookupGroup",
		false,
	)
}

func lookupserviceScope() lookupservice.Scope {
	return lookupservice.Scope{
		TenantID: runtimeKnowledgeTestTenant,
		OwnerID:  runtimeKnowledgeTestOwner,
	}
}

func startLookupClickHouse(
	t *testing.T,
) (context.Context, clickhousedriver.Conn, *clickhousedriver.Options) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	container, err := testsupport.StartClickHouse(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ClickHouse image: %s", container.Image)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupCtx); closeErr != nil {
			t.Errorf("close lookup ClickHouse fixture: %v", closeErr)
		}
	})

	options := &clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
	}
	connection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatalf("open lookup ClickHouse connection: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("close lookup ClickHouse connection: %v", closeErr)
		}
	})
	if err := connection.Ping(ctx); err != nil {
		t.Fatalf("ping lookup ClickHouse connection: %v", err)
	}
	if err := server.ApplyClickHouseMigrations(ctx, connection, migrations.ClickHouse()); err != nil {
		t.Fatalf("migrate lookup ClickHouse fixture: %v", err)
	}
	return ctx, connection, options
}

func requireLookupInspection(
	t *testing.T,
	ctx context.Context,
	service *searchinspection.Service,
	jobID, operator string,
	wantSourceRange bool,
) {
	t.Helper()
	result, err := service.Inspect(
		ctx,
		searchjobs.AccessScope{
			TenantID: runtimeKnowledgeTestTenant,
			OwnerID:  runtimeKnowledgeTestOwner,
		},
		searchinspection.Request{SearchJobID: jobID},
	)
	if err != nil {
		t.Fatalf("inspect retained %s lookup execution: %v", operator, err)
	}
	if err := searchinspection.ValidateResult(result); err != nil {
		t.Fatalf("validate retained %s lookup inspection: %v", operator, err)
	}
	for _, stage := range result.Plan.Stages {
		if stage.Operator != operator {
			continue
		}
		if (stage.SourceRange != nil) != wantSourceRange ||
			!slices.Contains(stage.InputFields, "service_key") ||
			!slices.Contains(
				stage.OutputFields,
				map[bool]string{true: "service_owner", false: "automatic_owner"}[wantSourceRange],
			) || len(stage.KnowledgeObjects) != 0 ||
			len(stage.OutputProvenance) != 0 {
			t.Fatalf("retained %s lookup inspection stage = %#v", operator, stage)
		}
		return
	}
	t.Fatalf("retained lookup inspection omitted %s: %#v", operator, result.Plan)
}

type lookupEvent struct {
	id         string
	key        any
	keyType    eventfields.StoredValueType
	priorOwner *string
}

func insertLookupEvents(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()
	empty := ""
	events := []lookupEvent{
		{id: "lookup-01-exact", key: "api", keyType: eventfields.StoredValueTypeString},
		{id: "lookup-02-empty-key", key: "", keyType: eventfields.StoredValueTypeString},
		{id: "lookup-03-present-empty", key: "api", keyType: eventfields.StoredValueTypeString, priorOwner: &empty},
		{id: "lookup-04-number", key: int64(7), keyType: eventfields.StoredValueTypeSint64},
		{id: "lookup-05-case-mismatch", key: "API", keyType: eventfields.StoredValueTypeString},
	}
	batch, err := connection.PrepareBatch(ctx, `
		INSERT INTO open_splunk.events
		(
			event_id, tenant_id, index_name, event_time, index_time,
			collected_at, event_time_source, host, source, sourcetype,
			service, severity, level, body, raw, raw_encoding, trace_id,
			span_id, fields, field_names, field_types, field_metadata_version,
			collector_id, ingest_source_kind, ingest_source_id, batch_id,
			batch_sequence, expires_at, visibility_seq
		)`)
	if err != nil {
		t.Fatalf("prepare lookup events: %v", err)
	}
	defer func() { _ = batch.Close() }()
	base := time.Date(2026, time.August, 8, 10, 5, 0, 0, time.UTC)
	indexTime := time.Date(2026, time.August, 8, 11, 30, 0, 0, time.UTC)
	expiresAt := time.Date(2099, time.January, 1, 0, 0, 0, 0, time.UTC)
	for index, event := range events {
		document := clickhousedriver.NewJSON()
		document.SetValueAtPath("service_key", clickhousedriver.NewDynamic(event.key))
		fieldNames := []string{"service_key"}
		fieldTypes := []uint8{uint8(event.keyType)}
		if event.priorOwner != nil {
			document.SetValueAtPath("service_owner", clickhousedriver.NewDynamic(*event.priorOwner))
			fieldNames = append(fieldNames, "service_owner")
			fieldTypes = append(fieldTypes, uint8(eventfields.StoredValueTypeString))
		}
		if err := batch.Append(
			event.id,
			runtimeKnowledgeTestTenant,
			"main",
			base.Add(time.Duration(index)*time.Minute),
			indexTime,
			nil,
			uint8(1),
			"lookup-runtime",
			"lookup-runtime",
			"lookup-runtime",
			nil,
			uint8(1),
			nil,
			nil,
			[]byte(event.id),
			uint8(1),
			nil,
			nil,
			document,
			fieldNames,
			fieldTypes,
			eventfields.CurrentFieldMetadataVersion,
			"lookup-runtime",
			uint8(1),
			"lookup-runtime",
			"lookup-runtime-batch",
			uint64(index+1),
			expiresAt,
			uint64(1),
		); err != nil {
			_ = batch.Abort()
			t.Fatalf("append lookup event %q: %v", event.id, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send lookup events: %v", err)
	}
}

func waitForLookupTerminal(
	t *testing.T,
	manager *searchjobs.Manager,
	jobID string,
) searchjobs.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err := manager.Get(jobID)
		if err != nil {
			t.Fatalf("get lookup job %q: %v", jobID, err)
		}
		if job.State.Terminal() {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lookup job %q did not reach a terminal state", jobID)
	return searchjobs.Job{}
}

func requireLookupResults(t *testing.T, page searchjobs.ResultPage) {
	t.Helper()
	if !page.Complete || page.TotalRows != 5 || len(page.Rows) != 5 ||
		len(page.Schema.Columns) != 2 ||
		page.Schema.Columns[0].Name != "event_id" ||
		page.Schema.Columns[1].Name != "service_owner" {
		t.Fatalf("lookup result page = %#v", page)
	}
	want := map[string]*string{
		"lookup-01-exact":         new("platform"),
		"lookup-02-empty-key":     new("empty-match"),
		"lookup-03-present-empty": new(""),
		"lookup-04-number":        nil,
		"lookup-05-case-mismatch": nil,
	}
	for _, row := range page.Rows {
		if len(row.Values) != 2 {
			t.Fatalf("lookup row = %#v", row)
		}
		id, ok := row.Values[0].String()
		if !ok {
			t.Fatalf("lookup event_id = %#v", row.Values[0])
		}
		expected, exists := want[id]
		if !exists {
			t.Fatalf("unexpected lookup event_id %q", id)
		}
		delete(want, id)
		if expected == nil {
			if !row.Values[1].IsNull() {
				t.Fatalf("lookup result %q = %#v, want null", id, row.Values[1])
			}
			continue
		}
		value, valueOK := row.Values[1].String()
		if !valueOK || value != *expected {
			t.Fatalf("lookup result %q = %#v, want %q", id, row.Values[1], *expected)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing lookup results: %#v", want)
	}
}

func requireLookupAutomaticResults(t *testing.T, page searchjobs.ResultPage) {
	t.Helper()
	if !page.Complete || page.TotalRows != 5 || len(page.Rows) != 5 ||
		len(page.Schema.Columns) != 2 ||
		page.Schema.Columns[0].Name != "event_id" ||
		page.Schema.Columns[1].Name != "automatic_owner" {
		t.Fatalf("automatic lookup result page = %#v", page)
	}
	want := map[string]*string{
		"lookup-01-exact":         new("automatic-platform"),
		"lookup-02-empty-key":     new("automatic-empty"),
		"lookup-03-present-empty": new("automatic-platform"),
		"lookup-04-number":        nil,
		"lookup-05-case-mismatch": nil,
	}
	for _, row := range page.Rows {
		if len(row.Values) != 2 {
			t.Fatalf("automatic lookup row = %#v", row)
		}
		id, ok := row.Values[0].String()
		if !ok {
			t.Fatalf("automatic lookup event_id = %#v", row.Values[0])
		}
		expected, exists := want[id]
		if !exists {
			t.Fatalf("unexpected automatic lookup event_id %q", id)
		}
		delete(want, id)
		if expected == nil {
			if !row.Values[1].IsNull() {
				t.Fatalf("automatic lookup result %q = %#v, want null", id, row.Values[1])
			}
			continue
		}
		value, valueOK := row.Values[1].String()
		if !valueOK || value != *expected {
			t.Fatalf(
				"automatic lookup result %q = %#v, want %q",
				id,
				row.Values[1],
				*expected,
			)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing automatic lookup results: %#v", want)
	}
}
