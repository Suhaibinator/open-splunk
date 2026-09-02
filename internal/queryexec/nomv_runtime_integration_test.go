package queryexec

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

// TestNoMVPresentationThroughManagerAgainstClickHouse pins the documented
// nomv runtime (docs/spl.md § nomv, corpus rule SPL-NOMV-001) end to end:
// runtime-typed lists stored through the production Store reach the search
// job schema as typed multivalue cells carrying the newline presentation,
// the presentation follows rename/fields and clears on overwrite, downstream
// grouping consumes the typed members, and a present non-list scalar fails
// the whole job atomically.
func TestNoMVPresentationThroughManagerAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
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
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if closeErr := container.Close(cleanupCtx); closeErr != nil {
			t.Errorf("close ClickHouse nomv container: %v", closeErr)
		}
	})
	queryIntegrationMigrate(t, ctx, container.Name, container.Password)

	options := &clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
	}
	queryConnection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = queryConnection.Close() })
	if err := queryConnection.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	executor, err := New(queryConnection, Config{ReadAdmission: indexread.UnfencedAdmission{}})
	if err != nil {
		t.Fatal(err)
	}

	storeConnection, err := clickhousedriver.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "visibility.sqlite"))
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := sequencer.Close(); closeErr != nil {
			t.Errorf("close nomv visibility sequencer: %v", closeErr)
		}
	})
	store, err := clickhouse.NewStore(
		storeConnection,
		clickhouse.RetentionProviderFunc(func(context.Context, string, string) (time.Duration, error) {
			return 24 * time.Hour, nil
		}),
		sequencer,
	)
	if err != nil {
		_ = storeConnection.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close nomv store: %v", closeErr)
		}
	})

	// The events table enforces expires_at with a physical TTL, so the
	// fixture is anchored to now (second-stable) rather than a fixed date.
	const index = "nomv-runtime"
	indexTime := time.Now().UTC().Truncate(time.Second)
	nomvRuntimeStoreFixture(t, ctx, store, indexTime, index)
	cutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var nextID atomic.Uint64
	executionErrors := make(chan error, 1)
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     queryIntegrationSnapshotter(cutoff),
		Compiler:        clickhouse.Compiler{},
		MaxConcurrent:   1,
		MaxQueued:       1,
		CleanupInterval: -1,
		Now:             func() time.Time { return indexTime.Add(time.Second) },
		NewID: func() string {
			return fmt.Sprintf("nomv-runtime-%02d", nextID.Add(1))
		},
		CursorKey:   []byte("0123456789abcdef0123456789abcdef"),
		CursorScope: "nomv-runtime",
		OnFailure: func(notification searchjobs.FailureNotification) {
			select {
			case executionErrors <- notification.Cause:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	create := func(source string) searchjobs.Job {
		t.Helper()
		created, createErr := manager.Create(ctx, searchjobs.CreateRequest{
			SPL: source, OwnerID: "owner", TenantID: "tenant",
			AuthorizedIndexes: []string{index},
			RequestedIndexes:  []string{index},
			TimeRange: queryIntegrationTimeRange(
				t, indexTime.Add(-time.Hour), indexTime.Add(time.Hour),
			),
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		return queryIntegrationWaitForTerminal(t, manager, created.ID)
	}
	run := func(source string) (searchjobs.Job, searchjobs.ResultPage) {
		t.Helper()
		terminal := create(source)
		if terminal.State != searchjobs.StateCompleted || terminal.Failure != nil {
			t.Fatalf("terminal job for %q = state %v failure %#v", source, terminal.State, terminal.Failure)
		}
		page, resultErr := manager.Results(terminal.ID, searchjobs.PageRequest{Limit: 10})
		if resultErr != nil {
			t.Fatal(resultErr)
		}
		return terminal, page
	}
	lists := `index=` + index + ` source="nomv-lists"`

	t.Run("typed list transport carries the newline presentation", func(t *testing.T) {
		terminal, page := run(lists + ` | nomv values | sort event_id | table event_id,values`)
		if terminal.RowCount != 4 || len(page.Rows) != 4 {
			t.Fatalf("rows = %d/%d, want 4", terminal.RowCount, len(page.Rows))
		}
		queryIntegrationAssertColumns(t, page, []string{"event_id", "values"})
		nomvRuntimeRequireColumn(t, page.Schema.Columns[1], searchjobs.Column{
			Name:                       "values",
			Kind:                       searchjobs.ValueKindList,
			Nullable:                   true,
			Multivalue:                 true,
			FlatMultivalueDelimiter:    "\n",
			HasFlatMultivalueDelimiter: true,
		})
		nomvRuntimeRequireCells(t, page, 1, map[string][]string{
			"l-1": {"alpha", "beta"},
			"l-2": {"beta", "gamma"},
		}, "l-3")
	})

	t.Run("presentation follows rename and fields", func(t *testing.T) {
		// fields keeps the implicit _time and friends, so locate the renamed
		// column by name instead of position.
		_, page := run(lists + ` | nomv values | rename values AS labels | sort event_id | fields event_id,labels`)
		labels := queryIntegrationColumnIndex(t, page, "labels")
		if queryIntegrationColumnIndex(t, page, "event_id") != 0 {
			t.Fatalf("event_id is not the first column: %#v", page.Schema)
		}
		nomvRuntimeRequireColumn(t, page.Schema.Columns[labels], searchjobs.Column{
			Name:                       "labels",
			Kind:                       searchjobs.ValueKindList,
			Nullable:                   true,
			Multivalue:                 true,
			FlatMultivalueDelimiter:    "\n",
			HasFlatMultivalueDelimiter: true,
		})
		nomvRuntimeRequireCells(t, page, labels, map[string][]string{
			"l-1": {"alpha", "beta"},
			"l-2": {"beta", "gamma"},
		}, "l-3")
	})

	t.Run("overwrite clears the presentation and a later nomv reapplies it", func(t *testing.T) {
		_, cleared := run(lists +
			` | nomv values | eval values=mvappend("new", "member") | sort event_id | table event_id,values`)
		queryIntegrationAssertColumns(t, cleared, []string{"event_id", "values"})
		column := cleared.Schema.Columns[1]
		if column.HasFlatMultivalueDelimiter || column.FlatMultivalueDelimiter != "" ||
			column.Kind != searchjobs.ValueKindList || !column.Multivalue {
			t.Fatalf("overwritten column = %#v, want a typed list without presentation", column)
		}
		nomvRuntimeRequireCells(t, cleared, 1, map[string][]string{
			"l-1": {"new", "member"},
			"l-2": {"new", "member"},
			"l-3": {"new", "member"},
			"l-4": {"new", "member"},
		})

		_, reapplied := run(lists +
			` | nomv values | eval values=mvappend("new", "member") | nomv values | sort event_id | table event_id,values`)
		queryIntegrationAssertColumns(t, reapplied, []string{"event_id", "values"})
		column = reapplied.Schema.Columns[1]
		if !column.HasFlatMultivalueDelimiter || column.FlatMultivalueDelimiter != "\n" ||
			column.Kind != searchjobs.ValueKindList || !column.Multivalue {
			t.Fatalf("reapplied column = %#v, want the newline presentation", column)
		}
	})

	t.Run("downstream grouping consumes the typed members", func(t *testing.T) {
		terminal, page := run(lists + ` | nomv values | stats count AS events BY values | sort 0 +values`)
		if terminal.RowCount != 3 || len(page.Rows) != 3 {
			t.Fatalf("grouped rows = %d/%d, want 3", terminal.RowCount, len(page.Rows))
		}
		queryIntegrationAssertColumns(t, page, []string{"values", "events"})
		if page.Schema.Columns[0].HasFlatMultivalueDelimiter || page.Schema.Columns[0].Multivalue {
			t.Fatalf("group column = %#v, want a scalar key without presentation", page.Schema.Columns[0])
		}
		got := make(map[string]uint64, len(page.Rows))
		for _, row := range page.Rows {
			member, ok := row.Values[0].String()
			if !ok {
				t.Fatalf("group key = %#v, want string", row.Values[0])
			}
			events, ok := row.Values[1].Unsigned()
			if !ok {
				t.Fatalf("group count = %#v, want unsigned", row.Values[1])
			}
			got[member] = events
		}
		want := map[string]uint64{"alpha": 1, "beta": 2, "gamma": 1}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("grouped counts = %#v, want %#v", got, want)
		}
	})

	t.Run("present scalar fails the job atomically", func(t *testing.T) {
		terminal := create(`index=` + index + ` source="nomv-scalar" | nomv values | sort event_id | table event_id,values`)
		if terminal.State != searchjobs.StateFailed || terminal.RowCount != 0 || terminal.Schema != nil ||
			terminal.ResultsTruncated || terminal.Failure == nil ||
			terminal.Failure.Code != searchjobs.FailureUnsupportedSPL {
			t.Fatalf("atomic failure terminal = %#v", terminal)
		}
		if _, resultErr := manager.Results(terminal.ID, searchjobs.PageRequest{Limit: 10}); !errors.Is(resultErr, searchjobs.ErrResultsUnavailable) {
			t.Fatalf("failed result access = %v, want ErrResultsUnavailable", resultErr)
		}
		select {
		case cause := <-executionErrors:
			if !errors.Is(cause, searchjobs.ErrUnsupportedValue) {
				t.Fatalf("execution cause = %v, want ErrUnsupportedValue", cause)
			}
		default:
			t.Fatal("manager did not report the unsupported nomv value cause")
		}
	})
}

func nomvRuntimeStoreFixture(
	t *testing.T,
	ctx context.Context,
	store *clickhouse.Store,
	indexTime time.Time,
	index string,
) {
	t.Helper()
	list := func(members ...string) *opensplunk.TypedValue {
		values := make([]*opensplunk.TypedValue, len(members))
		for position, member := range members {
			values[position] = authoredExpressionVerticalString(member)
		}
		return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_ListValue{
			ListValue: &opensplunk.TypedValueList{Values: values},
		}}
	}
	event := func(id, source string, fields ...*opensplunk.TypedObjectField) *ingest.StoredEvent {
		eventTime := indexTime.Add(-time.Minute)
		message := "nomv runtime fixture"
		return &ingest.StoredEvent{
			TenantID: "tenant", CollectorID: "nomv-collector",
			BatchID: "nomv-batch", IndexTime: indexTime,
			Event: &opensplunk.LogEvent{
				EventId: id, IndexName: index, EventTime: timestamppb.New(eventTime),
				CollectedAt:     timestamppb.New(eventTime.Add(time.Second)),
				EventTimeSource: opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
				Host:            "nomv-host", Source: source, Sourcetype: "open-splunk:nomv",
				Severity: opensplunk.LogSeverity_LOG_SEVERITY_INFO, Message: &message,
				Raw: []byte(message), RawEncoding: opensplunk.RawEncoding_RAW_ENCODING_UTF8,
				Fields: &opensplunk.TypedObject{Fields: fields},
			},
		}
	}
	events := []*ingest.StoredEvent{
		event("l-1", "nomv-lists",
			authoredExpressionVerticalField("values", list("alpha", "beta")),
		),
		event("l-2", "nomv-lists",
			authoredExpressionVerticalField("values", list("beta", "gamma")),
		),
		// l-3 has no values field at all; l-4 carries an explicit null. The
		// two must stay distinct through the typed transport.
		event("l-3", "nomv-lists",
			authoredExpressionVerticalField("label", authoredExpressionVerticalString("missing")),
		),
		event("l-4", "nomv-lists",
			authoredExpressionVerticalField("values", authoredExpressionVerticalNull()),
		),
		event("s-1", "nomv-scalar",
			authoredExpressionVerticalField("values", list("x")),
		),
		event("s-2", "nomv-scalar",
			authoredExpressionVerticalField("values", authoredExpressionVerticalString("scalar")),
		),
	}
	digest := sha256.Sum256([]byte("nomv-runtime-fixture"))
	result, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        "nomv-collector",
		BatchID:            "nomv-batch",
		BatchSequence:      132,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  digest,
		ReceivedAt:         indexTime,
		Events:             events,
		RetentionByIndex:   map[string]time.Duration{index: 24 * time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Accepted != uint32(len(events)) || result.Duplicate != 0 {
		t.Fatalf("store result = %#v", result)
	}
}

func nomvRuntimeRequireColumn(t *testing.T, got, want searchjobs.Column) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("column = %#v, want %#v", got, want)
	}
	if !got.ValidFlatMultivaluePresentation() {
		t.Fatalf("column %#v is not a valid flat multivalue presentation", got)
	}
}

// nomvRuntimeRequireCells asserts the typed list members per event ID in the
// given column; missingIDs must be absent (not null) and every remaining
// fixture row must be an explicit null.
func nomvRuntimeRequireCells(
	t *testing.T,
	page searchjobs.ResultPage,
	column int,
	wantLists map[string][]string,
	missingIDs ...string,
) {
	t.Helper()
	seen := make(map[string]struct{}, len(page.Rows))
	for _, row := range page.Rows {
		id, ok := row.Values[0].String()
		if !ok {
			t.Fatalf("event_id = %#v, want string", row.Values[0])
		}
		seen[id] = struct{}{}
		cell := row.Values[column]
		if wantMembers, expected := wantLists[id]; expected {
			members, isList := cell.List()
			if !isList {
				t.Fatalf("%s cell = %#v, want typed list", id, cell)
			}
			got := make([]string, len(members))
			for position, member := range members {
				text, isString := member.String()
				if !isString {
					t.Fatalf("%s member %d = %#v, want string", id, position, member)
				}
				got[position] = text
			}
			if !reflect.DeepEqual(got, wantMembers) {
				t.Fatalf("%s members = %#v, want %#v", id, got, wantMembers)
			}
			continue
		}
		wantMissing := false
		for _, missing := range missingIDs {
			wantMissing = wantMissing || missing == id
		}
		switch {
		case wantMissing && !cell.IsMissing():
			t.Fatalf("%s cell = %#v, want missing", id, cell)
		case !wantMissing && !cell.IsNull():
			t.Fatalf("%s cell = %#v, want explicit null", id, cell)
		}
	}
	for id := range wantLists {
		if _, ok := seen[id]; !ok {
			t.Fatalf("page omits %s: %#v", id, page.Rows)
		}
	}
}
