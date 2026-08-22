package queryexec

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestStatsByFixedMultivalueStringOrBytesAgainstClickHouse pins the complete
// production boundary for a fixed Array(String) stats result feeding stats BY:
// ClickHouse preserves an invalid UTF-8 member, the executor publishes that
// scalar as Bytes under a compiler-sealed Mixed schema, and the atomic manager
// commits the complete result while downstream UTF-8 functions fail closed.
func TestStatsByFixedMultivalueStringOrBytesAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
			t.Errorf("close stats-BY String-or-Bytes container: %v", closeErr)
		}
	})
	queryIntegrationMigrate(t, ctx, container.Name, container.Password)

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{container.Address},
		Auth: clickhousedriver.Auth{
			Database: container.Database,
			Username: container.Username,
			Password: container.Password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	indexTime := time.Date(2026, time.August, 12, 21, 0, 0, 0, time.UTC)
	invalid := []byte{0xff, 'a', 's', 'c', 'i', 'i'}
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
		t.Fatalf("prepare stats-BY String-or-Bytes fixture: %v", err)
	}
	for index, host := range []string{"ASCII", string(invalid)} {
		if err := batch.Append(
			"stats-by-string-or-bytes-"+string(rune('a'+index)),
			"tenant",
			"byte-transport",
			indexTime,
			indexTime,
			nil,
			uint8(1),
			host,
			"stats-by-string-or-bytes",
			"test",
			nil,
			uint8(1),
			nil,
			nil,
			[]byte("fixture"),
			uint8(1),
			nil,
			nil,
			clickhousedriver.NewJSON(),
			[]string{},
			[]uint8{},
			eventfields.CurrentFieldMetadataVersion,
			"stats-by-string-or-bytes",
			uint8(1),
			"stats-by-string-or-bytes",
			"batch",
			uint64(index+1),
			clickhouse.MaximumSearchTime(),
			uint64(1),
		); err != nil {
			_ = batch.Abort()
			t.Fatalf("append stats-BY String-or-Bytes fixture %d: %v", index, err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send stats-BY String-or-Bytes fixtures: %v", err)
	}

	executor, err := New(connection, Config{ReadAdmission: indexread.UnfencedAdmission{}})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        executor,
		Snapshotter:     queryIntegrationSnapshotter(1),
		Compiler:        clickhouse.Compiler{},
		MaxConcurrent:   1,
		MaxQueued:       1,
		CleanupInterval: -1,
		Now:             func() time.Time { return indexTime.Add(time.Millisecond) },
		NewID:           func() string { return "stats-by-string-or-bytes" },
		CursorKey:       []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })

	job, err := manager.Create(ctx, searchjobs.CreateRequest{
		SPL: `index=byte-transport | stats values(host) AS members` +
			` | stats count BY members` +
			` | eval lowered=lower(members), measured=len(members),` +
			` piece=substr(members,1,1)` +
			` | table members count lowered measured piece`,
		OwnerID:           "owner",
		TenantID:          "tenant",
		AuthorizedIndexes: []string{"byte-transport"},
		RequestedIndexes:  []string{"byte-transport"},
		TimeRange: queryIntegrationTimeRange(
			t,
			indexTime.Add(-time.Hour),
			indexTime.Add(time.Hour),
		),
	})
	if err != nil {
		t.Fatalf("create stats-BY String-or-Bytes search: %v", err)
	}
	completed := queryIntegrationWaitForTerminal(t, manager, job.ID)
	if completed.State != searchjobs.StateCompleted {
		t.Fatalf("job state = %v, failure=%#v", completed.State, completed.Failure)
	}
	page, err := manager.Results(job.ID, searchjobs.PageRequest{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Schema.Columns) != 5 ||
		page.Schema.Columns[0] != (searchjobs.Column{
			Name: "members", Kind: searchjobs.ValueKindMixed, Nullable: true,
		}) || len(page.Rows) != 2 || !page.Complete {
		t.Fatalf("page shape = schema %#v, rows %d, complete=%t", page.Schema, len(page.Rows), page.Complete)
	}
	var sawString, sawBytes bool
	for _, row := range page.Rows {
		if count, ok := row.Values[1].Unsigned(); !ok || count != 1 {
			t.Fatalf("stats-BY count = %#v, want unsigned 1", row.Values[1])
		}
		if member, ok := row.Values[0].String(); ok {
			sawString = true
			if member != "ASCII" {
				t.Fatalf("String member = %q, want ASCII", member)
			}
			if lowered, ok := row.Values[2].String(); !ok || lowered != "ascii" {
				t.Fatalf("valid lower = %#v", row.Values[2])
			}
			if measured, ok := row.Values[3].Unsigned(); !ok || measured != 5 {
				t.Fatalf("valid len = %#v", row.Values[3])
			}
			if piece, ok := row.Values[4].String(); !ok || piece != "A" {
				t.Fatalf("valid substr = %#v", row.Values[4])
			}
			continue
		}
		member, ok := row.Values[0].Bytes()
		if !ok || !bytes.Equal(member, invalid) {
			t.Fatalf("byte member = %#v, want %v", row.Values[0], invalid)
		}
		sawBytes = true
		for index := 2; index < len(row.Values); index++ {
			if !row.Values[index].IsNull() {
				t.Fatalf("invalid member derived cell %d = %#v, want null", index, row.Values[index])
			}
		}
	}
	if !sawString || !sawBytes {
		t.Fatalf("published String/Bytes domains = %t/%t", sawString, sawBytes)
	}
}
