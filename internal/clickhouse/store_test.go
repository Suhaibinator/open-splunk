package clickhouse

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStoreNativeBatchContractAndEventOrder(t *testing.T) {
	t.Parallel()
	indexTime := time.Date(2026, 7, 21, 3, 4, 6, 987654321, time.FixedZone("offset", -7*60*60))
	committedAt := time.Date(2026, 7, 21, 10, 4, 8, 999999999, time.FixedZone("commit", 2*60*60))
	conn := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	retention := &fakeRetentionProvider{periods: map[string]time.Duration{"main": 72 * time.Hour}}
	store := mustTestStore(t, conn, retention)
	store.clock = func() time.Time { return committedAt }

	first := testStoredEvent("event-2", "main", indexTime)
	first.Event.Raw = []byte{0xff, 0, 'r', 'a', 'w'}
	first.Event.Service = stringPointer("")
	first.Event.Level = nil
	first.Event.Message = stringPointer("")
	first.Event.TraceId = nil
	first.Event.SpanId = stringPointer("")
	second := testStoredEvent("event-1", "main", indexTime.Add(time.Second))
	sequence := uint64(19)
	input := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "batch", BatchSequence: sequence,
		Events: []*ingest.StoredEvent{first, second},
	}
	result, err := store.Store(context.Background(), input)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if conn.prepareCalls != 1 || conn.query != eventsInsertSQL || strings.Contains(conn.query, "?") {
		t.Fatalf("native prepare contract calls=%d query=%q", conn.prepareCalls, conn.query)
	}
	if conn.maxVisibilityCalls != 1 || conn.maxVisibilityQuery != buildMaxVisibilitySQL("open_splunk", "events") {
		t.Fatalf("visibility lookup calls=%d query=%q", conn.maxVisibilityCalls, conn.maxVisibilityQuery)
	}
	wantSettings := map[string]any{
		"async_insert": uint8(0), "wait_for_async_insert": uint8(1),
		"insert_deduplication_token":                                             deduplicationToken(input),
		"input_format_json_read_numbers_as_strings":                              uint8(0),
		"input_format_json_read_bools_as_numbers":                                uint8(0),
		"input_format_json_read_bools_as_strings":                                uint8(0),
		"input_format_json_infer_array_of_dynamic_from_array_of_different_types": uint8(1),
		"input_format_try_infer_dates":                                           uint8(0),
		"input_format_try_infer_datetimes":                                       uint8(0),
	}
	for name, want := range wantSettings {
		if got := conn.settings[name]; !reflect.DeepEqual(got, want) {
			t.Errorf("setting %s = %#v (%T), want %#v", name, got, got, want)
		}
	}
	if len(conn.batch.rows) != 2 {
		t.Fatalf("rows = %d", len(conn.batch.rows))
	}
	if got := []string{conn.batch.rows[0][0].(string), conn.batch.rows[1][0].(string)}; !slices.Equal(got, []string{"event-2", "event-1"}) {
		t.Fatalf("event order = %v", got)
	}
	if got, ok := conn.batch.rows[0][14].([]byte); !ok || !slices.Equal(got, first.Event.Raw) {
		t.Fatalf("raw = %#v (%T), want byte-safe []byte", conn.batch.rows[0][14], conn.batch.rows[0][14])
	}
	for _, column := range []int{3, 4, 5, 23} {
		value, ok := conn.batch.rows[0][column].(time.Time)
		if !ok || value.Location() != time.UTC {
			t.Errorf("time column %d = %#v (%T), want UTC time.Time", column, conn.batch.rows[0][column], conn.batch.rows[0][column])
		}
	}
	assertOptionalString(t, conn.batch.rows[0][10], true, "")
	assertOptionalString(t, conn.batch.rows[0][12], false, "")
	assertOptionalString(t, conn.batch.rows[0][13], true, "")
	assertOptionalString(t, conn.batch.rows[0][16], false, "")
	assertOptionalString(t, conn.batch.rows[0][17], true, "")
	wantIndexTime := indexTime.UTC().Truncate(time.Millisecond)
	if got := conn.batch.rows[0][4]; got != wantIndexTime {
		t.Fatalf("index_time = %v, want %v", got, wantIndexTime)
	}
	if got := conn.batch.rows[0][23]; got != wantIndexTime.Add(72*time.Hour) {
		t.Fatalf("expires_at = %v", got)
	}
	if got := conn.batch.rows[0][24]; got != uint64(1) {
		t.Fatalf("visibility_seq = %#v, want 1", got)
	}
	if conn.batch.sendCalls != 1 || conn.batch.abortCalls != 0 || conn.batch.closeCalls != 1 {
		t.Fatalf("batch lifecycle send=%d abort=%d close=%d", conn.batch.sendCalls, conn.batch.abortCalls, conn.batch.closeCalls)
	}
	if result.Accepted != 2 || result.Duplicate != 0 || result.AcknowledgedThrough == nil || *result.AcknowledgedThrough != sequence {
		t.Fatalf("result = %+v", result)
	}
	if result.CommittedAt != committedAt.UTC() {
		t.Fatalf("committed_at = %v", result.CommittedAt)
	}
	if !slices.Equal(retention.calls, []string{"tenant/main"}) {
		t.Fatalf("retention calls = %v", retention.calls)
	}
}

func TestStoreAssignsCommitOrderedVisibilityAndCapturesCutoff(t *testing.T) {
	t.Parallel()
	conn := &fakeStoreConnection{batch: &fakeWriteBatch{}, maxVisibility: 9}
	store := mustTestStore(t, conn, fixedRetention(time.Hour))

	if _, err := store.Store(context.Background(), validStoreBatch()); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if got := conn.batch.rows[0][24]; got != uint64(10) {
		t.Fatalf("stored visibility = %#v, want 10", got)
	}
	conn.maxVisibility = 10
	cutoff, err := store.VisibilityCutoff(context.Background())
	if err != nil {
		t.Fatalf("VisibilityCutoff: %v", err)
	}
	if cutoff != 10 || conn.maxVisibilityCalls != 2 {
		t.Fatalf("cutoff=%d calls=%d, want 10 and 2", cutoff, conn.maxVisibilityCalls)
	}
}

func TestVisibilityLookupFailureIsClassified(t *testing.T) {
	t.Parallel()
	connectionErr := &net.OpError{Op: "read", Net: "tcp", Err: io.EOF}
	conn := &fakeStoreConnection{maxVisibilityErr: connectionErr}
	store := mustTestStore(t, conn, fixedRetention(time.Hour))

	if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
		t.Fatalf("Store error = %v, want transient visibility lookup failure", err)
	}
	if _, err := store.VisibilityCutoff(context.Background()); !isTransient(err) {
		t.Fatalf("VisibilityCutoff error = %v, want transient failure", err)
	}
}

func TestConvertTypedObjectPreservesTypesTagsAndEscapedNames(t *testing.T) {
	t.Parallel()
	timestamp := time.Date(2026, 7, 21, 3, 4, 5, 123456789, time.UTC)
	object := typedObjectValue(
		typedField("unsigned", typedUint(^uint64(0))),
		typedField("signed", typedSint(-1<<63)),
		typedField("ratio", typedDouble(1.25)),
		typedField("ok", typedBool(true)),
		typedField("nothing", typedNull()),
		typedField("text", typedString("2026-07-21T03:04:05Z")),
		typedField("literal.dot", typedString("literal")),
		typedField("percent%2Ekey", typedString("percent")),
		typedField("nested", typedObject(typedField("slash\\key", typedString("kept")), typedField("nil", typedNull()))),
		typedField("mixed", typedList(typedSint(1), typedString("two"), typedBool(true), typedNull(), typedObject(typedField("inside.dot", typedUint(3))))),
		typedField("bytes", typedBytes([]byte{0, 0xff, 0x10})),
		typedField("timestamp", typedTimestamp(timestamp)),
		typedField("duration", typedDuration(3*time.Second+4*time.Nanosecond)),
		typedField("decimal", typedDecimal("-12345678901234567890.00100e+12")),
	)
	document, names, err := convertTypedObject(object)
	if err != nil {
		t.Fatalf("convertTypedObject: %v", err)
	}
	wantNames := []string{
		"bytes", "decimal", "duration", "literal\\.dot", "mixed", "nested.nil", "nested.slash\\\\key",
		"nothing", "ok", "percent%2Ekey", "ratio", "signed", "text", "timestamp", "unsigned",
	}
	if !slices.IsSorted(names) || !slices.Equal(names, wantNames) {
		t.Fatalf("field_names = %#v, want %#v", names, wantNames)
	}
	assertJSONPath(t, document, "signed", int64(-1<<63))
	assertJSONPath(t, document, "unsigned", ^uint64(0))
	assertJSONPath(t, document, "ratio", 1.25)
	ratioValue, ratioExists := document.ValueAtPath("ratio")
	ratioDynamic, ratioTyped := ratioValue.(clickhousedriver.Dynamic)
	if !ratioExists || !ratioTyped || ratioDynamic.Type() != "Float64" {
		t.Fatalf("ratio did not retain forced Float64 type: %#v", ratioValue)
	}
	assertJSONPath(t, document, "ok", true)
	assertJSONPath(t, document, "nothing", nil)
	assertJSONPath(t, document, "text", "2026-07-21T03:04:05Z")
	assertJSONPath(t, document, "literal%2Edot", "literal")
	assertJSONPath(t, document, "percent%252Ekey", "percent")
	assertJSONPath(t, document, "nested.slash\\key", "kept")
	assertJSONPath(t, document, "nested.nil", nil)

	value, _ := document.ValueAtPath("mixed")
	mixed, ok := value.(clickhousedriver.Dynamic)
	if !ok || mixed.Type() != "Array(Dynamic)" {
		t.Fatalf("mixed = %#v (%T)", value, value)
	}
	items, ok := mixed.Any().([]clickhousedriver.Dynamic)
	if !ok || len(items) != 5 || items[0].Any() != int64(1) || items[1].Any() != "two" || !items[3].Nil() {
		t.Fatalf("mixed payload = %#v", mixed.Any())
	}
	itemObject, ok := items[4].Any().(map[string]clickhousedriver.Dynamic)
	if !ok || itemObject["inside.dot"].Any() != uint64(3) {
		t.Fatalf("list object = %#v", items[4].Any())
	}
	assertTagged(t, document, "bytes", "bytes/v1", "AP8Q")
	assertTagged(t, document, "timestamp", "timestamp/v1", "2026-07-21T03:04:05.123456789Z")
	assertTagged(t, document, "duration", "duration/v1", "3:4")
	assertTagged(t, document, "decimal", "decimal/v1", "-12345678901234567890.00100e+12")

	nativeColumn, err := column.Type("JSON(max_dynamic_paths=256, max_dynamic_types=16)").Column("fields", &column.ServerContext{
		VersionMajor: 26, VersionMinor: 3, VersionPatch: 17, Timezone: time.UTC,
	})
	if err != nil {
		t.Fatalf("construct native JSON column: %v", err)
	}
	if err := nativeColumn.AppendRow(document); err != nil {
		t.Fatalf("native JSON driver rejected converted value: %v", err)
	}
}

func TestConvertTypedObjectAvoidsDottedPathCollisions(t *testing.T) {
	t.Parallel()
	object := typedObjectValue(
		typedField("a.b", typedString("literal dot")),
		typedField("a%2Eb", typedString("escape-looking")),
		typedField("a", typedObject(typedField("b", typedString("nested")))),
	)
	document, names, err := convertTypedObject(object)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONPath(t, document, "a%2Eb", "literal dot")
	assertJSONPath(t, document, "a%252Eb", "escape-looking")
	assertJSONPath(t, document, "a.b", "nested")
	if !slices.Equal(names, []string{"a%2Eb", "a.b", "a\\.b"}) {
		t.Fatalf("field_names = %#v", names)
	}
}

func TestPhysicalJSONPathEncodingContractForCompiler(t *testing.T) {
	t.Parallel()
	for source, want := range map[string]string{
		"plain":       "plain",
		"literal.dot": "literal%2Edot",
		"percent%2E":  "percent%252E",
		"%":           "%25",
	} {
		if got := encodePhysicalPathSegment(source); got != want {
			t.Errorf("encodePhysicalPathSegment(%q) = %q, want %q", source, got, want)
		}
	}
}

func TestDeduplicationTokenStableAndLengthFramed(t *testing.T) {
	t.Parallel()
	base := ingest.StoreBatch{TenantID: "tenant", CollectorID: "collector", BatchID: "batch", BatchSequence: 1}
	first := deduplicationToken(base)
	if deduplicationToken(base) != first || !strings.HasPrefix(first, "open-splunk-ingest-v1-") || len(first) != len("open-splunk-ingest-v1-")+64 {
		t.Fatalf("unstable or malformed token %q", first)
	}
	changed := []ingest.StoreBatch{
		{TenantID: "tenant2", CollectorID: "collector", BatchID: "batch"},
		{TenantID: "tenant", CollectorID: "collector2", BatchID: "batch"},
		{TenantID: "tenant", CollectorID: "collector", BatchID: "batch2"},
	}
	for _, candidate := range changed {
		if deduplicationToken(candidate) == first {
			t.Fatalf("token collision for %+v", candidate)
		}
	}
	a := deduplicationToken(ingest.StoreBatch{TenantID: "ab", CollectorID: "c", BatchID: "d"})
	b := deduplicationToken(ingest.StoreBatch{TenantID: "a", CollectorID: "bc", BatchID: "d"})
	if a == b {
		t.Fatal("unframed tuple collision")
	}
}

func TestStoreRetentionLookupIsCachedPerIndex(t *testing.T) {
	t.Parallel()
	conn := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	provider := &fakeRetentionProvider{periods: map[string]time.Duration{"main": time.Hour, "audit": 30 * 24 * time.Hour}}
	store := mustTestStore(t, conn, provider)
	base := time.Date(2026, 7, 21, 1, 2, 3, 456789123, time.UTC)
	_, err := store.Store(context.Background(), ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "batch", BatchSequence: 7,
		Events: []*ingest.StoredEvent{
			testStoredEvent("one", "main", base),
			testStoredEvent("two", "audit", base.Add(time.Minute)),
			testStoredEvent("three", "main", base.Add(2*time.Minute)),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(provider.calls, []string{"tenant/main", "tenant/audit"}) {
		t.Fatalf("retention calls = %v", provider.calls)
	}
	wants := []time.Time{
		base.Truncate(time.Millisecond).Add(time.Hour),
		base.Add(time.Minute).Truncate(time.Millisecond).Add(30 * 24 * time.Hour),
		base.Add(2 * time.Minute).Truncate(time.Millisecond).Add(time.Hour),
	}
	for i, want := range wants {
		if got := conn.batch.rows[i][23]; got != want {
			t.Errorf("row %d expires_at = %v, want %v", i, got, want)
		}
	}
}

func TestStoreClassifiesErrorsAndReleasesBatch(t *testing.T) {
	valid := validStoreBatch()
	tests := []struct {
		name       string
		prepareErr error
		sendErr    error
		wantReason opensplunkv1.RetryBatchReason
		permanent  bool
	}{
		{name: "network", prepareErr: &net.OpError{Op: "dial", Net: "tcp", Err: io.EOF}, wantReason: opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE},
		{name: "pool busy", prepareErr: clickhousedriver.ErrAcquireConnTimeout, wantReason: opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY},
		{name: "rate limited", prepareErr: &clickhousedriver.Exception{Code: 364, Name: "RECEIVED_ERROR_TOO_MANY_REQUESTS"}, wantReason: opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED},
		{name: "send EOF", sendErr: io.ErrUnexpectedEOF, wantReason: opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE},
		{name: "schema", prepareErr: &clickhousedriver.Exception{Code: 60, Name: "UNKNOWN_TABLE"}, permanent: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			batch := &fakeWriteBatch{sendErr: test.sendErr}
			conn := &fakeStoreConnection{batch: batch, prepareErr: test.prepareErr}
			store := mustTestStore(t, conn, fixedRetention(time.Hour))
			_, err := store.Store(context.Background(), valid)
			if err == nil {
				t.Fatal("Store succeeded")
			}
			if test.permanent {
				if isTransient(err) {
					t.Fatalf("permanent error wrapped as transient: %v", err)
				}
			} else {
				assertTransient(t, err, test.wantReason)
			}
			if test.sendErr != nil && (batch.sendCalls != 1 || batch.abortCalls != 1 || batch.closeCalls != 1) {
				t.Fatalf("send lifecycle send=%d abort=%d close=%d", batch.sendCalls, batch.abortCalls, batch.closeCalls)
			}
		})
	}

	t.Run("append permanent", func(t *testing.T) {
		batch := &fakeWriteBatch{appendErr: errors.New("bad native value")}
		store := mustTestStore(t, &fakeStoreConnection{batch: batch}, fixedRetention(time.Hour))
		_, err := store.Store(context.Background(), valid)
		if err == nil || isTransient(err) || batch.abortCalls != 1 || batch.closeCalls != 1 || batch.sendCalls != 0 {
			t.Fatalf("err=%v send=%d abort=%d close=%d", err, batch.sendCalls, batch.abortCalls, batch.closeCalls)
		}
	})
	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		store := mustTestStore(t, &fakeStoreConnection{prepareErr: context.Canceled}, fixedRetention(time.Hour))
		_, err := store.Store(ctx, valid)
		if !errors.Is(err, context.Canceled) || isTransient(err) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestStoreRejectsInvalidInputsBeforePrepare(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		batch     ingest.StoreBatch
		retention RetentionProvider
	}{
		{name: "empty", batch: ingest.StoreBatch{TenantID: "tenant", CollectorID: "collector", BatchID: "batch"}, retention: fixedRetention(time.Hour)},
		{name: "missing tenant", batch: ingest.StoreBatch{CollectorID: "collector", BatchID: "batch", Events: []*ingest.StoredEvent{testStoredEvent("e", "main", time.Now())}}, retention: fixedRetention(time.Hour)},
		{name: "nil event", batch: ingest.StoreBatch{TenantID: "tenant", CollectorID: "collector", BatchID: "batch", Events: []*ingest.StoredEvent{nil}}, retention: fixedRetention(time.Hour)},
		{name: "zero retention", batch: validStoreBatch(), retention: fixedRetention(0)},
	}
	mismatch := validStoreBatch()
	mismatch.Events[0].TenantID = "other"
	tests = append(tests, struct {
		name      string
		batch     ingest.StoreBatch
		retention RetentionProvider
	}{name: "metadata mismatch", batch: mismatch, retention: fixedRetention(time.Hour)})
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			conn := &fakeStoreConnection{batch: &fakeWriteBatch{}}
			store := mustTestStore(t, conn, test.retention)
			if _, err := store.Store(context.Background(), test.batch); err == nil {
				t.Fatal("Store succeeded")
			}
			if conn.prepareCalls != 0 {
				t.Fatalf("prepare calls = %d", conn.prepareCalls)
			}
		})
	}
}

func TestConfigAndConnectionLifecycle(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	if !slices.Equal(config.Addresses, []string{"127.0.0.1:9000"}) || config.Database != "open_splunk" || config.Table != "events" {
		t.Fatalf("DefaultConfig = %+v", config)
	}
	tlsConfig := &tls.Config{ServerName: "clickhouse.example", MinVersion: tls.VersionTLS13}
	config.TLS = tlsConfig
	options, normalized, err := config.clickHouseOptions()
	if err != nil {
		t.Fatal(err)
	}
	if options.Protocol != clickhousedriver.Native || options.TLS == tlsConfig || options.TLS.ServerName != tlsConfig.ServerName ||
		options.Compression == nil || options.Compression.Method != clickhousedriver.CompressionLZ4 || normalized.RetryAfter <= 0 {
		t.Fatalf("unsafe options/config: %+v / %+v", options, normalized)
	}
	invalid := DefaultConfig()
	invalid.Addresses = []string{"not-a-host-port"}
	if _, _, err := invalid.clickHouseOptions(); err == nil {
		t.Fatal("invalid address accepted")
	}
	invalid = DefaultConfig()
	invalid.Password = "very-secret"
	invalid.Table = "events; DROP"
	if _, _, err := invalid.clickHouseOptions(); err == nil || strings.Contains(err.Error(), invalid.Password) {
		t.Fatalf("unsafe config error = %v", err)
	}
	conn := &fakeStoreConnection{batch: &fakeWriteBatch{}, pingErr: errors.New("ping failed"), closeErr: errors.New("close failed")}
	store := mustTestStore(t, conn, fixedRetention(time.Hour))
	if err := store.Ping(context.Background()); !errors.Is(err, conn.pingErr) {
		t.Fatalf("Ping = %v", err)
	}
	if err := store.Close(); !errors.Is(err, conn.closeErr) {
		t.Fatalf("Close = %v", err)
	}
}

type fakeStoreConnection struct {
	prepareCalls       int
	query              string
	settings           clickhousedriver.Settings
	prepareErr         error
	batch              *fakeWriteBatch
	pingErr, closeErr  error
	maxVisibilityCalls int
	maxVisibilityQuery string
	maxVisibility      uint64
	maxVisibilityErr   error
}

func (c *fakeStoreConnection) prepare(_ context.Context, query string, settings clickhousedriver.Settings) (writeBatch, error) {
	c.prepareCalls++
	c.query = query
	c.settings = make(clickhousedriver.Settings, len(settings))
	for key, value := range settings {
		c.settings[key] = value
	}
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	if c.batch == nil {
		c.batch = &fakeWriteBatch{}
	}
	return c.batch, nil
}
func (c *fakeStoreConnection) maxVisibilitySequence(_ context.Context, query string) (uint64, error) {
	c.maxVisibilityCalls++
	c.maxVisibilityQuery = query
	return c.maxVisibility, c.maxVisibilityErr
}
func (c *fakeStoreConnection) Ping(context.Context) error { return c.pingErr }
func (c *fakeStoreConnection) Close() error               { return c.closeErr }

type fakeWriteBatch struct {
	rows                              [][]any
	appendErr, sendErr                error
	sendCalls, abortCalls, closeCalls int
}

func (b *fakeWriteBatch) Append(values ...any) error {
	if b.appendErr != nil {
		return b.appendErr
	}
	b.rows = append(b.rows, append([]any(nil), values...))
	return nil
}
func (b *fakeWriteBatch) Send() error  { b.sendCalls++; return b.sendErr }
func (b *fakeWriteBatch) Abort() error { b.abortCalls++; return nil }
func (b *fakeWriteBatch) Close() error { b.closeCalls++; return nil }

type fakeRetentionProvider struct {
	periods map[string]time.Duration
	calls   []string
	err     error
}

func (p *fakeRetentionProvider) RetentionForIndex(_ context.Context, tenant, index string) (time.Duration, error) {
	p.calls = append(p.calls, tenant+"/"+index)
	if p.err != nil {
		return 0, p.err
	}
	return p.periods[index], nil
}

func fixedRetention(period time.Duration) RetentionProvider {
	return RetentionProviderFunc(func(context.Context, string, string) (time.Duration, error) { return period, nil })
}
func mustTestStore(t *testing.T, conn storeConnection, retention RetentionProvider) *Store {
	t.Helper()
	store, err := newStore(conn, "open_splunk", "events", retention, time.Now, time.Second)
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	return store
}
func validStoreBatch() ingest.StoreBatch {
	now := time.Date(2026, 7, 21, 1, 2, 3, 0, time.UTC)
	return ingest.StoreBatch{TenantID: "tenant", CollectorID: "collector", BatchID: "batch", BatchSequence: 1,
		Events: []*ingest.StoredEvent{testStoredEvent("event", "main", now)}}
}
func testStoredEvent(id, index string, indexTime time.Time) *ingest.StoredEvent {
	eventTime := time.Date(2026, 7, 21, 3, 4, 5, 123456789, time.FixedZone("event-offset", 5*60*60))
	return &ingest.StoredEvent{
		TenantID: "tenant", CollectorID: "collector", BatchID: "batch", IndexTime: indexTime,
		Event: &opensplunkv1.LogEvent{
			EventId: id, IndexName: index, EventTime: timestamppb.New(eventTime), CollectedAt: timestamppb.New(eventTime.Add(-time.Second)),
			EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            "host", Source: "app.log", Sourcetype: "go:zap:json", Severity: opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
			Raw: []byte("{\"message\":\"hello\"}"), RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
			Fields: typedObjectValue(typedField("status", typedUint(200))),
		},
	}
}

func assertOptionalString(t *testing.T, value any, present bool, want string) {
	t.Helper()
	if !present {
		if value != nil {
			t.Fatalf("optional = %#v, want nil", value)
		}
		return
	}
	pointer, ok := value.(*string)
	if !ok || pointer == nil || *pointer != want {
		t.Fatalf("optional = %#v (%T), want %q", value, value, want)
	}
}
func assertJSONPath(t *testing.T, document *clickhousedriver.JSON, path string, want any) {
	t.Helper()
	got, ok := document.ValueAtPath(path)
	if dynamic, isDynamic := got.(clickhousedriver.Dynamic); isDynamic {
		if dynamic.Nil() {
			got = nil
		} else {
			got = dynamic.Any()
		}
	}
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("path %q = %#v (%T), want %#v", path, got, got, want)
	}
}
func assertTagged(t *testing.T, document *clickhousedriver.JSON, path, wantType, wantValue string) {
	t.Helper()
	value, ok := document.ValueAtPath(path)
	dynamic, dynamicOK := value.(clickhousedriver.Dynamic)
	if !ok || !dynamicOK || dynamic.Type() != "Map(String, String)" {
		t.Fatalf("tag %q = %#v (%T)", path, value, value)
	}
	tag, ok := dynamic.Any().(map[string]string)
	if !ok || len(tag) != 2 || tag[extendedTypeKey] != wantType || tag[extendedValueKey] != wantValue {
		t.Fatalf("tag %q payload = %#v", path, dynamic.Any())
	}
}
func assertTransient(t *testing.T, err error, reason opensplunkv1.RetryBatchReason) {
	t.Helper()
	var transient *ingest.TransientStoreError
	if !errors.As(err, &transient) || transient.Reason != reason || transient.RetryAfter <= 0 {
		t.Fatalf("error = %v, want transient reason %v", err, reason)
	}
}
func isTransient(err error) bool {
	var transient *ingest.TransientStoreError
	return errors.As(err, &transient)
}
func stringPointer(value string) *string { return &value }
func typedField(name string, value *opensplunkv1.TypedValue) *opensplunkv1.TypedObjectField {
	return &opensplunkv1.TypedObjectField{Name: name, Value: value}
}
func typedObjectValue(fields ...*opensplunkv1.TypedObjectField) *opensplunkv1.TypedObject {
	return &opensplunkv1.TypedObject{Fields: fields}
}
func typedNull() *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_NullValue{NullValue: opensplunkv1.NullValue_NULL_VALUE_NULL}}
}
func typedString(v string) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_StringValue{StringValue: v}}
}
func typedSint(v int64) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_Sint64Value{Sint64Value: v}}
}
func typedUint(v uint64) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_Uint64Value{Uint64Value: v}}
}
func typedDouble(v float64) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_DoubleValue{DoubleValue: v}}
}
func typedBool(v bool) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_BoolValue{BoolValue: v}}
}
func typedBytes(v []byte) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_BytesValue{BytesValue: v}}
}
func typedTimestamp(v time.Time) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_TimestampValue{TimestampValue: timestamppb.New(v)}}
}
func typedDuration(v time.Duration) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_DurationValue{DurationValue: durationpb.New(v)}}
}
func typedDecimal(v string) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_DecimalValue{DecimalValue: &opensplunkv1.DecimalValue{Value: v}}}
}
func typedList(v ...*opensplunkv1.TypedValue) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_ListValue{ListValue: &opensplunkv1.TypedValueList{Values: v}}}
}
func typedObject(fields ...*opensplunkv1.TypedObjectField) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_ObjectValue{ObjectValue: typedObjectValue(fields...)}}
}
