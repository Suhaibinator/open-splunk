package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"strings"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func TestFrozenIndexDataDeletionRequiresDrainAndResolvesTableGeneration(t *testing.T) {
	t.Parallel()

	connection := newMutationScriptConnection()
	connection.targets = []fakeMutationTarget{
		{uuid: "01234567-89ab-4cde-8fab-0123456789ab", engine: "MergeTree"},
	}
	store := mustTestStore(t, connection, fixedRetention(1))

	var escaped FrozenWrites
	err := store.WithWritesFrozen(
		context.Background(),
		func(ctx context.Context, frozen FrozenWrites) error {
			escaped = frozen
			if _, err := frozen.IndexDataDeletionTarget(ctx); !errors.Is(
				err,
				ErrWriteFreezeNotDrained,
			) {
				t.Fatalf(
					"IndexDataDeletionTarget before drain error = %v, want ErrWriteFreezeNotDrained",
					err,
				)
			}
			if err := frozen.DrainPending(ctx); err != nil {
				t.Fatalf("DrainPending(): %v", err)
			}
			target, err := frozen.IndexDataDeletionTarget(ctx)
			if err != nil {
				t.Fatalf("IndexDataDeletionTarget(): %v", err)
			}
			want := IndexDataDeletionTarget{
				Database:  "open_splunk",
				Table:     "events",
				TableUUID: "01234567-89ab-4cde-8fab-0123456789ab",
			}
			if target != want {
				t.Fatalf("target = %#v, want %#v", target, want)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("WithWritesFrozen(): %v", err)
	}
	if _, err := escaped.IndexDataDeletionTarget(
		context.Background(),
	); !errors.Is(err, ErrWriteFreezeInactive) {
		t.Fatalf("escaped target resolution error = %v, want ErrWriteFreezeInactive", err)
	}
}

func TestAdvanceIndexDataDeletionDoesNotDuplicatePendingMutation(t *testing.T) {
	t.Parallel()

	connection := newMutationScriptConnection()
	connection.targets = []fakeMutationTarget{validFakeMutationTarget()}
	connection.summaries = []fakeMutationSummary{{
		matching:      2,
		pending:       1,
		pendingParts:  7,
		latestBlock:   19,
		failureCode:   "CANNOT_READ_ALL_DATA",
		failureReason: "ClickHouse will retry this part",
	}}
	store := mustTestStore(t, connection, fixedRetention(1))
	request := validIndexDataDeletionRequest()

	var progress IndexDataDeletionProgress
	err := store.WithWritesFrozen(
		context.Background(),
		func(ctx context.Context, frozen FrozenWrites) error {
			if err := frozen.DrainPending(ctx); err != nil {
				return err
			}
			var err error
			progress, err = frozen.AdvanceIndexDataDeletion(ctx, request)
			return err
		},
	)
	if err != nil {
		t.Fatalf("AdvanceIndexDataDeletion(): %v", err)
	}
	if progress.State != IndexDataDeletionPending ||
		progress.MatchingMutations != 2 ||
		progress.PendingMutations != 1 ||
		progress.PendingParts != 7 ||
		progress.LatestMutationBlock != 19 ||
		progress.LatestFailureCode != "CANNOT_READ_ALL_DATA" ||
		progress.LatestFailureReason != "ClickHouse will retry this part" ||
		progress.SubmissionAttempted ||
		progress.SubmissionAccepted {
		t.Fatalf("progress = %#v", progress)
	}
	if connection.execCalls != 0 || connection.existenceCalls != 0 {
		t.Fatalf(
			"pending mutation caused exec/count calls = %d/%d",
			connection.execCalls,
			connection.existenceCalls,
		)
	}
}

func TestIndexDataDeletionStatusPollsWithoutFreezingOrDraining(t *testing.T) {
	t.Parallel()

	connection := newMutationScriptConnection()
	connection.targets = []fakeMutationTarget{validFakeMutationTarget()}
	connection.summaries = []fakeMutationSummary{{
		matching:     1,
		pending:      1,
		pendingParts: 4,
		latestBlock:  11,
	}}
	sequencer := &fakeVisibilitySequencer{
		pendingErr: errors.New("status unexpectedly drained the outbox"),
	}
	store := mustTestStoreWithVisibility(
		t,
		connection,
		fixedRetention(1),
		sequencer,
	)

	progress, err := store.IndexDataDeletionStatus(
		context.Background(),
		validIndexDataDeletionRequest(),
	)
	if err != nil {
		t.Fatalf("IndexDataDeletionStatus(): %v", err)
	}
	if progress.State != IndexDataDeletionPending ||
		progress.PendingMutations != 1 ||
		progress.PendingParts != 4 ||
		progress.LatestMutationBlock != 11 {
		t.Fatalf("progress = %#v", progress)
	}
	if sequencer.acquireCalls != 0 ||
		sequencer.pendingCalls != 0 ||
		connection.execCalls != 0 ||
		connection.existenceCalls != 0 {
		t.Fatalf(
			"read-only status side effects: acquire=%d usage=%d exec=%d count=%d",
			sequencer.acquireCalls,
			sequencer.pendingCalls,
			connection.execCalls,
			connection.existenceCalls,
		)
	}

	connection.targets = append(
		connection.targets,
		validFakeMutationTarget(),
	)
	connection.summaries = append(
		connection.summaries,
		fakeMutationSummary{matching: 1, latestBlock: 11},
	)
	progress, err = store.IndexDataDeletionStatus(
		context.Background(),
		validIndexDataDeletionRequest(),
	)
	if err != nil {
		t.Fatalf("IndexDataDeletionStatus(ready): %v", err)
	}
	if progress.State != IndexDataDeletionReady {
		t.Fatalf("ready progress = %#v", progress)
	}
}

func TestAdvanceIndexDataDeletionSubmitsStableScopedMutation(t *testing.T) {
	t.Parallel()

	connection := newMutationScriptConnection()
	connection.targets = []fakeMutationTarget{
		validFakeMutationTarget(),
		validFakeMutationTarget(),
		validFakeMutationTarget(),
	}
	connection.summaries = []fakeMutationSummary{
		{},
		{matching: 1, pending: 1, pendingParts: 3, latestBlock: 5},
	}
	connection.existence = []uint64{1}
	store := mustTestStore(t, connection, fixedRetention(1))
	request := validIndexDataDeletionRequest()

	progress := advanceIndexDataDeletionForTest(t, store, request)
	if progress.State != IndexDataDeletionPending ||
		!progress.SubmissionAttempted ||
		!progress.SubmissionAccepted ||
		progress.MatchingMutations != 1 ||
		progress.PendingMutations != 1 ||
		progress.LatestMutationBlock != 5 {
		t.Fatalf("progress = %#v", progress)
	}
	if connection.execCalls != 1 {
		t.Fatalf("Exec calls = %d, want 1", connection.execCalls)
	}
	for _, fragment := range []string{
		`ALTER TABLE "open_splunk"."events" DELETE WHERE`,
		"tenant_id = {tenant:String}",
		"index_name = {index:String}",
		"{correlation:String} = {correlation:String}",
	} {
		if !strings.Contains(connection.execQuery, fragment) {
			t.Errorf("mutation query %q does not contain %q", connection.execQuery, fragment)
		}
	}
	wantMarker := indexDataDeletionCorrelationMarker(request)
	wantParameters := clickhousedriver.Parameters{
		"tenant":      request.TenantID,
		"index":       request.IndexName,
		"correlation": wantMarker,
	}
	if !reflect.DeepEqual(connection.execParameters, wantParameters) {
		t.Fatalf(
			"Exec parameters = %#v, want %#v",
			connection.execParameters,
			wantParameters,
		)
	}
	if got := connection.execSettings["mutations_sync"]; got != uint8(0) {
		t.Fatalf("mutations_sync = %#v (%T), want uint8(0)", got, got)
	}
	if connection.execQueryID == "" ||
		!strings.HasPrefix(connection.execQueryID, "os-del-") {
		t.Fatalf("query ID = %q", connection.execQueryID)
	}
}

func TestIndexDataDeletionStatusRejectsNonCanonicalTenantBeforeClickHouse(
	t *testing.T,
) {
	t.Parallel()

	for name, tenantID := range map[string]string{
		"leading space":    " tenant",
		"trailing space":   "tenant ",
		"leading newline":  "\ntenant",
		"trailing newline": "tenant\n",
		"C1 control":       "tenant\u0085",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			connection := newMutationScriptConnection()
			store := mustTestStore(t, connection, fixedRetention(1))
			request := validIndexDataDeletionRequest()
			request.TenantID = tenantID

			if _, err := store.IndexDataDeletionStatus(
				context.Background(),
				request,
			); err == nil {
				t.Fatal("IndexDataDeletionStatus() error = nil")
			}
			if connection.targetCalls != 0 ||
				connection.summaryCalls != 0 ||
				connection.existenceCalls != 0 ||
				connection.execCalls != 0 {
				t.Fatalf(
					"invalid tenant reached ClickHouse: target=%d summary=%d existence=%d exec=%d",
					connection.targetCalls,
					connection.summaryCalls,
					connection.existenceCalls,
					connection.execCalls,
				)
			}
		})
	}
}

func TestIndexDataDeletionCorrelationMarkerBindsImmutableRequest(t *testing.T) {
	t.Parallel()

	request := validIndexDataDeletionRequest()
	marker := indexDataDeletionCorrelationMarker(request)
	const prefix = "__open_splunk_delete_v1_"
	const suffix = "__"
	if !strings.HasPrefix(marker, prefix) ||
		!strings.HasSuffix(marker, suffix) ||
		len(marker) != len(prefix)+64+len(suffix) ||
		strings.Contains(marker, request.CorrelationID) {
		t.Fatalf("correlation marker = %q", marker)
	}
	if got := indexDataDeletionCorrelationMarker(request); got != marker {
		t.Fatalf("repeated marker = %q, want %q", got, marker)
	}
	if got := indexDataDeletionQueryID(request); got !=
		"os-del-"+strings.TrimSuffix(strings.TrimPrefix(marker, prefix), suffix) {
		t.Fatalf("query ID = %q, marker = %q", got, marker)
	}

	mutations := []func(*IndexDataDeletionRequest){
		func(value *IndexDataDeletionRequest) {
			value.OperationID = "idxdel_other"
		},
		func(value *IndexDataDeletionRequest) {
			value.CorrelationID = "idxmut_other"
		},
		func(value *IndexDataDeletionRequest) {
			value.TenantID = "other-tenant"
		},
		func(value *IndexDataDeletionRequest) {
			value.IndexName = "other-index"
		},
		func(value *IndexDataDeletionRequest) {
			value.Database = "other_database"
		},
		func(value *IndexDataDeletionRequest) {
			value.Table = "other_table"
		},
		func(value *IndexDataDeletionRequest) {
			value.TableUUID = "11234567-89ab-4cde-8fab-0123456789ab"
		},
		func(value *IndexDataDeletionRequest) {
			value.ProtocolVersion++
		},
	}
	for index, mutate := range mutations {
		changed := request
		mutate(&changed)
		if got := indexDataDeletionCorrelationMarker(changed); got == marker {
			t.Errorf("mutation %d did not change marker", index)
		}
	}
}

func TestAdvanceIndexDataDeletionHandlesCompletedHistoryAndPhysicalProof(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		existence  uint64
		wantState  IndexDataDeletionState
		wantSubmit bool
		summaries  []fakeMutationSummary
		targets    []fakeMutationTarget
	}{
		{
			name:      "completed and empty",
			existence: 0,
			wantState: IndexDataDeletionPhysicallyEmpty,
			summaries: []fakeMutationSummary{{
				matching: 1, latestBlock: 8,
			}},
			targets: []fakeMutationTarget{
				validFakeMutationTarget(),
				validFakeMutationTarget(),
			},
		},
		{
			name:       "completed then late row",
			existence:  1,
			wantState:  IndexDataDeletionPending,
			wantSubmit: true,
			summaries: []fakeMutationSummary{
				{matching: 1, latestBlock: 8},
				{matching: 2, pending: 1, latestBlock: 9},
			},
			targets: []fakeMutationTarget{
				validFakeMutationTarget(),
				validFakeMutationTarget(),
				validFakeMutationTarget(),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := newMutationScriptConnection()
			connection.targets = test.targets
			connection.summaries = test.summaries
			connection.existence = []uint64{test.existence}
			store := mustTestStore(t, connection, fixedRetention(1))

			progress := advanceIndexDataDeletionForTest(
				t,
				store,
				validIndexDataDeletionRequest(),
			)
			if progress.State != test.wantState ||
				progress.SubmissionAttempted != test.wantSubmit ||
				connection.execCalls != boolInt(test.wantSubmit) {
				t.Fatalf(
					"progress=%#v execCalls=%d, want state=%v submitted=%v",
					progress,
					connection.execCalls,
					test.wantState,
					test.wantSubmit,
				)
			}
			if connection.physicalProofCalls != 1 {
				t.Fatalf(
					"combined physical-proof calls = %d, want 1",
					connection.physicalProofCalls,
				)
			}
		})
	}
}

func TestAdvanceIndexDataDeletionReconcilesOutcomeAmbiguousSubmission(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		summaries  []fakeMutationSummary
		existence  []uint64
		targets    []fakeMutationTarget
		wantErr    error
		wantState  IndexDataDeletionState
		wantAccept bool
	}{
		{
			name: "accepted then EOF",
			summaries: []fakeMutationSummary{
				{},
				{matching: 1, pending: 1, latestBlock: 3},
			},
			existence: []uint64{1},
			targets: []fakeMutationTarget{
				validFakeMutationTarget(),
				validFakeMutationTarget(),
				validFakeMutationTarget(),
			},
			wantState:  IndexDataDeletionPending,
			wantAccept: true,
		},
		{
			name: "no observation and rows remain",
			summaries: []fakeMutationSummary{
				{},
				{},
			},
			existence: []uint64{1, 1},
			targets: []fakeMutationTarget{
				validFakeMutationTarget(),
				validFakeMutationTarget(),
				validFakeMutationTarget(),
				validFakeMutationTarget(),
			},
			wantErr: io.ErrUnexpectedEOF,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection := newMutationScriptConnection()
			connection.execErr = io.ErrUnexpectedEOF
			connection.targets = test.targets
			connection.summaries = test.summaries
			connection.existence = test.existence
			store := mustTestStore(t, connection, fixedRetention(1))

			var progress IndexDataDeletionProgress
			err := store.WithWritesFrozen(
				context.Background(),
				func(ctx context.Context, frozen FrozenWrites) error {
					if err := frozen.DrainPending(ctx); err != nil {
						return err
					}
					var advanceErr error
					progress, advanceErr = frozen.AdvanceIndexDataDeletion(
						ctx,
						validIndexDataDeletionRequest(),
					)
					return advanceErr
				},
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if progress.State != test.wantState ||
				!progress.SubmissionAttempted ||
				progress.SubmissionAccepted != test.wantAccept ||
				connection.execCalls != 1 {
				t.Fatalf("progress=%#v execCalls=%d", progress, connection.execCalls)
			}
		})
	}
}

func TestAdvanceIndexDataDeletionFailsClosedOnTargetOrMutationCorruption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		target      fakeMutationTarget
		summary     fakeMutationSummary
		want        error
		wantMessage string
	}{
		{
			name: "table UUID changed",
			target: fakeMutationTarget{
				uuid:   "11234567-89ab-4cde-8fab-0123456789ab",
				engine: "MergeTree",
			},
			want: ErrIndexDataDeletionTargetChanged,
		},
		{
			name:        "unexpected command",
			target:      validFakeMutationTarget(),
			summary:     fakeMutationSummary{matching: 1, invalidCommands: 1},
			wantMessage: "unexpected command",
		},
		{
			name: "unsupported engine",
			target: fakeMutationTarget{
				uuid:   validFakeMutationTarget().uuid,
				engine: "ReplicatedMergeTree",
			},
			wantMessage: "unsupported engine",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			connection := newMutationScriptConnection()
			connection.targets = []fakeMutationTarget{test.target}
			connection.summaries = []fakeMutationSummary{test.summary}
			store := mustTestStore(t, connection, fixedRetention(1))

			err := store.WithWritesFrozen(
				context.Background(),
				func(ctx context.Context, frozen FrozenWrites) error {
					if err := frozen.DrainPending(ctx); err != nil {
						return err
					}
					_, err := frozen.AdvanceIndexDataDeletion(
						ctx,
						validIndexDataDeletionRequest(),
					)
					return err
				},
			)
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if test.wantMessage != "" &&
				!strings.Contains(errString(err), test.wantMessage) {
				t.Fatalf("error = %v, want message containing %q", err, test.wantMessage)
			}
			if connection.execCalls != 0 {
				t.Fatalf("fail-closed path made %d Exec calls", connection.execCalls)
			}
		})
	}
}

func advanceIndexDataDeletionForTest(
	t *testing.T,
	store *Store,
	request IndexDataDeletionRequest,
) IndexDataDeletionProgress {
	t.Helper()
	var progress IndexDataDeletionProgress
	err := store.WithWritesFrozen(
		context.Background(),
		func(ctx context.Context, frozen FrozenWrites) error {
			if err := frozen.DrainPending(ctx); err != nil {
				return err
			}
			var err error
			progress, err = frozen.AdvanceIndexDataDeletion(ctx, request)
			return err
		},
	)
	if err != nil {
		t.Fatalf("AdvanceIndexDataDeletion(): %v", err)
	}
	return progress
}

func validIndexDataDeletionRequest() IndexDataDeletionRequest {
	return IndexDataDeletionRequest{
		OperationID:     "idxdel_operation",
		CorrelationID:   "idxmut_correlation",
		TenantID:        "tenant",
		IndexName:       "main",
		Database:        "open_splunk",
		Table:           "events",
		TableUUID:       validFakeMutationTarget().uuid,
		ProtocolVersion: 1,
	}
}

type fakeMutationTarget struct {
	uuid   string
	engine string
	err    error
}

func validFakeMutationTarget() fakeMutationTarget {
	return fakeMutationTarget{
		uuid:   "01234567-89ab-4cde-8fab-0123456789ab",
		engine: "MergeTree",
	}
}

type fakeMutationSummary struct {
	matching        uint64
	pending         uint64
	pendingParts    int64
	latestBlock     int64
	invalidCommands uint64
	failureCode     string
	failureReason   string
	err             error
}

type mutationScriptConnection struct {
	*fakeStoreConnection

	targets            []fakeMutationTarget
	summaries          []fakeMutationSummary
	existence          []uint64
	execErr            error
	execCalls          int
	targetCalls        int
	summaryCalls       int
	existenceCalls     int
	physicalProofCalls int

	execQuery      string
	execSettings   clickhousedriver.Settings
	execParameters clickhousedriver.Parameters
	execQueryID    string
}

func newMutationScriptConnection() *mutationScriptConnection {
	return &mutationScriptConnection{
		fakeStoreConnection: &fakeStoreConnection{},
	}
}

func (connection *mutationScriptConnection) exec(
	_ context.Context,
	query string,
	settings clickhousedriver.Settings,
	parameters clickhousedriver.Parameters,
	queryID string,
) error {
	connection.execCalls++
	connection.execQuery = query
	connection.execSettings = cloneSettings(settings)
	connection.execParameters = cloneParameters(parameters)
	connection.execQueryID = queryID
	return connection.execErr
}

func (connection *mutationScriptConnection) queryRow(
	_ context.Context,
	query string,
	parameters clickhousedriver.Parameters,
) storeQueryRow {
	switch {
	case strings.Contains(query, "FROM system.tables") &&
		strings.Contains(query, "system.mutations"):
		targetIndex := connection.targetCalls
		connection.targetCalls++
		summaryIndex := connection.summaryCalls
		connection.summaryCalls++
		if targetIndex >= len(connection.targets) ||
			summaryIndex >= len(connection.summaries) {
			return fakeStoreQueryRow{
				err: errors.New("unexpected combined mutation-reconciliation query"),
			}
		}
		target := connection.targets[targetIndex]
		summary := connection.summaries[summaryIndex]
		if target.err != nil {
			return fakeStoreQueryRow{err: target.err}
		}
		return fakeStoreQueryRow{
			values: []any{
				target.uuid,
				target.engine,
				summary.matching,
				summary.pending,
				summary.pendingParts,
				summary.latestBlock,
				summary.invalidCommands,
				summary.failureCode,
				summary.failureReason,
			},
			err: summary.err,
		}
	case strings.Contains(query, "FROM system.tables") &&
		strings.Contains(query, "PREWHERE tenant_id"):
		targetIndex := connection.targetCalls
		connection.targetCalls++
		existenceIndex := connection.existenceCalls
		connection.existenceCalls++
		connection.physicalProofCalls++
		if targetIndex >= len(connection.targets) ||
			existenceIndex >= len(connection.existence) {
			return fakeStoreQueryRow{
				err: errors.New("unexpected combined physical-proof query"),
			}
		}
		target := connection.targets[targetIndex]
		if target.err != nil {
			return fakeStoreQueryRow{err: target.err}
		}
		return fakeStoreQueryRow{
			values: []any{
				target.uuid,
				target.engine,
				connection.existence[existenceIndex],
			},
		}
	case strings.Contains(query, "FROM system.tables"):
		index := connection.targetCalls
		connection.targetCalls++
		if index >= len(connection.targets) {
			return fakeStoreQueryRow{err: errors.New("unexpected table-target query")}
		}
		target := connection.targets[index]
		return fakeStoreQueryRow{
			values: []any{target.uuid, target.engine},
			err:    target.err,
		}
	case strings.Contains(query, "FROM system.mutations"):
		index := connection.summaryCalls
		connection.summaryCalls++
		if index >= len(connection.summaries) {
			return fakeStoreQueryRow{err: errors.New("unexpected mutation-summary query")}
		}
		summary := connection.summaries[index]
		return fakeStoreQueryRow{
			values: []any{
				summary.matching,
				summary.pending,
				summary.pendingParts,
				summary.latestBlock,
				summary.invalidCommands,
				summary.failureCode,
				summary.failureReason,
			},
			err: summary.err,
		}
	case strings.Contains(query, "LIMIT 1"):
		index := connection.existenceCalls
		connection.existenceCalls++
		if index >= len(connection.existence) {
			return fakeStoreQueryRow{err: errors.New("unexpected physical-existence query")}
		}
		return fakeStoreQueryRow{values: []any{connection.existence[index]}}
	default:
		return fakeStoreQueryRow{
			err: fmt.Errorf("unexpected query: %s (parameters=%v)", query, parameters),
		}
	}
}

type fakeStoreQueryRow struct {
	values []any
	err    error
}

func (row fakeStoreQueryRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(destinations) != len(row.values) {
		return fmt.Errorf(
			"scan destination count = %d, want %d",
			len(destinations),
			len(row.values),
		)
	}
	for index, destination := range destinations {
		source := row.values[index]
		switch target := destination.(type) {
		case *string:
			value, ok := source.(string)
			if !ok {
				return fmt.Errorf("scan value %d = %T, want string", index, source)
			}
			*target = value
		case *uint64:
			value, ok := source.(uint64)
			if !ok {
				return fmt.Errorf("scan value %d = %T, want uint64", index, source)
			}
			*target = value
		case *int64:
			value, ok := source.(int64)
			if !ok {
				return fmt.Errorf("scan value %d = %T, want int64", index, source)
			}
			*target = value
		default:
			return fmt.Errorf("scan destination %d = %T", index, destination)
		}
	}
	return nil
}

func cloneSettings(input clickhousedriver.Settings) clickhousedriver.Settings {
	result := make(clickhousedriver.Settings, len(input))
	maps.Copy(result, input)
	return result
}

func cloneParameters(
	input clickhousedriver.Parameters,
) clickhousedriver.Parameters {
	result := make(clickhousedriver.Parameters, len(input))
	maps.Copy(result, input)
	return result
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
