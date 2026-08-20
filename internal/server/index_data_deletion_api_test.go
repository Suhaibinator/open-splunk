package server

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
)

func TestDeleteIndexDataAdmissionPersistsTrustedScopeAndWakes(t *testing.T) {
	t.Parallel()

	const tenantID = "trusted-delete-tenant"
	waker := &indexDataDeletionWakeRecorder{}
	handler, db := newIndexDataDeletionTestHandler(
		t,
		tenantID,
		nil,
		waker,
	)
	archived := createArchivedIndexForDeletionAPI(t, db, "physical-target")
	waker.wake = func() {
		operation, err := db.NextIndexDeletionOperation(
			context.Background(),
		)
		if err != nil {
			t.Fatalf("Wake observed no durable operation: %v", err)
		}
		if operation.IndexID != archived.ID {
			t.Fatalf(
				"Wake operation index ID = %q, want %q",
				operation.IndexID,
				archived.ID,
			)
		}
		deleting, err := db.GetIndex(
			context.Background(),
			archived.ID,
		)
		if err != nil {
			t.Fatalf("Wake observed no deleting index: %v", err)
		}
		if deleting.State != control.IndexStateDeleting ||
			deleting.Version != archived.Version+1 {
			t.Fatalf("Wake observed index = %+v", deleting)
		}
	}
	request := physicalDeleteIndexRequest(archived)

	response := postProto(
		t,
		handler,
		"/api/indexes/delete",
		request,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"DELETE_DATA status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var admitted opensplunk.DeleteIndexResponse
	unmarshalResponse(t, response, &admitted)
	if admitted.GetIndexId() != archived.ID ||
		admitted.DeletionOperationId == nil ||
		admitted.GetDeletionOperationId() == "" {
		t.Fatalf("DELETE_DATA response = %+v", &admitted)
	}
	operation, err := db.GetIndexDeletionOperation(
		context.Background(),
		admitted.GetDeletionOperationId(),
	)
	if err != nil {
		t.Fatalf("GetIndexDeletionOperation(): %v", err)
	}
	if operation.TenantID != tenantID ||
		operation.IndexID != archived.ID ||
		operation.IndexName != archived.Definition.Name ||
		operation.ArchivedVersion != archived.Version ||
		operation.DeletingVersion != archived.Version+1 {
		t.Fatalf("admitted operation = %+v", operation)
	}
	deleting, err := db.GetIndex(context.Background(), archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(deleting): %v", err)
	}
	if deleting.State != control.IndexStateDeleting ||
		deleting.Version != archived.Version+1 {
		t.Fatalf("deleting index = %+v", deleting)
	}
	if calls := waker.calls.Load(); calls != 1 {
		t.Fatalf("Wake calls = %d, want 1", calls)
	}

	retryResponse := postProto(
		t,
		handler,
		"/api/indexes/delete",
		request,
	)
	if retryResponse.Code != http.StatusOK {
		t.Fatalf(
			"retry DELETE_DATA status = %d, body = %s",
			retryResponse.Code,
			retryResponse.Body.String(),
		)
	}
	var retried opensplunk.DeleteIndexResponse
	unmarshalResponse(t, retryResponse, &retried)
	if !proto.Equal(&admitted, &retried) {
		t.Fatalf("retry response = %+v, want %+v", &retried, &admitted)
	}
	if calls := waker.calls.Load(); calls != 2 {
		t.Fatalf("Wake calls after retry = %d, want 2", calls)
	}
}

func TestDeleteIndexDataAdmissionConcurrentRetriesConverge(t *testing.T) {
	t.Parallel()

	const requestCount = 24
	waker := &indexDataDeletionWakeRecorder{}
	handler, db := newIndexDataDeletionTestHandler(
		t,
		"concurrent-delete-tenant",
		nil,
		waker,
	)
	archived := createArchivedIndexForDeletionAPI(
		t,
		db,
		"concurrent-physical-target",
	)
	payload, err := proto.Marshal(physicalDeleteIndexRequest(archived))
	if err != nil {
		t.Fatalf("marshal DELETE_DATA request: %v", err)
	}

	start := make(chan struct{})
	results := make(chan *httptest.ResponseRecorder, requestCount)
	var callers sync.WaitGroup
	callers.Add(requestCount)
	for range requestCount {
		go func() {
			defer callers.Done()
			<-start
			request := httptest.NewRequestWithContext(
				context.Background(),
				http.MethodPost,
				"http://example.com/api/indexes/delete",
				bytes.NewReader(payload),
			)
			request.Header.Set("Content-Type", "application/x-protobuf")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			results <- response
		}()
	}
	close(start)
	callers.Wait()
	close(results)

	var operationID string
	for response := range results {
		if response.Code != http.StatusOK {
			t.Fatalf(
				"concurrent DELETE_DATA status = %d, body = %s",
				response.Code,
				response.Body.String(),
			)
		}
		var admitted opensplunk.DeleteIndexResponse
		unmarshalResponse(t, response, &admitted)
		if admitted.GetIndexId() != archived.ID ||
			admitted.GetDeletionOperationId() == "" {
			t.Fatalf("concurrent response = %+v", &admitted)
		}
		if operationID == "" {
			operationID = admitted.GetDeletionOperationId()
		} else if admitted.GetDeletionOperationId() != operationID {
			t.Fatalf(
				"concurrent operation ID = %q, want %q",
				admitted.GetDeletionOperationId(),
				operationID,
			)
		}
	}
	if calls := waker.calls.Load(); calls != requestCount {
		t.Fatalf("Wake calls = %d, want %d", calls, requestCount)
	}
	operation, err := db.NextIndexDeletionOperation(context.Background())
	if err != nil {
		t.Fatalf("NextIndexDeletionOperation(): %v", err)
	}
	if operation.ID != operationID {
		t.Fatalf("durable operation ID = %q, want %q", operation.ID, operationID)
	}
}

func TestConcurrentKeepAndPhysicalIndexDeletionLeaveOneHTTPWinner(
	t *testing.T,
) {
	t.Parallel()

	t.Run("physical admission commits first", func(t *testing.T) {
		t.Parallel()

		admitted := make(chan error, 1)
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseAdmission := func() {
			releaseOnce.Do(func() {
				close(release)
			})
		}
		t.Cleanup(releaseAdmission)
		var db *control.DB
		admission := indexDataDeletionAdmissionFunc(
			func(
				ctx context.Context,
				scope control.IndexDataDeletionScope,
				indexID string,
				expectedVersion uint64,
				confirmationName string,
			) (control.IndexDeletionOperation, error) {
				operation, err := db.BeginIndexDataDeletion(
					ctx,
					scope,
					indexID,
					expectedVersion,
					confirmationName,
				)
				admitted <- err
				if err == nil {
					<-release
				}
				return operation, err
			},
		)
		waker := &indexDataDeletionWakeRecorder{}
		handler, openedDB := newIndexDataDeletionTestHandler(
			t,
			"physical-mode-winner-tenant",
			admission,
			waker,
		)
		db = openedDB
		archived := createArchivedIndexForDeletionAPI(
			t,
			db,
			"physical-mode-winner",
		)
		deleteRequest := physicalDeleteIndexRequest(archived)
		keepRequest := proto.Clone(deleteRequest).(*opensplunk.DeleteIndexRequest)
		keepRequest.DataDeletionMode = opensplunk.
			IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA

		deleteResult := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			deleteResult <- postProto(
				t,
				handler,
				"/api/indexes/delete",
				deleteRequest,
			)
		}()
		select {
		case err := <-admitted:
			if err != nil {
				t.Fatalf("BeginIndexDataDeletion(): %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("physical deletion did not commit")
		}
		keepResponse := postProto(
			t,
			handler,
			"/api/indexes/delete",
			keepRequest,
		)
		if keepResponse.Code != http.StatusConflict {
			t.Fatalf(
				"KEEP_DATA loser status = %d, want %d; body = %s",
				keepResponse.Code,
				http.StatusConflict,
				keepResponse.Body.String(),
			)
		}
		releaseAdmission()
		deleteResponse := <-deleteResult
		if deleteResponse.Code != http.StatusOK {
			t.Fatalf(
				"DELETE_DATA winner status = %d, body = %s",
				deleteResponse.Code,
				deleteResponse.Body.String(),
			)
		}
		var decoded opensplunk.DeleteIndexResponse
		unmarshalResponse(t, deleteResponse, &decoded)
		operation, err := db.NextIndexDeletionOperation(
			context.Background(),
		)
		if err != nil {
			t.Fatalf("NextIndexDeletionOperation(): %v", err)
		}
		if decoded.GetIndexId() != archived.ID ||
			decoded.GetDeletionOperationId() != operation.ID {
			t.Fatalf(
				"DELETE_DATA winner response = %+v, operation = %+v",
				&decoded,
				operation,
			)
		}
		if calls := waker.calls.Load(); calls != 1 {
			t.Fatalf("Wake calls = %d, want 1", calls)
		}
		current, err := db.GetIndex(context.Background(), archived.ID)
		if err != nil {
			t.Fatalf("GetIndex(physical winner): %v", err)
		}
		if current.State != control.IndexStateDeleting ||
			current.Version != archived.Version+1 {
			t.Fatalf("physical winner index = %+v", current)
		}
	})

	t.Run("KEEP_DATA commits first", func(t *testing.T) {
		t.Parallel()

		entered := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseAdmission := func() {
			releaseOnce.Do(func() {
				close(release)
			})
		}
		t.Cleanup(releaseAdmission)
		var db *control.DB
		admission := indexDataDeletionAdmissionFunc(
			func(
				ctx context.Context,
				scope control.IndexDataDeletionScope,
				indexID string,
				expectedVersion uint64,
				confirmationName string,
			) (control.IndexDeletionOperation, error) {
				close(entered)
				<-release
				return db.BeginIndexDataDeletion(
					ctx,
					scope,
					indexID,
					expectedVersion,
					confirmationName,
				)
			},
		)
		waker := &indexDataDeletionWakeRecorder{}
		handler, openedDB := newIndexDataDeletionTestHandler(
			t,
			"keep-mode-winner-tenant",
			admission,
			waker,
		)
		db = openedDB
		archived := createArchivedIndexForDeletionAPI(
			t,
			db,
			"keep-mode-winner",
		)
		deleteRequest := physicalDeleteIndexRequest(archived)
		keepRequest := proto.Clone(deleteRequest).(*opensplunk.DeleteIndexRequest)
		keepRequest.DataDeletionMode = opensplunk.
			IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA

		deleteResult := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			deleteResult <- postProto(
				t,
				handler,
				"/api/indexes/delete",
				deleteRequest,
			)
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("physical deletion did not reach admission")
		}
		keepResponse := postProto(
			t,
			handler,
			"/api/indexes/delete",
			keepRequest,
		)
		if keepResponse.Code != http.StatusOK {
			t.Fatalf(
				"KEEP_DATA winner status = %d, body = %s",
				keepResponse.Code,
				keepResponse.Body.String(),
			)
		}
		var kept opensplunk.DeleteIndexResponse
		unmarshalResponse(t, keepResponse, &kept)
		if kept.GetIndexId() != archived.ID ||
			kept.DeletionOperationId != nil {
			t.Fatalf("KEEP_DATA winner response = %+v", &kept)
		}
		releaseAdmission()
		deleteResponse := <-deleteResult
		if deleteResponse.Code != http.StatusNotFound &&
			deleteResponse.Code != http.StatusConflict {
			t.Fatalf(
				"DELETE_DATA loser status = %d, body = %s",
				deleteResponse.Code,
				deleteResponse.Body.String(),
			)
		}
		assertNoIndexDataDeletionOperation(t, db)
		if calls := waker.calls.Load(); calls != 0 {
			t.Fatalf("Wake calls = %d, want 0", calls)
		}
		if _, err := db.GetIndex(
			context.Background(),
			archived.ID,
		); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf(
				"GetIndex(KEEP_DATA winner) error = %v, want ErrNotFound",
				err,
			)
		}
	})
}

func TestDeleteIndexDataAdmissionRequiresAuthenticationBeforeAdmission(
	t *testing.T,
) {
	t.Parallel()

	var admissions atomic.Int64
	admission := indexDataDeletionAdmissionFunc(
		func(
			context.Context,
			control.IndexDataDeletionScope,
			string,
			uint64,
			string,
		) (control.IndexDeletionOperation, error) {
			admissions.Add(1)
			return control.IndexDeletionOperation{}, errors.New(
				"unexpected unauthenticated admission",
			)
		},
	)
	waker := &indexDataDeletionWakeRecorder{}
	handler, db := newIndexDataDeletionTestHandler(
		t,
		"authenticated-delete-tenant",
		admission,
		waker,
	)
	archived := createArchivedIndexForDeletionAPI(
		t,
		db,
		"authenticated-delete-target",
	)

	response := postProto(
		t,
		handler.raw,
		"/api/indexes/delete",
		physicalDeleteIndexRequest(archived),
	)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf(
			"unauthenticated DELETE_DATA status = %d, want %d; body = %s",
			response.Code,
			http.StatusUnauthorized,
			response.Body.String(),
		)
	}
	if calls := admissions.Load(); calls != 0 {
		t.Fatalf("unauthenticated admissions = %d, want 0", calls)
	}
	if calls := waker.calls.Load(); calls != 0 {
		t.Fatalf("unauthenticated Wake calls = %d, want 0", calls)
	}
	unchanged, err := db.GetIndex(context.Background(), archived.ID)
	if err != nil {
		t.Fatalf("GetIndex(unauthenticated): %v", err)
	}
	if unchanged.State != control.IndexStateArchived ||
		unchanged.Version != archived.Version {
		t.Fatalf("index mutated before authentication = %+v", unchanged)
	}
}

func TestDeleteIndexDataAdmissionRejectsWithoutMutationOrWake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		mutate     func(*opensplunk.DeleteIndexRequest, control.Index)
	}{
		{
			name:       "zero version before selector",
			wantStatus: http.StatusBadRequest,
			mutate: func(
				request *opensplunk.DeleteIndexRequest,
				_ control.Index,
			) {
				request.ExpectedVersion = 0
				request.Selector = nil
			},
		},
		{
			name:       "final SQLite version",
			wantStatus: http.StatusBadRequest,
			mutate: func(
				request *opensplunk.DeleteIndexRequest,
				_ control.Index,
			) {
				request.ExpectedVersion = math.MaxInt64
				request.Selector = nil
			},
		},
		{
			name:       "version above SQLite range before selector",
			wantStatus: http.StatusBadRequest,
			mutate: func(
				request *opensplunk.DeleteIndexRequest,
				_ control.Index,
			) {
				request.ExpectedVersion = uint64(math.MaxInt64) + 1
				request.Selector = nil
			},
		},
		{
			name:       "invalid mode before confirmation and selector",
			wantStatus: http.StatusBadRequest,
			mutate: func(
				request *opensplunk.DeleteIndexRequest,
				_ control.Index,
			) {
				request.DataDeletionMode =
					opensplunk.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_UNSPECIFIED
				request.ConfirmationName = " PHYSICAL-TARGET "
				request.Selector = nil
			},
		},
		{
			name:       "noncanonical confirmation before selector",
			wantStatus: http.StatusBadRequest,
			mutate: func(
				request *opensplunk.DeleteIndexRequest,
				_ control.Index,
			) {
				request.ConfirmationName = " PHYSICAL-TARGET "
				request.Selector = nil
			},
		},
		{
			name:       "missing selector",
			wantStatus: http.StatusBadRequest,
			mutate: func(
				request *opensplunk.DeleteIndexRequest,
				_ control.Index,
			) {
				request.Selector = nil
			},
		},
		{
			name:       "wrong confirmation",
			wantStatus: http.StatusBadRequest,
			mutate: func(
				request *opensplunk.DeleteIndexRequest,
				_ control.Index,
			) {
				request.ConfirmationName = "other-index"
			},
		},
		{
			name:       "stale version",
			wantStatus: http.StatusConflict,
			mutate: func(
				request *opensplunk.DeleteIndexRequest,
				index control.Index,
			) {
				request.ExpectedVersion = index.Version - 1
			},
		},
		{
			name:       "stale version before wrong canonical confirmation",
			wantStatus: http.StatusConflict,
			mutate: func(
				request *opensplunk.DeleteIndexRequest,
				index control.Index,
			) {
				request.ExpectedVersion = index.Version - 1
				request.ConfirmationName = "other-index"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			waker := &indexDataDeletionWakeRecorder{}
			handler, db := newIndexDataDeletionTestHandler(
				t,
				"rejected-delete-tenant",
				nil,
				waker,
			)
			archived := createArchivedIndexForDeletionAPI(
				t,
				db,
				"physical-target",
			)
			request := physicalDeleteIndexRequest(archived)
			test.mutate(request, archived)

			response := postProto(
				t,
				handler,
				"/api/indexes/delete",
				request,
			)
			if response.Code != test.wantStatus {
				t.Fatalf(
					"rejected DELETE_DATA status = %d, want %d; body = %s",
					response.Code,
					test.wantStatus,
					response.Body.String(),
				)
			}
			unchanged, err := db.GetIndex(
				context.Background(),
				archived.ID,
			)
			if err != nil {
				t.Fatalf("GetIndex(rejected): %v", err)
			}
			if unchanged.State != control.IndexStateArchived ||
				unchanged.Version != archived.Version {
				t.Fatalf("index mutated after rejection = %+v", unchanged)
			}
			if _, err := db.NextIndexDeletionOperation(
				context.Background(),
			); !errors.Is(err, control.ErrNotFound) {
				t.Fatalf(
					"NextIndexDeletionOperation() error = %v, want ErrNotFound",
					err,
				)
			}
			if calls := waker.calls.Load(); calls != 0 {
				t.Fatalf("Wake calls = %d, want 0", calls)
			}
		})
	}
}

func TestDeleteIndexDataAdmissionRejectsMissingAndNonArchivedIndexes(
	t *testing.T,
) {
	t.Parallel()

	t.Run("active index", func(t *testing.T) {
		t.Parallel()

		waker := &indexDataDeletionWakeRecorder{}
		handler, db := newIndexDataDeletionTestHandler(
			t,
			"active-delete-tenant",
			nil,
			waker,
		)
		active, err := db.CreateIndex(
			context.Background(),
			adminTestIndex("active-target"),
		)
		if err != nil {
			t.Fatalf("CreateIndex(active-target): %v", err)
		}
		request := physicalDeleteIndexRequest(active)
		request.ConfirmationName = "other-index"
		response := postProto(
			t,
			handler,
			"/api/indexes/delete",
			request,
		)
		if response.Code != http.StatusConflict {
			t.Fatalf(
				"active DELETE_DATA status = %d, want %d; body = %s",
				response.Code,
				http.StatusConflict,
				response.Body.String(),
			)
		}
		unchanged, err := db.GetIndex(context.Background(), active.ID)
		if err != nil {
			t.Fatalf("GetIndex(active): %v", err)
		}
		if unchanged.State != control.IndexStateActive ||
			unchanged.Version != active.Version {
			t.Fatalf("active index mutated = %+v", unchanged)
		}
		assertNoIndexDataDeletionOperation(t, db)
		if calls := waker.calls.Load(); calls != 0 {
			t.Fatalf("Wake calls = %d, want 0", calls)
		}
	})

	t.Run("missing index", func(t *testing.T) {
		t.Parallel()

		waker := &indexDataDeletionWakeRecorder{}
		handler, db := newIndexDataDeletionTestHandler(
			t,
			"missing-delete-tenant",
			nil,
			waker,
		)
		request := &opensplunk.DeleteIndexRequest{
			Selector: &opensplunk.IndexSelector{
				Selector: &opensplunk.IndexSelector_IndexId{
					IndexId: "idx_missing",
				},
			},
			ExpectedVersion: 1,
			DataDeletionMode: opensplunk.
				IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_DELETE_DATA,
			ConfirmationName: "missing-target",
		}
		response := postProto(
			t,
			handler,
			"/api/indexes/delete",
			request,
		)
		if response.Code != http.StatusNotFound {
			t.Fatalf(
				"missing DELETE_DATA status = %d, want %d; body = %s",
				response.Code,
				http.StatusNotFound,
				response.Body.String(),
			)
		}
		assertNoIndexDataDeletionOperation(t, db)
		if calls := waker.calls.Load(); calls != 0 {
			t.Fatalf("Wake calls = %d, want 0", calls)
		}
	})
}

func TestKeepIndexDataWithPhysicalServicesDoesNotAdmitOrWake(t *testing.T) {
	t.Parallel()

	var admissions atomic.Int64
	admission := indexDataDeletionAdmissionFunc(
		func(
			context.Context,
			control.IndexDataDeletionScope,
			string,
			uint64,
			string,
		) (control.IndexDeletionOperation, error) {
			admissions.Add(1)
			return control.IndexDeletionOperation{}, errors.New(
				"unexpected physical deletion admission",
			)
		},
	)
	waker := &indexDataDeletionWakeRecorder{}
	handler, db := newIndexDataDeletionTestHandler(
		t,
		"keep-data-tenant",
		admission,
		waker,
	)
	archived := createArchivedIndexForDeletionAPI(
		t,
		db,
		"keep-data-target",
	)
	request := physicalDeleteIndexRequest(archived)
	request.DataDeletionMode = opensplunk.
		IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA

	response := postProto(
		t,
		handler,
		"/api/indexes/delete",
		request,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"KEEP_DATA status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var deleted opensplunk.DeleteIndexResponse
	unmarshalResponse(t, response, &deleted)
	if deleted.GetIndexId() != archived.ID ||
		deleted.DeletionOperationId != nil {
		t.Fatalf("KEEP_DATA response = %+v", &deleted)
	}
	if _, err := db.GetIndex(
		context.Background(),
		archived.ID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("GetIndex(KEEP_DATA) error = %v, want ErrNotFound", err)
	}
	if calls := admissions.Load(); calls != 0 {
		t.Fatalf("physical admissions = %d, want 0", calls)
	}
	if calls := waker.calls.Load(); calls != 0 {
		t.Fatalf("Wake calls = %d, want 0", calls)
	}
}

func TestDeleteIndexDataAdmissionMapsErrorsWithoutWaking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{
			name:       "invalid argument",
			err:        control.ErrInvalidArgument,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found",
			err:        control.ErrNotFound,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "version conflict",
			err:        control.ErrVersionConflict,
			wantStatus: http.StatusConflict,
		},
		{
			name:       "dependency conflict",
			err:        control.ErrDependencyConflict,
			wantStatus: http.StatusConflict,
		},
		{
			name:       "request canceled",
			err:        context.Canceled,
			wantStatus: http.StatusRequestTimeout,
		},
		{
			name:       "storage unavailable",
			err:        errors.New("storage failed"),
			wantStatus: http.StatusServiceUnavailable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			admission := indexDataDeletionAdmissionFunc(
				func(
					context.Context,
					control.IndexDataDeletionScope,
					string,
					uint64,
					string,
				) (control.IndexDeletionOperation, error) {
					return control.IndexDeletionOperation{}, test.err
				},
			)
			waker := &indexDataDeletionWakeRecorder{}
			handler, db := newIndexDataDeletionTestHandler(
				t,
				"failed-admission-tenant",
				admission,
				waker,
			)
			archived := createArchivedIndexForDeletionAPI(
				t,
				db,
				"failed-admission-target",
			)
			response := postProto(
				t,
				handler,
				"/api/indexes/delete",
				physicalDeleteIndexRequest(archived),
			)
			if response.Code != test.wantStatus {
				t.Fatalf(
					"admission error status = %d, want %d; body = %s",
					response.Code,
					test.wantStatus,
					response.Body.String(),
				)
			}
			unchanged, err := db.GetIndex(
				context.Background(),
				archived.ID,
			)
			if err != nil {
				t.Fatalf("GetIndex(failed admission): %v", err)
			}
			if unchanged.State != control.IndexStateArchived ||
				unchanged.Version != archived.Version {
				t.Fatalf("index mutated after failed admission = %+v", unchanged)
			}
			assertNoIndexDataDeletionOperation(t, db)
			if calls := waker.calls.Load(); calls != 0 {
				t.Fatalf("Wake calls = %d, want 0", calls)
			}
		})
	}
}

func TestDeleteIndexDataAdmissionValidatesSuccessAfterWaking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*control.IndexDeletionOperation)
	}{
		{
			name: "invalid operation ID",
			mutate: func(operation *control.IndexDeletionOperation) {
				operation.ID = "-invalid"
			},
		},
		{
			name: "wrong tenant",
			mutate: func(operation *control.IndexDeletionOperation) {
				operation.TenantID = "other-tenant"
			},
		},
		{
			name: "wrong index ID",
			mutate: func(operation *control.IndexDeletionOperation) {
				operation.IndexID = "idx_other"
			},
		},
		{
			name: "wrong index name",
			mutate: func(operation *control.IndexDeletionOperation) {
				operation.IndexName = "other-index"
			},
		},
		{
			name: "wrong archived version",
			mutate: func(operation *control.IndexDeletionOperation) {
				operation.ArchivedVersion--
			},
		},
		{
			name: "wrong deleting version",
			mutate: func(operation *control.IndexDeletionOperation) {
				operation.DeletingVersion++
			},
		},
		{
			name: "missing creation time",
			mutate: func(operation *control.IndexDeletionOperation) {
				operation.CreatedAt = time.Time{}
			},
		},
		{
			name: "pre-epoch creation time",
			mutate: func(operation *control.IndexDeletionOperation) {
				operation.CreatedAt = time.Unix(-1, 0).UTC()
			},
		},
		{
			name: "creation time outside protobuf range",
			mutate: func(operation *control.IndexDeletionOperation) {
				operation.CreatedAt = time.Date(
					10_000,
					time.January,
					1,
					0,
					0,
					0,
					0,
					time.UTC,
				)
			},
		},
		{
			name: "creation time has sub-microsecond precision",
			mutate: func(operation *control.IndexDeletionOperation) {
				operation.CreatedAt = operation.CreatedAt.Add(
					time.Nanosecond,
				)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			const tenantID = "malformed-admission-tenant"
			var operation control.IndexDeletionOperation
			admission := indexDataDeletionAdmissionFunc(
				func(
					context.Context,
					control.IndexDataDeletionScope,
					string,
					uint64,
					string,
				) (control.IndexDeletionOperation, error) {
					return operation, nil
				},
			)
			waker := &indexDataDeletionWakeRecorder{}
			handler, db := newIndexDataDeletionTestHandler(
				t,
				tenantID,
				admission,
				waker,
			)
			archived := createArchivedIndexForDeletionAPI(
				t,
				db,
				"malformed-admission-target",
			)
			operation = validIndexDataDeletionOperation(
				tenantID,
				archived,
			)
			test.mutate(&operation)

			response := postProto(
				t,
				handler,
				"/api/indexes/delete",
				physicalDeleteIndexRequest(archived),
			)
			if response.Code != http.StatusInternalServerError {
				t.Fatalf(
					"malformed admission status = %d, want %d; body = %s",
					response.Code,
					http.StatusInternalServerError,
					response.Body.String(),
				)
			}
			if calls := waker.calls.Load(); calls != 1 {
				t.Fatalf("Wake calls = %d, want 1", calls)
			}
		})
	}
}

func TestDeleteIndexDataAdmissionSuccessWinsContextCancellation(
	t *testing.T,
) {
	t.Parallel()

	const tenantID = "canceled-success-tenant"
	var archived control.Index
	var cancel context.CancelFunc
	admission := indexDataDeletionAdmissionFunc(
		func(
			context.Context,
			control.IndexDataDeletionScope,
			string,
			uint64,
			string,
		) (control.IndexDeletionOperation, error) {
			cancel()
			return validIndexDataDeletionOperation(tenantID, archived), nil
		},
	)
	waker := &indexDataDeletionWakeRecorder{}
	handler, db := newIndexDataDeletionTestHandler(
		t,
		tenantID,
		admission,
		waker,
	)
	archived = createArchivedIndexForDeletionAPI(
		t,
		db,
		"canceled-success-target",
	)
	ctx, cancelRequest := context.WithCancel(context.Background())
	cancel = cancelRequest
	response := postProtoContext(
		t,
		ctx,
		handler,
		"/api/indexes/delete",
		physicalDeleteIndexRequest(archived),
	)
	cancelRequest()
	if response.Code != http.StatusOK {
		t.Fatalf(
			"canceled successful admission status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	if calls := waker.calls.Load(); calls != 1 {
		t.Fatalf("Wake calls = %d, want 1", calls)
	}
}

func TestDeleteIndexDataAdmissionSurvivesControlPlaneRestart(t *testing.T) {
	t.Parallel()

	const tenantID = "restart-delete-tenant"
	path := t.TempDir() + "/control.sqlite"
	firstDB, err := control.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("control.Open(first): %v", err)
	}
	firstWaker := &indexDataDeletionWakeRecorder{}
	firstHandler := newIndexDataDeletionHandlerWithDB(
		t,
		tenantID,
		firstDB,
		nil,
		firstWaker,
	)
	archived := createArchivedIndexForDeletionAPI(
		t,
		firstDB,
		"restart-delete-target",
	)
	request := physicalDeleteIndexRequest(archived)
	firstResponse := postProto(
		t,
		firstHandler,
		"/api/indexes/delete",
		request,
	)
	if firstResponse.Code != http.StatusOK {
		t.Fatalf(
			"first admission status = %d, body = %s",
			firstResponse.Code,
			firstResponse.Body.String(),
		)
	}
	var first opensplunk.DeleteIndexResponse
	unmarshalResponse(t, firstResponse, &first)
	if err := firstDB.Close(); err != nil {
		t.Fatalf("close first control DB: %v", err)
	}

	reopened, err := control.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("control.Open(restart): %v", err)
	}
	t.Cleanup(func() {
		if closeErr := reopened.Close(); closeErr != nil {
			t.Errorf("close reopened control DB: %v", closeErr)
		}
	})
	retryWaker := &indexDataDeletionWakeRecorder{}
	retryHandler := newIndexDataDeletionHandlerWithDB(
		t,
		tenantID,
		reopened,
		nil,
		retryWaker,
	)
	request.Selector = &opensplunk.IndexSelector{
		Selector: &opensplunk.IndexSelector_IndexName{
			IndexName: archived.Definition.Name,
		},
	}
	retryResponse := postProto(
		t,
		retryHandler,
		"/api/indexes/delete",
		request,
	)
	if retryResponse.Code != http.StatusOK {
		t.Fatalf(
			"restart retry status = %d, body = %s",
			retryResponse.Code,
			retryResponse.Body.String(),
		)
	}
	var retried opensplunk.DeleteIndexResponse
	unmarshalResponse(t, retryResponse, &retried)
	if !proto.Equal(&first, &retried) {
		t.Fatalf("restart retry = %+v, want %+v", &retried, &first)
	}
	if calls := retryWaker.calls.Load(); calls != 1 {
		t.Fatalf("restart Wake calls = %d, want 1", calls)
	}

	crossTenantWaker := &indexDataDeletionWakeRecorder{}
	crossTenantHandler := newIndexDataDeletionHandlerWithDB(
		t,
		"other-restart-tenant",
		reopened,
		nil,
		crossTenantWaker,
	)
	crossTenantResponse := postProto(
		t,
		crossTenantHandler,
		"/api/indexes/delete",
		request,
	)
	if crossTenantResponse.Code != http.StatusConflict {
		t.Fatalf(
			"cross-tenant retry status = %d, want %d; body = %s",
			crossTenantResponse.Code,
			http.StatusConflict,
			crossTenantResponse.Body.String(),
		)
	}
	if calls := crossTenantWaker.calls.Load(); calls != 0 {
		t.Fatalf("cross-tenant Wake calls = %d, want 0", calls)
	}
	operation, err := reopened.GetIndexDeletionOperation(
		context.Background(),
		first.GetDeletionOperationId(),
	)
	if err != nil {
		t.Fatalf("GetIndexDeletionOperation(restart): %v", err)
	}
	if operation.TenantID != tenantID {
		t.Fatalf(
			"restart operation tenant = %q, want %q",
			operation.TenantID,
			tenantID,
		)
	}
}

func TestNewHandlerRequiresCompleteIndexDataDeletionServices(t *testing.T) {
	t.Parallel()

	admission := indexDataDeletionAdmissionFunc(
		func(
			context.Context,
			control.IndexDataDeletionScope,
			string,
			uint64,
			string,
		) (control.IndexDeletionOperation, error) {
			return control.IndexDeletionOperation{}, nil
		},
	)
	waker := &indexDataDeletionWakeRecorder{}
	var typedNilAdmission indexDataDeletionAdmissionFunc
	var typedNilWaker *indexDataDeletionWakeRecorder
	for name, configure := range map[string]func(*Config){
		"admission only": func(config *Config) {
			config.IndexDataDeletionAdmission = admission
		},
		"waker only": func(config *Config) {
			config.IndexDataDeletionWaker = waker
		},
		"typed nil admission with waker": func(config *Config) {
			config.IndexDataDeletionAdmission = typedNilAdmission
			config.IndexDataDeletionWaker = waker
		},
		"admission with typed nil waker": func(config *Config) {
			config.IndexDataDeletionAdmission = admission
			config.IndexDataDeletionWaker = typedNilWaker
		},
		"complete pair without index administration": func(config *Config) {
			config.IndexDataDeletionAdmission = admission
			config.IndexDataDeletionWaker = waker
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			config := Config{
				SearchJobs:    &fakeSearchJobs{},
				Indexes:       fakeIndexCatalog{},
				SavedSearches: &fakeSavedSearches{},
				WebUI:         testUI(),
			}
			configure(&config)
			if handler, err := NewHandler(config); err == nil ||
				handler != nil {
				t.Fatalf(
					"NewHandler(%s) = (%v, %v), want configuration error",
					name,
					handler,
					err,
				)
			}
		})
	}

	handler, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		IndexDataDeletionAdmission: typedNilAdmission,
		IndexDataDeletionWaker:     typedNilWaker,
		SavedSearches:              &fakeSavedSearches{},
		WebUI:                      testUI(),
	})
	if err != nil {
		t.Fatalf("NewHandler(both deletion services typed nil): %v", err)
	}
	if handler == nil {
		t.Fatal("NewHandler(both deletion services typed nil) returned nil")
	}
}

type indexDataDeletionAdmissionFunc func(
	context.Context,
	control.IndexDataDeletionScope,
	string,
	uint64,
	string,
) (control.IndexDeletionOperation, error)

func (admission indexDataDeletionAdmissionFunc) BeginIndexDataDeletion(
	ctx context.Context,
	scope control.IndexDataDeletionScope,
	indexID string,
	expectedVersion uint64,
	confirmationName string,
) (control.IndexDeletionOperation, error) {
	return admission(
		ctx,
		scope,
		indexID,
		expectedVersion,
		confirmationName,
	)
}

type indexDataDeletionWakeRecorder struct {
	calls atomic.Int64
	wake  func()
}

func (waker *indexDataDeletionWakeRecorder) Wake() {
	if waker != nil {
		waker.calls.Add(1)
		if waker.wake != nil {
			waker.wake()
		}
	}
}

func newIndexDataDeletionTestHandler(
	t *testing.T,
	tenantID string,
	admission IndexDataDeletionAdmission,
	waker IndexDataDeletionWaker,
) (*adminIntegrationHandler, *control.DB) {
	t.Helper()

	db, err := control.Open(
		context.Background(),
		t.TempDir()+"/control.sqlite",
	)
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("control DB close: %v", err)
		}
	})
	if admission == nil {
		admission = db
	}
	return newIndexDataDeletionHandlerWithDB(
		t,
		tenantID,
		db,
		admission,
		waker,
	), db
}

func newIndexDataDeletionHandlerWithDB(
	t *testing.T,
	tenantID string,
	db *control.DB,
	admission IndexDataDeletionAdmission,
	waker IndexDataDeletionWaker,
) *adminIntegrationHandler {
	t.Helper()

	if admission == nil {
		admission = db
	}
	browserAuthenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(adminIntegrationBearerToken),
		tenantID,
		"single-user",
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatalf("auth.NewBearerTokenAuthenticator: %v", err)
	}
	raw := newTestHandler(t, Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    db,
		IndexDataDeletionAdmission: admission,
		IndexDataDeletionWaker:     waker,
		SavedSearches:              &fakeSavedSearches{},
		WebUI:                      testUI(),
		TenantID:                   tenantID,
		OwnerID:                    "single-user",
		AdministrativeAllowedHosts: []string{"example.com"},
		BrowserAuthenticator:       browserAuthenticator,
	})
	return &adminIntegrationHandler{
		raw:   raw,
		token: adminIntegrationBearerToken,
	}
}

func createArchivedIndexForDeletionAPI(
	t *testing.T,
	db *control.DB,
	name string,
) control.Index {
	t.Helper()

	created, err := db.CreateIndex(
		context.Background(),
		adminTestIndex(name),
	)
	if err != nil {
		t.Fatalf("CreateIndex(%q): %v", name, err)
	}
	archived, err := db.SetIndexState(
		context.Background(),
		created.ID,
		created.Version,
		control.IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive index %q: %v", name, err)
	}
	return archived
}

func physicalDeleteIndexRequest(
	index control.Index,
) *opensplunk.DeleteIndexRequest {
	return &opensplunk.DeleteIndexRequest{
		Selector: &opensplunk.IndexSelector{
			Selector: &opensplunk.IndexSelector_IndexId{
				IndexId: index.ID,
			},
		},
		ExpectedVersion: index.Version,
		DataDeletionMode: opensplunk.
			IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_DELETE_DATA,
		ConfirmationName: index.Definition.Name,
	}
}

func validIndexDataDeletionOperation(
	tenantID string,
	index control.Index,
) control.IndexDeletionOperation {
	return control.IndexDeletionOperation{
		ID:              "idxdel_valid-operation",
		TenantID:        tenantID,
		IndexID:         index.ID,
		IndexName:       index.Definition.Name,
		ArchivedVersion: index.Version,
		DeletingVersion: index.Version + 1,
		CreatedAt:       testNow,
	}
}

func assertNoIndexDataDeletionOperation(t *testing.T, db *control.DB) {
	t.Helper()

	if _, err := db.NextIndexDeletionOperation(
		context.Background(),
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"NextIndexDeletionOperation() error = %v, want ErrNotFound",
			err,
		)
	}
}
