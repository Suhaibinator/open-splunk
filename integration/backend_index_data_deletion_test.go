//go:build !windows

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

const (
	backendIndexDeletionTenant        = "backend-index-deletion-tenant"
	backendIndexDeletionForeignTenant = "backend-index-deletion-foreign"
	backendIndexDeletionTargetName    = "physical-delete-target"
	backendIndexDeletionNeighborName  = "physical-delete-neighbor"
	backendIndexDeletionMarkerPrefix  = "__open_splunk_delete_v1_"
)

type backendIndexDeletionPhysicalScope struct {
	targetRows       uint64
	targetPartitions uint64
	foreignRows      uint64
	neighborRows     uint64
}

type backendIndexDeletionControlCounts struct {
	operations  int
	attempts    int
	tombstones  int
	completions int
}

// TestBackendIndexDataDeletionLifecycle proves the administrator DELETE_DATA
// boundary against the real server, durable SQLite state, and digest-pinned
// ClickHouse. It deliberately holds an accepted mutation behind STOP MERGES,
// crash-restarts the server with pending work, and verifies exact scope,
// outstanding-operation idempotency, and the bounded terminal retry contract.
func TestBackendIndexDataDeletionLifecycle(t *testing.T) {
	if os.Getenv(backendIntegrationFlag) != "1" {
		t.Skip("set " + backendIntegrationFlag + "=1 to run the backend index deletion integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("docker CLI is required when %s=1: %v", backendIntegrationFlag, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()

	repository := repositoryRoot(t)
	work := t.TempDir()
	buildDir := t.TempDir()
	serverRuntimeDir := t.TempDir()
	stagedBackendRepository := buildBackendFrontend(t, ctx, repository)
	serverBinary := filepath.Join(buildDir, "open-splunk-server")
	buildBinary(
		t,
		ctx,
		stagedBackendRepository,
		serverBinary,
		"./cmd/open-splunk-server",
	)

	image := backendIndexDeletionPinnedImage(t)
	clickhouse, err := testsupport.StartClickHouseWithServicePrincipals(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			20*time.Second,
		)
		defer cleanupCancel()
		if err := clickhouse.Close(cleanupCtx); err != nil {
			t.Errorf("ClickHouse cleanup: %v", err)
		}
	})

	httpAddress := unusedLoopbackAddress(t)
	collectorAddress := unusedLoopbackAddress(t)
	controlDBPath := filepath.Join(work, "control.sqlite")
	administratorTokenPath, administratorToken := provisionAdministratorToken(
		t,
		work,
	)
	assertEmptyDirectory(t, serverRuntimeDir)
	serverEnvironment := clickHouseServerEnvironment(
		os.Environ(),
		clickhouse,
	)
	serverEnvironment = environmentWithValue(
		serverEnvironment,
		"PATH",
		filepath.Join(serverRuntimeDir, "no-external-runtime"),
	)
	serverArguments := []string{
		serverBinary,
		"-http-listen-address=" + httpAddress,
		"-control-database-file=" + controlDBPath,
		"-master-key-file=" + filepath.Join(work, "server.key"),
		"-administrator-token-file=" + administratorTokenPath,
		"-collector-grpc-listen-address=" + collectorAddress,
		"-collector-grpc-plaintext-enabled",
		"-tenant-id=" + backendIndexDeletionTenant,
	}
	serverArguments = append(
		serverArguments,
		clickHouseServerArguments(clickhouse)...,
	)

	var serverProcesses []*managedProcess
	protectedValues := []string{
		administratorToken,
		clickhouse.Password,
		clickhouse.MigrationPassword,
		clickhouse.RuntimePassword,
		clickhouse.DeletionPassword,
	}
	t.Cleanup(func() {
		for i, process := range serverProcesses {
			assertManagedProcessLogsComplete(
				t,
				fmt.Sprintf("index deletion server %d", i+1),
				process,
				protectedValues...,
			)
			assertProcessLogsDoNotLeak(
				t,
				process.Logs(),
				protectedValues...,
			)
		}
	})

	baseURL := "http://" + httpAddress
	httpClient := &http.Client{Timeout: 10 * time.Second}
	startServer := func() *managedProcess {
		process := startProcess(
			t,
			serverRuntimeDir,
			serverArguments,
			serverEnvironment,
		)
		serverProcesses = append(serverProcesses, process)
		waitForHealth(
			t,
			ctx,
			httpClient,
			baseURL,
			process,
			protectedValues...,
		)
		return process
	}

	firstServer := startServer()
	runtimeConnection := backendIndexDeletionClickHouseConnection(
		t,
		ctx,
		clickhouse,
		clickhouse.RuntimeUsername,
		clickhouse.RuntimePassword,
	)
	t.Cleanup(func() {
		if err := runtimeConnection.Close(); err != nil {
			t.Errorf("close runtime ClickHouse connection: %v", err)
		}
	})
	deletionConnection := backendIndexDeletionClickHouseConnection(
		t,
		ctx,
		clickhouse,
		clickhouse.DeletionUsername,
		clickhouse.DeletionPassword,
	)
	t.Cleanup(func() {
		if err := deletionConnection.Close(); err != nil {
			t.Errorf("close deletion ClickHouse connection: %v", err)
		}
	})

	target := createBackendIndex(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		backendIndexDeletionTargetName,
		"Physical deletion integration "+
			backendIndexDeletionTargetName,
	)
	target = backendIndexDeletionArchiveIndex(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		target,
	)
	neighbor := createBackendIndex(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		backendIndexDeletionNeighborName,
		"Physical deletion integration "+
			backendIndexDeletionNeighborName,
	)
	if target.GetVersion() != 2 ||
		target.GetState() != opensplunk.IndexState_INDEX_STATE_ARCHIVED {
		t.Fatalf("archived target = %+v", target)
	}
	if neighbor.GetVersion() != 1 ||
		neighbor.GetState() != opensplunk.IndexState_INDEX_STATE_ACTIVE {
		t.Fatalf("active neighbor = %+v", neighbor)
	}

	backendIndexDeletionSeedRows(
		t,
		ctx,
		runtimeConnection,
	)
	seededPhysicalScope := backendIndexDeletionPhysicalScope{
		targetRows:       2,
		targetPartitions: 2,
		foreignRows:      1,
		neighborRows:     1,
	}
	backendIndexDeletionAssertPhysicalScope(
		t,
		ctx,
		runtimeConnection,
		seededPhysicalScope,
	)
	tableUUID := backendIndexDeletionTableUUID(
		t,
		ctx,
		deletionConnection,
	)

	baseDeleteRequest := backendIndexDeletionPhysicalRequestByID(target)
	zeroVersion := proto.Clone(baseDeleteRequest).(*opensplunk.DeleteIndexRequest)
	zeroVersion.ExpectedVersion = 0
	zeroVersion.Selector = nil
	finalSQLiteVersion := proto.Clone(baseDeleteRequest).(*opensplunk.DeleteIndexRequest)
	finalSQLiteVersion.ExpectedVersion = math.MaxInt64
	finalSQLiteVersion.Selector = nil
	aboveSQLiteRange := proto.Clone(baseDeleteRequest).(*opensplunk.DeleteIndexRequest)
	aboveSQLiteRange.ExpectedVersion = uint64(math.MaxInt64) + 1
	aboveSQLiteRange.Selector = nil
	unspecifiedMode := proto.Clone(baseDeleteRequest).(*opensplunk.DeleteIndexRequest)
	unspecifiedMode.DataDeletionMode =
		opensplunk.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_UNSPECIFIED
	unspecifiedMode.Selector = nil
	unknownMode := proto.Clone(baseDeleteRequest).(*opensplunk.DeleteIndexRequest)
	unknownMode.DataDeletionMode = opensplunk.IndexDataDeletionMode(99)
	unknownMode.Selector = nil
	noncanonicalConfirmation := proto.Clone(baseDeleteRequest).(*opensplunk.DeleteIndexRequest)
	noncanonicalConfirmation.ConfirmationName =
		" " + strings.ToUpper(backendIndexDeletionTargetName) + " "
	noncanonicalConfirmation.Selector = nil
	missingSelector := proto.Clone(baseDeleteRequest).(*opensplunk.DeleteIndexRequest)
	missingSelector.Selector = nil
	missingIndex := proto.Clone(baseDeleteRequest).(*opensplunk.DeleteIndexRequest)
	missingIndex.Selector = &opensplunk.IndexSelector{
		Selector: &opensplunk.IndexSelector_IndexId{
			IndexId: "idx_missing_backend_delete",
		},
	}
	activeIndex := backendIndexDeletionPhysicalRequestByID(neighbor)
	staleVersion := proto.Clone(baseDeleteRequest).(*opensplunk.DeleteIndexRequest)
	staleVersion.ExpectedVersion--
	wrongConfirmation := proto.Clone(baseDeleteRequest).(*opensplunk.DeleteIndexRequest)
	wrongConfirmation.ConfirmationName = backendIndexDeletionNeighborName

	rejections := []struct {
		name  string
		token string
		input *opensplunk.DeleteIndexRequest
		want  int
	}{
		{
			name:  "unauthenticated",
			input: baseDeleteRequest,
			want:  http.StatusUnauthorized,
		},
		{
			name:  "zero version before selector",
			token: administratorToken,
			input: zeroVersion,
			want:  http.StatusBadRequest,
		},
		{
			name:  "final SQLite version before selector",
			token: administratorToken,
			input: finalSQLiteVersion,
			want:  http.StatusBadRequest,
		},
		{
			name:  "version above SQLite range before selector",
			token: administratorToken,
			input: aboveSQLiteRange,
			want:  http.StatusBadRequest,
		},
		{
			name:  "unspecified mode before selector",
			token: administratorToken,
			input: unspecifiedMode,
			want:  http.StatusBadRequest,
		},
		{
			name:  "unknown mode before selector",
			token: administratorToken,
			input: unknownMode,
			want:  http.StatusBadRequest,
		},
		{
			name:  "noncanonical confirmation before selector",
			token: administratorToken,
			input: noncanonicalConfirmation,
			want:  http.StatusBadRequest,
		},
		{
			name:  "missing selector",
			token: administratorToken,
			input: missingSelector,
			want:  http.StatusBadRequest,
		},
		{
			name:  "missing index",
			token: administratorToken,
			input: missingIndex,
			want:  http.StatusNotFound,
		},
		{
			name:  "active index",
			token: administratorToken,
			input: activeIndex,
			want:  http.StatusConflict,
		},
		{
			name:  "stale archived version",
			token: administratorToken,
			input: staleVersion,
			want:  http.StatusConflict,
		},
		{
			name:  "wrong confirmation",
			token: administratorToken,
			input: wrongConfirmation,
			want:  http.StatusBadRequest,
		},
	}
	for _, rejection := range rejections {
		status, body, requestErr := backendIndexDeletionPostProto(
			ctx,
			httpClient,
			baseURL+"/api/indexes/delete",
			rejection.token,
			rejection.input,
			nil,
		)
		if requestErr != nil {
			t.Fatalf("%s DELETE_DATA: %v", rejection.name, requestErr)
		}
		if status != rejection.want {
			t.Fatalf(
				"%s DELETE_DATA status = %d, want %d; body = %q",
				rejection.name,
				status,
				rejection.want,
				body,
			)
		}
	}

	if err := firstServer.Interrupt(20 * time.Second); err != nil {
		t.Fatalf("gracefully stop server after rejected requests: %v", err)
	}
	backendIndexDeletionAssertRejectedControlState(
		t,
		ctx,
		controlDBPath,
		target,
		neighbor,
	)
	backendIndexDeletionAssertPhysicalScope(
		t,
		ctx,
		runtimeConnection,
		seededPhysicalScope,
	)

	secondServer := startServer()
	if err := clickhouse.ExecuteBootstrapSQLForTest(
		ctx,
		"SYSTEM STOP MERGES open_splunk.events",
	); err != nil {
		t.Fatalf("stop ClickHouse merges: %v", err)
	}
	mergesStopped := true
	t.Cleanup(func() {
		if !mergesStopped {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			20*time.Second,
		)
		defer cleanupCancel()
		if err := clickhouse.ExecuteBootstrapSQLForTest(
			cleanupCtx,
			"SYSTEM START MERGES open_splunk.events",
		); err != nil {
			t.Errorf("restart ClickHouse merges during cleanup: %v", err)
		}
	})

	const concurrentAdmissions = 16
	type admissionResult struct {
		status   int
		body     []byte
		response *opensplunk.DeleteIndexResponse
		err      error
	}
	startAdmissions := make(chan struct{})
	admissionResults := make(chan admissionResult, concurrentAdmissions)
	var admissionCallers sync.WaitGroup
	admissionCallers.Add(concurrentAdmissions)
	for range concurrentAdmissions {
		go func() {
			defer admissionCallers.Done()
			<-startAdmissions
			var response opensplunk.DeleteIndexResponse
			status, body, requestErr := backendIndexDeletionPostProto(
				ctx,
				httpClient,
				baseURL+"/api/indexes/delete",
				administratorToken,
				backendIndexDeletionPhysicalRequestByID(target),
				&response,
			)
			admissionResults <- admissionResult{
				status:   status,
				body:     body,
				response: &response,
				err:      requestErr,
			}
		}()
	}
	close(startAdmissions)
	admissionCallers.Wait()
	close(admissionResults)

	var operationID string
	for result := range admissionResults {
		if result.err != nil {
			t.Fatalf("concurrent DELETE_DATA: %v", result.err)
		}
		if result.status != http.StatusOK {
			t.Fatalf(
				"concurrent DELETE_DATA status = %d, body = %q",
				result.status,
				result.body,
			)
		}
		if result.response.GetIndexId() != target.GetIndexId() ||
			result.response.GetDeletionOperationId() == "" {
			t.Fatalf("concurrent DELETE_DATA response = %+v", result.response)
		}
		if operationID == "" {
			operationID = result.response.GetDeletionOperationId()
		} else if result.response.GetDeletionOperationId() != operationID {
			t.Fatalf(
				"concurrent DELETE_DATA operation ID = %q, want %q",
				result.response.GetDeletionOperationId(),
				operationID,
			)
		}
	}
	if operationID == "" {
		t.Fatal("concurrent DELETE_DATA returned no operation ID")
	}

	backendIndexDeletionWaitForMutation(
		t,
		ctx,
		deletionConnection,
		secondServer,
		protectedValues,
		1,
		1,
	)
	backendIndexDeletionAssertPhysicalScope(
		t,
		ctx,
		runtimeConnection,
		seededPhysicalScope,
	)
	if err := secondServer.Kill(10 * time.Second); err != nil {
		t.Fatalf("crash server with pending ClickHouse mutation: %v", err)
	}

	operation, attempt := backendIndexDeletionInspectPendingControl(
		t,
		ctx,
		controlDBPath,
		operationID,
		target,
		tableUUID,
	)
	thirdServer := startServer()

	var restartedResponse opensplunk.DeleteIndexResponse
	restartedStatus, restartedBody, restartedErr :=
		backendIndexDeletionPostProto(
			ctx,
			httpClient,
			baseURL+"/api/indexes/delete",
			administratorToken,
			backendIndexDeletionPhysicalRequestByName(target),
			&restartedResponse,
		)
	if restartedErr != nil {
		t.Fatalf("restart DELETE_DATA retry: %v", restartedErr)
	}
	if restartedStatus != http.StatusOK {
		t.Fatalf(
			"restart DELETE_DATA status = %d, body = %q",
			restartedStatus,
			restartedBody,
		)
	}
	if restartedResponse.GetIndexId() != target.GetIndexId() ||
		restartedResponse.GetDeletionOperationId() != operationID {
		t.Fatalf(
			"restart DELETE_DATA response = %+v, want index %q operation %q",
			&restartedResponse,
			target.GetIndexId(),
			operationID,
		)
	}
	backendIndexDeletionWaitForMutation(
		t,
		ctx,
		deletionConnection,
		thirdServer,
		protectedValues,
		1,
		1,
	)
	backendIndexDeletionAssertPhysicalScope(
		t,
		ctx,
		runtimeConnection,
		seededPhysicalScope,
	)

	if err := clickhouse.ExecuteBootstrapSQLForTest(
		ctx,
		"SYSTEM START MERGES open_splunk.events",
	); err != nil {
		t.Fatalf("start ClickHouse merges: %v", err)
	}
	mergesStopped = false

	backendIndexDeletionWaitForMutation(
		t,
		ctx,
		deletionConnection,
		thirdServer,
		protectedValues,
		1,
		0,
	)
	backendIndexDeletionAssertPhysicalScope(
		t,
		ctx,
		runtimeConnection,
		backendIndexDeletionPhysicalScope{
			foreignRows:  1,
			neighborRows: 1,
		},
	)
	backendIndexDeletionWaitForCatalogTombstone(
		t,
		ctx,
		httpClient,
		baseURL,
		administratorToken,
		target,
		thirdServer,
		protectedValues,
	)

	postTerminalStatus, postTerminalBody, postTerminalErr :=
		backendIndexDeletionPostProto(
			ctx,
			httpClient,
			baseURL+"/api/indexes/delete",
			administratorToken,
			backendIndexDeletionPhysicalRequestByID(target),
			nil,
		)
	if postTerminalErr != nil {
		t.Fatalf("post-terminal DELETE_DATA retry: %v", postTerminalErr)
	}
	if postTerminalStatus != http.StatusNotFound {
		t.Fatalf(
			"post-terminal DELETE_DATA status = %d, want %d; body = %q",
			postTerminalStatus,
			http.StatusNotFound,
			postTerminalBody,
		)
	}
	if err := thirdServer.Kill(10 * time.Second); err != nil {
		t.Fatalf("crash server after terminal completion: %v", err)
	}

	completion := backendIndexDeletionAssertTerminalControlState(
		t,
		ctx,
		controlDBPath,
		operation,
		attempt,
	)
	restartedCompletion := backendIndexDeletionAssertTerminalControlState(
		t,
		ctx,
		controlDBPath,
		operation,
		attempt,
	)
	if !backendIndexDeletionSameCompletion(
		restartedCompletion,
		completion,
	) {
		t.Fatalf(
			"reopened terminal completion = %#v, want %#v",
			restartedCompletion,
			completion,
		)
	}
}

func backendIndexDeletionPinnedImage(t *testing.T) string {
	t.Helper()

	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func backendIndexDeletionClickHouseConnection(
	t *testing.T,
	ctx context.Context,
	clickhouse *testsupport.ClickHouseContainer,
	username string,
	password string,
) clickhousedriver.Conn {
	t.Helper()

	connection, err := clickhousedriver.Open(&clickhousedriver.Options{
		Addr: []string{clickhouse.Address},
		Auth: clickhousedriver.Auth{
			Database: clickhouse.Database,
			Username: username,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("open ClickHouse connection for %q: %v", username, err)
	}
	if err := connection.Ping(ctx); err != nil {
		_ = connection.Close()
		t.Fatalf("ping ClickHouse connection for %q: %v", username, err)
	}
	return connection
}

func backendIndexDeletionArchiveIndex(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	index *opensplunk.Index,
) *opensplunk.Index {
	t.Helper()

	var response opensplunk.SetIndexStateResponse
	status, body, err := backendIndexDeletionPostProto(
		ctx,
		client,
		baseURL+"/api/indexes/state/set",
		administratorToken,
		&opensplunk.SetIndexStateRequest{
			Selector: &opensplunk.IndexSelector{
				Selector: &opensplunk.IndexSelector_IndexId{
					IndexId: index.GetIndexId(),
				},
			},
			ExpectedVersion: index.GetVersion(),
			State: opensplunk.
				IndexState_INDEX_STATE_ARCHIVED,
		},
		&response,
	)
	if err != nil {
		t.Fatalf("archive index %q: %v", index.GetDefinition().GetName(), err)
	}
	if status != http.StatusOK {
		t.Fatalf(
			"archive index %q status = %d, body = %q",
			index.GetDefinition().GetName(),
			status,
			body,
		)
	}
	archived := response.GetIndex()
	if archived.GetIndexId() != index.GetIndexId() ||
		archived.GetVersion() != index.GetVersion()+1 ||
		archived.GetState() != opensplunk.IndexState_INDEX_STATE_ARCHIVED {
		t.Fatalf("archived index = %+v, created = %+v", archived, index)
	}
	return archived
}

func backendIndexDeletionPhysicalRequestByID(
	index *opensplunk.Index,
) *opensplunk.DeleteIndexRequest {
	return &opensplunk.DeleteIndexRequest{
		Selector: &opensplunk.IndexSelector{
			Selector: &opensplunk.IndexSelector_IndexId{
				IndexId: index.GetIndexId(),
			},
		},
		ExpectedVersion: index.GetVersion(),
		DataDeletionMode: opensplunk.
			IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_DELETE_DATA,
		ConfirmationName: index.GetDefinition().GetName(),
	}
}

func backendIndexDeletionPhysicalRequestByName(
	index *opensplunk.Index,
) *opensplunk.DeleteIndexRequest {
	return &opensplunk.DeleteIndexRequest{
		Selector: &opensplunk.IndexSelector{
			Selector: &opensplunk.IndexSelector_IndexName{
				IndexName: " " + strings.ToUpper(
					index.GetDefinition().GetName(),
				) + " ",
			},
		},
		ExpectedVersion: index.GetVersion(),
		DataDeletionMode: opensplunk.
			IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_DELETE_DATA,
		ConfirmationName: index.GetDefinition().GetName(),
	}
}

func backendIndexDeletionPostProto(
	ctx context.Context,
	client *http.Client,
	url string,
	bearerToken string,
	input proto.Message,
	output proto.Message,
) (int, []byte, error) {
	response, err := performProtoRequestWithBearer(
		ctx,
		client,
		url,
		bearerToken,
		input,
	)
	if err != nil {
		return 0, nil, err
	}
	if response.statusCode != http.StatusOK || output == nil {
		return response.statusCode, response.body, nil
	}
	if response.contentType !=
		"application/x-protobuf" {
		return 0, nil, fmt.Errorf(
			"POST %s content type = %q",
			url,
			response.contentType,
		)
	}
	if err := proto.Unmarshal(response.body, output); err != nil {
		return 0, nil, fmt.Errorf("decode POST %s: %w", url, err)
	}
	return response.statusCode, response.body, nil
}

func backendIndexDeletionSeedRows(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()

	fixtures := []struct {
		eventID   string
		tenantID  string
		indexName string
		eventTime time.Time
		sequence  uint64
	}{
		{
			eventID:   "physical-delete-target-january",
			tenantID:  backendIndexDeletionTenant,
			indexName: backendIndexDeletionTargetName,
			eventTime: time.Date(2025, time.January, 15, 1, 2, 3, 0, time.UTC),
			sequence:  1,
		},
		{
			eventID:   "physical-delete-target-february",
			tenantID:  backendIndexDeletionTenant,
			indexName: backendIndexDeletionTargetName,
			eventTime: time.Date(2025, time.February, 15, 1, 2, 3, 0, time.UTC),
			sequence:  2,
		},
		{
			eventID:   "physical-delete-foreign-tenant",
			tenantID:  backendIndexDeletionForeignTenant,
			indexName: backendIndexDeletionTargetName,
			eventTime: time.Date(2025, time.January, 16, 1, 2, 3, 0, time.UTC),
			sequence:  3,
		},
		{
			eventID:   "physical-delete-neighbor-index",
			tenantID:  backendIndexDeletionTenant,
			indexName: backendIndexDeletionNeighborName,
			eventTime: time.Date(2025, time.February, 16, 1, 2, 3, 0, time.UTC),
			sequence:  4,
		},
	}
	indexTime := time.Now().UTC().Truncate(time.Millisecond)
	expiresAt := indexTime.Add(24 * time.Hour)
	source := ingest.NativeCollectorSource("backend-index-deletion-fixture-collector")
	batch, err := connection.PrepareBatch(
		ctx,
		`INSERT INTO open_splunk.events
		    (event_id, tenant_id, index_name, event_time, index_time,
		     field_metadata_version,
		     collector_id, ingest_source_kind, ingest_source_id,
		     expires_at, visibility_seq)`,
	)
	if err != nil {
		t.Fatalf("prepare ClickHouse event fixture batch: %v", err)
	}
	for _, fixture := range fixtures {
		if err := batch.Append(
			fixture.eventID,
			fixture.tenantID,
			fixture.indexName,
			fixture.eventTime,
			indexTime,
			eventfields.CurrentFieldMetadataVersion,
			source.CollectorID,
			uint8(source.Kind),
			source.ID,
			expiresAt,
			fixture.sequence,
		); err != nil {
			t.Fatalf(
				"append ClickHouse event fixture %q: %v",
				fixture.eventID,
				err,
			)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send ClickHouse event fixture batch: %v", err)
	}
}

func backendIndexDeletionAssertPhysicalScope(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	want backendIndexDeletionPhysicalScope,
) {
	t.Helper()

	got, err := backendIndexDeletionReadPhysicalScope(ctx, connection)
	if err != nil {
		t.Fatalf("query physical deletion scope: %v", err)
	}
	if got != want {
		t.Fatalf(
			"physical deletion scope = %+v, want %+v",
			got,
			want,
		)
	}
}

func backendIndexDeletionReadPhysicalScope(
	ctx context.Context,
	connection clickhousedriver.Conn,
) (backendIndexDeletionPhysicalScope, error) {
	var scope backendIndexDeletionPhysicalScope
	err := connection.QueryRow(
		ctx,
		`SELECT
		     countIf(tenant_id = ? AND index_name = ?),
		     uniqExactIf(
		       toYYYYMM(event_time),
		       tenant_id = ? AND index_name = ?
		     ),
		     countIf(tenant_id = ? AND index_name = ?),
		     countIf(tenant_id = ? AND index_name = ?)
		 FROM open_splunk.events`,
		backendIndexDeletionTenant,
		backendIndexDeletionTargetName,
		backendIndexDeletionTenant,
		backendIndexDeletionTargetName,
		backendIndexDeletionForeignTenant,
		backendIndexDeletionTargetName,
		backendIndexDeletionTenant,
		backendIndexDeletionNeighborName,
	).Scan(
		&scope.targetRows,
		&scope.targetPartitions,
		&scope.foreignRows,
		&scope.neighborRows,
	)
	return scope, err
}

func backendIndexDeletionTableUUID(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) string {
	t.Helper()

	var tableUUID, engine string
	if err := connection.QueryRow(
		ctx,
		`SELECT toString(uuid), engine
		 FROM system.tables
		 WHERE database = 'open_splunk'
		   AND name = 'events'
		 LIMIT 1`,
	).Scan(&tableUUID, &engine); err != nil {
		t.Fatalf("resolve deletion ClickHouse target: %v", err)
	}
	if tableUUID == "" || engine != "MergeTree" {
		t.Fatalf(
			"deletion ClickHouse target = UUID %q engine %q",
			tableUUID,
			engine,
		)
	}
	return tableUUID
}

func backendIndexDeletionAssertRejectedControlState(
	t *testing.T,
	ctx context.Context,
	controlDBPath string,
	target *opensplunk.Index,
	neighbor *opensplunk.Index,
) {
	t.Helper()

	db := backendIndexDeletionOpenControl(t, ctx, controlDBPath)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close rejected-state control plane: %v", err)
		}
	}()

	backendIndexDeletionAssertStoredIndex(
		t,
		ctx,
		db,
		target,
		control.IndexStateArchived,
		target.GetVersion(),
	)
	backendIndexDeletionAssertStoredIndex(
		t,
		ctx,
		db,
		neighbor,
		control.IndexStateActive,
		neighbor.GetVersion(),
	)
	if _, err := db.NextIndexDeletionOperation(ctx); !errors.Is(
		err,
		control.ErrNotFound,
	) {
		t.Fatalf(
			"NextIndexDeletionOperation(rejected) error = %v, want ErrNotFound",
			err,
		)
	}
	backendIndexDeletionAssertControlCounts(
		t,
		ctx,
		db,
		backendIndexDeletionControlCounts{},
	)
}

func backendIndexDeletionInspectPendingControl(
	t *testing.T,
	ctx context.Context,
	controlDBPath string,
	operationID string,
	target *opensplunk.Index,
	tableUUID string,
) (
	control.IndexDeletionOperation,
	control.IndexDeletionMutationAttempt,
) {
	t.Helper()

	db := backendIndexDeletionOpenControl(t, ctx, controlDBPath)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close pending-state control plane: %v", err)
		}
	}()

	operation, err := db.GetIndexDeletionOperation(ctx, operationID)
	if err != nil {
		t.Fatalf("GetIndexDeletionOperation(pending): %v", err)
	}
	if operation.ID != operationID ||
		operation.TenantID != backendIndexDeletionTenant ||
		operation.IndexID != target.GetIndexId() ||
		operation.IndexName != backendIndexDeletionTargetName ||
		operation.ArchivedVersion != target.GetVersion() ||
		operation.DeletingVersion != target.GetVersion()+1 ||
		operation.CreatedAt.IsZero() {
		t.Fatalf(
			"pending deletion operation = %#v, target = %+v",
			operation,
			target,
		)
	}
	next, err := db.NextIndexDeletionOperation(ctx)
	if err != nil {
		t.Fatalf("NextIndexDeletionOperation(pending): %v", err)
	}
	if next != operation {
		t.Fatalf("next pending operation = %#v, want %#v", next, operation)
	}
	attempt, err := db.GetIndexDeletionMutationAttempt(ctx, operationID)
	if err != nil {
		t.Fatalf("GetIndexDeletionMutationAttempt(pending): %v", err)
	}
	wantTarget := control.IndexDeletionMutationTarget{
		TenantID:  backendIndexDeletionTenant,
		Database:  "open_splunk",
		Table:     "events",
		TableUUID: tableUUID,
	}
	if attempt.CorrelationID == "" ||
		attempt.DeletionOperationID != operationID ||
		attempt.IndexID != target.GetIndexId() ||
		attempt.IndexName != backendIndexDeletionTargetName ||
		attempt.Target != wantTarget ||
		attempt.ProtocolVersion !=
			control.IndexDeletionMutationProtocolVersion ||
		attempt.CreatedAt.Before(operation.CreatedAt) {
		t.Fatalf(
			"pending mutation attempt = %#v, operation = %#v, target = %#v",
			attempt,
			operation,
			wantTarget,
		)
	}
	backendIndexDeletionAssertStoredIndex(
		t,
		ctx,
		db,
		target,
		control.IndexStateDeleting,
		target.GetVersion()+1,
	)
	if _, err := db.GetIndexDataDeletionCompletion(
		ctx,
		operationID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"GetIndexDataDeletionCompletion(pending) error = %v, want ErrNotFound",
			err,
		)
	}
	backendIndexDeletionAssertControlCounts(
		t,
		ctx,
		db,
		backendIndexDeletionControlCounts{
			operations: 1,
			attempts:   1,
		},
	)
	return operation, attempt
}

func backendIndexDeletionAssertTerminalControlState(
	t *testing.T,
	ctx context.Context,
	controlDBPath string,
	operation control.IndexDeletionOperation,
	attempt control.IndexDeletionMutationAttempt,
) control.IndexDataDeletionCompletion {
	t.Helper()

	db := backendIndexDeletionOpenControl(t, ctx, controlDBPath)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close terminal-state control plane: %v", err)
		}
	}()

	completion, err := db.GetIndexDataDeletionCompletion(ctx, operation.ID)
	if err != nil {
		t.Fatalf("GetIndexDataDeletionCompletion(terminal): %v", err)
	}
	if completion.DeletionOperationID != operation.ID ||
		completion.CorrelationID != attempt.CorrelationID ||
		completion.IndexID != operation.IndexID ||
		completion.IndexName != operation.IndexName ||
		completion.ArchivedVersion != operation.ArchivedVersion ||
		completion.DeletedVersion != operation.DeletingVersion ||
		completion.Target != attempt.Target ||
		completion.ProtocolVersion !=
			control.IndexDeletionMutationProtocolVersion ||
		!completion.OperationCreatedAt.Equal(operation.CreatedAt) ||
		!completion.MutationCreatedAt.Equal(attempt.CreatedAt) ||
		completion.CompletedAt.Before(completion.MutationCreatedAt) {
		t.Fatalf(
			"terminal completion = %#v, operation = %#v, attempt = %#v",
			completion,
			operation,
			attempt,
		)
	}
	if _, err := db.GetIndex(ctx, operation.IndexID); !errors.Is(
		err,
		control.ErrNotFound,
	) {
		t.Fatalf("GetIndex(terminal) error = %v, want ErrNotFound", err)
	}
	if _, err := db.GetIndexDeletionOperation(
		ctx,
		operation.ID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"GetIndexDeletionOperation(terminal) error = %v, want ErrNotFound",
			err,
		)
	}
	if _, err := db.GetIndexDeletionMutationAttempt(
		ctx,
		operation.ID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"GetIndexDeletionMutationAttempt(terminal) error = %v, want ErrNotFound",
			err,
		)
	}
	if _, err := db.NextIndexDeletionOperation(ctx); !errors.Is(
		err,
		control.ErrNotFound,
	) {
		t.Fatalf(
			"NextIndexDeletionOperation(terminal) error = %v, want ErrNotFound",
			err,
		)
	}
	if _, err := db.CreateIndex(ctx, control.IndexDefinition{
		Name:             operation.IndexName,
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf(
			"CreateIndex(reserved terminal name) error = %v, want ErrAlreadyExists",
			err,
		)
	}

	var (
		state             string
		version           uint64
		tombstoneName     string
		tombstoneVersion  uint64
		tombstoneUnixTime int64
	)
	if err := db.SQLDB().QueryRowContext(
		ctx,
		`SELECT
		     indexes.state,
		     indexes.version,
		     index_deletion_tombstones.name,
		     index_deletion_tombstones.deleted_version,
		     index_deletion_tombstones.deleted_at_unix_micro
		 FROM indexes
		 JOIN index_deletion_tombstones
		   ON index_deletion_tombstones.index_id = indexes.index_id
		 WHERE indexes.index_id = ?`,
		operation.IndexID,
	).Scan(
		&state,
		&version,
		&tombstoneName,
		&tombstoneVersion,
		&tombstoneUnixTime,
	); err != nil {
		t.Fatalf("read terminal retained index and tombstone: %v", err)
	}
	if state != string(control.IndexStateDeleting) ||
		version != operation.DeletingVersion ||
		tombstoneName != operation.IndexName ||
		tombstoneVersion != operation.DeletingVersion ||
		tombstoneUnixTime != completion.CompletedAt.UnixMicro() {
		t.Fatalf(
			"terminal row = state %q version %d tombstone %q/%d/%d; operation=%#v completion=%#v",
			state,
			version,
			tombstoneName,
			tombstoneVersion,
			tombstoneUnixTime,
			operation,
			completion,
		)
	}
	backendIndexDeletionAssertControlCounts(
		t,
		ctx,
		db,
		backendIndexDeletionControlCounts{
			tombstones:  1,
			completions: 1,
		},
	)
	return completion
}

func backendIndexDeletionOpenControl(
	t *testing.T,
	ctx context.Context,
	controlDBPath string,
) *control.DB {
	t.Helper()

	db, err := control.Open(ctx, controlDBPath)
	if err != nil {
		t.Fatalf("open deletion control plane %q: %v", controlDBPath, err)
	}
	return db
}

func backendIndexDeletionAssertStoredIndex(
	t *testing.T,
	ctx context.Context,
	db *control.DB,
	want *opensplunk.Index,
	state control.IndexState,
	version uint64,
) {
	t.Helper()

	index, err := db.GetIndex(ctx, want.GetIndexId())
	if err != nil {
		t.Fatalf("GetIndex(%q): %v", want.GetIndexId(), err)
	}
	if index.ID != want.GetIndexId() ||
		index.Definition.Name != want.GetDefinition().GetName() ||
		index.State != state ||
		index.Version != version {
		t.Fatalf(
			"stored index = %#v, want ID %q name %q state %q version %d",
			index,
			want.GetIndexId(),
			want.GetDefinition().GetName(),
			state,
			version,
		)
	}
}

func backendIndexDeletionAssertControlCounts(
	t *testing.T,
	ctx context.Context,
	db *control.DB,
	want backendIndexDeletionControlCounts,
) {
	t.Helper()

	tables := []struct {
		name string
		want int
	}{
		{"index_deletion_operations", want.operations},
		{"index_deletion_mutation_attempts", want.attempts},
		{"index_deletion_tombstones", want.tombstones},
		{"index_data_deletion_completions", want.completions},
	}
	for _, table := range tables {
		var got int
		if err := db.SQLDB().QueryRowContext(
			ctx,
			"SELECT count(*) FROM "+table.name,
		).Scan(&got); err != nil {
			t.Fatalf("count %s: %v", table.name, err)
		}
		if got != table.want {
			t.Fatalf(
				"%s rows = %d, want %d",
				table.name,
				got,
				table.want,
			)
		}
	}
}

func backendIndexDeletionWaitForMutation(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	process *managedProcess,
	protectedValues []string,
	wantMatching uint64,
	wantPending uint64,
) {
	t.Helper()

	backendIndexDeletionWaitForCondition(
		t,
		ctx,
		45*time.Second,
		process,
		protectedValues,
		"correlated deletion mutation",
		func(probeCtx context.Context) (bool, string, error) {
			var matching, pending uint64
			err := connection.QueryRow(
				probeCtx,
				`SELECT
				     countIf(position(command, ?) != 0),
				     countIf(position(command, ?) != 0 AND is_done = 0)
				 FROM system.mutations
				 WHERE database = 'open_splunk'
				   AND table = 'events'`,
				backendIndexDeletionMarkerPrefix,
				backendIndexDeletionMarkerPrefix,
			).Scan(&matching, &pending)
			diagnostic := fmt.Sprintf(
				"%d/%d pending, want %d/%d",
				matching,
				pending,
				wantMatching,
				wantPending,
			)
			// A submitted asynchronous mutation remains pending while it
			// converges to a zero-pending target. Only another matching row
			// proves that the coordinator submitted a duplicate mutation.
			if err == nil && matching > wantMatching {
				t.Fatalf(
					"correlated deletion mutations = %s",
					diagnostic,
				)
			}
			return err == nil &&
				matching == wantMatching &&
				pending == wantPending, diagnostic, err
		},
	)
}

func backendIndexDeletionWaitForCatalogTombstone(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	target *opensplunk.Index,
	process *managedProcess,
	protectedValues []string,
) {
	t.Helper()

	backendIndexDeletionWaitForCondition(
		t,
		ctx,
		45*time.Second,
		process,
		protectedValues,
		"terminal catalog tombstone",
		func(probeCtx context.Context) (bool, string, error) {
			var response opensplunk.GetIndexResponse
			status, body, err := backendIndexDeletionPostProto(
				probeCtx,
				client,
				baseURL+"/api/indexes/get",
				administratorToken,
				&opensplunk.GetIndexRequest{
					Selector: &opensplunk.IndexSelector{
						Selector: &opensplunk.IndexSelector_IndexId{
							IndexId: target.GetIndexId(),
						},
					},
				},
				&response,
			)
			diagnostic := fmt.Sprintf(
				"status %d, body %q",
				status,
				body,
			)
			if err != nil {
				return false, diagnostic, err
			}
			if status == http.StatusNotFound {
				return true, diagnostic, nil
			}
			if status != http.StatusOK {
				t.Fatalf(
					"catalog GET while deletion completes %s",
					diagnostic,
				)
			}
			index := response.GetIndex()
			if index.GetIndexId() != target.GetIndexId() ||
				index.GetState() !=
					opensplunk.IndexState_INDEX_STATE_DELETING ||
				index.GetVersion() != target.GetVersion()+1 {
				t.Fatalf(
					"catalog index while deletion completes = %+v",
					index,
				)
			}
			return false, diagnostic, nil
		},
	)
}

func backendIndexDeletionWaitForCondition(
	t *testing.T,
	ctx context.Context,
	timeout time.Duration,
	process *managedProcess,
	protectedValues []string,
	label string,
	probe func(context.Context) (bool, string, error),
) {
	t.Helper()

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastDiagnostic string
	var lastErr error
	for {
		probeCtx, cancelProbe := context.WithTimeout(
			waitCtx,
			5*time.Second,
		)
		done, diagnostic, err := probe(probeCtx)
		cancelProbe()
		lastDiagnostic = diagnostic
		lastErr = err
		if done {
			return
		}
		if process.Exited() {
			t.Fatalf(
				"server exited waiting for %s: %v (last %s, err %v)\nlogs:\n%s",
				label,
				process.Err(),
				redactForFailure(lastDiagnostic, protectedValues...),
				lastErr,
				redactForFailure(process.Logs(), protectedValues...),
			)
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf(
				"wait for %s: %v (last %s, err %v)\nlogs:\n%s",
				label,
				waitCtx.Err(),
				redactForFailure(lastDiagnostic, protectedValues...),
				lastErr,
				redactForFailure(process.Logs(), protectedValues...),
			)
		case <-ticker.C:
		}
	}
}

func backendIndexDeletionSameCompletion(
	left control.IndexDataDeletionCompletion,
	right control.IndexDataDeletionCompletion,
) bool {
	return left.DeletionOperationID == right.DeletionOperationID &&
		left.CorrelationID == right.CorrelationID &&
		left.IndexID == right.IndexID &&
		left.IndexName == right.IndexName &&
		left.ArchivedVersion == right.ArchivedVersion &&
		left.DeletedVersion == right.DeletedVersion &&
		left.Target == right.Target &&
		left.ProtocolVersion == right.ProtocolVersion &&
		left.OperationCreatedAt.Equal(right.OperationCreatedAt) &&
		left.MutationCreatedAt.Equal(right.MutationCreatedAt) &&
		left.CompletedAt.Equal(right.CompletedAt)
}
