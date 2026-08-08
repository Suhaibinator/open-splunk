package knowledgecatalog

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const (
	writerRecoveryChildMarkerEnv   = "OPEN_SPLUNK_WRITER_RECOVERY_CHILD"
	writerRecoveryDatabasePathEnv  = "OPEN_SPLUNK_WRITER_RECOVERY_DATABASE"
	writerRecoveryBoundaryEnv      = "OPEN_SPLUNK_WRITER_RECOVERY_BOUNDARY"
	writerRecoveryRouteEnv         = "OPEN_SPLUNK_WRITER_RECOVERY_ROUTE"
	writerRecoveryReadyFDEnv       = "OPEN_SPLUNK_WRITER_RECOVERY_READY_FD"
	writerRecoveryGateFDEnv        = "OPEN_SPLUNK_WRITER_RECOVERY_GATE_FD"
	writerRecoveryTenantID         = "tenant-writer-subprocess-recovery"
	writerRecoveryOwnerID          = "owner-writer-subprocess-recovery"
	writerRecoveryAppID            = "app_000000000300000000001A"
	writerRecoveryObjectID         = "ko_writer_subprocess_recovery_0001"
	writerRecoveryActorID          = "writer-subprocess-recovery-admin"
	writerRecoveryClientRequestID  = "writer-recovery-create-request-0001"
	writerRecoveryBaselineRequest  = "writer-recovery-baseline-request-001"
	writerRecoveryUpdateRequest    = "writer-recovery-update-request-0001"
	writerRecoveryStateRequest     = "writer-recovery-state-request-00001"
	writerRecoveryDeleteRequest    = "writer-recovery-delete-request-0001"
	writerRecoveryReadyFD          = 3
	writerRecoveryGateFD           = 4
	writerRecoveryChildWaitTimeout = 30 * time.Second
)

var writerRecoveryCursorKey = []byte("knowledge-writer-subprocess-recovery-cursor-key-v1")

func TestWriterSubprocessCrashRecoveryRollsBackPrecommit(t *testing.T) {
	boundaries := []writerHookBoundary{
		writerHookPrepared,
		writerHookIdempotencyChecked,
		writerHookCatalogLedgersReady,
		writerHookCapacityChecked,
		writerHookDefinitionBlobReady,
		writerHookVersionInserted,
		writerHookDependencyRowsInserted,
		writerHookDependencySealed,
		writerHookProjectionInserted,
		writerHookSelectorRowsInserted,
		writerHookProjectionSealed,
		writerHookRegistryPublished,
		writerHookSuccessAuditAppended,
		writerHookCatalogRevisionAdvanced,
		writerHookCommitAuthorityRecorded,
		writerHookIdempotencyOutcomeRecorded,
		writerHookBeforeCommit,
	}
	for _, boundary := range boundaries {
		t.Run(string(boundary), func(t *testing.T) {
			exerciseWriterRecoveryCrash(t, mutationRouteCreate, boundary)
		})
	}
}

func TestWriterSubprocessCrashRecoveryPreservesCommittedOutcome(t *testing.T) {
	exerciseWriterRecoveryCrash(t, mutationRouteCreate, writerHookAfterCommit)
}

func TestWriterSubprocessExistingRoutesRecoverAtCommitBoundary(t *testing.T) {
	for _, route := range []string{mutationRouteUpdate, mutationRouteSetState, mutationRouteDelete} {
		t.Run(route, func(t *testing.T) {
			for _, boundary := range []writerHookBoundary{writerHookBeforeCommit, writerHookAfterCommit} {
				t.Run(string(boundary), func(t *testing.T) {
					exerciseWriterRecoveryCrash(t, route, boundary)
				})
			}
		})
	}
}

func exerciseWriterRecoveryCrash(t *testing.T, route string, boundary writerHookBoundary) {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "control.sqlite")
	before := initializeWriterRecoveryDatabase(t, databasePath, route)
	killWriterRecoveryChildAtBoundary(t, databasePath, route, boundary)

	// Deliberately do not unlink, rename, checkpoint, or copy the WAL/SHM. Open
	// must recover the exact database and sidecars left by the killed process.
	database := openWriterRecoveryDatabase(t, databasePath)
	defer closeWriterRecoveryDatabase(t, database)
	assertWriterRecoveryIntegrity(t, database)
	afterCrash := readWriterRecoverySnapshot(t, database)
	if boundary == writerHookAfterCommit {
		if reflect.DeepEqual(afterCrash, before) {
			t.Fatalf("after-commit SIGKILL lost the committed %s mutation", route)
		}
		assertWriterRecoveryRouteCommitted(t, database, route, nil)
	} else {
		assertWriterRecoverySnapshotsEqual(
			t,
			afterCrash,
			before,
			"pre-commit SIGKILL exposed partial mutation authority",
		)
	}

	var idCalls atomic.Int64
	writer, actorContext := newWriterRecoveryWriter(t, database, func() (string, error) {
		idCalls.Add(1)
		if route == mutationRouteCreate && boundary != writerHookAfterCommit {
			return writerRecoveryObjectID, nil
		}
		return "", errors.New("writer recovery replay unexpectedly requested an object identity")
	})
	result, err := invokeWriterRecoveryRoute(writer, actorContext, route)
	if err != nil {
		t.Fatalf("invoke %s after SIGKILL at %s: %v", route, boundary, err)
	}
	wantIDCalls := int64(0)
	if route == mutationRouteCreate && boundary != writerHookAfterCommit {
		wantIDCalls = 1
	}
	if idCalls.Load() != wantIDCalls {
		t.Fatalf("%s ID allocations after SIGKILL at %s = %d, want %d", route, boundary, idCalls.Load(), wantIDCalls)
	}
	assertWriterRecoveryRouteCommitted(t, database, route, result)
	committed := readWriterRecoverySnapshot(t, database)
	if boundary == writerHookAfterCommit {
		assertWriterRecoverySnapshotsEqual(t, committed, afterCrash, "committed replay changed durable authority")
	}

	replayed, err := invokeWriterRecoveryRoute(writer, actorContext, route)
	if err != nil {
		t.Fatalf("second %s replay after %s: %v", route, boundary, err)
	}
	if idCalls.Load() != wantIDCalls {
		t.Fatalf("second %s replay ID calls = %d, want %d", route, idCalls.Load(), wantIDCalls)
	}
	if !proto.Equal(result, replayed) {
		t.Fatalf("%s outcome replays differ after %s:\nfirst: %v\nsecond: %v", route, boundary, result, replayed)
	}
	assertWriterRecoverySnapshotsEqual(
		t,
		readWriterRecoverySnapshot(t, database),
		committed,
		"exact replay changed durable authority",
	)
	assertWriterRecoveryIntegrity(t, database)
}

// TestWriterSubprocessRecoveryHelper is entered only by the test binary that
// the parent launches with two inherited pipes. The hook writes one bounded
// boundary name and then blocks on the gate pipe while the transaction remains
// live. The parent deliberately never releases the gate: it sends SIGKILL.
func TestWriterSubprocessRecoveryHelper(t *testing.T) {
	if os.Getenv(writerRecoveryChildMarkerEnv) == "" {
		return
	}
	if os.Getenv(writerRecoveryChildMarkerEnv) != "1" {
		t.Fatalf("invalid subprocess recovery child marker")
	}
	databasePath := requireWriterRecoveryChildPath(t)
	boundary := requireWriterRecoveryChildBoundary(t)
	route := requireWriterRecoveryChildRoute(t)
	readyFD := requireWriterRecoveryChildFD(t, writerRecoveryReadyFDEnv, writerRecoveryReadyFD)
	gateFD := requireWriterRecoveryChildFD(t, writerRecoveryGateFDEnv, writerRecoveryGateFD)
	ready := os.NewFile(uintptr(readyFD), "writer-recovery-ready")
	gate := os.NewFile(uintptr(gateFD), "writer-recovery-gate")
	if ready == nil || gate == nil {
		t.Fatal("subprocess recovery inherited pipe is unavailable")
	}
	defer ready.Close()
	defer gate.Close()

	database := openWriterRecoveryDatabase(t, databasePath)
	defer closeWriterRecoveryDatabase(t, database)
	writer, actorContext := newWriterRecoveryWriter(t, database, func() (string, error) {
		return writerRecoveryObjectID, nil
	})
	writer.hook = func(_ context.Context, event writerHookEvent) error {
		if event.Boundary != boundary {
			return nil
		}
		wantVersion := uint64(1)
		if route != mutationRouteCreate {
			wantVersion = 2
		}
		wantIdentity := writerRecoveryHookHasIdentity(route, boundary)
		if event.Route != route || event.IDAttempt != 0 ||
			wantIdentity && (event.KnowledgeObjectID != writerRecoveryObjectID || event.Version != wantVersion) ||
			!wantIdentity && (event.KnowledgeObjectID != "" || event.Version != 0) {
			return fmt.Errorf("unexpected recovery hook event: %+v", event)
		}
		if _, err := fmt.Fprintln(ready, boundary); err != nil {
			return fmt.Errorf("signal recovery boundary: %w", err)
		}
		var release [1]byte
		if _, err := gate.Read(release[:]); err != nil {
			return fmt.Errorf("wait for recovery parent: %w", err)
		}
		return errors.New("subprocess recovery parent unexpectedly released the hook")
	}

	response, err := invokeWriterRecoveryRoute(writer, actorContext, route)
	t.Fatalf("subprocess recovery mutation escaped blocking hook %s: response=%v error=%v", boundary, response, err)
}

func initializeWriterRecoveryDatabase(t *testing.T, databasePath, route string) writerRecoverySnapshot {
	t.Helper()
	if !filepath.IsAbs(databasePath) {
		t.Fatalf("writer recovery database path is not absolute: %q", databasePath)
	}
	database := openWriterRecoveryDatabase(t, databasePath)
	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: writerRecoveryCursorKey,
		Clock: func() time.Time {
			return writerRecoveryMutationTime().Add(-time.Second)
		},
		IDGenerator: func() (string, error) { return writerRecoveryAppID, nil },
	})
	if err != nil {
		closeWriterRecoveryDatabase(t, database)
		t.Fatalf("control.NewAppCatalog(recovery): %v", err)
	}
	created, err := apps.CreateApp(
		t.Context(),
		control.AppAccessScope{TenantID: writerRecoveryTenantID},
		control.AppDefinition{Slug: "writer-recovery", DisplayName: "Writer recovery"},
	)
	if err != nil {
		closeWriterRecoveryDatabase(t, database)
		t.Fatalf("CreateApp(recovery): %v", err)
	}
	if created.ID != writerRecoveryAppID {
		closeWriterRecoveryDatabase(t, database)
		t.Fatalf("CreateApp(recovery) ID = %q, want %q", created.ID, writerRecoveryAppID)
	}
	if route != mutationRouteCreate {
		writer, actorContext := newWriterRecoveryWriter(t, database, func() (string, error) {
			return writerRecoveryObjectID, nil
		})
		baseline, err := writer.Create(actorContext, writerRecoveryScope(), writerRecoveryBaselineCreateRequest())
		if err != nil {
			closeWriterRecoveryDatabase(t, database)
			t.Fatalf("create %s recovery baseline: %v", route, err)
		}
		if baseline.GetKnowledgeObject().GetKnowledgeObjectId() != writerRecoveryObjectID ||
			baseline.GetKnowledgeObject().GetVersion() != 1 || baseline.GetTenantCatalogRevision() != 1 {
			closeWriterRecoveryDatabase(t, database)
			t.Fatalf("invalid %s recovery baseline: %v", route, baseline)
		}
	}
	assertWriterRecoveryIntegrity(t, database)
	snapshot := readWriterRecoverySnapshot(t, database)
	closeWriterRecoveryDatabase(t, database)
	return snapshot
}

func openWriterRecoveryDatabase(t *testing.T, databasePath string) *control.DB {
	t.Helper()
	database, err := control.Open(t.Context(), databasePath)
	if err != nil {
		t.Fatalf("control.Open(writer recovery %q): %v", databasePath, err)
	}
	return database
}

func closeWriterRecoveryDatabase(t *testing.T, database *control.DB) {
	t.Helper()
	if database == nil {
		return
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close writer recovery database: %v", err)
	}
}

func newWriterRecoveryWriter(
	t *testing.T,
	database *control.DB,
	idGenerator func() (string, error),
) (*Writer, context.Context) {
	t.Helper()
	auditStore, err := audit.NewStore(database, audit.StoreOptions{CursorKey: writerRecoveryCursorKey})
	if err != nil {
		t.Fatalf("audit.NewStore(writer recovery): %v", err)
	}
	actorContext, err := audit.WithActor(t.Context(), audit.Actor{
		Kind: audit.ActorKindBrowser,
		ID:   writerRecoveryActorID,
		Role: audit.ActorRoleAdministrator,
	})
	if err != nil {
		t.Fatalf("audit.WithActor(writer recovery): %v", err)
	}
	writer, err := NewWriter(database, auditStore, WriterOptions{
		Clock:                writerRecoveryMutationTime,
		IDGenerator:          idGenerator,
		IdempotencyRetention: 8 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewWriter(writer recovery): %v", err)
	}
	return writer, actorContext
}

func writerRecoveryMutationTime() time.Time {
	return time.Date(2026, time.August, 7, 12, 34, 56, 123456000, time.UTC)
}

func writerRecoveryScope() WriteScope {
	return WriteScope{
		TenantID:       writerRecoveryTenantID,
		OwnerID:        writerRecoveryOwnerID,
		WritableAppIDs: []string{writerRecoveryAppID},
	}
}

func writerRecoveryCreateRequest() *opensplunkv1.CreateKnowledgeObjectRequest {
	description := "definition committed across a process crash boundary"
	return &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition: &opensplunkv1.KnowledgeObjectDefinition{
			AppId:        writerRecoveryAppID,
			Name:         "writer-subprocess-recovery",
			Description:  &description,
			SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			Selector: &opensplunkv1.KnowledgeSelector{
				HostPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
					MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
					Value:     "writer-recovery-host",
				}},
			},
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
				FieldAlias: &opensplunkv1.FieldAliasDefinition{
					SourceField:       "source_field",
					DestinationField:  "destination_field",
					OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
				},
			},
		},
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: writerRecoveryClientRequestID,
	}
}

func writerRecoveryBaselineCreateRequest() *opensplunkv1.CreateKnowledgeObjectRequest {
	request := proto.Clone(writerRecoveryCreateRequest()).(*opensplunkv1.CreateKnowledgeObjectRequest)
	request.ClientRequestId = writerRecoveryBaselineRequest
	return request
}

func writerRecoveryUpdateRequestValue() *opensplunkv1.UpdateKnowledgeObjectRequest {
	definition := proto.Clone(writerRecoveryCreateRequest().GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
	description := "definition updated after a process crash boundary"
	definition.Description = &description
	return &opensplunkv1.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: writerRecoveryObjectID,
		ExpectedVersion:   1,
		Definition:        definition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   writerRecoveryUpdateRequest,
	}
}

func invokeWriterRecoveryRoute(
	writer *Writer,
	actorContext context.Context,
	route string,
) (proto.Message, error) {
	switch route {
	case mutationRouteCreate:
		return writer.Create(actorContext, writerRecoveryScope(), writerRecoveryCreateRequest())
	case mutationRouteUpdate:
		return writer.Update(actorContext, writerRecoveryScope(), writerRecoveryUpdateRequestValue())
	case mutationRouteSetState:
		return writer.SetState(actorContext, writerRecoveryScope(), &opensplunkv1.SetKnowledgeObjectStateRequest{
			KnowledgeObjectId: writerRecoveryObjectID,
			ExpectedVersion:   1,
			State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			ClientRequestId:   writerRecoveryStateRequest,
		})
	case mutationRouteDelete:
		return writer.Delete(actorContext, writerRecoveryScope(), &opensplunkv1.DeleteKnowledgeObjectRequest{
			KnowledgeObjectId: writerRecoveryObjectID,
			ExpectedVersion:   1,
			ClientRequestId:   writerRecoveryDeleteRequest,
		})
	default:
		return nil, fmt.Errorf("unsupported writer recovery route %q", route)
	}
}

func writerRecoveryHookHasIdentity(route string, boundary writerHookBoundary) bool {
	switch boundary {
	case writerHookPrepared, writerHookIdempotencyChecked, writerHookCatalogLedgersReady:
		return false
	case writerHookCapacityChecked:
		return route != mutationRouteCreate
	default:
		return true
	}
}

func killWriterRecoveryChildAtBoundary(
	t *testing.T,
	databasePath string,
	route string,
	boundary writerHookBoundary,
) {
	t.Helper()
	readyReader, readyWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("create writer recovery ready pipe: %v", err)
	}
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		_ = readyReader.Close()
		_ = readyWriter.Close()
		t.Fatalf("create writer recovery gate pipe: %v", err)
	}
	defer readyReader.Close()
	defer gateWriter.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestWriterSubprocessRecoveryHelper$")
	command.Env = writerRecoveryChildEnvironment(databasePath, route, boundary)
	command.ExtraFiles = []*os.File{readyWriter, gateReader}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		_ = readyWriter.Close()
		_ = gateReader.Close()
		t.Fatalf("start writer recovery child at %s: %v", boundary, err)
	}
	if err := readyWriter.Close(); err != nil {
		_ = command.Process.Signal(syscall.SIGKILL)
		_ = command.Wait()
		_ = gateReader.Close()
		t.Fatalf("close parent recovery ready writer: %v", err)
	}
	if err := gateReader.Close(); err != nil {
		_ = command.Process.Signal(syscall.SIGKILL)
		_ = command.Wait()
		t.Fatalf("close parent recovery gate reader: %v", err)
	}

	type readyResult struct {
		line string
		err  error
	}
	readyResults := make(chan readyResult, 1)
	go func() {
		line, readErr := bufio.NewReader(readyReader).ReadString('\n')
		readyResults <- readyResult{line: strings.TrimSpace(line), err: readErr}
	}()

	timer := time.NewTimer(writerRecoveryChildWaitTimeout)
	defer timer.Stop()
	select {
	case result := <-readyResults:
		if result.err != nil || result.line != string(boundary) {
			_ = command.Process.Signal(syscall.SIGKILL)
			waitErr := command.Wait()
			t.Fatalf(
				"writer recovery child did not block at %s: line=%q read=%v wait=%v stdout=%q stderr=%q",
				boundary, result.line, result.err, waitErr, stdout.String(), stderr.String(),
			)
		}
	case <-timer.C:
		_ = command.Process.Signal(syscall.SIGKILL)
		waitErr := command.Wait()
		t.Fatalf(
			"writer recovery child timed out before %s: wait=%v stdout=%q stderr=%q",
			boundary, waitErr, stdout.String(), stderr.String(),
		)
	}

	if err := command.Process.Signal(syscall.SIGKILL); err != nil {
		waitErr := command.Wait()
		t.Fatalf(
			"SIGKILL writer recovery child at %s: %v (wait=%v stdout=%q stderr=%q)",
			boundary, err, waitErr, stdout.String(), stderr.String(),
		)
	}
	waitErr := command.Wait()
	exitErr, ok := waitErr.(*exec.ExitError)
	status, statusOK := command.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || exitErr == nil || !statusOK || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf(
			"writer recovery child exit at %s = %v (%v), want SIGKILL; stdout=%q stderr=%q",
			boundary, waitErr, status, stdout.String(), stderr.String(),
		)
	}
}

func writerRecoveryChildEnvironment(databasePath, route string, boundary writerHookBoundary) []string {
	values := map[string]string{
		writerRecoveryChildMarkerEnv:  "1",
		writerRecoveryDatabasePathEnv: databasePath,
		writerRecoveryBoundaryEnv:     string(boundary),
		writerRecoveryRouteEnv:        route,
		writerRecoveryReadyFDEnv:      strconv.Itoa(writerRecoveryReadyFD),
		writerRecoveryGateFDEnv:       strconv.Itoa(writerRecoveryGateFD),
	}
	environment := make([]string, 0, len(os.Environ())+len(values))
	for _, entry := range os.Environ() {
		key, _, found := strings.Cut(entry, "=")
		if _, replaced := values[key]; !found || replaced {
			continue
		}
		environment = append(environment, entry)
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		environment = append(environment, key+"="+values[key])
	}
	return environment
}

func requireWriterRecoveryChildPath(t *testing.T) string {
	t.Helper()
	value := os.Getenv(writerRecoveryDatabasePathEnv)
	if value == "" || !filepath.IsAbs(value) || filepath.Clean(value) != value {
		t.Fatalf("invalid writer recovery database path %q", value)
	}
	return value
}

func requireWriterRecoveryChildBoundary(t *testing.T) writerHookBoundary {
	t.Helper()
	value := writerHookBoundary(os.Getenv(writerRecoveryBoundaryEnv))
	switch value {
	case writerHookPrepared,
		writerHookIdempotencyChecked,
		writerHookCatalogLedgersReady,
		writerHookCapacityChecked,
		writerHookDefinitionBlobReady,
		writerHookVersionInserted,
		writerHookDependencyRowsInserted,
		writerHookDependencySealed,
		writerHookProjectionInserted,
		writerHookSelectorRowsInserted,
		writerHookProjectionSealed,
		writerHookRegistryPublished,
		writerHookSuccessAuditAppended,
		writerHookCatalogRevisionAdvanced,
		writerHookCommitAuthorityRecorded,
		writerHookIdempotencyOutcomeRecorded,
		writerHookBeforeCommit,
		writerHookAfterCommit:
		return value
	default:
		t.Fatalf("invalid writer recovery boundary %q", value)
		return ""
	}
}

func requireWriterRecoveryChildRoute(t *testing.T) string {
	t.Helper()
	value := os.Getenv(writerRecoveryRouteEnv)
	switch value {
	case mutationRouteCreate, mutationRouteUpdate, mutationRouteSetState, mutationRouteDelete:
		return value
	default:
		t.Fatalf("invalid writer recovery route %q", value)
		return ""
	}
}

func requireWriterRecoveryChildFD(t *testing.T, key string, want int) int {
	t.Helper()
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value != want {
		t.Fatalf("invalid writer recovery descriptor %s=%q, want %d", key, os.Getenv(key), want)
	}
	return value
}

type writerRecoverySnapshot map[string][]string

func readWriterRecoverySnapshot(t *testing.T, database *control.DB) writerRecoverySnapshot {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(t.Context(), `
		SELECT name
		FROM sqlite_schema
		WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
		ORDER BY name`)
	if err != nil {
		t.Fatalf("list writer recovery tables: %v", err)
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			_ = rows.Close()
			t.Fatalf("scan writer recovery table: %v", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate writer recovery tables: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close writer recovery table list: %v", err)
	}

	snapshot := make(writerRecoverySnapshot)
	for _, table := range tables {
		if !writerRecoveryTableHasTenantID(t, database, table) {
			continue
		}
		quotedTable := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
		resultRows, err := database.SQLDB().QueryContext(
			t.Context(),
			`SELECT * FROM `+quotedTable+` WHERE tenant_id = ?`,
			writerRecoveryTenantID,
		)
		if err != nil {
			t.Fatalf("snapshot writer recovery table %s: %v", table, err)
		}
		columns, err := resultRows.Columns()
		if err != nil {
			_ = resultRows.Close()
			t.Fatalf("snapshot writer recovery columns %s: %v", table, err)
		}
		encodedRows := make([]string, 0)
		for resultRows.Next() {
			values := make([]any, len(columns))
			destinations := make([]any, len(columns))
			for index := range values {
				destinations[index] = &values[index]
			}
			if err := resultRows.Scan(destinations...); err != nil {
				_ = resultRows.Close()
				t.Fatalf("snapshot writer recovery row %s: %v", table, err)
			}
			var encoded strings.Builder
			for index, value := range values {
				encoded.WriteString(strconv.Quote(columns[index]))
				encoded.WriteByte('=')
				encoded.WriteString(writerFaultSQLValue(value))
				encoded.WriteByte(';')
			}
			encodedRows = append(encodedRows, encoded.String())
		}
		if err := resultRows.Err(); err != nil {
			_ = resultRows.Close()
			t.Fatalf("iterate writer recovery snapshot %s: %v", table, err)
		}
		if err := resultRows.Close(); err != nil {
			t.Fatalf("close writer recovery snapshot %s: %v", table, err)
		}
		sort.Strings(encodedRows)
		snapshot[table] = encodedRows
	}
	return snapshot
}

func writerRecoveryTableHasTenantID(t *testing.T, database *control.DB, table string) bool {
	t.Helper()
	quotedTable := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
	rows, err := database.SQLDB().QueryContext(t.Context(), `PRAGMA table_info(`+quotedTable+`)`)
	if err != nil {
		t.Fatalf("inspect writer recovery table %s: %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var sequence int64
		var name string
		var columnType string
		var notNull int64
		var defaultValue any
		var primaryKey int64
		if err := rows.Scan(&sequence, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan writer recovery table info %s: %v", table, err)
		}
		if name == "tenant_id" {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate writer recovery table info %s: %v", table, err)
	}
	return false
}

func assertWriterRecoverySnapshotsEqual(
	t *testing.T,
	got writerRecoverySnapshot,
	want writerRecoverySnapshot,
	reason string,
) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}
	keys := make(map[string]struct{}, len(got)+len(want))
	for key := range got {
		keys[key] = struct{}{}
	}
	for key := range want {
		keys[key] = struct{}{}
	}
	tables := make([]string, 0, len(keys))
	for key := range keys {
		tables = append(tables, key)
	}
	sort.Strings(tables)
	for _, table := range tables {
		if !reflect.DeepEqual(got[table], want[table]) {
			t.Errorf("%s: table %s:\n got: %#v\nwant: %#v", reason, table, got[table], want[table])
		}
	}
	t.FailNow()
}

func assertWriterRecoveryRouteCommitted(
	t *testing.T,
	database *control.DB,
	route string,
	response proto.Message,
) {
	t.Helper()
	if route == mutationRouteCreate {
		var createResponse *opensplunkv1.CreateKnowledgeObjectResponse
		if response != nil {
			var ok bool
			createResponse, ok = response.(*opensplunkv1.CreateKnowledgeObjectResponse)
			if !ok {
				t.Fatalf("create recovery response has type %T", response)
			}
		}
		assertWriterRecoveryCommitted(t, database, writerRecoveryCreateRequest(), createResponse)
		return
	}
	assertWriterRecoveryExistingRouteCommitted(t, database, route, response)
}

func assertWriterRecoveryExistingRouteCommitted(
	t *testing.T,
	database *control.DB,
	route string,
	response proto.Message,
) {
	t.Helper()
	wantBlobs := int64(1)
	wantState := StateDraft
	wantMutation := ""
	wantAuditAction := ""
	wantRequestID := ""
	wantDefinition := writerRecoveryBaselineCreateRequest().GetDefinition()
	switch route {
	case mutationRouteUpdate:
		wantBlobs = 2
		wantMutation = "update"
		wantAuditAction = "knowledge.object.update"
		wantRequestID = writerRecoveryUpdateRequest
		wantDefinition = writerRecoveryUpdateRequestValue().GetDefinition()
	case mutationRouteSetState:
		wantState = StateDisabled
		wantMutation = "disable"
		wantAuditAction = "knowledge.object.disable"
		wantRequestID = writerRecoveryStateRequest
	case mutationRouteDelete:
		wantState = StateDeleted
		wantMutation = "delete"
		wantAuditAction = "knowledge.object.delete"
		wantRequestID = writerRecoveryDeleteRequest
	default:
		t.Fatalf("unsupported committed writer recovery route %q", route)
	}

	expectedCounts := map[string]int64{
		"audit_events":                            2,
		"audit_tenant_state":                      1,
		"knowledge_catalog_revision_heads":        1,
		"knowledge_catalog_tenants":               1,
		"knowledge_definition_blobs":              wantBlobs,
		"knowledge_mutation_commit_authorities":   2,
		"knowledge_mutation_idempotency":          2,
		"knowledge_object_acl":                    0,
		"knowledge_object_dependencies":           0,
		"knowledge_object_dependency_seals":       2,
		"knowledge_object_list_order_keys":        1,
		"knowledge_object_list_projection_seals":  1,
		"knowledge_object_list_projections":       1,
		"knowledge_object_list_selector_patterns": 1,
		"knowledge_object_version_lifecycle":      2,
		"knowledge_object_versions":               2,
		"knowledge_objects":                       1,
		"knowledge_projection_tenant_ledgers":     1,
		"knowledge_recovery_audit":                0,
	}
	for table, want := range expectedCounts {
		if got := writerRecoveryTableCount(t, database, table); got != want {
			t.Errorf("committed %s recovery %s rows = %d, want %d", route, table, got, want)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	store, err := New(database, Options{CursorKey: writerRecoveryCursorKey})
	if err != nil {
		t.Fatalf("New(writer recovery reader): %v", err)
	}
	readScope := ReadScope{
		TenantID:       writerRecoveryTenantID,
		OwnerID:        writerRecoveryOwnerID,
		ReadableAppIDs: []string{writerRecoveryAppID},
	}
	versionOne := uint64(1)
	prior, err := store.Get(t.Context(), readScope, writerRecoveryObjectID, &versionOne)
	if err != nil {
		t.Fatalf("read immutable prior %s recovery version: %v", route, err)
	}
	current, err := store.Get(t.Context(), readScope, writerRecoveryObjectID, nil)
	if err != nil {
		t.Fatalf("read current %s recovery version: %v", route, err)
	}
	baselineDefinition := writerRecoveryBaselineCreateRequest().GetDefinition()
	if prior.Version != 1 || prior.State != StateDraft || !prior.UpdatedAt.Equal(writerRecoveryMutationTime()) ||
		prior.DisabledAt != nil || prior.DeletedAt != nil || !proto.Equal(prior.Definition, baselineDefinition) ||
		len(prior.DefinitionSHA256) != persistedKnowledgeDefinitionDigestBytes {
		t.Fatalf("immutable prior %s recovery version = %+v", route, prior)
	}
	wantUpdatedAt := writerRecoveryMutationTime().Add(time.Microsecond)
	if current.Version != 2 || current.State != wantState || !current.UpdatedAt.Equal(wantUpdatedAt) ||
		!proto.Equal(current.Definition, wantDefinition) ||
		len(current.DefinitionSHA256) != persistedKnowledgeDefinitionDigestBytes {
		t.Fatalf("current %s recovery version = %+v", route, current)
	}
	if (route == mutationRouteSetState &&
		(current.DisabledAt == nil || !current.DisabledAt.Equal(wantUpdatedAt))) ||
		(route != mutationRouteSetState && current.DisabledAt != nil) ||
		(route == mutationRouteDelete &&
			(current.DeletedAt == nil || !current.DeletedAt.Equal(wantUpdatedAt))) ||
		(route != mutationRouteDelete && current.DeletedAt != nil) {
		t.Fatalf("current %s recovery lifecycle = %+v", route, current)
	}
	if (route == mutationRouteUpdate && bytes.Equal(prior.DefinitionSHA256, current.DefinitionSHA256)) ||
		(route != mutationRouteUpdate && !bytes.Equal(prior.DefinitionSHA256, current.DefinitionSHA256)) {
		t.Fatalf("%s recovery definition digest transition is invalid", route)
	}

	var immutableJoined int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM knowledge_object_versions AS version
		JOIN knowledge_object_version_lifecycle AS lifecycle
		  ON lifecycle.tenant_id = version.tenant_id
		 AND lifecycle.knowledge_object_id = version.knowledge_object_id
		 AND lifecycle.object_version = version.object_version
		JOIN knowledge_object_dependency_seals AS dependencies
		  ON dependencies.tenant_id = version.tenant_id
		 AND dependencies.knowledge_object_id = version.knowledge_object_id
		 AND dependencies.object_version = version.object_version
		WHERE version.tenant_id = ?
		  AND version.knowledge_object_id = ?
		  AND version.object_version IN (1, 2)
		  AND lifecycle.state = version.state
		  AND version.dependency_count = 0
		  AND dependencies.dependency_count = 0`,
		writerRecoveryTenantID,
		writerRecoveryObjectID,
	).Scan(&immutableJoined); err != nil {
		t.Fatalf("validate immutable %s recovery publications: %v", route, err)
	}
	if immutableJoined != 2 {
		t.Fatalf("complete immutable %s recovery publication rows = %d, want 2", route, immutableJoined)
	}

	var currentProjectionCount int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM knowledge_objects AS object
		JOIN knowledge_object_versions AS version
		  ON version.tenant_id = object.tenant_id
		 AND version.knowledge_object_id = object.knowledge_object_id
		 AND version.object_version = object.current_version
		JOIN knowledge_object_list_projections AS projection
		  ON projection.tenant_id = object.tenant_id
		 AND projection.knowledge_object_id = object.knowledge_object_id
		 AND projection.object_version = object.current_version
		JOIN knowledge_object_list_projection_seals AS projection_seal
		  ON projection_seal.tenant_id = projection.tenant_id
		 AND projection_seal.knowledge_object_id = projection.knowledge_object_id
		 AND projection_seal.object_version = projection.object_version
		JOIN knowledge_object_list_order_keys AS order_key
		  ON order_key.tenant_id = projection.tenant_id
		 AND order_key.knowledge_object_id = projection.knowledge_object_id
		 AND order_key.object_version = projection.object_version
		JOIN knowledge_object_list_selector_patterns AS selector
		  ON selector.tenant_id = projection.tenant_id
		 AND selector.knowledge_object_id = projection.knowledge_object_id
		 AND selector.object_version = projection.object_version
		WHERE object.tenant_id = ? AND object.knowledge_object_id = ?
		  AND object.current_version = 2
		  AND projection.app_id = version.app_id
		  AND projection.owner_id = version.owner_id
		  AND projection.object_type = version.object_type
		  AND projection.name = version.name
		  AND projection.sharing_scope = version.sharing_scope
		  AND projection.state = version.state
		  AND projection.host_selector_count = 1
		  AND projection.index_selector_count = 0
		  AND projection.source_selector_count = 0
		  AND projection.sourcetype_selector_count = 0
		  AND projection_seal.projection_bytes = projection.projection_bytes
		  AND projection_seal.canonical_selector_bytes = projection.canonical_selector_bytes
		  AND order_key.created_at_unix_micro = ?
		  AND order_key.updated_at_unix_micro = version.created_at_unix_micro
		  AND selector.dimension = 'host' AND selector.ordinal = 0
		  AND selector.match_kind = 'exact' AND selector.value = 'writer-recovery-host'
		  AND NOT EXISTS (
		      SELECT 1 FROM knowledge_object_list_projections AS historical
		      WHERE historical.tenant_id = object.tenant_id
		        AND historical.knowledge_object_id = object.knowledge_object_id
		        AND historical.object_version = 1)`,
		writerRecoveryTenantID,
		writerRecoveryObjectID,
		writerRecoveryMutationTime().UnixMicro(),
	).Scan(&currentProjectionCount); err != nil {
		t.Fatalf("validate current %s recovery projection: %v", route, err)
	}
	if currentProjectionCount != 1 {
		t.Fatalf("complete current %s recovery projection rows = %d, want 1", route, currentProjectionCount)
	}

	var provenanceCount int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM knowledge_mutation_idempotency AS receipt
		JOIN knowledge_object_versions AS version
		  ON version.tenant_id = receipt.tenant_id
		 AND version.knowledge_object_id = receipt.knowledge_object_id
		 AND version.object_version = receipt.object_version
		JOIN audit_events AS event
		  ON event.tenant_id = receipt.tenant_id
		 AND event.sequence = receipt.successful_audit_sequence
		JOIN knowledge_mutation_commit_authorities AS committed
		  ON committed.tenant_id = receipt.tenant_id
		 AND committed.catalog_revision = receipt.committed_catalog_revision
		 AND committed.catalog_state_token = receipt.committed_catalog_state_token
		 AND committed.mutation_kind = receipt.mutation_kind
		 AND committed.knowledge_object_id = receipt.knowledge_object_id
		 AND committed.object_version = receipt.object_version
		 AND committed.occurred_at_unix_micro = receipt.created_at_unix_micro
		 AND committed.successful_audit_sequence = receipt.successful_audit_sequence
		WHERE receipt.tenant_id = ?
		  AND receipt.knowledge_object_id = ?
		  AND receipt.object_version IN (1, 2)
		  AND receipt.committed_catalog_revision = receipt.object_version
		  AND receipt.request_digest_format_version = 1
		  AND length(receipt.request_digest) = 32
		  AND receipt.outcome_format_version = 1
		  AND length(receipt.outcome_proto) BETWEEN 1 AND 1024
		  AND receipt.created_at_unix_micro = version.created_at_unix_micro
		  AND receipt.retention_anchor_unix_micro >= receipt.created_at_unix_micro
		  AND receipt.retain_until_unix_micro = receipt.retention_anchor_unix_micro + ?
		  AND event.actor_kind = receipt.actor_kind
		  AND event.actor_id = receipt.actor_id
		  AND event.actor_role = 'administrator'
		  AND event.target_kind = 'knowledge_object'
		  AND event.target_id = receipt.knowledge_object_id
		  AND event.target_version = receipt.object_version
		  AND event.app_id = version.app_id
		  AND event.object_type = version.object_type
		  AND event.sharing_scope = version.sharing_scope
		  AND ((receipt.object_version = 1
		        AND receipt.route = 'objects.create'
		        AND receipt.client_request_id = ?
		        AND receipt.mutation_kind = 'create'
		        AND event.action = 'knowledge.object.create')
		    OR (receipt.object_version = 2
		        AND receipt.route = ?
		        AND receipt.client_request_id = ?
		        AND receipt.mutation_kind = ?
		        AND event.action = ?))`,
		writerRecoveryTenantID,
		writerRecoveryObjectID,
		int64((8*24*time.Hour)/time.Microsecond),
		writerRecoveryBaselineRequest,
		route,
		wantRequestID,
		wantMutation,
		wantAuditAction,
	).Scan(&provenanceCount); err != nil {
		t.Fatalf("validate %s recovery provenance: %v", route, err)
	}
	if provenanceCount != 2 {
		t.Fatalf("complete %s recovery provenance rows = %d, want 2", route, provenanceCount)
	}

	var currentHeadCount int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM knowledge_catalog_tenants AS tenant
		JOIN knowledge_projection_tenant_ledgers AS projection_ledger
		  ON projection_ledger.tenant_id = tenant.tenant_id
		JOIN knowledge_catalog_revision_heads AS head
		  ON head.tenant_id = tenant.tenant_id
		 AND head.catalog_revision = tenant.catalog_revision
		JOIN knowledge_mutation_idempotency AS receipt
		  ON receipt.tenant_id = head.tenant_id
		 AND receipt.committed_catalog_revision = head.catalog_revision
		 AND receipt.committed_catalog_state_token = head.state_token
		WHERE tenant.tenant_id = ?
		  AND tenant.catalog_revision = 2
		  AND tenant.identity_count = 1
		  AND tenant.version_count = 2
		  AND tenant.idempotency_count = 2
		  AND tenant.active_object_count = 0
		  AND tenant.recovery_audit_count = 0
		  AND tenant.definition_body_bytes = (
		      SELECT coalesce(sum(definition_bytes), 0)
		      FROM knowledge_definition_blobs
		      WHERE tenant_id = tenant.tenant_id)
		  AND projection_ledger.projection_bytes = (
		      SELECT coalesce(sum(projection_bytes), 0)
		      FROM knowledge_object_list_projections
		      WHERE tenant_id = tenant.tenant_id)
		  AND receipt.route = ?
		  AND receipt.client_request_id = ?`,
		writerRecoveryTenantID,
		route,
		wantRequestID,
	).Scan(&currentHeadCount); err != nil {
		t.Fatalf("validate current %s recovery catalog head: %v", route, err)
	}
	if currentHeadCount != 1 {
		t.Fatalf("current %s recovery catalog head rows = %d, want 1", route, currentHeadCount)
	}

	if response != nil {
		var expectedToken []byte
		if err := database.SQLDB().QueryRowContext(t.Context(), `
			SELECT committed_catalog_state_token
			FROM knowledge_mutation_idempotency
			WHERE tenant_id = ? AND route = ? AND client_request_id = ?`,
			writerRecoveryTenantID,
			route,
			wantRequestID,
		).Scan(&expectedToken); err != nil {
			t.Fatalf("read %s recovery response token authority: %v", route, err)
		}
		assertWriterRecoveryExistingResponse(t, route, response, current, expectedToken)
	}
}

func assertWriterRecoveryExistingResponse(
	t *testing.T,
	route string,
	response proto.Message,
	current Object,
	expectedToken []byte,
) {
	t.Helper()
	var object *opensplunkv1.KnowledgeObject
	var objectID string
	var version uint64
	var revision uint64
	var token []byte
	switch typed := response.(type) {
	case *opensplunkv1.UpdateKnowledgeObjectResponse:
		if route != mutationRouteUpdate {
			t.Fatalf("%s recovery response has type %T", route, response)
		}
		object = typed.GetKnowledgeObject()
		revision = typed.GetTenantCatalogRevision()
		token = typed.GetTenantCatalogStateToken()
	case *opensplunkv1.SetKnowledgeObjectStateResponse:
		if route != mutationRouteSetState {
			t.Fatalf("%s recovery response has type %T", route, response)
		}
		object = typed.GetKnowledgeObject()
		revision = typed.GetTenantCatalogRevision()
		token = typed.GetTenantCatalogStateToken()
	case *opensplunkv1.DeleteKnowledgeObjectResponse:
		if route != mutationRouteDelete {
			t.Fatalf("%s recovery response has type %T", route, response)
		}
		objectID = typed.GetKnowledgeObjectId()
		version = typed.GetDeletedVersion()
		revision = typed.GetTenantCatalogRevision()
		token = typed.GetTenantCatalogStateToken()
	default:
		t.Fatalf("%s recovery response has type %T", route, response)
	}
	if object != nil {
		objectID = object.GetKnowledgeObjectId()
		version = object.GetVersion()
		projected, err := ObjectToProto(current)
		if err != nil {
			t.Fatalf("project current %s recovery object: %v", route, err)
		}
		if !proto.Equal(object, projected) {
			t.Fatalf("%s recovery response object disagrees with current authority:\n got: %v\nwant: %v", route, object, projected)
		}
	}
	if objectID != writerRecoveryObjectID || version != 2 || revision != 2 ||
		len(token) != catalogStateTokenBytes || !bytes.Equal(token, expectedToken) {
		t.Fatalf("%s recovery response authority = id %q version %d revision %d token %d bytes", route, objectID, version, revision, len(token))
	}
}

func assertWriterRecoveryCommitted(
	t *testing.T,
	database *control.DB,
	request *opensplunkv1.CreateKnowledgeObjectRequest,
	response *opensplunkv1.CreateKnowledgeObjectResponse,
) {
	t.Helper()
	expectedCounts := map[string]int64{
		"audit_events":                            1,
		"audit_tenant_state":                      1,
		"knowledge_catalog_revision_heads":        1,
		"knowledge_catalog_tenants":               1,
		"knowledge_definition_blobs":              1,
		"knowledge_mutation_commit_authorities":   1,
		"knowledge_mutation_idempotency":          1,
		"knowledge_object_acl":                    0,
		"knowledge_object_dependencies":           0,
		"knowledge_object_dependency_seals":       1,
		"knowledge_object_list_order_keys":        1,
		"knowledge_object_list_projection_seals":  1,
		"knowledge_object_list_projections":       1,
		"knowledge_object_list_selector_patterns": 1,
		"knowledge_object_version_lifecycle":      1,
		"knowledge_object_versions":               1,
		"knowledge_objects":                       1,
		"knowledge_projection_tenant_ledgers":     1,
		"knowledge_recovery_audit":                0,
	}
	for table, want := range expectedCounts {
		if got := writerRecoveryTableCount(t, database, table); got != want {
			t.Errorf("committed writer recovery %s rows = %d, want %d", table, got, want)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	var joinedCount int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*)
		FROM knowledge_objects AS object
		JOIN knowledge_object_versions AS version
		  ON version.tenant_id = object.tenant_id
		 AND version.knowledge_object_id = object.knowledge_object_id
		 AND version.object_version = object.current_version
		JOIN knowledge_object_version_lifecycle AS lifecycle
		  ON lifecycle.tenant_id = version.tenant_id
		 AND lifecycle.knowledge_object_id = version.knowledge_object_id
		 AND lifecycle.object_version = version.object_version
		JOIN knowledge_object_dependency_seals AS dependencies
		  ON dependencies.tenant_id = version.tenant_id
		 AND dependencies.knowledge_object_id = version.knowledge_object_id
		 AND dependencies.object_version = version.object_version
		JOIN knowledge_object_list_projections AS projection
		  ON projection.tenant_id = version.tenant_id
		 AND projection.knowledge_object_id = version.knowledge_object_id
		 AND projection.object_version = version.object_version
		JOIN knowledge_object_list_projection_seals AS projection_seal
		  ON projection_seal.tenant_id = projection.tenant_id
		 AND projection_seal.knowledge_object_id = projection.knowledge_object_id
		 AND projection_seal.object_version = projection.object_version
		JOIN knowledge_object_list_order_keys AS order_key
		  ON order_key.tenant_id = projection.tenant_id
		 AND order_key.knowledge_object_id = projection.knowledge_object_id
		 AND order_key.object_version = projection.object_version
		JOIN knowledge_mutation_idempotency AS receipt
		  ON receipt.tenant_id = version.tenant_id
		 AND receipt.knowledge_object_id = version.knowledge_object_id
		 AND receipt.object_version = version.object_version
		JOIN audit_events AS event
		  ON event.tenant_id = receipt.tenant_id
		 AND event.sequence = receipt.successful_audit_sequence
		JOIN knowledge_mutation_commit_authorities AS committed
		  ON committed.tenant_id = receipt.tenant_id
		 AND committed.catalog_revision = receipt.committed_catalog_revision
		 AND committed.catalog_state_token = receipt.committed_catalog_state_token
		 AND committed.mutation_kind = receipt.mutation_kind
		 AND committed.knowledge_object_id = receipt.knowledge_object_id
		 AND committed.object_version = receipt.object_version
		 AND committed.occurred_at_unix_micro = receipt.created_at_unix_micro
		 AND committed.successful_audit_sequence = receipt.successful_audit_sequence
		JOIN knowledge_catalog_revision_heads AS head
		  ON head.tenant_id = receipt.tenant_id
		 AND head.catalog_revision = receipt.committed_catalog_revision
		 AND head.state_token = receipt.committed_catalog_state_token
		WHERE object.tenant_id = ?
		  AND object.knowledge_object_id = ?
		  AND object.current_version = 1
		  AND object.app_id = ?
		  AND object.owner_id = ?
		  AND object.object_type = 'field_alias'
		  AND object.name = ?
		  AND object.sharing_scope = 'private'
		  AND object.state = 'draft'
		  AND object.definition_digest = version.definition_digest
		  AND version.mutation_kind = 'create'
		  AND version.state = 'draft'
		  AND version.dependency_count = 0
		  AND lifecycle.state = 'draft'
		  AND dependencies.dependency_count = 0
		  AND projection.state = 'draft'
		  AND projection.host_selector_count = 1
		  AND projection_seal.projection_bytes = projection.projection_bytes
		  AND projection_seal.canonical_selector_bytes = projection.canonical_selector_bytes
		  AND order_key.created_at_unix_micro = object.created_at_unix_micro
		  AND order_key.updated_at_unix_micro = object.updated_at_unix_micro
		  AND receipt.actor_kind = 'browser'
		  AND receipt.actor_id = ?
		  AND receipt.route = 'objects.create'
		  AND receipt.client_request_id = ?
		  AND receipt.mutation_kind = 'create'
		  AND receipt.request_digest_format_version = 1
		  AND length(receipt.request_digest) = 32
		  AND receipt.outcome_format_version = 1
		  AND length(receipt.outcome_proto) BETWEEN 1 AND 1024
		  AND receipt.committed_catalog_revision = 1
		  AND receipt.created_at_unix_micro = ?
		  AND receipt.retention_anchor_unix_micro >= receipt.created_at_unix_micro
		  AND receipt.retain_until_unix_micro = receipt.retention_anchor_unix_micro + ?
		  AND event.occurred_at_unix_micro = receipt.created_at_unix_micro
		  AND event.actor_kind = receipt.actor_kind
		  AND event.actor_id = receipt.actor_id
		  AND event.actor_role = 'administrator'
		  AND event.action = 'knowledge.object.create'
		  AND event.target_kind = 'knowledge_object'
		  AND event.target_id = object.knowledge_object_id
		  AND event.target_version = object.current_version
		  AND event.app_id = object.app_id
		  AND event.object_type = object.object_type
		  AND event.sharing_scope = object.sharing_scope`,
		writerRecoveryTenantID,
		writerRecoveryObjectID,
		writerRecoveryAppID,
		writerRecoveryOwnerID,
		request.GetDefinition().GetName(),
		writerRecoveryActorID,
		writerRecoveryClientRequestID,
		writerRecoveryMutationTime().UnixMicro(),
		int64((8*24*time.Hour)/time.Microsecond),
	).Scan(&joinedCount); err != nil {
		t.Fatalf("validate committed writer recovery authority: %v", err)
	}
	if joinedCount != 1 {
		t.Fatalf("complete joined writer recovery authority rows = %d, want 1", joinedCount)
	}

	var records []idempotencyRecord
	if err := database.GORMDB().Where(
		"tenant_id = ? AND actor_kind = ? AND actor_id = ? AND route = ? AND client_request_id = ?",
		writerRecoveryTenantID,
		audit.ActorKindBrowser,
		writerRecoveryActorID,
		mutationRouteCreate,
		writerRecoveryClientRequestID,
	).Limit(2).Find(&records).Error; err != nil {
		t.Fatalf("read committed writer recovery receipt: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("committed writer recovery receipts = %d, want 1", len(records))
	}
	version, found, err := readVersionRecord(
		database.GORMDB(), writerRecoveryTenantID, writerRecoveryObjectID, 1,
	)
	if err != nil || !found || len(version.DefinitionDigest) != persistedKnowledgeDefinitionDigestBytes {
		t.Fatalf("read committed writer recovery version: found=%t version=%+v error=%v", found, version, err)
	}
	store, err := New(database, Options{CursorKey: writerRecoveryCursorKey})
	if err != nil {
		t.Fatalf("New(committed writer recovery reader): %v", err)
	}
	durableObject, err := store.Get(t.Context(), ReadScope{
		TenantID:       writerRecoveryTenantID,
		OwnerID:        writerRecoveryOwnerID,
		ReadableAppIDs: []string{writerRecoveryAppID},
	}, writerRecoveryObjectID, nil)
	if err != nil {
		t.Fatalf("read committed writer recovery object: %v", err)
	}
	if durableObject.Version != 1 || durableObject.State != StateDraft ||
		!proto.Equal(durableObject.Definition, request.GetDefinition()) ||
		!bytes.Equal(durableObject.DefinitionSHA256, version.DefinitionDigest) {
		t.Fatalf("committed writer recovery object = %+v", durableObject)
	}

	var ledger struct {
		Revision        int64 `gorm:"column:catalog_revision"`
		Identities      int64 `gorm:"column:identity_count"`
		Versions        int64 `gorm:"column:version_count"`
		DefinitionBytes int64 `gorm:"column:definition_body_bytes"`
		Idempotency     int64 `gorm:"column:idempotency_count"`
		Active          int64 `gorm:"column:active_object_count"`
		Recovery        int64 `gorm:"column:recovery_audit_count"`
	}
	if err := database.GORMDB().Table("knowledge_catalog_tenants").Where(
		"tenant_id = ?", writerRecoveryTenantID,
	).Take(&ledger).Error; err != nil {
		t.Fatalf("read committed writer recovery ledger: %v", err)
	}
	if ledger.Revision != 1 || ledger.Identities != 1 || ledger.Versions != 1 ||
		ledger.DefinitionBytes < 1 || ledger.Idempotency != 1 || ledger.Active != 0 || ledger.Recovery != 0 {
		t.Fatalf("committed writer recovery ledger = %+v", ledger)
	}

	if response == nil {
		return
	}
	object := response.GetKnowledgeObject()
	if object == nil || object.GetKnowledgeObjectId() != writerRecoveryObjectID ||
		object.GetTenantId() != writerRecoveryTenantID || object.GetAppId() != writerRecoveryAppID ||
		object.GetOwnerId() != writerRecoveryOwnerID ||
		object.GetObjectType() != opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS ||
		object.GetName() != request.GetDefinition().GetName() || object.GetVersion() != 1 ||
		object.GetSharingScope() != opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE ||
		object.GetState() != opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT ||
		!proto.Equal(object.GetDefinition(), request.GetDefinition()) ||
		!bytes.Equal(object.GetDefinitionSha256(), version.DefinitionDigest) ||
		object.GetCreatedAt() == nil || object.GetUpdatedAt() == nil ||
		!object.GetCreatedAt().AsTime().Equal(writerRecoveryMutationTime()) ||
		!object.GetUpdatedAt().AsTime().Equal(writerRecoveryMutationTime()) ||
		object.GetDisabledAt() != nil || object.GetQuarantinedAt() != nil || object.GetDeletedAt() != nil ||
		response.GetTenantCatalogRevision() != uint64(records[0].CommittedCatalogRevision) ||
		!bytes.Equal(response.GetTenantCatalogStateToken(), records[0].CommittedCatalogStateToken) {
		t.Fatalf("writer recovery response disagrees with durable authority: %v", response)
	}
}

func writerRecoveryTableCount(t *testing.T, database *control.DB, table string) int64 {
	t.Helper()
	quotedTable := `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
	var count int64
	if err := database.SQLDB().QueryRowContext(
		t.Context(),
		`SELECT count(*) FROM `+quotedTable+` WHERE tenant_id = ?`,
		writerRecoveryTenantID,
	).Scan(&count); err != nil {
		t.Fatalf("count committed writer recovery table %s: %v", table, err)
	}
	return count
}

func assertWriterRecoveryIntegrity(t *testing.T, database *control.DB) {
	t.Helper()
	assertWriterFaultIntegrity(t, database)
}
