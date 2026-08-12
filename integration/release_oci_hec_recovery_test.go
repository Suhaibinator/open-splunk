//go:build !windows

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

const releaseOCIHECRecoveryChannel = "123e4567-e89b-42d3-a456-426614174099"

type releaseOCIHECRecoveryState struct {
	indexName        string
	plaintextToken   string
	tokenID          string
	tokenPrefix      string
	indexedSource    string
	pendingSource    string
	postBackupSource string
	indexedAck       int64
	pendingAck       int64
	postBackupAck    int64
}

func (fixture *releaseOCIRecoveryFixture) seedHECPreBackupState() {
	t := fixture.t
	t.Helper()
	state := &releaseOCIHECRecoveryState{
		indexName:        "oci-hec-recovery-" + fixture.suffix,
		indexedSource:    "oci-hec-indexed-" + fixture.suffix,
		pendingSource:    "oci-hec-pending-" + fixture.suffix,
		postBackupSource: "oci-hec-post-backup-" + fixture.suffix,
	}
	fixture.hecRecovery = state
	fixture.preBackupIndexes = append(fixture.preBackupIndexes, state.indexName)
	releaseOCICreateIndex(
		t,
		fixture.ctx,
		fixture.client,
		fixture.baseURL,
		fixture.administratorToken,
		state.indexName,
	)
	state.plaintextToken, state.tokenID, state.tokenPrefix = releaseOCICreateHECRecoveryToken(
		t,
		fixture.ctx,
		fixture.client,
		fixture.baseURL,
		fixture.administratorToken,
		state.indexName,
	)
	fixture.stack.retainedSecrets = append(
		fixture.stack.retainedSecrets,
		state.plaintextToken,
		state.tokenPrefix,
	)
	state.indexedAck = backendHECIngest(
		t,
		fixture.ctx,
		fixture.client,
		fixture.baseURL+"/services/collector/event",
		state.plaintextToken,
		releaseOCIHECRecoveryChannel,
		"application/json",
		releaseOCIHECRecoveryEventBody(
			t,
			fixture.fixtureTime,
			state.indexedSource,
			"paired recovery indexed HEC event",
		),
	)
	releaseOCIWaitForHECAcknowledgment(
		t,
		fixture.ctx,
		fixture.client,
		fixture.baseURL,
		state,
		state.indexedAck,
	)
}

// stagePendingHECAndStopServer freezes the exact cross-store backup boundary:
// SQLite has durably accepted the request and ACK, while paused ClickHouse
// cannot yet report an outcome. Stopping the server before unpausing preserves
// the pending outbox and any legitimately ambiguous late-send possibility for
// the paired backup.
func (fixture *releaseOCIRecoveryFixture) stagePendingHECAndStopServer() {
	t := fixture.t
	t.Helper()
	state := fixture.requireHECRecoveryState()
	paused := false
	backendHECDocker(t, fixture.ctx, "pause", fixture.originalClickHouseID)
	paused = true
	defer func() {
		if !paused {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := backendHECDockerError(cleanupCtx, "unpause", fixture.originalClickHouseID); err != nil {
			t.Errorf("unpause ClickHouse after HEC recovery-boundary failure: %v", err)
		}
	}()

	state.pendingAck = backendHECIngest(
		t,
		fixture.ctx,
		fixture.client,
		fixture.baseURL+"/services/collector/event",
		state.plaintextToken,
		releaseOCIHECRecoveryChannel,
		"application/json",
		releaseOCIHECRecoveryEventBody(
			t,
			fixture.fixtureTime.Add(time.Second),
			state.pendingSource,
			"paired recovery pending HEC outbox",
		),
	)
	if backendHECQueryAcknowledgments(
		t,
		fixture.ctx,
		fixture.client,
		fixture.baseURL,
		state.plaintextToken,
		releaseOCIHECRecoveryChannel,
		state.pendingAck,
	)[state.pendingAck] {
		t.Fatalf("HEC acknowledgment %d became true before paired backup", state.pendingAck)
	}
	fixture.stack.mustCompose(
		t,
		fixture.ctx,
		"stop server with pending HEC outbox before paired deployment backup",
		"stop",
		"--timeout",
		"40",
		"server",
	)
	backendHECDocker(t, fixture.ctx, "unpause", fixture.originalClickHouseID)
	paused = false
}

func (fixture *releaseOCIRecoveryFixture) recordPostBackupHECMutation(
	client *http.Client,
	baseURL string,
) {
	t := fixture.t
	t.Helper()
	state := fixture.requireHECRecoveryState()
	// The original branch may now reconcile the request captured as pending in
	// the backup. This does not mutate the already published recovery set.
	releaseOCIWaitForHECAcknowledgment(
		t,
		fixture.ctx,
		client,
		baseURL,
		state,
		state.pendingAck,
	)
	state.postBackupAck = backendHECIngest(
		t,
		fixture.ctx,
		client,
		baseURL+"/services/collector/event",
		state.plaintextToken,
		releaseOCIHECRecoveryChannel,
		"application/json",
		releaseOCIHECRecoveryEventBody(
			t,
			fixture.fixtureTime.Add(2*time.Second),
			state.postBackupSource,
			"discarded post-backup HEC branch",
		),
	)
	releaseOCIWaitForHECAcknowledgment(
		t,
		fixture.ctx,
		client,
		baseURL,
		state,
		state.postBackupAck,
	)
	if state.indexedAck == state.pendingAck || state.indexedAck == state.postBackupAck ||
		state.pendingAck == state.postBackupAck {
		t.Fatalf(
			"HEC recovery acknowledgments are not distinct: indexed=%d pending=%d post-backup=%d",
			state.indexedAck,
			state.pendingAck,
			state.postBackupAck,
		)
	}
}

func (fixture *releaseOCIRecoveryFixture) assertRestoredHECState() {
	t := fixture.t
	t.Helper()
	state := fixture.requireHECRecoveryState()
	releaseOCIAssertRestoredHECToken(
		t,
		fixture.ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		fixture.administratorToken,
		state,
	)
	releaseOCIWaitForHECAcknowledgment(
		t,
		fixture.ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		state,
		state.indexedAck,
	)
	// This transition is the restored-outbox proof: the selected SQLite image
	// carried a false ACK and normalized block without a committed visibility
	// outcome. Only restored reconciliation against the paired ClickHouse image
	// can make it true, whether the paused send was absent or outcome-ambiguous.
	releaseOCIWaitForHECAcknowledgment(
		t,
		fixture.ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		state,
		state.pendingAck,
	)
	statuses := backendHECQueryAcknowledgments(
		t,
		fixture.ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		state.plaintextToken,
		releaseOCIHECRecoveryChannel,
		state.indexedAck,
		state.pendingAck,
		state.postBackupAck,
	)
	if !statuses[state.indexedAck] || !statuses[state.pendingAck] || statuses[state.postBackupAck] {
		t.Fatalf(
			"restored HEC acknowledgment states = %v, want indexed=true pending=true discarded=false",
			statuses,
		)
	}

	wantSources := []string{state.indexedSource, state.pendingSource}
	slices.Sort(wantSources)
	provenanceQuery := fmt.Sprintf(`
		SELECT count(),
		       countIf(ingest_source_kind = 2),
		       countIf(empty(collector_id)),
		       uniqExact(ingest_source_id),
		       any(ingest_source_id),
		       arrayStringConcat(arraySort(groupUniqArray(source)), ',')
		FROM open_splunk.events
		WHERE index_name = '%s'
		FORMAT TSVRaw`, releaseOCIHECSafeName(t, state.indexName))
	wantProvenance := fmt.Sprintf(
		"2\t2\t2\t1\t%s\t%s",
		state.tokenID,
		strings.Join(wantSources, ","),
	)
	if got := releaseOCIQueryClickHouseBootstrap(
		t,
		fixture.ctx,
		fixture.stack,
		"inspect restored HEC paired provenance",
		provenanceQuery,
	); got != wantProvenance {
		t.Fatalf("restored HEC provenance = %q, want %q", got, wantProvenance)
	}

	eventIDsQuery := fmt.Sprintf(`
		SELECT event_id
		FROM open_splunk.events
		WHERE index_name = '%s'
		ORDER BY event_id
		FORMAT TSVRaw`, releaseOCIHECSafeName(t, state.indexName))
	eventIDsWire := releaseOCIQueryClickHouseBootstrap(
		t,
		fixture.ctx,
		fixture.stack,
		"read restored HEC event identities",
		eventIDsQuery,
	)
	eventIDs := strings.Split(eventIDsWire, "\n")
	if len(eventIDs) != 2 || eventIDs[0] == "" || eventIDs[1] == "" || eventIDs[0] == eventIDs[1] {
		t.Fatalf("restored HEC event identities = %q, want two distinct rows", eventIDsWire)
	}
	releaseOCIAssertSearchEventIDs(
		t,
		fixture.ctx,
		fixture.restoredClient,
		fixture.restoredBaseURL,
		state.indexName,
		fixture.fixtureTime,
		eventIDs,
	)
}

func (fixture *releaseOCIRecoveryFixture) requireHECRecoveryState() *releaseOCIHECRecoveryState {
	fixture.t.Helper()
	if fixture.hecRecovery == nil {
		fixture.t.Fatal("HEC paired recovery state is unavailable")
	}
	return fixture.hecRecovery
}

func releaseOCICreateHECRecoveryToken(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	indexName string,
) (plaintext string, tokenID string, tokenPrefix string) {
	t.Helper()
	defaultIndex := indexName
	var created opensplunkv1.CreateIngestionTokenResponse
	postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/ingestion-tokens/create",
		administratorToken,
		&opensplunkv1.CreateIngestionTokenRequest{
			Definition: &opensplunkv1.IngestionTokenDefinition{
				Name:    "Release OCI paired HEC recovery",
				Purpose: opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC,
				Constraints: &opensplunkv1.IngestionTokenConstraints{
					AllowedIndexNames: []string{indexName},
				},
				HecProfile: &opensplunkv1.IngestionTokenHecProfile{
					DefaultIndexName:      &defaultIndex,
					IndexerAcknowledgment: true,
				},
			},
		},
		&created,
	)
	metadata := created.GetIngestionToken()
	plaintext = created.GetPlaintextToken()
	if plaintext == "" || metadata.GetIngestionTokenId() == "" ||
		metadata.GetTokenPrefix() == "" || !strings.HasPrefix(plaintext, metadata.GetTokenPrefix()) ||
		metadata.GetPurpose() != opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC ||
		metadata.GetHecProfile().GetDefaultIndexName() != indexName ||
		!metadata.GetHecProfile().GetIndexerAcknowledgment() ||
		!slices.Equal(metadata.GetConstraints().GetAllowedIndexNames(), []string{indexName}) {
		t.Fatalf("created paired-recovery HEC token = %+v, plaintext length %d", metadata, len(plaintext))
	}
	return plaintext, metadata.GetIngestionTokenId(), metadata.GetTokenPrefix()
}

func releaseOCIAssertRestoredHECToken(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	administratorToken string,
	state *releaseOCIHECRecoveryState,
) {
	t.Helper()
	var response opensplunkv1.GetIngestionTokenResponse
	wire := postAdministratorProto(
		t,
		ctx,
		client,
		baseURL+"/api/v1/ingestion-tokens/get",
		administratorToken,
		&opensplunkv1.GetIngestionTokenRequest{IngestionTokenId: state.tokenID},
		&response,
	)
	if bytes.Contains(wire, []byte(state.plaintextToken)) {
		t.Fatal("restored HEC token response exposed plaintext")
	}
	metadata := response.GetIngestionToken()
	if metadata.GetIngestionTokenId() != state.tokenID ||
		metadata.GetTokenPrefix() != state.tokenPrefix ||
		metadata.GetState() != opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_ACTIVE ||
		metadata.GetPurpose() != opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC ||
		metadata.GetHecProfile().GetDefaultIndexName() != state.indexName ||
		!metadata.GetHecProfile().GetIndexerAcknowledgment() ||
		!slices.Equal(metadata.GetConstraints().GetAllowedIndexNames(), []string{state.indexName}) {
		t.Fatalf("restored paired-recovery HEC token = %+v", metadata)
	}
}

func releaseOCIWaitForHECAcknowledgment(
	t *testing.T,
	ctx context.Context,
	client *http.Client,
	baseURL string,
	state *releaseOCIHECRecoveryState,
	acknowledgmentID int64,
) {
	t.Helper()
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if backendHECQueryAcknowledgments(
			t,
			ctx,
			client,
			baseURL,
			state.plaintextToken,
			releaseOCIHECRecoveryChannel,
			acknowledgmentID,
		)[acknowledgmentID] {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for HEC acknowledgment %d: %v", acknowledgmentID, ctx.Err())
		case <-deadline.C:
			t.Fatalf("wait for HEC acknowledgment %d: timed out", acknowledgmentID)
		case <-ticker.C:
		}
	}
}

func releaseOCIHECRecoveryEventBody(
	t *testing.T,
	eventTime time.Time,
	source string,
	event string,
) []byte {
	t.Helper()
	body, err := json.Marshal(struct {
		Time       json.Number `json:"time"`
		Host       string      `json:"host"`
		Source     string      `json:"source"`
		Sourcetype string      `json:"sourcetype"`
		Event      string      `json:"event"`
	}{
		Time:       json.Number(backendHECEpochSeconds(eventTime)),
		Host:       "oci-hec-recovery-host",
		Source:     source,
		Sourcetype: "open-splunk:paired-recovery",
		Event:      event,
	})
	if err != nil {
		t.Fatalf("encode paired-recovery HEC event: %v", err)
	}
	return body
}

func releaseOCIHECSafeName(t *testing.T, value string) string {
	t.Helper()
	if value == "" || strings.IndexFunc(value, func(character rune) bool {
		return character != '-' && character != '_' &&
			(character < '0' || character > '9') &&
			(character < 'A' || character > 'Z') &&
			(character < 'a' || character > 'z')
	}) >= 0 {
		t.Fatalf("unsafe HEC recovery fixture name %q", value)
	}
	return value
}
