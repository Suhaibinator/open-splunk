package knowledgecatalog_test

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const writerStateMachineSeedCount = 20

// TestWriterDeterministicStateMachine exercises the public mutation boundary
// against a real, fully migrated SQLite catalog. Each seed has the same
// required lifecycle coverage, while deterministic shuffling varies when
// retries and rejected contenders are observed relative to later commits.
func TestWriterDeterministicStateMachine(t *testing.T) {
	harness := newWriterBlackboxHarness(t)
	suite := &writerStateMachineSuite{
		harness:           harness,
		definitionDigests: make(map[[sha256.Size]byte]int),
	}

	seed := uint64(0x4b4f5f5752495445)
	for index := 0; index < writerStateMachineSeedCount; index++ {
		seed += 0x9e3779b97f4a7c15
		currentSeed := seed
		t.Run(fmt.Sprintf("seed_%016x", currentSeed), func(t *testing.T) {
			machine := &writerStateMachine{
				t:     t,
				suite: suite,
				seed:  currentSeed,
				rng:   writerStateMachineRNG{state: currentSeed},
			}
			machine.run()
		})
	}
}

type writerStateMachineSuite struct {
	harness           *writerBlackboxHarness
	committed         int64
	objects           int64
	lastCatalogToken  [sha256.Size]byte
	definitionDigests map[[sha256.Size]byte]int
}

type writerStateMachine struct {
	t     *testing.T
	suite *writerStateMachineSuite
	seed  uint64
	step  string
	rng   writerStateMachineRNG
	model writerStateMachineModel
}

type writerStateMachineModel struct {
	objectID   string
	version    uint64
	state      knowledgecatalog.State
	definition *opensplunkv1.KnowledgeObjectDefinition
	history    map[uint64]writerStateMachineVersion
	commits    []*writerStateMachineCommit
}

type writerStateMachineVersion struct {
	state      knowledgecatalog.State
	definition *opensplunkv1.KnowledgeObjectDefinition
}

type writerStateMachineCommit struct {
	label           string
	route           string
	requestID       string
	mutationKind    string
	auditAction     audit.Action
	version         uint64
	catalogRevision uint64
	catalogToken    []byte
	request         proto.Message
	response        proto.Message
}

type writerStateMachineStep struct {
	name string
	run  func()
}

type writerStateMachineRNG struct {
	state uint64
}

func (rng *writerStateMachineRNG) next() uint64 {
	// xorshift64* is deliberately deterministic; this is sequencing entropy,
	// not a security primitive.
	rng.state ^= rng.state >> 12
	rng.state ^= rng.state << 25
	rng.state ^= rng.state >> 27
	return rng.state * 0x2545f4914f6cdd1d
}

func (machine *writerStateMachine) run() {
	machine.runStep("create_draft", machine.createDraft)
	machine.runShuffled([]writerStateMachineStep{
		{name: "early_create_replay", run: func() { machine.replay(machine.model.commits[0]) }},
		{name: "early_altered_create_conflict", run: machine.alteredCreateConflict},
	})

	machine.runStep("draft_metadata_update", func() { machine.updateMetadata("description") })
	machine.runShuffled([]writerStateMachineStep{
		{name: "early_update_replay", run: func() { machine.replay(machine.model.commits[1]) }},
		{name: "stale_update", run: machine.staleUpdate},
	})

	machine.runStep("disable", machine.disable)
	machine.runShuffled([]writerStateMachineStep{
		{name: "disabled_noop_state", run: machine.noopDisable},
		{name: "early_disable_replay", run: func() { machine.replay(machine.model.commits[2]) }},
	})

	machine.runStep("disabled_metadata_update", func() { machine.updateMetadata("name") })
	machine.runStep("delete", machine.delete)
	machine.runShuffled([]writerStateMachineStep{
		{name: "terminal_update", run: machine.terminalUpdate},
		{name: "terminal_delete", run: machine.terminalDelete},
	})

	// Replay every successful outcome only after the object has accumulated
	// later immutable versions (and, for the oldest outcomes, a tombstone).
	late := make([]writerStateMachineStep, 0, len(machine.model.commits)+1)
	for _, commit := range machine.model.commits {
		commit := commit
		late = append(late, writerStateMachineStep{
			name: "late_replay_" + commit.label,
			run:  func() { machine.replay(commit) },
		})
	}
	late = append(late, writerStateMachineStep{
		name: "late_altered_create_conflict",
		run:  machine.alteredCreateConflict,
	})
	machine.runShuffled(late)
}

func (machine *writerStateMachine) runShuffled(steps []writerStateMachineStep) {
	for index := len(steps) - 1; index > 0; index-- {
		swap := int(machine.rng.next() % uint64(index+1))
		steps[index], steps[swap] = steps[swap], steps[index]
	}
	for _, step := range steps {
		machine.runStep(step.name, step.run)
	}
}

func (machine *writerStateMachine) runStep(name string, run func()) {
	machine.step = name
	run()
}

func (machine *writerStateMachine) createDraft() {
	description := fmt.Sprintf("state-machine description %016x", machine.seed)
	request := &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition: writerAliasDefinition(
			writerTestApp,
			fmt.Sprintf("state-machine-%016x", machine.seed),
			&description,
			opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			fmt.Sprintf("host-%016x", machine.seed),
			"source_field",
			fmt.Sprintf("destination_%016x", machine.seed),
		),
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: machine.requestID("create"),
	}
	submitted := proto.Clone(request).(*opensplunkv1.CreateKnowledgeObjectRequest)
	before := machine.snapshotIfInitialized()
	response, err := machine.suite.harness.writer.Create(
		machine.suite.harness.actorCtx,
		machine.suite.harness.writeScope,
		request,
	)
	if err != nil {
		machine.Fatalf("Create: %v", err)
	}
	machine.assertRequestDetached(request, submitted)
	if response.GetKnowledgeObject() == nil {
		machine.Fatalf("Create returned a nil knowledge object")
	}
	machine.model = writerStateMachineModel{
		objectID:   response.GetKnowledgeObject().GetKnowledgeObjectId(),
		state:      knowledgecatalog.StateDraft,
		definition: proto.Clone(submitted.GetDefinition()).(*opensplunkv1.KnowledgeObjectDefinition),
		history:    make(map[uint64]writerStateMachineVersion),
	}
	machine.finishCommit(before, &writerStateMachineCommit{
		label:        "create",
		route:        "objects.create",
		requestID:    submitted.GetClientRequestId(),
		mutationKind: "create",
		auditAction:  audit.ActionKnowledgeObjectCreate,
		request:      submitted,
		response:     proto.Clone(response),
	}, response)
	machine.poisonRequest(request)
	machine.poisonResponse(response)
	machine.assertAllInvariants()
}

func (machine *writerStateMachine) updateMetadata(path string) {
	incomingDescription := fmt.Sprintf("incoming-%s-%016x-%d", path, machine.seed, machine.model.version)
	incoming := writerAliasDefinition(
		writerTestAppTwo,
		fmt.Sprintf("incoming-name-%016x-%d", machine.seed, machine.model.version),
		&incomingDescription,
		opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL,
		fmt.Sprintf("ignored-host-%016x", machine.seed),
		"ignored_source",
		"ignored_destination",
	)
	request := &opensplunkv1.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: machine.model.objectID,
		ExpectedVersion:   machine.model.version,
		Definition:        incoming,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{path}},
		ClientRequestId:   machine.requestID("update-" + path),
	}
	submitted := proto.Clone(request).(*opensplunkv1.UpdateKnowledgeObjectRequest)
	wantDefinition := proto.Clone(machine.model.definition).(*opensplunkv1.KnowledgeObjectDefinition)
	switch path {
	case "description":
		wantDefinition.Description = writerStringPointer(incomingDescription)
	case "name":
		wantDefinition.Name = incoming.GetName()
	default:
		machine.Fatalf("unsupported model update path %q", path)
	}
	before := machine.snapshot()
	response, err := machine.suite.harness.writer.Update(
		machine.suite.harness.actorCtx,
		machine.suite.harness.writeScope,
		request,
	)
	if err != nil {
		machine.Fatalf("Update(%s): %v", path, err)
	}
	machine.assertRequestDetached(request, submitted)
	machine.model.definition = wantDefinition
	machine.finishCommit(&before, &writerStateMachineCommit{
		label:        "update_" + path,
		route:        "objects.update",
		requestID:    submitted.GetClientRequestId(),
		mutationKind: "update",
		auditAction:  audit.ActionKnowledgeObjectUpdate,
		request:      submitted,
		response:     proto.Clone(response),
	}, response)
	machine.poisonRequest(request)
	machine.poisonResponse(response)
	machine.assertAllInvariants()
}

func (machine *writerStateMachine) disable() {
	request := &opensplunkv1.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: machine.model.objectID,
		ExpectedVersion:   machine.model.version,
		State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		ClientRequestId:   machine.requestID("disable"),
	}
	submitted := proto.Clone(request).(*opensplunkv1.SetKnowledgeObjectStateRequest)
	before := machine.snapshot()
	response, err := machine.suite.harness.writer.SetState(
		machine.suite.harness.actorCtx,
		machine.suite.harness.writeScope,
		request,
	)
	if err != nil {
		machine.Fatalf("SetState(DISABLED): %v", err)
	}
	machine.assertRequestDetached(request, submitted)
	machine.model.state = knowledgecatalog.StateDisabled
	machine.finishCommit(&before, &writerStateMachineCommit{
		label:        "disable",
		route:        "objects.set_state",
		requestID:    submitted.GetClientRequestId(),
		mutationKind: "disable",
		auditAction:  audit.ActionKnowledgeObjectDisable,
		request:      submitted,
		response:     proto.Clone(response),
	}, response)
	machine.poisonRequest(request)
	machine.poisonResponse(response)
	machine.assertAllInvariants()
}

func (machine *writerStateMachine) delete() {
	request := &opensplunkv1.DeleteKnowledgeObjectRequest{
		KnowledgeObjectId: machine.model.objectID,
		ExpectedVersion:   machine.model.version,
		ClientRequestId:   machine.requestID("delete"),
	}
	submitted := proto.Clone(request).(*opensplunkv1.DeleteKnowledgeObjectRequest)
	before := machine.snapshot()
	response, err := machine.suite.harness.writer.Delete(
		machine.suite.harness.actorCtx,
		machine.suite.harness.writeScope,
		request,
	)
	if err != nil {
		machine.Fatalf("Delete: %v", err)
	}
	machine.assertRequestDetached(request, submitted)
	machine.model.state = knowledgecatalog.StateDeleted
	machine.finishCommit(&before, &writerStateMachineCommit{
		label:        "delete",
		route:        "objects.delete",
		requestID:    submitted.GetClientRequestId(),
		mutationKind: "delete",
		auditAction:  audit.ActionKnowledgeObjectDelete,
		request:      submitted,
		response:     proto.Clone(response),
	}, response)
	machine.poisonRequest(request)
	machine.poisonResponse(response)
	machine.assertAllInvariants()
}

func (machine *writerStateMachine) staleUpdate() {
	definition := proto.Clone(machine.model.definition).(*opensplunkv1.KnowledgeObjectDefinition)
	definition.Description = writerStringPointer("stale contender must never publish")
	request := &opensplunkv1.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: machine.model.objectID,
		ExpectedVersion:   machine.model.version - 1,
		Definition:        definition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   machine.requestID("stale"),
	}
	machine.rejectUnchanged(request, control.ErrVersionConflict)
}

func (machine *writerStateMachine) noopDisable() {
	request := &opensplunkv1.SetKnowledgeObjectStateRequest{
		KnowledgeObjectId: machine.model.objectID,
		ExpectedVersion:   machine.model.version,
		State:             opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		ClientRequestId:   machine.requestID("noop-disable"),
	}
	machine.rejectUnchanged(request, control.ErrInvalidArgument)
}

func (machine *writerStateMachine) terminalUpdate() {
	definition := proto.Clone(machine.model.definition).(*opensplunkv1.KnowledgeObjectDefinition)
	definition.Description = writerStringPointer("terminal update must never publish")
	request := &opensplunkv1.UpdateKnowledgeObjectRequest{
		KnowledgeObjectId: machine.model.objectID,
		ExpectedVersion:   machine.model.version,
		Definition:        definition,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"description"}},
		ClientRequestId:   machine.requestID("terminal-update"),
	}
	machine.rejectUnchanged(request, control.ErrVersionConflict)
}

func (machine *writerStateMachine) terminalDelete() {
	request := &opensplunkv1.DeleteKnowledgeObjectRequest{
		KnowledgeObjectId: machine.model.objectID,
		ExpectedVersion:   machine.model.version,
		ClientRequestId:   machine.requestID("terminal-delete"),
	}
	machine.rejectUnchanged(request, control.ErrVersionConflict)
}

func (machine *writerStateMachine) alteredCreateConflict() {
	commit := machine.model.commits[0]
	request := proto.Clone(commit.request).(*opensplunkv1.CreateKnowledgeObjectRequest)
	request.Definition.Name = fmt.Sprintf("altered-%016x-%d", machine.seed, machine.rng.next()%1000)
	machine.rejectUnchanged(request, knowledgecatalog.ErrIdempotencyConflict)
}

func (machine *writerStateMachine) rejectUnchanged(request proto.Message, want error) {
	submitted := proto.Clone(request)
	before := machine.snapshot()
	var response proto.Message
	var err error
	switch typed := request.(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		response, err = machine.suite.harness.writer.Create(
			machine.suite.harness.actorCtx, machine.suite.harness.writeScope, typed,
		)
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		response, err = machine.suite.harness.writer.Update(
			machine.suite.harness.actorCtx, machine.suite.harness.writeScope, typed,
		)
	case *opensplunkv1.SetKnowledgeObjectStateRequest:
		response, err = machine.suite.harness.writer.SetState(
			machine.suite.harness.actorCtx, machine.suite.harness.writeScope, typed,
		)
	case *opensplunkv1.DeleteKnowledgeObjectRequest:
		response, err = machine.suite.harness.writer.Delete(
			machine.suite.harness.actorCtx, machine.suite.harness.writeScope, typed,
		)
	default:
		machine.Fatalf("unsupported rejected request type %T", request)
	}
	if response != nil && !reflect.ValueOf(response).IsNil() || !errors.Is(err, want) {
		machine.Fatalf("rejected %T = (%v, %v), want nil and %v", request, response, err, want)
	}
	machine.assertRequestDetached(request, submitted)
	assertWriterAuthoritySnapshotsEqual(machine.t, machine.snapshot(), before)
	machine.poisonRequest(request)
	machine.assertAllInvariants()
}

func (machine *writerStateMachine) replay(commit *writerStateMachineCommit) {
	request := proto.Clone(commit.request)
	submitted := proto.Clone(request)
	before := machine.snapshot()
	var response proto.Message
	var err error
	switch typed := request.(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		response, err = machine.suite.harness.writer.Create(
			machine.suite.harness.actorCtx, machine.suite.harness.writeScope, typed,
		)
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		response, err = machine.suite.harness.writer.Update(
			machine.suite.harness.actorCtx, machine.suite.harness.writeScope, typed,
		)
	case *opensplunkv1.SetKnowledgeObjectStateRequest:
		response, err = machine.suite.harness.writer.SetState(
			machine.suite.harness.actorCtx, machine.suite.harness.writeScope, typed,
		)
	case *opensplunkv1.DeleteKnowledgeObjectRequest:
		response, err = machine.suite.harness.writer.Delete(
			machine.suite.harness.actorCtx, machine.suite.harness.writeScope, typed,
		)
	default:
		machine.Fatalf("unsupported replay request type %T", request)
	}
	if err != nil || !proto.Equal(response, commit.response) {
		machine.Fatalf("replay %s = (%v, %v), want %v", commit.label, response, err, commit.response)
	}
	machine.assertRequestDetached(request, submitted)
	assertWriterAuthoritySnapshotsEqual(machine.t, machine.snapshot(), before)
	machine.poisonRequest(request)
	machine.poisonResponse(response)
	machine.assertAllInvariants()
}

func (machine *writerStateMachine) finishCommit(
	before *writerAuthoritySnapshot,
	commit *writerStateMachineCommit,
	response proto.Message,
) {
	revision, token := writerStateMachineResponseCatalog(machine.t, response)
	wantRevision := uint64(machine.suite.committed + 1)
	if revision != wantRevision || len(token) != sha256.Size ||
		bytes.Equal(token, make([]byte, sha256.Size)) {
		machine.Fatalf("commit response branch = revision %d token %x, want revision %d and a nonzero 32-byte token", revision, token, wantRevision)
	}
	if machine.suite.committed > 0 && bytes.Equal(token, machine.suite.lastCatalogToken[:]) {
		machine.Fatalf("commit reused prior catalog state token %x", token)
	}

	machine.model.version++
	commit.version = machine.model.version
	commit.catalogRevision = revision
	commit.catalogToken = bytes.Clone(token)
	machine.model.history[machine.model.version] = writerStateMachineVersion{
		state:      machine.model.state,
		definition: proto.Clone(machine.model.definition).(*opensplunkv1.KnowledgeObjectDefinition),
	}
	machine.model.commits = append(machine.model.commits, commit)
	machine.suite.committed++
	if machine.model.version == 1 {
		machine.suite.objects++
	}
	digest, size := writerStateMachineDefinitionDigest(machine.t, machine.model.definition)
	if _, found := machine.suite.definitionDigests[digest]; !found {
		machine.suite.definitionDigests[digest] = size
	}
	copy(machine.suite.lastCatalogToken[:], token)

	machine.assertResponseMatchesModel(response)
	after := machine.snapshot()
	if before != nil {
		if after.CatalogRevision != before.CatalogRevision+1 ||
			after.VersionCount != before.VersionCount+1 ||
			after.IdempotencyCount != before.IdempotencyCount+1 ||
			after.AuditEventCount != before.AuditEventCount+1 ||
			after.AuditNextSequence != before.AuditNextSequence+1 ||
			bytes.Equal(after.CatalogStateToken[:], before.CatalogStateToken[:]) {
			machine.Fatalf("successful commit did not advance each authority exactly once:\n before=%#v\n after=%#v", *before, after)
		}
	}
}

func (machine *writerStateMachine) assertResponseMatchesModel(response proto.Message) {
	var object *opensplunkv1.KnowledgeObject
	switch typed := response.(type) {
	case *opensplunkv1.CreateKnowledgeObjectResponse:
		object = typed.GetKnowledgeObject()
	case *opensplunkv1.UpdateKnowledgeObjectResponse:
		object = typed.GetKnowledgeObject()
	case *opensplunkv1.SetKnowledgeObjectStateResponse:
		object = typed.GetKnowledgeObject()
	case *opensplunkv1.DeleteKnowledgeObjectResponse:
		if typed.GetKnowledgeObjectId() != machine.model.objectID || typed.GetDeletedVersion() != machine.model.version {
			machine.Fatalf("Delete response = %v, want %q version %d", typed, machine.model.objectID, machine.model.version)
		}
		return
	default:
		machine.Fatalf("unsupported successful response type %T", response)
	}
	if object == nil || object.GetKnowledgeObjectId() != machine.model.objectID ||
		object.GetVersion() != machine.model.version ||
		object.GetState() != writerStateMachineProtoState(machine.model.state) ||
		!proto.Equal(object.GetDefinition(), machine.model.definition) {
		machine.Fatalf("response object = %v, want id=%q version=%d state=%q definition=%v", object, machine.model.objectID, machine.model.version, machine.model.state, machine.model.definition)
	}
	wantDigest, _ := writerStateMachineDefinitionDigest(machine.t, machine.model.definition)
	if !bytes.Equal(object.GetDefinitionSha256(), wantDigest[:]) {
		machine.Fatalf("response definition digest = %x, want %x", object.GetDefinitionSha256(), wantDigest)
	}
}

func (machine *writerStateMachine) assertAllInvariants() {
	snapshot := machine.snapshot()
	if snapshot.CatalogRevision != machine.suite.committed ||
		snapshot.IdentityCount != machine.suite.objects ||
		snapshot.VersionCount != machine.suite.committed ||
		snapshot.IdempotencyCount != machine.suite.committed ||
		snapshot.ActiveObjectCount != 0 ||
		snapshot.AuditEventCount != machine.suite.committed ||
		snapshot.AuditNextSequence != machine.suite.committed+1 ||
		!bytes.Equal(snapshot.CatalogStateToken[:], machine.suite.lastCatalogToken[:]) {
		machine.Fatalf("global writer authority = %#v, committed=%d objects=%d token=%x", snapshot, machine.suite.committed, machine.suite.objects, machine.suite.lastCatalogToken)
	}
	wantDefinitionBytes := int64(0)
	for _, size := range machine.suite.definitionDigests {
		wantDefinitionBytes += int64(size)
	}
	if snapshot.DefinitionBytes != wantDefinitionBytes {
		machine.Fatalf("definition byte ledger = %d, want %d", snapshot.DefinitionBytes, wantDefinitionBytes)
	}
	assertWriterTableCounts(machine.t, snapshot, map[string]int64{
		"knowledge_definition_blobs":              int64(len(machine.suite.definitionDigests)),
		"knowledge_objects":                       machine.suite.objects,
		"knowledge_object_versions":               machine.suite.committed,
		"knowledge_object_version_lifecycle":      machine.suite.committed,
		"knowledge_object_dependencies":           0,
		"knowledge_object_dependency_seals":       machine.suite.committed,
		"knowledge_object_list_projections":       machine.suite.objects,
		"knowledge_object_list_selector_patterns": machine.suite.objects,
		"knowledge_object_list_projection_seals":  machine.suite.objects,
		"knowledge_object_list_order_keys":        machine.suite.objects,
		"knowledge_mutation_commit_authorities":   machine.suite.committed,
		"knowledge_mutation_idempotency":          machine.suite.committed,
		"audit_events":                            machine.suite.committed,
	})

	machine.assertStoredVersions()
	machine.assertCommitProvenance()
	assertWriterCatalogIntegrity(machine.t, machine.suite.harness.database)
}

func (machine *writerStateMachine) assertStoredVersions() {
	var versionCount int64
	if err := machine.suite.harness.database.SQLDB().QueryRowContext(machine.t.Context(), `
		SELECT count(*)
		FROM knowledge_object_versions
		WHERE tenant_id = ? AND knowledge_object_id = ?`,
		writerTestTenant,
		machine.model.objectID,
	).Scan(&versionCount); err != nil {
		machine.Fatalf("count immutable versions: %v", err)
	}
	if versionCount != int64(machine.model.version) || len(machine.model.history) != int(machine.model.version) {
		machine.Fatalf("immutable version count = %d/model %d, want %d", versionCount, len(machine.model.history), machine.model.version)
	}

	for version := uint64(1); version <= machine.model.version; version++ {
		want := machine.model.history[version]
		got := getWriterObject(machine.t, machine.suite.harness, machine.model.objectID, &version)
		machine.assertStoredObject(got, version, want)
	}
	current := getWriterObject(machine.t, machine.suite.harness, machine.model.objectID, nil)
	machine.assertStoredObject(current, machine.model.version, machine.model.history[machine.model.version])
}

func (machine *writerStateMachine) assertStoredObject(
	got knowledgecatalog.Object,
	version uint64,
	want writerStateMachineVersion,
) {
	if got.KnowledgeObjectID != machine.model.objectID || got.TenantID != writerTestTenant ||
		got.OwnerID != writerTestOwner || got.Version != version || got.State != want.state ||
		got.AppID != want.definition.GetAppId() || got.Name != want.definition.GetName() ||
		!proto.Equal(got.Definition, want.definition) {
		machine.Fatalf("stored object v%d = %#v, want state=%q definition=%v", version, got, want.state, want.definition)
	}
	digest, _ := writerStateMachineDefinitionDigest(machine.t, want.definition)
	if !bytes.Equal(got.DefinitionSHA256, digest[:]) {
		machine.Fatalf("stored object v%d digest = %x, want %x", version, got.DefinitionSHA256, digest)
	}
}

func (machine *writerStateMachine) assertCommitProvenance() {
	database := machine.suite.harness.database.SQLDB()
	var receiptCount, receiptAuditCount, receiptAuditDistinct, auditCount int64
	if err := database.QueryRowContext(machine.t.Context(), `
		SELECT count(*),
		       count(successful_audit_sequence),
		       count(DISTINCT successful_audit_sequence)
		FROM knowledge_mutation_idempotency
		WHERE tenant_id = ? AND knowledge_object_id = ?`,
		writerTestTenant,
		machine.model.objectID,
	).Scan(&receiptCount, &receiptAuditCount, &receiptAuditDistinct); err != nil {
		machine.Fatalf("count object receipts: %v", err)
	}
	if err := database.QueryRowContext(machine.t.Context(), `
		SELECT count(*)
		FROM audit_events
		WHERE tenant_id = ? AND target_kind = ? AND target_id = ?`,
		writerTestTenant,
		audit.TargetKindKnowledgeObject,
		machine.model.objectID,
	).Scan(&auditCount); err != nil {
		machine.Fatalf("count object audits: %v", err)
	}
	want := int64(len(machine.model.commits))
	if receiptCount != want || receiptAuditCount != want || receiptAuditDistinct != want || auditCount != want {
		machine.Fatalf("object provenance counts = receipts %d linked %d distinct %d audits %d, want %d", receiptCount, receiptAuditCount, receiptAuditDistinct, auditCount, want)
	}

	for _, commit := range machine.model.commits {
		var mutationKind string
		var requestFormat, objectVersion, catalogRevision, auditSequence, outcomeBytes int64
		var requestDigest, catalogToken []byte
		var recoverySequence sql.NullInt64
		if err := database.QueryRowContext(machine.t.Context(), `
			SELECT mutation_kind, request_digest_format_version, request_digest,
			       object_version, committed_catalog_revision,
			       committed_catalog_state_token, successful_audit_sequence,
			       recovery_audit_sequence, length(outcome_proto)
			FROM knowledge_mutation_idempotency
			WHERE tenant_id = ? AND actor_kind = ? AND actor_id = ?
			  AND route = ? AND client_request_id = ?`,
			writerTestTenant,
			audit.ActorKindBrowser,
			"writer-blackbox-administrator",
			commit.route,
			commit.requestID,
		).Scan(
			&mutationKind,
			&requestFormat,
			&requestDigest,
			&objectVersion,
			&catalogRevision,
			&catalogToken,
			&auditSequence,
			&recoverySequence,
			&outcomeBytes,
		); err != nil {
			machine.Fatalf("read receipt %s: %v", commit.label, err)
		}
		expectedDigest := writerExpectedRequestDigest(machine.t, commit.route, writerTestOwner, commit.request)
		if mutationKind != commit.mutationKind || requestFormat != 1 ||
			!bytes.Equal(requestDigest, expectedDigest[:]) ||
			objectVersion != int64(commit.version) || catalogRevision != int64(commit.catalogRevision) ||
			!bytes.Equal(catalogToken, commit.catalogToken) || auditSequence < 1 ||
			recoverySequence.Valid || outcomeBytes < 1 || outcomeBytes > 1024 {
			machine.Fatalf("receipt %s = kind=%q request=v%d/%x object=v%d branch=%d/%x audit=%d recovery=%v outcome=%d", commit.label, mutationKind, requestFormat, requestDigest, objectVersion, catalogRevision, catalogToken, auditSequence, recoverySequence, outcomeBytes)
		}

		var action audit.Action
		var actorKind audit.ActorKind
		var actorID string
		var actorRole audit.ActorRole
		var targetKind audit.TargetKind
		var targetID string
		var targetVersion int64
		if err := database.QueryRowContext(machine.t.Context(), `
			SELECT action, actor_kind, actor_id, actor_role,
			       target_kind, target_id, target_version
			FROM audit_events
			WHERE tenant_id = ? AND sequence = ?`,
			writerTestTenant,
			auditSequence,
		).Scan(
			&action,
			&actorKind,
			&actorID,
			&actorRole,
			&targetKind,
			&targetID,
			&targetVersion,
		); err != nil {
			machine.Fatalf("read audit for %s: %v", commit.label, err)
		}
		if action != commit.auditAction || actorKind != audit.ActorKindBrowser ||
			actorID != "writer-blackbox-administrator" || actorRole != audit.ActorRoleAdministrator ||
			targetKind != audit.TargetKindKnowledgeObject || targetID != machine.model.objectID ||
			targetVersion != int64(commit.version) {
			machine.Fatalf("audit for %s = action=%q actor=%q/%q/%q target=%q/%q/v%d", commit.label, action, actorKind, actorID, actorRole, targetKind, targetID, targetVersion)
		}
	}
}

func (machine *writerStateMachine) snapshotIfInitialized() *writerAuthoritySnapshot {
	if machine.suite.committed == 0 {
		return nil
	}
	snapshot := machine.snapshot()
	return &snapshot
}

func (machine *writerStateMachine) snapshot() writerAuthoritySnapshot {
	return readWriterAuthoritySnapshot(machine.t, machine.suite.harness.database)
}

func (machine *writerStateMachine) requestID(operation string) string {
	return fmt.Sprintf("sm-%016x-%s-%d", machine.seed, operation, machine.rng.next()%1_000_000)
}

func (machine *writerStateMachine) assertRequestDetached(got, want proto.Message) {
	if !proto.Equal(got, want) {
		machine.Fatalf("Writer mutated caller-owned %T: got %v want %v", got, got, want)
	}
}

func (machine *writerStateMachine) poisonRequest(request proto.Message) {
	switch typed := request.(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		typed.Definition.Name = "caller-poisoned-create"
		typed.ClientRequestId = "caller-poisoned-create-request"
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		typed.Definition.Name = "caller-poisoned-update"
		typed.KnowledgeObjectId = "caller-poisoned-update-object"
	case *opensplunkv1.SetKnowledgeObjectStateRequest:
		typed.KnowledgeObjectId = "caller-poisoned-set-state-object"
		typed.ExpectedVersion++
	case *opensplunkv1.DeleteKnowledgeObjectRequest:
		typed.KnowledgeObjectId = "caller-poisoned-delete-object"
		typed.ExpectedVersion++
	default:
		machine.Fatalf("unsupported poison request type %T", request)
	}
}

func (machine *writerStateMachine) poisonResponse(response proto.Message) {
	switch typed := response.(type) {
	case *opensplunkv1.CreateKnowledgeObjectResponse:
		typed.KnowledgeObject.Definition.Name = "caller-poisoned-create-response"
		typed.TenantCatalogStateToken[0] ^= 0xff
	case *opensplunkv1.UpdateKnowledgeObjectResponse:
		typed.KnowledgeObject.Definition.Name = "caller-poisoned-update-response"
		typed.TenantCatalogStateToken[0] ^= 0xff
	case *opensplunkv1.SetKnowledgeObjectStateResponse:
		typed.KnowledgeObject.Definition.Name = "caller-poisoned-state-response"
		typed.TenantCatalogStateToken[0] ^= 0xff
	case *opensplunkv1.DeleteKnowledgeObjectResponse:
		typed.KnowledgeObjectId = "caller-poisoned-delete-response"
		typed.TenantCatalogStateToken[0] ^= 0xff
	default:
		machine.Fatalf("unsupported poison response type %T", response)
	}
}

func (machine *writerStateMachine) Fatalf(format string, arguments ...any) {
	machine.t.Helper()
	prefix := fmt.Sprintf("seed=%016x step=%s: ", machine.seed, machine.step)
	machine.t.Fatalf(prefix+format, arguments...)
}

func writerStateMachineResponseCatalog(t *testing.T, response proto.Message) (uint64, []byte) {
	t.Helper()
	switch typed := response.(type) {
	case *opensplunkv1.CreateKnowledgeObjectResponse:
		return typed.GetTenantCatalogRevision(), typed.GetTenantCatalogStateToken()
	case *opensplunkv1.UpdateKnowledgeObjectResponse:
		return typed.GetTenantCatalogRevision(), typed.GetTenantCatalogStateToken()
	case *opensplunkv1.SetKnowledgeObjectStateResponse:
		return typed.GetTenantCatalogRevision(), typed.GetTenantCatalogStateToken()
	case *opensplunkv1.DeleteKnowledgeObjectResponse:
		return typed.GetTenantCatalogRevision(), typed.GetTenantCatalogStateToken()
	default:
		t.Fatalf("unsupported response catalog type %T", response)
		return 0, nil
	}
}

func writerStateMachineDefinitionDigest(
	t *testing.T,
	definition *opensplunkv1.KnowledgeObjectDefinition,
) ([sha256.Size]byte, int) {
	t.Helper()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil {
		t.Fatalf("marshal state-machine definition: %v", err)
	}
	return sha256.Sum256(encoded), len(encoded)
}

func writerStateMachineProtoState(state knowledgecatalog.State) opensplunkv1.KnowledgeObjectState {
	switch state {
	case knowledgecatalog.StateDraft:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT
	case knowledgecatalog.StateDisabled:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED
	case knowledgecatalog.StateDeleted:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED
	default:
		return opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_UNSPECIFIED
	}
}
