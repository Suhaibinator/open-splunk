package knowledgecatalog

import (
	"bytes"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func FuzzKnowledgeUpdateMaskCanonicalization(f *testing.F) {
	validMask := &fieldmaskpb.FieldMask{Paths: []string{"description", "name"}}
	validPaths, err := normalizeKnowledgeUpdateMask(validMask)
	if err != nil || !slices.Equal(validPaths, validMask.GetPaths()) {
		f.Fatalf("known-valid mask normalization = (%q, %v), want %q", validPaths, err, validMask.GetPaths())
	}

	for _, seed := range [][]byte{
		[]byte("description\x00name"),
		[]byte("field_alias"),
		[]byte("name\x00name"),
		[]byte("*"),
		[]byte("definition.name"),
		{0xff, 0x00, 0x01},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 4096 {
			t.Skip()
		}
		paths := strings.Split(string(raw), "\x00")
		mask := &fieldmaskpb.FieldMask{Paths: slices.Clone(paths)}
		before := proto.Clone(mask).(*fieldmaskpb.FieldMask)
		first, firstErr := normalizeKnowledgeUpdateMask(mask)
		second, secondErr := normalizeKnowledgeUpdateMask(mask)
		if !sameWriterFuzzError(firstErr, secondErr) || !slices.Equal(first, second) {
			t.Fatalf("mask normalization is unstable: (%q, %v) != (%q, %v)", first, firstErr, second, secondErr)
		}
		if !proto.Equal(mask, before) {
			t.Fatalf("mask normalization mutated caller input: got %v want %v", mask, before)
		}
		if firstErr != nil {
			return
		}
		if !slices.IsSorted(first) || len(first) == 0 || len(first) > len(knowledgeUpdatePaths) {
			t.Fatalf("successful mask is noncanonical: %q", first)
		}
		for index, path := range first {
			if _, ok := knowledgeUpdatePaths[path]; !ok || index > 0 && first[index-1] == path {
				t.Fatalf("successful mask retained invalid path %q in %q", path, first)
			}
		}
		first[0] = "caller-mutation"
		if !proto.Equal(mask, before) {
			t.Fatal("successful mask result aliases caller input")
		}
	})
}

func FuzzApplyKnowledgeDefinitionMaskIsDetachedAndStable(f *testing.F) {
	validCurrentDescription := "current"
	validIncomingDescription := "updated description"
	validCurrent := aliasDefinition(
		testApp, "current", SharingScopePrivate, &validCurrentDescription, "host-current",
	)
	validIncoming := aliasDefinition(
		testApp, "updated", SharingScopePrivate, &validIncomingDescription, "host-updated",
	)
	validApplied, err := applyKnowledgeDefinitionMask(
		validCurrent,
		validIncoming,
		[]string{"description", "name"},
	)
	if err != nil || validApplied.GetName() != "updated" ||
		validApplied.GetDescription() != validIncomingDescription {
		f.Fatalf("known-valid mask application = (%v, %v)", validApplied, err)
	}

	for _, seed := range []struct {
		name, description, host, rawPaths string
	}{
		{"updated", "description", "host-a", "description\x00name"},
		{"", "", "", "selector"},
		{"updated", "description", "host-*", "field_alias"},
		{"updated", "description", "host-a", "calculated_field"},
	} {
		f.Add(seed.name, seed.description, seed.host, seed.rawPaths)
	}
	f.Fuzz(func(t *testing.T, name, description, host, rawPaths string) {
		if len(name)+len(description)+len(host)+len(rawPaths) > 8192 {
			t.Skip()
		}
		currentDescription := "current"
		current := aliasDefinition(testApp, "current", SharingScopePrivate, &currentDescription, "host-current")
		incoming := aliasDefinition(testApp, name, SharingScopePrivate, &description, host)
		paths, maskErr := normalizeKnowledgeUpdateMask(&fieldmaskpb.FieldMask{Paths: strings.Split(rawPaths, "\x00")})
		if maskErr != nil {
			return
		}
		currentBefore := proto.Clone(current).(*opensplunkv1.KnowledgeObjectDefinition)
		incomingBefore := proto.Clone(incoming).(*opensplunkv1.KnowledgeObjectDefinition)
		first, firstErr := applyKnowledgeDefinitionMask(current, incoming, paths)
		second, secondErr := applyKnowledgeDefinitionMask(current, incoming, paths)
		if !sameWriterFuzzError(firstErr, secondErr) || !proto.Equal(first, second) {
			t.Fatalf("mask application is unstable: (%v, %v) != (%v, %v)", first, firstErr, second, secondErr)
		}
		if !proto.Equal(current, currentBefore) || !proto.Equal(incoming, incomingBefore) {
			t.Fatal("mask application mutated caller definitions")
		}
		if firstErr != nil {
			return
		}
		first.Name = "caller-mutation"
		if !proto.Equal(current, currentBefore) || !proto.Equal(incoming, incomingBefore) || second.GetName() == first.GetName() {
			t.Fatal("mask application result aliases caller or repeated result")
		}
	})
}

func FuzzPrepareCreateMutationCanonicalDigest(f *testing.F) {
	validDescription := "description"
	validRequest := &opensplunkv1.CreateKnowledgeObjectRequest{
		Definition: aliasDefinition(
			testApp, "writer-fuzz-valid", SharingScopePrivate, &validDescription, "host-valid",
		),
		InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		ClientRequestId: "writer-fuzz-valid-request-0001",
	}
	validScope := normalizedWriteScope{
		tenantID: testTenant, ownerID: testOwner, writableAppIDs: []string{testApp},
	}
	validActor := audit.Actor{
		Kind: audit.ActorKindBrowser, ID: testOwner, Role: audit.ActorRoleAdministrator,
	}
	validPrepared, err := prepareCreateMutation(validScope, validActor, validRequest)
	if err != nil || len(validPrepared.requestBytes) == 0 ||
		validPrepared.requestDigest != digestMutationRequest(
			mutationRouteCreate,
			testOwner,
			validPrepared.requestBytes,
		) {
		f.Fatalf("known-valid Create preparation = (%v, %v)", validPrepared, err)
	}

	f.Add("writer-fuzz-request-0001", "name", "description", "host")
	f.Add("short", "", "", "")
	f.Add("writer-fuzz-request-0002", "name\x00", "description\n", "host-*")
	f.Fuzz(func(t *testing.T, requestID, name, description, host string) {
		if len(requestID)+len(name)+len(description)+len(host) > 16<<10 {
			t.Skip()
		}
		request := &opensplunkv1.CreateKnowledgeObjectRequest{
			Definition:      aliasDefinition(testApp, name, SharingScopePrivate, &description, host),
			InitialState:    opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			ClientRequestId: requestID,
		}
		before := proto.Clone(request).(*opensplunkv1.CreateKnowledgeObjectRequest)
		scope := normalizedWriteScope{tenantID: testTenant, ownerID: testOwner, writableAppIDs: []string{testApp}}
		actor := audit.Actor{Kind: audit.ActorKindBrowser, ID: testOwner, Role: audit.ActorRoleAdministrator}
		first, firstErr := prepareCreateMutation(scope, actor, request)
		second, secondErr := prepareCreateMutation(scope, actor, request)
		if !sameWriterFuzzError(firstErr, secondErr) {
			t.Fatalf("request preparation classification is unstable: %v != %v", firstErr, secondErr)
		}
		if !proto.Equal(request, before) {
			t.Fatal("request preparation mutated caller input")
		}
		if firstErr != nil {
			return
		}
		if first.clientRequestID != requestID || first.requestDigest != second.requestDigest ||
			!bytes.Equal(first.requestBytes, second.requestBytes) ||
			first.requestDigest != digestMutationRequest(mutationRouteCreate, testOwner, first.requestBytes) {
			t.Fatal("successful request preparation is not canonical")
		}
		decoded := &opensplunkv1.CreateKnowledgeObjectRequest{}
		if err := proto.Unmarshal(first.requestBytes, decoded); err != nil || decoded.GetClientRequestId() != "" {
			t.Fatalf("canonical request payload retained key or failed decode: %v, %q", err, decoded.GetClientRequestId())
		}
		request.Definition.Name = "caller-mutation"
		request.ClientRequestId = "caller-mutation"
		if !bytes.Equal(first.requestBytes, second.requestBytes) || first.requestDigest != second.requestDigest {
			t.Fatal("prepared request aliases caller storage")
		}
	})
}

func FuzzKnowledgeOutcomeReferenceStrictDecode(f *testing.F) {
	digest := bytes.Repeat([]byte{0x5a}, persistedKnowledgeDefinitionDigestBytes)
	const occurredAtUnixMicro int64 = 29
	const retentionAnchorUnixMicro int64 = 31
	retainUntilUnixMicro := retentionAnchorUnixMicro + int64(minimumIdempotencyRetention/time.Microsecond)
	valid, err := encodeOutcomeReference(mutationOutcomeAuthority{
		route:                    mutationRouteUpdate,
		mutationKind:             "update",
		objectID:                 "ko-writer-fuzz-outcome",
		version:                  17,
		digest:                   digest,
		catalogRevision:          23,
		catalogStateToken:        bytes.Repeat([]byte{0x6b}, catalogStateTokenBytes),
		successfulAuditSequence:  19,
		occurredAtUnixMicro:      occurredAtUnixMicro,
		retentionAnchorUnixMicro: retentionAnchorUnixMicro,
		retainUntilUnixMicro:     retainUntilUnixMicro,
	})
	if err != nil {
		f.Fatalf("encode valid outcome seed: %v", err)
	}
	auditSequence := int64(19)
	validRecord := idempotencyRecord{
		OutcomeFormatVersion:       mutationOutcomeFormatVersion,
		OutcomeProto:               bytes.Clone(valid),
		Route:                      mutationRouteUpdate,
		MutationKind:               "update",
		KnowledgeObjectID:          "ko-writer-fuzz-outcome",
		ObjectVersion:              17,
		CommittedCatalogRevision:   23,
		CommittedCatalogStateToken: bytes.Repeat([]byte{0x6b}, catalogStateTokenBytes),
		SuccessfulAuditSequence:    &auditSequence,
		CreatedAtUnixMicro:         occurredAtUnixMicro,
		RetentionAnchorUnixMicro:   retentionAnchorUnixMicro,
		RetainUntilUnixMicro:       retainUntilUnixMicro,
	}
	if reference, err := decodeOutcomeReference(validRecord); err != nil ||
		reference.GetKnowledgeObjectId() != validRecord.KnowledgeObjectID {
		f.Fatalf("valid outcome seed does not reach successful decode: (%v, %v)", reference, err)
	}
	f.Add(valid)
	f.Add([]byte{})
	f.Add([]byte{0xff, 0x01, 0x00})
	f.Add(bytes.Repeat([]byte{0x01}, maximumMutationOutcomeBytes+1))
	withTopLevelUnknown := bytes.Clone(valid)
	withTopLevelUnknown = protowire.AppendTag(withTopLevelUnknown, 127, protowire.VarintType)
	withTopLevelUnknown = protowire.AppendVarint(withTopLevelUnknown, 1)
	f.Add(withTopLevelUnknown)
	withNestedUnknown := &opensplunkv1.KnowledgeMutationOutcomeRecord{}
	if err := proto.Unmarshal(valid, withNestedUnknown); err != nil {
		f.Fatalf("decode valid nested-unknown seed: %v", err)
	}
	nestedUnknown := protowire.AppendTag(nil, 127, protowire.VarintType)
	nestedUnknown = protowire.AppendVarint(nestedUnknown, 1)
	withNestedUnknown.GetObject().ProtoReflect().SetUnknown(nestedUnknown)
	nestedUnknownWire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(withNestedUnknown)
	if err != nil {
		f.Fatalf("encode nested-unknown seed: %v", err)
	}
	f.Add(nestedUnknownWire)
	duplicateKnown := bytes.Clone(valid)
	duplicateKnown = protowire.AppendTag(duplicateKnown, 1, protowire.BytesType)
	duplicateKnown = protowire.AppendString(duplicateKnown, mutationRouteUpdate)
	f.Add(duplicateKnown)
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > maximumMutationOutcomeBytes+1024 {
			t.Skip()
		}
		record := validRecord
		record.OutcomeProto = bytes.Clone(encoded)
		first, firstErr := decodeOutcomeReference(record)
		second, secondErr := decodeOutcomeReference(record)
		if !sameWriterFuzzError(firstErr, secondErr) || !proto.Equal(first, second) {
			t.Fatalf("outcome decode is unstable: (%v, %v) != (%v, %v)", first, firstErr, second, secondErr)
		}
		if firstErr != nil {
			if !errors.Is(firstErr, ErrCorrupt) {
				t.Fatalf("malformed outcome classified unexpectedly: %v", firstErr)
			}
			return
		}
		outcome := &opensplunkv1.KnowledgeMutationOutcomeRecord{}
		unmarshalErr := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, outcome)
		canonical, marshalErr := (proto.MarshalOptions{Deterministic: true}).Marshal(outcome)
		if unmarshalErr != nil || marshalErr != nil || !bytes.Equal(canonical, encoded) ||
			first.GetKnowledgeObjectId() != record.KnowledgeObjectID ||
			first.GetVersion() != uint64(record.ObjectVersion) ||
			len(first.GetDefinitionSha256()) != persistedKnowledgeDefinitionDigestBytes ||
			first.GetVersion() > math.MaxInt64 {
			t.Fatalf("successful outcome decode is noncanonical: %v, unmarshal=%v marshal=%v", first, unmarshalErr, marshalErr)
		}
		first.DefinitionSha256[0] ^= 0xff
		if bytes.Equal(first.GetDefinitionSha256(), second.GetDefinitionSha256()) {
			t.Fatal("decoded outcomes alias each other")
		}
	})
}

func sameWriterFuzzError(left, right error) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Error() == right.Error()
}
