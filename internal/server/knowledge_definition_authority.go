package server

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"errors"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
)

// validKnowledgeCatalogDefinitionAuthority validates an injected catalog's
// detached definition before ObjectToProto clones it. The concrete catalog
// performs the same checks while hydrating SQLite state, but the server accepts
// a configurable KnowledgeCatalog and therefore cannot rely on that
// implementation detail at its response boundary.
func validKnowledgeCatalogDefinitionAuthority(object knowledgecatalog.Object) bool {
	state, stateOK := knowledgeCatalogStateToProto(object.State)
	if !stateOK {
		return false
	}
	return validKnowledgeDefinitionAuthority(knowledgeDefinitionAuthority{
		definition:   object.Definition,
		digest:       object.DefinitionSHA256,
		state:        state,
		objectType:   object.ObjectType,
		appID:        object.AppID,
		name:         object.Name,
		sharingScope: object.SharingScope,
	})
}

func validKnowledgeProtoDefinitionAuthority(
	object *opensplunk.KnowledgeObject,
) bool {
	if object == nil {
		return false
	}
	objectType, typeOK := knowledgeObjectTypeFromProto(object.GetObjectType())
	state, stateOK := knowledgeStateFromProto(object.GetState())
	sharingScope, scopeOK := knowledgeSharingScopeFromProto(object.GetSharingScope())
	if !typeOK || !stateOK || !scopeOK {
		return false
	}
	protoState, stateOK := knowledgeCatalogStateToProto(state)
	if !stateOK {
		return false
	}
	return validKnowledgeDefinitionAuthority(knowledgeDefinitionAuthority{
		definition:   object.GetDefinition(),
		digest:       object.GetDefinitionSha256(),
		state:        protoState,
		objectType:   objectType,
		appID:        object.GetAppId(),
		name:         object.GetName(),
		sharingScope: sharingScope,
	})
}

type knowledgeDefinitionAuthority struct {
	definition   *opensplunk.KnowledgeObjectDefinition
	digest       []byte
	state        opensplunk.KnowledgeObjectState
	objectType   knowledgecatalog.ObjectType
	appID        string
	name         string
	sharingScope knowledgecatalog.SharingScope
}

func validKnowledgeDefinitionAuthority(authority knowledgeDefinitionAuthority) bool {
	if authority.state == opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED {
		return authority.definition == nil && len(authority.digest) == 0
	}
	if authority.definition == nil || len(authority.digest) != sha256.Size {
		return false
	}
	normalized, normalizeErr := knowledgedefinition.Normalize(authority.definition)
	if objectType, known := knowledgeDefinitionObjectType(authority.definition); known {
		if normalizeErr != nil {
			return false
		}
		encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(authority.definition)
		if err != nil {
			return false
		}
		convertedType, typeOK := knowledgeObjectTypeFromProto(objectType)
		convertedScope, scopeOK := knowledgeSharingScopeFromProto(normalized.SharingScope)
		return typeOK && scopeOK &&
			bytes.Equal(encoded, normalized.Bytes) &&
			subtle.ConstantTimeCompare(authority.digest, normalized.Digest[:]) == 1 &&
			convertedType == authority.objectType &&
			normalized.AppID == authority.appID &&
			normalized.Name == authority.name &&
			convertedScope == authority.sharingScope
	}
	// Normalize's shape preflight runs before it clones or marshals. A canonical
	// future body must stop only at the deliberately unknown top-level oneof;
	// resource or metadata failures must not reach the wire decoder.
	if !errors.Is(normalizeErr, knowledgedefinition.ErrUnknownFields) {
		return false
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(authority.definition)
	if err != nil {
		return false
	}

	future, err := knowledgedefinition.DecodeCanonicalInactiveFutureBody(
		encoded,
		authority.digest,
		authority.state,
	)
	if err != nil {
		return false
	}
	convertedScope, scopeOK := knowledgeSharingScopeFromProto(future.SharingScope)
	return scopeOK &&
		future.AppID == authority.appID &&
		future.Name == authority.name &&
		convertedScope == authority.sharingScope
}

func knowledgeCatalogStateToProto(
	state knowledgecatalog.State,
) (opensplunk.KnowledgeObjectState, bool) {
	switch state {
	case knowledgecatalog.StateDraft:
		return opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT, true
	case knowledgecatalog.StateActive:
		return opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE, true
	case knowledgecatalog.StateDisabled:
		return opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED, true
	case knowledgecatalog.StateQuarantined:
		return opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED, true
	case knowledgecatalog.StateDeleted:
		return opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED, true
	default:
		return opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_UNSPECIFIED, false
	}
}
