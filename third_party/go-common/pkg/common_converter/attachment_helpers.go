package common_converter

import (
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

// publicAssetObjectName resolves the storage object name an attachment may be
// exposed under as a public asset path. It returns a name only for a row that
// IsPubliclyAvailable, which requires public visibility, so the object is known
// to live in the anonymously readable public bucket. Every other row — nil, not
// preloaded, internal, not yet vetted, deleted, or stored privately — yields an
// empty string, because a private or temporary object is only reachable through
// a backend-issued presigned URL (see CommonServiceHandler.GetAttachmentURL).
func publicAssetObjectName(a *common_persistence_models.Attachment) string {
	if !a.IsPubliclyAvailable() {
		return ""
	}
	return a.FinalObjectName
}

// publicAvatarObjectName resolves the public asset path for an avatar. It applies
// the profile-picture object-type and MIME policy on top of the public-availability
// checks, so a row of some other object type can never be rendered as an avatar.
func publicAvatarObjectName(a *common_persistence_models.Attachment) string {
	if !a.IsValidPublicAvatar() {
		return ""
	}
	return a.FinalObjectName
}
