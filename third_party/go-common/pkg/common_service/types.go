package common_service

import (
	"time"

	attachmentpkg "github.com/Suhaibinator/go-common/pkg/common_service/attachment"
)

// AttachmentRequest describes one file of a CreateEntityAttachments batch.
type AttachmentRequest = attachmentpkg.UploadRequest

// AttachmentResponse defines the common_service package data shape used in the backend request and persistence flow.
// It is used to carry state between handlers, service orchestration, and storage integrations in this package.
type AttachmentResponse struct {
	ID       uint64
	URL      string
	FormData map[string]string
	Filename string
	Expiry   time.Time
}
