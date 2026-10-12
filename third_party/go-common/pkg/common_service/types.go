package common_service

import "time"

// AttachmentRequest defines the common_service package data shape used in the backend request and persistence flow.
// It is used to carry state between handlers, service orchestration, and storage integrations in this package.
type AttachmentRequest struct {
	Filename    string
	ContentType string
	// ContentHash is the hash the client declared for the file; it is stored unverified as the
	// upload's DeclaredFileHash.
	ContentHash string
}

// AttachmentResponse defines the common_service package data shape used in the backend request and persistence flow.
// It is used to carry state between handlers, service orchestration, and storage integrations in this package.
type AttachmentResponse struct {
	ID       uint64
	URL      string
	FormData map[string]string
	Filename string
	Expiry   time.Time
}
