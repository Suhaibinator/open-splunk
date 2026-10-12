package attachment

import (
	"path/filepath"
	"regexp"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// FilenameSanitizer sanitizes filenames for safe storage.
// It removes potentially dangerous characters and ensures the filename
func FilenameSanitizer(filename string) string {
	if filename == "" {
		return ""
	}

	// Extract base name to remove any path components
	filename = filepath.Base(filename)

	// Split name and extension
	ext := filepath.Ext(filename)
	name := strings.TrimSuffix(filename, ext)

	// Remove all non-alphanumeric characters except dots, dashes, and underscores
	// This regex keeps letters, numbers, dots, dashes, and underscores
	reg := regexp.MustCompile(`[^a-zA-Z0-9._-]+`)
	name = reg.ReplaceAllString(name, "_")

	// Remove leading/trailing dots and underscores
	name = strings.Trim(name, "._")

	// Limit the length (reserve space for extension)
	const maxNameLength = 200
	const minNameLength = 1
	if len(name) > maxNameLength {
		name = name[:maxNameLength]
	}

	if len(name) < minNameLength {
		name = "file" // Fallback name
	}

	// Reconstruct filename
	if ext != "" {
		// Sanitize extension too (remove dots except the first one)
		ext = "." + reg.ReplaceAllString(strings.TrimPrefix(ext, "."), "")
		filename = name + ext
	} else {
		filename = name
	}

	// Final safety check
	if filename == "" || filename == "." {
		filename = "file"
	}

	return filename
}

// Static helper functions for attachment operations

// CollectUniqueAttachmentIDs gathers unique attachment IDs from a slice of entities before batch attachment fetches.
//
// Parameters:
//   - entities: Source entities that may reference attachment IDs.
//   - getIDs: Callback that extracts attachment IDs from each entity.
//
// Returns:
//   - []uint64: Deduplicated attachment IDs, or `nil` when none are present.
func CollectUniqueAttachmentIDs[T any](entities []T, getIDs func(T) []uint64) []uint64 {
	uniqueIDs := make(map[uint64]bool)
	for _, entity := range entities {
		for _, id := range getIDs(entity) {
			uniqueIDs[id] = true
		}
	}

	if len(uniqueIDs) == 0 {
		return nil
	}

	// Convert to slice
	result := make([]uint64, 0, len(uniqueIDs))
	for id := range uniqueIDs {
		result = append(result, id)
	}
	return result
}

// MapAttachmentsToEntities maps fetched attachments back onto entities while preserving each entity's original attachment order.
//
// Parameters:
//   - entities: Entities that should receive attachment objects.
//   - attachmentMap: Attachment lookup keyed by attachment ID.
//   - getIDs: Callback that extracts ordered attachment IDs from an entity.
//   - setAttachments: Callback that stores resolved attachments back onto an entity.
func MapAttachmentsToEntities[T any](
	entities []T,
	attachmentMap map[uint64]*proto_models.UploadedAttachment,
	getIDs func(T) []uint64,
	setAttachments func(T, []*proto_models.UploadedAttachment),
) {
	for _, entity := range entities {
		entityIDs := getIDs(entity)
		if len(entityIDs) == 0 {
			continue
		}

		// Build ordered attachment list
		attachments := make([]*proto_models.UploadedAttachment, 0, len(entityIDs))
		for _, id := range entityIDs {
			if attachment, found := attachmentMap[id]; found {
				attachments = append(attachments, attachment)
			}
		}
		setAttachments(entity, attachments)
	}
}
