package attachment

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// ResolveToolMediaDisplayReference returns a browser-consumable source for one persisted tool
// result media part after validating its owner and run/call/part binding. URL-capable stores are
// preferred so chat reads do not inline large blobs; the bounded byte resolver remains a safe
// fallback for stores without URL support.
func ResolveToolMediaDisplayReference(
	ctx context.Context,
	store ports.InternalAttachmentBoundedStore,
	attachmentID, ownerUserID, agentRunID uint64,
	toolCallID string,
	partIndex int,
	contentType, filename string,
	maxBytes uint64,
) (*ports.ResolvedToolMedia, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: internal attachment store is not configured", ErrInternalAttachmentAccess)
	}
	if urlStore, ok := store.(ports.InternalAttachmentURLStore); ok {
		resolvedURL, ref, err := urlStore.ResolveInternalURL(ctx, attachmentID, maxBytes)
		if err == nil {
			if err := validateToolMediaRef(ref, ownerUserID, ToolMediaEntityID(agentRunID, toolCallID, partIndex), contentType, filename); err != nil {
				return nil, err
			}
			parsed, parseErr := url.Parse(strings.TrimSpace(resolvedURL))
			if parseErr == nil && (strings.EqualFold(parsed.Scheme, "http") || strings.EqualFold(parsed.Scheme, "https")) && parsed.Host != "" {
				return &ports.ResolvedToolMedia{Source: resolvedURL, SizeBytes: ref.SizeBytes}, nil
			}
		}
	}
	return ResolveToolMediaReference(ctx, store, attachmentID, ownerUserID, agentRunID, toolCallID, partIndex, contentType, filename, maxBytes)
}
