// Package partmetadata defines the metadata go-common stamps on Genkit parts:
// provider-neutral media provenance and Vertex-specific function-call identity.
package partmetadata

const trustedHTTPSourceKey = "go-common.trusted_http_media_source"

// trustedHTTPSourceMarker is intentionally unexported so decoded metadata cannot forge trust.
type trustedHTTPSourceMarker struct{}

func MarkTrustedHTTPSource(metadata map[string]any) map[string]any {
	if metadata == nil {
		metadata = make(map[string]any)
	}
	metadata[trustedHTTPSourceKey] = trustedHTTPSourceMarker{}
	return metadata
}

func IsTrustedHTTPSource(metadata map[string]any) bool {
	if metadata == nil {
		return false
	}
	_, ok := metadata[trustedHTTPSourceKey].(trustedHTTPSourceMarker)
	return ok
}
