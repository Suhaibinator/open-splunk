package searchjobproto

import (
	"slices"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// Diagnostics projects detached, user-safe SPL diagnostics for every search
// transport. Keeping this conversion shared prevents HTTP and WebSocket
// failures from disagreeing about source coordinates.
func Diagnostics(diagnostics []searchjobs.Diagnostic) []*opensplunkv1.Diagnostic {
	result := make([]*opensplunkv1.Diagnostic, len(diagnostics))
	for index, diagnostic := range diagnostics {
		converted := &opensplunkv1.Diagnostic{
			Code:        diagnostic.Code,
			Severity:    opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
			Message:     diagnostic.Message,
			Suggestions: slices.Clone(diagnostic.Suggestions),
		}
		if diagnostic.ValidSourceRange() {
			converted.SourceRange = &opensplunkv1.SourceRange{
				// #nosec G115 -- ValidSourceRange proves offsets non-negative
				// and protobuf-representable line and column values.
				Start: &opensplunkv1.SourcePosition{
					ByteOffset: uint64(diagnostic.ByteOffset),
					Line:       uint32(diagnostic.Line),
					Column:     uint32(diagnostic.Column),
				},
				// #nosec G115 -- ValidSourceRange proves offsets non-negative
				// and protobuf-representable line and column values.
				End: &opensplunkv1.SourcePosition{
					ByteOffset: uint64(diagnostic.EndByteOffset),
					Line:       uint32(diagnostic.EndLine),
					Column:     uint32(diagnostic.EndColumn),
				},
			}
		}
		result[index] = converted
	}
	return result
}
