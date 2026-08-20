package searchjobproto

import (
	"slices"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// Diagnostics projects detached, user-safe SPL diagnostics for every search
// transport. Keeping this conversion shared prevents HTTP and WebSocket
// failures from disagreeing about source coordinates.
func Diagnostics(diagnostics []searchjobs.Diagnostic) []*opensplunk.Diagnostic {
	result := make([]*opensplunk.Diagnostic, len(diagnostics))
	for index, diagnostic := range diagnostics {
		converted := &opensplunk.Diagnostic{
			Code:        diagnostic.Code,
			Severity:    opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
			Message:     diagnostic.Message,
			Suggestions: slices.Clone(diagnostic.Suggestions),
		}
		converted.SourceRange = SourceRange(diagnostic)
		result[index] = converted
	}
	return result
}

// SourceRange projects a diagnostic's validated source coordinates, or nil
// when the diagnostic carries no representable range.
func SourceRange(diagnostic searchjobs.Diagnostic) *opensplunk.SourceRange {
	if !diagnostic.ValidSourceRange() {
		return nil
	}
	return &opensplunk.SourceRange{
		// #nosec G115 -- ValidSourceRange proves offsets non-negative
		// and protobuf-representable line and column values.
		Start: &opensplunk.SourcePosition{
			ByteOffset: uint64(diagnostic.ByteOffset),
			Line:       uint32(diagnostic.Line),
			Column:     uint32(diagnostic.Column),
		},
		// #nosec G115 -- ValidSourceRange proves offsets non-negative
		// and protobuf-representable line and column values.
		End: &opensplunk.SourcePosition{
			ByteOffset: uint64(diagnostic.EndByteOffset),
			Line:       uint32(diagnostic.EndLine),
			Column:     uint32(diagnostic.EndColumn),
		},
	}
}
