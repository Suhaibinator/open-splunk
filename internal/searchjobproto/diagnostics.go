package searchjobproto

import (
	"slices"

	"fortio.org/safecast"

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
		Start: &opensplunk.SourcePosition{
			ByteOffset: safecast.MustConv[uint64](diagnostic.ByteOffset),
			Line:       safecast.MustConv[uint32](diagnostic.Line),
			Column:     safecast.MustConv[uint32](diagnostic.Column),
		},
		End: &opensplunk.SourcePosition{
			ByteOffset: safecast.MustConv[uint64](diagnostic.EndByteOffset),
			Line:       safecast.MustConv[uint32](diagnostic.EndLine),
			Column:     safecast.MustConv[uint32](diagnostic.EndColumn),
		},
	}
}
