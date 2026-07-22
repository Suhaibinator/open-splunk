package collector

import (
	"errors"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

// errNotImplemented is returned by contract stubs during the skeleton phase.
var errNotImplemented = errors.New("collector: not implemented")

// Processor transforms or filters one decoded event. It runs after decoding and
// before the event is appended to the durable queue. A Processor must not mutate
// canonical security metadata (index_name, host, source, sourcetype); it acts on
// dynamic fields and message/level content only.
//
// Process returns the (possibly modified) event to keep it, or (nil, nil) to
// drop it. A returned error is a configuration/logic fault, not a per-event
// rejection, and aborts the pipeline for that event.
type Processor interface {
	Process(event *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error)
	// Name identifies the processor in diagnostics.
	Name() string
}

// NewAllowProcessor keeps only the named dynamic fields, dropping all others.
func NewAllowProcessor(fields []string) (Processor, error) {
	return &stubProcessor{name: "allow"}, nil
}

// NewDenyProcessor drops the named dynamic fields.
func NewDenyProcessor(fields []string) (Processor, error) {
	return &stubProcessor{name: "deny"}, nil
}

// NewRenameProcessor renames dynamic field from to to.
func NewRenameProcessor(from, to string) (Processor, error) {
	return &stubProcessor{name: "rename"}, nil
}

// NewRedactProcessor replaces the value of each named field (recursively) with
// replacement. Redaction is not bypassable by payload contents and never emits
// the original secret. This is the collector-side defense-in-depth counterpart
// to the server's mandatory redaction.
func NewRedactProcessor(fields []string, replacement string) (Processor, error) {
	return &stubProcessor{name: "redact"}, nil
}

// stubProcessor is a placeholder Processor for the contracts phase.
type stubProcessor struct{ name string }

func (p *stubProcessor) Process(event *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
	return nil, errNotImplemented
}

func (p *stubProcessor) Name() string { return p.name }

// Pipeline is an ordered processor chain applied to each event. A nil or empty
// Pipeline passes events through unchanged.
type Pipeline struct {
	processors []Processor
}

// NewPipeline returns a Pipeline that applies processors in order.
func NewPipeline(processors ...Processor) *Pipeline {
	return &Pipeline{processors: processors}
}

// Process runs the chain over event, returning the surviving event or (nil, nil)
// if a processor dropped it.
func (p *Pipeline) Process(event *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
	return nil, errNotImplemented
}

var _ Processor = (*stubProcessor)(nil)
