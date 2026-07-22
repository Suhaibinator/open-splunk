package collector

import (
	"errors"
	"fmt"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

// errNotImplemented is returned by contract stubs that other files in this
// package still leave unimplemented (e.g. daemon.go). The processor chain below
// is fully implemented and never returns it.
var errNotImplemented = errors.New("collector: not implemented")

// Processor transforms or filters one decoded event. It runs after decoding and
// before the event is appended to the durable queue.
//
// Field semantics shared by every Processor in this package:
//
//   - Processors act ONLY on dynamic fields (event.Fields, a *TypedObject). They
//     never read or mutate canonical metadata (event_id, index_name, event_time,
//     collected_at, host, source, sourcetype, service, severity, level, message,
//     raw, trace_id, span_id, origin). Those carry trusted, security-relevant
//     provenance set by the decoder and must survive the chain untouched.
//   - Dynamic field-name matching is EXACT and case-sensitive. "Token" does not
//     match "token"; "user.id" is a single literal key, not a path expression.
//
// Clone-on-write: a Processor that changes anything returns a deep clone
// (proto.Clone) of the event with the change applied, leaving the caller's
// pointer fully intact. A Processor that changes nothing returns the SAME pointer
// it was given. This guarantees the WAL and dead-letter paths never observe a
// half-processed event and lets callers cheaply detect no-ops by pointer
// identity.
//
// Process returns the (possibly cloned) event to keep it, or (nil, nil) to drop
// it. A returned error is a configuration/logic fault, not a per-event
// rejection, and aborts the pipeline for that event. The processors below never
// return an error from Process; errors surface only at construction time.
type Processor interface {
	Process(event *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error)
	// Name identifies the processor in diagnostics.
	Name() string
}

// NewAllowProcessor keeps only the named TOP-LEVEL dynamic fields and drops all
// others. Nested objects under a kept field are kept whole (no descent). Matching
// is exact and case-sensitive. An empty field list is a configuration error: an
// allow-list that keeps nothing would silently strip every dynamic field, which
// is almost always a misconfiguration rather than intent.
func NewAllowProcessor(fields []string) (Processor, error) {
	if len(fields) == 0 {
		return nil, errors.New("collector: allow processor requires at least one field")
	}
	keep := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		keep[f] = struct{}{}
	}
	return &allowProcessor{keep: keep}, nil
}

// NewDenyProcessor removes the named TOP-LEVEL dynamic fields. Matching is exact
// and case-sensitive. Nested fields are not inspected. Unlike the allow
// processor, an empty deny list is permitted and yields an identity processor
// (deny-nothing is a harmless no-op, whereas allow-nothing is destructive).
func NewDenyProcessor(fields []string) (Processor, error) {
	deny := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		deny[f] = struct{}{}
	}
	return &denyProcessor{deny: deny}, nil
}

// NewRenameProcessor renames TOP-LEVEL dynamic field from to to. Matching is
// exact and case-sensitive. It is a configuration error for from or to to be
// empty, or for from to equal to. Renaming a field that is absent is a no-op
// (the input pointer is returned unchanged). If a field named to already exists,
// the renamed field REPLACES it: the pre-existing to is removed and the from
// field is renamed in place at from's original position. Nested fields are not
// inspected.
func NewRenameProcessor(from, to string) (Processor, error) {
	if from == "" || to == "" {
		return nil, errors.New("collector: rename processor requires non-empty from and to")
	}
	if from == to {
		return nil, fmt.Errorf("collector: rename processor from and to are identical (%q)", from)
	}
	return &renameProcessor{from: from, to: to}, nil
}

// NewRedactProcessor replaces the ENTIRE value of every field whose name matches
// one of fields with a string TypedValue holding replacement. Unlike the other
// processors, redaction matches RECURSIVELY at every nesting depth: inside nested
// objects, and inside objects that appear as elements of lists. Matching is exact
// and case-sensitive on the field name. When a field matches, its whole value
// (scalar, object, or list) is replaced and its former contents are not descended
// into. A field list must be non-empty and replacement must be non-empty.
//
// Scope: redaction here covers STRUCTURED dynamic fields only. It deliberately
// does NOT scrub the canonical raw body (event.Raw) or event.Message — scrubbing
// unstructured raw payloads is the decoder/ingest layer's responsibility, and the
// server enforces its own mandatory redaction. Collector-side redaction is
// defense in depth over the structured fields, mirroring the server's policy; do
// not rely on it to sanitize free-form text.
func NewRedactProcessor(fields []string, replacement string) (Processor, error) {
	if len(fields) == 0 {
		return nil, errors.New("collector: redact processor requires at least one field")
	}
	if replacement == "" {
		return nil, errors.New("collector: redact processor requires a non-empty replacement")
	}
	targets := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		targets[f] = struct{}{}
	}
	return &redactProcessor{targets: targets, replacement: replacement}, nil
}

// allowProcessor keeps only the listed top-level dynamic fields.
type allowProcessor struct{ keep map[string]struct{} }

func (p *allowProcessor) Name() string { return "allow" }

func (p *allowProcessor) Process(event *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
	if event == nil || event.Fields == nil || len(event.Fields.Fields) == 0 {
		return event, nil
	}
	// Only clone if at least one field would be dropped.
	drop := false
	for _, f := range event.Fields.Fields {
		if _, ok := p.keep[f.Name]; !ok {
			drop = true
			break
		}
	}
	if !drop {
		return event, nil
	}
	clone := proto.Clone(event).(*opensplunkv1.LogEvent)
	kept := make([]*opensplunkv1.TypedObjectField, 0, len(clone.Fields.Fields))
	for _, f := range clone.Fields.Fields {
		if _, ok := p.keep[f.Name]; ok {
			kept = append(kept, f)
		}
	}
	clone.Fields.Fields = kept
	return clone, nil
}

// denyProcessor removes the listed top-level dynamic fields.
type denyProcessor struct{ deny map[string]struct{} }

func (p *denyProcessor) Name() string { return "deny" }

func (p *denyProcessor) Process(event *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
	if event == nil || event.Fields == nil || len(event.Fields.Fields) == 0 {
		return event, nil
	}
	hit := false
	for _, f := range event.Fields.Fields {
		if _, ok := p.deny[f.Name]; ok {
			hit = true
			break
		}
	}
	if !hit {
		return event, nil
	}
	clone := proto.Clone(event).(*opensplunkv1.LogEvent)
	kept := make([]*opensplunkv1.TypedObjectField, 0, len(clone.Fields.Fields))
	for _, f := range clone.Fields.Fields {
		if _, ok := p.deny[f.Name]; ok {
			continue
		}
		kept = append(kept, f)
	}
	clone.Fields.Fields = kept
	return clone, nil
}

// renameProcessor renames one top-level dynamic field.
type renameProcessor struct{ from, to string }

func (p *renameProcessor) Name() string { return "rename" }

func (p *renameProcessor) Process(event *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
	if event == nil || event.Fields == nil || len(event.Fields.Fields) == 0 {
		return event, nil
	}
	fromIdx := -1
	for i, f := range event.Fields.Fields {
		if f.Name == p.from {
			fromIdx = i
			break
		}
	}
	if fromIdx == -1 {
		// Nothing to rename.
		return event, nil
	}
	clone := proto.Clone(event).(*opensplunkv1.LogEvent)
	src := clone.Fields.Fields
	kept := make([]*opensplunkv1.TypedObjectField, 0, len(src))
	for i, f := range src {
		if i == fromIdx {
			// Rename in place at from's original position.
			f.Name = p.to
			kept = append(kept, f)
			continue
		}
		if f.Name == p.to {
			// A pre-existing field named to is replaced by the renamed field.
			continue
		}
		kept = append(kept, f)
	}
	clone.Fields.Fields = kept
	return clone, nil
}

// redactProcessor replaces matched field values recursively.
type redactProcessor struct {
	targets     map[string]struct{}
	replacement string
}

func (p *redactProcessor) Name() string { return "redact" }

func (p *redactProcessor) Process(event *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
	if event == nil || event.Fields == nil || len(event.Fields.Fields) == 0 {
		return event, nil
	}
	if !p.objectNeedsRedact(event.Fields) {
		return event, nil
	}
	clone := proto.Clone(event).(*opensplunkv1.LogEvent)
	p.redactObject(clone.Fields)
	return clone, nil
}

// objectNeedsRedact reports whether any field in obj (at any depth) matches a
// redaction target. Read-only.
func (p *redactProcessor) objectNeedsRedact(obj *opensplunkv1.TypedObject) bool {
	if obj == nil {
		return false
	}
	for _, f := range obj.Fields {
		if _, ok := p.targets[f.Name]; ok {
			return true
		}
		if p.valueNeedsRedact(f.Value) {
			return true
		}
	}
	return false
}

func (p *redactProcessor) valueNeedsRedact(v *opensplunkv1.TypedValue) bool {
	if v == nil {
		return false
	}
	switch k := v.Kind.(type) {
	case *opensplunkv1.TypedValue_ObjectValue:
		return p.objectNeedsRedact(k.ObjectValue)
	case *opensplunkv1.TypedValue_ListValue:
		if k.ListValue == nil {
			return false
		}
		for _, item := range k.ListValue.Values {
			if p.valueNeedsRedact(item) {
				return true
			}
		}
	}
	return false
}

// redactObject mutates obj in place, replacing matched field values with the
// replacement string. It is only ever called on a freshly cloned event.
func (p *redactProcessor) redactObject(obj *opensplunkv1.TypedObject) {
	if obj == nil {
		return
	}
	for _, f := range obj.Fields {
		if _, ok := p.targets[f.Name]; ok {
			f.Value = &opensplunkv1.TypedValue{
				Kind: &opensplunkv1.TypedValue_StringValue{StringValue: p.replacement},
			}
			continue
		}
		p.redactValue(f.Value)
	}
}

func (p *redactProcessor) redactValue(v *opensplunkv1.TypedValue) {
	if v == nil {
		return
	}
	switch k := v.Kind.(type) {
	case *opensplunkv1.TypedValue_ObjectValue:
		p.redactObject(k.ObjectValue)
	case *opensplunkv1.TypedValue_ListValue:
		if k.ListValue == nil {
			return
		}
		for _, item := range k.ListValue.Values {
			p.redactValue(item)
		}
	}
}

// Pipeline is an ordered processor chain applied to each event. A nil or empty
// Pipeline passes events through unchanged.
type Pipeline struct {
	processors []Processor
}

// NewPipeline returns a Pipeline that applies processors in order.
func NewPipeline(processors ...Processor) *Pipeline {
	return &Pipeline{processors: processors}
}

// Process runs the chain over event in order. It stops and returns (nil, nil) as
// soon as a processor drops the event, and stops and returns (nil, err) as soon
// as a processor errors. A nil receiver or empty chain returns event unchanged.
// Each stage feeds its output to the next; the caller's original event is never
// mutated, because the first mutating stage clones it and every later mutating
// stage clones that intermediate (pipeline-owned) event again.
func (p *Pipeline) Process(event *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
	if p == nil {
		return event, nil
	}
	for _, proc := range p.processors {
		out, err := proc.Process(event)
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, nil
		}
		event = out
	}
	return event, nil
}

var (
	_ Processor = (*allowProcessor)(nil)
	_ Processor = (*denyProcessor)(nil)
	_ Processor = (*renameProcessor)(nil)
	_ Processor = (*redactProcessor)(nil)
)
