package collector

import (
	"errors"
	"fmt"
	"slices"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"google.golang.org/protobuf/proto"
)

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
	Process(event *opensplunk.LogEvent) (*opensplunk.LogEvent, error)
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
	if eventfields.IsCollectorReservedRoot(to) {
		return nil, fmt.Errorf("collector: rename processor destination %q is reserved canonical metadata", to)
	}
	return &renameProcessor{from: from, to: to}, nil
}

// pipelineMutator is implemented by the built-in clone-on-write processors.
// Pipeline uses the split read/mutate contract to make one lazy deep clone for
// an entire chain, while direct Processor callers retain the public
// clone-on-write guarantee.
type pipelineMutator interface {
	needsMutation(*opensplunk.LogEvent) bool
	mutate(*opensplunk.LogEvent)
}

// processCloneOnWrite applies one built-in mutator while preserving the public
// Processor identity contract: no-ops return event itself, and mutations happen
// only after taking an independent deep clone.
func processCloneOnWrite(
	event *opensplunk.LogEvent,
	mutator pipelineMutator,
) (*opensplunk.LogEvent, error) {
	if !mutator.needsMutation(event) {
		return event, nil
	}
	clone := proto.Clone(event).(*opensplunk.LogEvent)
	mutator.mutate(clone)
	return clone, nil
}

// NewRedactProcessor replaces the ENTIRE value of every field whose name matches
// one of fields with a string TypedValue holding replacement. Unlike the other
// processors, redaction matches RECURSIVELY at every nesting depth: inside nested
// objects, and inside objects that appear as elements of lists. Matching is exact
// and case-sensitive on the field name. When a field matches, its whole value
// (scalar, object, or list) is replaced and its former contents are not descended
// into. A field list must be non-empty and replacement must be non-empty.
//
// Scope: this ordered processor covers STRUCTURED dynamic fields only. The
// daemon compiles the same configured fields/replacement into a durability
// sanitizer before this chain. It scrubs raw/message text and follows the
// actual ordered lineage of top-level renames before WAL append.
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

func (p *allowProcessor) Process(event *opensplunk.LogEvent) (*opensplunk.LogEvent, error) {
	return processCloneOnWrite(event, p)
}

func (p *allowProcessor) needsMutation(event *opensplunk.LogEvent) bool {
	if event == nil || event.Fields == nil || len(event.Fields.Fields) == 0 {
		return false
	}
	for _, f := range event.Fields.Fields {
		if _, ok := p.keep[f.Name]; !ok {
			return true
		}
	}
	return false
}

func (p *allowProcessor) mutate(event *opensplunk.LogEvent) {
	fields := event.Fields.Fields
	kept := fields[:0]
	for _, f := range fields {
		if _, ok := p.keep[f.Name]; ok {
			kept = append(kept, f)
		}
	}
	clear(fields[len(kept):])
	event.Fields.Fields = kept
}

// denyProcessor removes the listed top-level dynamic fields.
type denyProcessor struct{ deny map[string]struct{} }

func (p *denyProcessor) Process(event *opensplunk.LogEvent) (*opensplunk.LogEvent, error) {
	return processCloneOnWrite(event, p)
}

func (p *denyProcessor) needsMutation(event *opensplunk.LogEvent) bool {
	if event == nil || event.Fields == nil || len(event.Fields.Fields) == 0 {
		return false
	}
	for _, f := range event.Fields.Fields {
		if _, ok := p.deny[f.Name]; ok {
			return true
		}
	}
	return false
}

func (p *denyProcessor) mutate(event *opensplunk.LogEvent) {
	fields := event.Fields.Fields
	kept := fields[:0]
	for _, f := range fields {
		if _, ok := p.deny[f.Name]; ok {
			continue
		}
		kept = append(kept, f)
	}
	clear(fields[len(kept):])
	event.Fields.Fields = kept
}

// renameProcessor renames one top-level dynamic field.
type renameProcessor struct{ from, to string }

func (p *renameProcessor) Process(event *opensplunk.LogEvent) (*opensplunk.LogEvent, error) {
	return processCloneOnWrite(event, p)
}

func (p *renameProcessor) needsMutation(event *opensplunk.LogEvent) bool {
	if event == nil || event.Fields == nil || len(event.Fields.Fields) == 0 {
		return false
	}
	for _, f := range event.Fields.Fields {
		if f.Name == p.from {
			return true
		}
	}
	return false
}

func (p *renameProcessor) mutate(event *opensplunk.LogEvent) {
	fromIdx := -1
	for i, f := range event.Fields.Fields {
		if f.Name == p.from {
			fromIdx = i
			break
		}
	}
	src := event.Fields.Fields
	kept := src[:0]
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
	clear(src[len(kept):])
	event.Fields.Fields = kept
}

// redactProcessor replaces matched field values recursively.
type redactProcessor struct {
	targets     map[string]struct{}
	replacement string
}

func (p *redactProcessor) Process(event *opensplunk.LogEvent) (*opensplunk.LogEvent, error) {
	return processCloneOnWrite(event, p)
}

func (p *redactProcessor) needsMutation(event *opensplunk.LogEvent) bool {
	return event != nil && event.Fields != nil && len(event.Fields.Fields) != 0 &&
		p.objectNeedsRedact(event.Fields)
}

func (p *redactProcessor) mutate(event *opensplunk.LogEvent) {
	p.redactObject(event.Fields)
}

// objectNeedsRedact reports whether any field in obj (at any depth) matches a
// redaction target. Read-only.
func (p *redactProcessor) objectNeedsRedact(obj *opensplunk.TypedObject) bool {
	if obj == nil {
		return false
	}
	for _, f := range obj.Fields {
		if _, ok := p.targets[f.Name]; ok {
			if current, stringValue := f.GetValue().GetKind().(*opensplunk.TypedValue_StringValue); stringValue &&
				current.StringValue == p.replacement {
				continue
			}
			return true
		}
		if p.valueNeedsRedact(f.Value) {
			return true
		}
	}
	return false
}

func (p *redactProcessor) valueNeedsRedact(v *opensplunk.TypedValue) bool {
	if v == nil {
		return false
	}
	switch k := v.Kind.(type) {
	case *opensplunk.TypedValue_ObjectValue:
		return p.objectNeedsRedact(k.ObjectValue)
	case *opensplunk.TypedValue_ListValue:
		if k.ListValue == nil {
			return false
		}
		if slices.ContainsFunc(k.ListValue.Values, p.valueNeedsRedact) {
			return true
		}
	}
	return false
}

// redactObject mutates obj in place, replacing matched field values with the
// replacement string. It is only ever called on a freshly cloned event.
func (p *redactProcessor) redactObject(obj *opensplunk.TypedObject) {
	if obj == nil {
		return
	}
	for _, f := range obj.Fields {
		if _, ok := p.targets[f.Name]; ok {
			f.Value = &opensplunk.TypedValue{
				Kind: &opensplunk.TypedValue_StringValue{StringValue: p.replacement},
			}
			continue
		}
		p.redactValue(f.Value)
	}
}

func (p *redactProcessor) redactValue(v *opensplunk.TypedValue) {
	if v == nil {
		return
	}
	switch k := v.Kind.(type) {
	case *opensplunk.TypedValue_ObjectValue:
		p.redactObject(k.ObjectValue)
	case *opensplunk.TypedValue_ListValue:
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
// Each stage feeds its output to the next. The caller's original event is never
// mutated. Built-in stages share one lazy deep clone across the whole chain;
// custom Processor implementations retain their public clone-on-write contract.
func (p *Pipeline) Process(event *opensplunk.LogEvent) (*opensplunk.LogEvent, error) {
	if p == nil {
		return event, nil
	}
	owned := false
	for _, proc := range p.processors {
		if mutator, ok := proc.(pipelineMutator); ok {
			if !mutator.needsMutation(event) {
				continue
			}
			if !owned {
				event = proto.Clone(event).(*opensplunk.LogEvent)
				owned = true
			}
			mutator.mutate(event)
			continue
		}
		out, err := proc.Process(event)
		if err != nil {
			return nil, err
		}
		if out == nil {
			return nil, nil
		}
		if out != event {
			owned = true
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
