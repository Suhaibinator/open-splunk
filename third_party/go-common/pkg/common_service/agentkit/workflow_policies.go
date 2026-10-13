package agentkit

import (
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"fmt"
)

// ReadProgressGuard is optional durable admission accounting, not a replacement
// for runtime generation/tool/time budgets. Persist it with the workflow state.
// Callers classify new evidence and meaningful progress; receipt replay must not
// call Admit or Record. Ownership must be validated before either operation.
type ReadProgressGuard struct {
	ReadCalls     uint32 `json:"read_calls_since_progress"`
	RepeatedReads uint32 `json:"repeated_reads_since_progress"`
	BlockedReads  uint32 `json:"blocked_reads_since_progress"`
	Exhausted     bool   `json:"exhausted"`
}

// ReadProgressLimits are application policy. Zero is an immediate limit.
type ReadProgressLimits struct {
	Reads         uint32
	RepeatedReads uint32
	BlockedReads  uint32
}

// Admit counts a refused invocation. New evidence may bypass progress limits;
// callers must bound that exception, for example by an immutable source manifest.
func (g *ReadProgressGuard) Admit(newEvidence, repeatedRead bool, limits ReadProgressLimits) bool {
	if newEvidence {
		return true
	}
	if !g.Exhausted && (!repeatedRead || g.RepeatedReads < limits.RepeatedReads) && g.ReadCalls < limits.Reads {
		return true
	}
	g.BlockedReads = incrementProgressCounter(g.BlockedReads)
	g.Exhausted = g.Exhausted || g.BlockedReads >= limits.BlockedReads
	return false
}

// Record counts an executed read; repeated includes failed evidence reads when
// the application's policy treats them as making no progress.
func (g *ReadProgressGuard) Record(repeated bool) {
	g.ReadCalls = incrementProgressCounter(g.ReadCalls)
	if repeated {
		g.RepeatedReads = incrementProgressCounter(g.RepeatedReads)
	}
}

func (g *ReadProgressGuard) Reset() { *g = ReadProgressGuard{} }

func incrementProgressCounter(n uint32) uint32 {
	if n == ^uint32(0) {
		return n
	}
	return n + 1
}

// DraftRevisionConflict reports optimistic revision mismatch. Check and apply
// the mutation within the same application transaction to prevent lost updates.
type DraftRevisionConflict struct{ Expected, Actual uint32 }

func (e *DraftRevisionConflict) Error() string {
	return fmt.Sprintf("draft revision conflict: expected %d, current %d", e.Expected, e.Actual)
}
func CheckDraftRevision(expected, actual uint32) error {
	if expected == 0 || expected != actual {
		return &DraftRevisionConflict{Expected: expected, Actual: actual}
	}
	return nil
}

// MergeDraftPatch applies object-root JSON merge patch semantics, then invokes
// application validation (identity, allowed repairs, schema). Arrays replace;
// null removes a field. Unpatched values keep their encoding, so numbers retain
// their precision; merged objects are written with sorted keys. Duplicate member
// names are rejected. Inputs are not mutated.
func MergeDraftPatch(original, patch jsontext.Value, validate func(jsontext.Value) error) (jsontext.Value, error) {
	target, err := decodeDraftObject(original)
	if err != nil {
		return nil, fmt.Errorf("stored draft is not a JSON object")
	}
	update, err := decodeDraftObject(patch)
	if err != nil {
		return nil, fmt.Errorf("patch must be a JSON object")
	}
	result, err := mergeDraftObject(target, update)
	if err != nil {
		return nil, err
	}
	if validate != nil {
		if err := validate(result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func decodeDraftObject(raw jsontext.Value) (map[string]jsontext.Value, error) {
	var object map[string]jsontext.Value
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, fmt.Errorf("object required")
	}
	return object, nil
}

// mergeDraftObject patches target in place and encodes it. A patch object
// replaces a non-object target value, as RFC 7386 specifies.
func mergeDraftObject(target, patch map[string]jsontext.Value) (jsontext.Value, error) {
	for key, value := range patch {
		switch value.Kind() {
		case 'n':
			delete(target, key)
		case '{':
			prior, err := decodeDraftObject(target[key])
			if err != nil {
				prior = make(map[string]jsontext.Value)
			}
			nested, err := decodeDraftObject(value)
			if err != nil {
				return nil, err
			}
			if target[key], err = mergeDraftObject(prior, nested); err != nil {
				return nil, err
			}
		default:
			target[key] = value
		}
	}
	return json.Marshal(target, json.Deterministic(true))
}
