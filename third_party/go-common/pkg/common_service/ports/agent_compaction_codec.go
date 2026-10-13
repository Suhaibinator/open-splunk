package ports

import json "encoding/json/v2"

// AgentCompactionRules is serialized into the ChatAgentSession config-snapshot CompactionPayload
// blob, which is the single source of truth for a session's compaction rules (there is no
// normalized compaction table). The codec lives here, next to the struct, so both the writer
// (common_service/chat) and the runtime/compaction reader (common_service/ai) share ONE definition
// without an import cycle. The struct's own json tags are the durable on-disk shape (defined in
// exactly one place), so EncodeCompactionPayload/DecodeCompactionPayload and a plain json/v2 Unmarshal
// into AgentCompactionRules all agree.
//
// DURABLE-FORMAT CONTRACT: this JSON shape persists across deploys. Evolve it additively only —
// never rename or remove a json tag, only add new optional ones — and reads tolerate unknown fields
// (plain json/v2 Unmarshal; never RejectUnknownMembers) so a newer binary reads older snapshots and an
// older binary survives newer ones.

// EncodeCompactionPayload serializes compaction rules into the snapshot blob.
//
// Parameters:
//   - rules: the effective compaction rules.
//
// Returns:
//   - result1: the JSON-encoded compaction payload.
//   - err: any marshalling error.
func EncodeCompactionPayload(rules AgentCompactionRules) ([]byte, error) {
	if err := ValidateCompactionRules(rules); err != nil {
		return nil, err
	}
	return json.Marshal(rules)
}

// DecodeCompactionPayload deserializes a compaction snapshot blob into rules. Empty input yields the
// zero-value rules (no-compaction). Unknown fields are tolerated by design. Prefer this over a raw
// json/v2 Unmarshal so both lanes share one decode path.
//
// Parameters:
//   - data: the JSON-encoded compaction payload.
//
// Returns:
//   - result1: the effective compaction rules.
//   - err: any unmarshalling error.
func DecodeCompactionPayload(data []byte) (AgentCompactionRules, error) {
	if len(data) == 0 {
		return AgentCompactionRules{}, nil
	}
	var rules AgentCompactionRules
	if err := json.Unmarshal(data, &rules); err != nil {
		return AgentCompactionRules{}, err
	}
	return rules, ValidateCompactionRules(rules)
}
