package common_util

import (
	"fmt"
	"strings"
)

// inferenceModelUpdateMaskPaths is the single source of truth for which
// inference-model fields an update mask may target.
var inferenceModelUpdateMaskPaths = map[string]struct{}{
	"provider_id":       {},
	"provider_model_id": {},
	"display_name":      {},
	"active":            {},
}

// InferenceModelUpdateMask is the result of sanitizing an inference-model update mask.
type InferenceModelUpdateMask struct {
	Paths  []string // sanitized, deduped, order-preserving
	masked map[string]struct{}
}

// Has reports whether the sanitized mask targets the given field.
func (m InferenceModelUpdateMask) Has(path string) bool {
	_, ok := m.masked[path]
	return ok
}

// SanitizeInferenceModelUpdateMask trims each path, rejects empty/dotted/wildcard
// paths and paths outside the allowlist, dedups (order-preserving), and enforces that
// provider and provider_model_id are masked together. It does NOT validate field
// values — callers do that against their own request/patch type.
func SanitizeInferenceModelUpdateMask(paths []string) (InferenceModelUpdateMask, error) {
	if len(paths) == 0 {
		return InferenceModelUpdateMask{}, fmt.Errorf("update mask is required")
	}
	sanitized := make([]string, 0, len(paths))
	masked := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" || strings.Contains(path, ".") || strings.Contains(path, "*") {
			return InferenceModelUpdateMask{}, fmt.Errorf("invalid update mask path %q", path)
		}
		if _, ok := inferenceModelUpdateMaskPaths[path]; !ok {
			return InferenceModelUpdateMask{}, fmt.Errorf("unsupported update mask path %q", path)
		}
		if _, seen := masked[path]; seen {
			continue
		}
		masked[path] = struct{}{}
		sanitized = append(sanitized, path)
	}
	_, providerMasked := masked["provider_id"]
	_, modelIDMasked := masked["provider_model_id"]
	if providerMasked != modelIDMasked {
		return InferenceModelUpdateMask{}, fmt.Errorf("provider and provider model id must be updated together")
	}
	return InferenceModelUpdateMask{Paths: sanitized, masked: masked}, nil
}
