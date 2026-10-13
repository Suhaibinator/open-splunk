package config

import (
	"strconv"
)

// Generation identifies one immutable configuration generation: the KMS
// release that was active when the generation was sampled. It is carried by
// views built with NewRootViewWithGeneration and survives facade narrowing, so
// any log line, metric, or error produced while handling one request or job
// can be correlated with the exact configuration in effect at that moment —
// the KMS release history (`parameter-store release show`) and the SDK's
// `kms config group` dump both key on the same activation revision.
type Generation struct {
	// Release is the release identity in "namespace/name" form.
	Release string
	// ReleaseVersion is the immutable release version.
	ReleaseVersion uint64
	// ActivationRevision is the store's monotonically increasing activation
	// counter; it distinguishes re-activations of the same release version.
	ActivationRevision uint64
	// SchemaID and SchemaVersion identify the parameter schema the release
	// was validated against.
	SchemaID      string
	SchemaVersion uint64
}

// IsZero reports whether g carries no release identity. Views produced by
// NewRootView or Static carry the zero Generation.
func (g Generation) IsZero() bool {
	return g == Generation{}
}

// String renders the generation as "namespace/name@version#revision", matching
// the KMS SDK's configstore.ReleaseIdentity.String(), or "" when zero.
func (g Generation) String() string {
	if g.IsZero() {
		return ""
	}
	return g.Release + "@" + strconv.FormatUint(g.ReleaseVersion, 10) + "#" + strconv.FormatUint(g.ActivationRevision, 10)
}

// GenerationOf returns the generation carried by a sampled view. The bool is
// false unless view is a RootView carrying a non-zero Generation.
func GenerationOf(view any) (Generation, bool) {
	root, ok := view.(RootView)
	if !ok {
		return Generation{}, false
	}
	generation := root.ConfigGeneration()
	return generation, !generation.IsZero()
}
