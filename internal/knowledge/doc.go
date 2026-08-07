// Package knowledge implements the versioned search-time knowledge catalog,
// resolver, immutable snapshots, and logical-plan enrichment.
//
// Runtime behavior must conform to docs/knowledge-compatibility-v0.1.md.
package knowledge

// CompatibilityVersion identifies the first bounded field-knowledge contract.
const CompatibilityVersion = "0.1"
