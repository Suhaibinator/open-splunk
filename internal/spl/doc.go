// Package spl implements the SPL lexer, parser, syntax tree, and semantic analysis.
package spl

// CompatibilityVersion is the public authored-search language identity exposed
// by every production admission, retention, export, and bootstrap surface.
// Knowledge calculated fields deliberately use a smaller, closed expression
// grammar within that same compatibility contract.
const CompatibilityVersion = "0.4"
