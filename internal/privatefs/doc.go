// Package privatefs provides the narrow filesystem primitives used to build
// owner-private, crash-durable publication transactions. It deliberately
// supports only the production Darwin and Linux targets: callers operate
// relative to pinned directory descriptors and never fall back to a
// check-then-rename sequence when the platform cannot provide no-replace
// publication.
package privatefs
