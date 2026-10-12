// Package controlplane is the go-common execution kernel for the AI control plane.
// It owns the route-driven attempt loop for direct model calls and go-common agent
// calls: retry classification, jittered exponential backoff, process-local
// request/token rate-limit buckets (with negative-balance token debt), and the
// attempt-lifecycle observer seam used by the recorder and metrics bridge.
//
// # Execution contract
//
// ControlPlane.ResolveRoute maps a numeric operation to an immutable ResolvedRoute via
// the installed RouteResolver and validates its policies. The caller resolves ONCE —
// per drive on the agent path, per call on the direct path — and executes against that
// immutable route: ControlPlane.Execute runs a buffered generation and
// ControlPlane.ExecuteStream runs a streaming generation, both taking the resolved
// route on the ExecuteRequest. Both mint an invocation ID up front and return it on
// success and failure. Execution repeats the struct-only policy validation so
// hand-built routes (see StaticRouteResolver) are guarded too.
//
// Streaming attempts receive a commit callback and must call it exactly once,
// immediately before the first downstream chunk. A failure after commit is terminal
// (replay could duplicate visible output); a failure before commit follows the same
// classifier and retry policy as buffered execution.
//
// # Invariants
//
//   - One invocation ID per Execute or ExecuteStream call, returned on success and
//     failure.
//   - Attempt numbers begin at one.
//   - Exactly one AttemptStarted and one AttemptFinished event per provider attempt.
//   - The effective deadline is the earlier of the caller deadline and the route
//     total timeout.
//   - The request bucket is charged before a provider call; the token bucket consumes
//     the actual reported tokens after every attempt and may go into debt.
//   - Post-commit streaming errors are never retried.
//   - The caller resolves a route once (ResolveRoute) and every execution against it
//     runs under that immutable decision; a route edit takes effect on the next
//     resolution, never mid-decision.
//   - Observer failures are recovered and logged; they never alter generation success.
package controlplane
