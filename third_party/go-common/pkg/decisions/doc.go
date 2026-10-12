// Package decisions provides a typed client for Decisions-compatible HTTP APIs.
// It sends Noul, Choice, and Score questions, validates the provider response,
// and returns answers as ordinary Go values.
//
// # Typed question lifecycle
//
// A question constructor returns a typed [Question] that serves two purposes. Add
// that handle to [Request.Questions], then use the same handle's Answer method on
// the [Response]. Answer returns T directly: callers do not marshal the response,
// parse JSON, look up a question ID, or use a type assertion. Different question
// types can share one request while preserving their individual answer types.
// A handle is tied to its definition, so a separately constructed question with
// the same ID cannot read its answer.
//
// Choice questions retain caller-defined named string types. For example, a
// Question[ChoiceAnswer[Department]] returns Department values in both Choice
// and Probabilities. The client rejects a selected choice or probability key
// that was not present in the question's criteria.
//
// # Answer semantics
//
// NoulAnswer.Noul is a probability in [0, 1], not a boolean. ScoreAnswer.Score
// is a position on the zero-based ordered criteria supplied to [NewScore]; it can
// be fractional between two criteria. Applications choose thresholds and other
// business rules. Confidence, probability distributions, and score legends are
// optional provider metadata, so their pointer or map fields may be nil.
//
// # Content and configuration
//
// [Text] creates text content. [JSON] snapshots a JSON string, object, or array for
// structured state or instructions. [Null] leaves a choice option without extra
// guidance. Question constructors validate and copy their inputs so
// handles can be reused safely.
//
// [NewClient] requires an explicit API key and otherwise uses the package's
// default endpoint, model, and 60-second HTTP timeout. [Config] can select another
// Decisions-compatible base URL, endpoint path, model, or HTTP client; a
// [Request.Model] overrides the client's model for one call. The package does not
// read credentials or configuration from the environment.
//
// # Cancellation and errors
//
// [Client.Decide] honors context cancellation and deadlines. Callers own retry policy;
// [HTTPError] exposes status, provider details, and Retry-After information for
// that decision. [ErrInvalidConfig], [ErrInvalidRequest], and [ErrInvalidResponse] can
// be inspected with [errors.Is], while [HTTPError] can be inspected with [errors.As].
// A successful Decide has already validated every requested answer, including
// answer kinds, numeric ranges, choice membership, and required response fields.
package decisions
