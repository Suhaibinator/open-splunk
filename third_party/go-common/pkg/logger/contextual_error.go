package logger

import (
	"errors"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// contextualError carries diagnostic metadata to the boundary that owns the
// final log record. It deliberately leaves Error and Unwrap transparent so
// errors.Is and errors.As retain their normal behavior.
type contextualError struct {
	err       error
	fields    []zap.Field
	level     *zapcore.Level
	invariant bool
}

func (e *contextualError) Error() string { return e.err.Error() }
func (e *contextualError) Unwrap() error { return e.err }

// WithErrorFields attaches copied structured fields to err. The fields can be
// recovered at an owning boundary with ErrorFields. Use WithErrorCause when a
// separate underlying error must remain in the error chain.
func WithErrorFields(err error, fields ...zap.Field) error {
	if err == nil {
		return nil
	}
	return newContextualError(err, nil, false, fields)
}

// WithErrorCause preserves a domain error and its underlying cause while
// attaching structured context for the owning log boundary.
func WithErrorCause(domainErr, cause error, fields ...zap.Field) error {
	switch {
	case domainErr == nil:
		domainErr = cause
	case cause == nil || errors.Is(domainErr, cause):
	case errors.Is(cause, domainErr):
		domainErr = cause
	default:
		domainErr = errors.Join(domainErr, cause)
	}
	return WithErrorFields(domainErr, fields...)
}

// WithErrorLevel attaches a severity override and copied structured fields to
// err. The outermost override wins when wrappers are nested.
func WithErrorLevel(err error, level zapcore.Level, fields ...zap.Field) error {
	if err == nil {
		return nil
	}
	return newContextualError(err, &level, false, fields)
}

// InvariantError reports a runtime-checkable internal assumption that failed.
// The error is forced to Error severity even if an outer boundary maps it to a
// client-facing status.
func InvariantError(invariant string, cause error, fields ...zap.Field) error {
	if cause == nil {
		cause = fmt.Errorf("invariant %q violated", invariant)
	}
	allFields := make([]zap.Field, 0, len(fields)+1)
	allFields = append(allFields, zap.String(logkeys.Invariant, invariant))
	allFields = append(allFields, fields...)
	level := zapcore.ErrorLevel
	return newContextualError(cause, &level, true, allFields)
}

func newContextualError(err error, level *zapcore.Level, invariant bool, fields []zap.Field) error {
	fieldsCopy := append([]zap.Field(nil), fields...)
	var levelCopy *zapcore.Level
	if level != nil {
		copied := *level
		levelCopy = &copied
	}
	return &contextualError{err: err, fields: fieldsCopy, level: levelCopy, invariant: invariant}
}

// ErrorFields returns contextual fields from the complete error chain. When a
// key appears more than once, the outermost context wins. The returned slice
// is independent from the wrappers and is safe for the caller to modify.
func ErrorFields(err error) []zap.Field {
	var fields []zap.Field
	seen := make(map[string]struct{})
	walkErrorChain(err, func(current error) bool {
		contextual, ok := current.(*contextualError)
		if !ok {
			return true
		}
		for _, field := range contextual.fields {
			if field.Key != "" {
				if _, exists := seen[field.Key]; exists {
					continue
				}
				seen[field.Key] = struct{}{}
			}
			fields = append(fields, field)
		}
		return true
	})
	return append([]zap.Field(nil), fields...)
}

// ErrorLevel returns the outermost contextual severity override.
func ErrorLevel(err error) (zapcore.Level, bool) {
	var level zapcore.Level
	found := false
	walkErrorChain(err, func(current error) bool {
		contextual, ok := current.(*contextualError)
		if ok && contextual.level != nil {
			level = *contextual.level
			found = true
			return false
		}
		return true
	})
	return level, found
}

// IsInvariantError reports whether any error in the wrapped or joined chain
// was created by InvariantError. Severity and field names do not imply
// invariant identity.
func IsInvariantError(err error) bool {
	found := false
	walkErrorChain(err, func(current error) bool {
		contextual, ok := current.(*contextualError)
		if ok && contextual.invariant {
			found = true
			return false
		}
		return true
	})
	return found
}

// LogInvariantViolation records an invariant failure immediately when a safe
// fallback keeps processing alive. Callers should include stage, expected,
// actual, entity identity, and the selected fallback where applicable. logger
// should already carry the operation's name and correlation fields (see For).
func LogInvariantViolation(
	logger *zap.Logger,
	invariant string,
	cause error,
	fields ...zap.Field,
) {
	if logger == nil {
		return
	}
	allFields := make([]zap.Field, 0, len(fields)+2)
	allFields = append(allFields, zap.String(logkeys.Invariant, invariant))
	if cause != nil {
		allFields = append(allFields, zap.Error(cause))
	}
	allFields = append(allFields, fields...)
	logger.Error("Invariant violated", deduplicateFields(allFields)...)
}

func deduplicateFields(fields []zap.Field) []zap.Field {
	deduplicated := make([]zap.Field, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field.Key != "" {
			if _, exists := seen[field.Key]; exists {
				continue
			}
			seen[field.Key] = struct{}{}
		}
		deduplicated = append(deduplicated, field)
	}
	return deduplicated
}

func walkErrorChain(err error, visit func(error) bool) {
	if err == nil {
		return
	}
	if !visit(err) {
		return
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			walkErrorChain(child, visit)
		}
		return
	}
	if unwrapped := errors.Unwrap(err); unwrapped != nil {
		walkErrorChain(unwrapped, visit)
	}
}
