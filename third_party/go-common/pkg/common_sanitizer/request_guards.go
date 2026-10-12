package common_sanitizer

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// paginationValidationFormat is the wrap applied to every pagination
// sanitization failure surfaced from a request sanitizer.
const paginationValidationFormat = "pagination validation failed: %w"

// requestIdentifier covers the integer widths proto generates for identifiers.
type requestIdentifier interface {
	~int32 | ~int64 | ~uint32 | ~uint64
}

// idGuard records whether a required request identifier was supplied, together
// with the error reported when it was not.
type idGuard struct {
	present bool
	missing error
}

// requiredID declares that id must be non-zero, reporting missing otherwise.
// Read id through the generated proto getter rather than the struct field so
// that the argument stays safe to evaluate while the request is still nil.
func requiredID[I requestIdentifier](id I, missing error) idGuard {
	return idGuard{present: id != 0, missing: missing}
}

// guardRequest rejects a nil request and then the first unset identifier,
// covering the guard chain shared by the sanitizers that carry no further
// domain validation. Guards are checked in declaration order so the reported
// error matches the order the fields appear in the request.
func guardRequest[T comparable](req T, ids ...idGuard) (T, error) {
	var zero T
	if req == zero {
		return zero, ErrInvalidRequest
	}
	for _, guard := range ids {
		if !guard.present {
			return zero, guard.missing
		}
	}
	return req, nil
}

// normalizePagination sanitizes a request's pagination field in place, applying
// the shared wrap so every sanitizer reports pagination failures identically.
func (h *Handler) normalizePagination(ctx context.Context, field **proto_models.PaginationRequest) error {
	sanitized, err := h.SanitizePaginationRequest(ctx, *field)
	if err != nil {
		return fmt.Errorf(paginationValidationFormat, err)
	}
	*field = sanitized
	return nil
}
