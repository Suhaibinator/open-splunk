package indexread

import (
	"context"
	"fmt"
)

// UnfencedAdmission deliberately admits reads without joining an index
// retirement fence. It is only appropriate for isolated tests and diagnostic
// utilities that cannot run concurrently with the index-deletion lifecycle.
// Production event-row readers must use a real shared admission dependency.
//
// The explicit type makes an unfenced reader visible at construction sites;
// a missing Admission dependency is always an error.
type UnfencedAdmission struct{}

var _ Admission = UnfencedAdmission{}

// Acquire returns the caller's context and a no-op release function. The
// consuming reader remains responsible for its ordinary scope and compiler-
// seal validation; this implementation only opts out of lifecycle joining.
func (UnfencedAdmission) Acquire(
	ctx context.Context,
	_ string,
	_ []string,
) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: nil context", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return ctx, func() {}, nil
}
