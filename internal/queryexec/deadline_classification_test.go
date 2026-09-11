package queryexec

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// Model the interval after a socket deadline expires but before the context's
// independent timer publishes Err. No scheduler timing is needed by the test.
type unpublishedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (ctx unpublishedDeadlineContext) Deadline() (time.Time, bool) { return ctx.deadline, true }

type cancelOnDeadlineContext struct {
	context.Context
	cancel context.CancelCauseFunc
	cause  error
}

func (ctx cancelOnDeadlineContext) Deadline() (time.Time, bool) {
	ctx.cancel(ctx.cause)
	return time.Now().Add(-time.Hour), true
}

func TestClassifyQueryErrorDeadlineSocketRace(t *testing.T) {
	past := unpublishedDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Hour)}
	future := unpublishedDeadlineContext{Context: context.Background(), deadline: time.Now().Add(time.Hour)}
	timeout := fmt.Errorf("read result: %w", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded})
	disconnected := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
	customCause := errors.New("admitted memory budget exhausted")
	canceled, cancel := context.WithCancelCause(context.Background())
	cancel(customCause)
	racing, cancelRacing := context.WithCancelCause(context.Background())
	defer cancelRacing(nil)
	tests := []struct {
		name  string
		ctx   context.Context
		input error
		want  error
	}{
		{"expired deadline before context timer", past, timeout, context.DeadlineExceeded},
		{"earlier network timeout", future, timeout, searchjobs.ErrStorageUnavailable},
		{"network timeout without deadline", context.Background(), timeout, searchjobs.ErrStorageUnavailable},
		{"non-timeout network failure after deadline", past, disconnected, searchjobs.ErrStorageUnavailable},
		{"success with unpublished deadline", past, nil, nil},
		{"visible custom cause wins", canceled, timeout, customCause},
		{"visible custom cause with expired deadline wins", unpublishedDeadlineContext{Context: canceled, deadline: past.deadline}, timeout, customCause},
		{"custom cause published during classification wins", cancelOnDeadlineContext{Context: racing, cancel: cancelRacing, cause: customCause}, timeout, customCause},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyQueryError(tt.ctx, tt.input)
			if !errors.Is(got, tt.want) {
				t.Fatalf("classification=%v; want %v", got, tt.want)
			}
		})
	}
}
