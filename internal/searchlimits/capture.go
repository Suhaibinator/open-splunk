package searchlimits

import "context"

type captureContextKey struct{}

// CaptureEnvelope separates full output bounds from the live preview policy.
type CaptureEnvelope struct{ Rows, Bytes uint64 }

func WithCaptureEnvelope(ctx context.Context, rows, bytes uint64) context.Context {
	return context.WithValue(ctx, captureContextKey{}, CaptureEnvelope{Rows: rows, Bytes: bytes})
}

func CaptureFromContext(ctx context.Context) (CaptureEnvelope, bool) {
	if ctx == nil {
		return CaptureEnvelope{}, false
	}
	value, ok := ctx.Value(captureContextKey{}).(CaptureEnvelope)
	return value, ok && value.Rows > 0 && value.Bytes > 0
}
