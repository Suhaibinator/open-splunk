package logger

import (
	"time"

	"go.uber.org/zap"
)

// DurationUS returns numeric microseconds, preserving fractional microseconds.
// key must end in _us. The value is independent of Zap's duration encoder.
func DurationUS(key string, d time.Duration) zap.Field {
	return zap.Float64(key, float64(d)/float64(time.Microsecond))
}

// DurationMS returns numeric milliseconds, preserving fractional milliseconds.
// key must end in _ms. The value is independent of Zap's duration encoder.
func DurationMS(key string, d time.Duration) zap.Field {
	return zap.Float64(key, float64(d)/float64(time.Millisecond))
}

// DurationSeconds returns numeric seconds, preserving fractional seconds.
// key must end in _s. The value is independent of Zap's duration encoder.
func DurationSeconds(key string, d time.Duration) zap.Field {
	return zap.Float64(key, d.Seconds())
}
