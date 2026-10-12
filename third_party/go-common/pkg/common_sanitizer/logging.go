package common_sanitizer

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

const (
	logValueSuffix      = "…"
	sanitizerLoggerName = "common_sanitizer"
)

// rejectedMessage is the message of the one Info line a sanitizer writes when
// it turns a request away. SRouter writes its own boundary line for the same
// request (status, method, path, trace_id); the two join on trace_id.
const rejectedMessage = "request rejected by sanitizer"

// reject logs the rejection of a request at the site that decided it and
// returns err unchanged, so callers write `return nil, h.reject(ctx, stage, err, ...)`.
//
// Call it exactly once per rejection, where the rejection originates. Never
// call it for an error returned by another Handler method: that method already
// logged. fields are explicit, allow-listed diagnostics — lengths, counts,
// identifiers, and user-controlled text bounded through boundedStringFields.
// Never attach a password, token, verification code, raw email, phone number,
// or free-text body.
func (h *Handler) reject(ctx context.Context, stage string, err error, fields ...zap.Field) error {
	if err == nil || h == nil || h.logger == nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fields = append(fields,
		zap.String(logkeys.Stage, stage),
		zap.String(appLogger.KeyOutcome, "invalid_input"),
		zap.Error(err),
	)
	appLogger.For(ctx, h.logger, sanitizerLoggerName).Info(rejectedMessage, fields...)
	return err
}

func boundedLogText(value string, maxRunes int) (preview string, truncated bool, originalRunes int) {
	var builder strings.Builder
	builder.Grow(min(len(value), maxRunes*utf8.UTFMax))
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		value = value[size:]
		if r == utf8.RuneError && size == 1 {
			r = unicode.ReplacementChar
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Bidi_Control, r) {
			r = ' '
		}
		originalRunes++
		if originalRunes <= maxRunes {
			builder.WriteRune(r)
		}
	}
	if originalRunes > maxRunes {
		preview := builder.String()
		_, lastRuneSize := utf8.DecodeLastRuneInString(preview)
		return preview[:len(preview)-lastRuneSize] + logValueSuffix, true, originalRunes
	}
	return builder.String(), false, originalRunes
}

// boundedStringFields returns the diagnostic fields for one user-controlled
// string: a normalized, rune-safe preview bounded by maxLength, whether it was
// truncated, and its original length in runes.
func boundedStringFields(key, value string, maxLength int) []zap.Field {
	preview, truncated, length := boundedLogText(value, maxLength)
	return []zap.Field{
		zap.String(key, preview),
		zap.Bool(key+"_truncated", truncated),
		zap.Int(key+"_length", length),
	}
}
