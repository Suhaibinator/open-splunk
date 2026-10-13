package common_service_models

import (
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	// MaxTranscriptModelPayloadBytes caps the structured descriptor beside an agent transcript row.
	MaxTranscriptModelPayloadBytes = 256 * 1024

	// Tool-result embeds use the ordinary chat defaults as hard server-side ceilings. Tool-result
	// writes bypass the public message handler, so they must enforce equivalent limits at their own
	// ingress boundaries rather than relying on deployment-configured handler validation.
	MaxToolResultEmbeds             = 10
	MaxToolResultEmbedDataBytes     = 64 * 1024
	MaxToolResultEmbedFallbackRunes = 500

	// Bound repeated-field fan-out independently of serialized bytes. Empty and nil protobuf
	// message entries have negligible encoded size but still allocate and iterate during durable
	// reconstruction. 4096 matches the largest provider-side multipart ceiling in this module.
	MaxToolResultContentParts = 4096
)

var (
	ErrToolResultContentJSONInvalid  = errors.New("tool result content JSON is invalid")
	ErrToolResultPayloadTooLarge     = errors.New("tool result payload exceeds transcript descriptor limit")
	ErrToolResultTooManyEmbeds       = errors.New("tool result has too many embeds")
	ErrToolResultEmbedDataTooLarge   = errors.New("tool result embed data is too large")
	ErrToolResultEmbedTextTooLarge   = errors.New("tool result embed fallback text is too large")
	ErrToolResultTooManyContentParts = errors.New("tool result has too many content parts")
	ErrToolResultTextInvalid         = errors.New("tool result text is not valid database text")
)

// ValidateToolResultPayload applies the common service/persistence ingress invariants. Binary
// media must already be externalized before payloadBytes is measured.
func ValidateToolResultPayload(contentJSON string, payloadBytes []byte) error {
	if contentJSON != "" && !jsontext.Value(contentJSON).IsValid() {
		return ErrToolResultContentJSONInvalid
	}
	if len(payloadBytes) > MaxTranscriptModelPayloadBytes {
		return ErrToolResultPayloadTooLarge
	}
	return nil
}

// ValidateToolResultEmbedCount bounds both persistence row fan-out and otherwise-empty embed
// slices. Count every input entry, including nils, because the slice itself is callback-controlled.
func ValidateToolResultEmbedCount(count int) error {
	if count > MaxToolResultEmbeds {
		return fmt.Errorf("%w: got %d, maximum %d", ErrToolResultTooManyEmbeds, count, MaxToolResultEmbeds)
	}
	return nil
}

// ValidateToolResultEmbed applies the ordinary chat embed-data byte ceiling and fallback-text
// character ceiling shared by executor, writer, and persistence ingress.
func ValidateToolResultEmbed(dataBytes int, fallback string) error {
	if dataBytes > MaxToolResultEmbedDataBytes {
		return fmt.Errorf("%w: got %d bytes, maximum %d", ErrToolResultEmbedDataTooLarge, dataBytes, MaxToolResultEmbedDataBytes)
	}
	if err := ValidateToolResultText(fallback); err != nil {
		return err
	}
	if fallbackRunes := utf8.RuneCountInString(fallback); fallbackRunes > MaxToolResultEmbedFallbackRunes {
		return fmt.Errorf("%w: got %d characters, maximum %d", ErrToolResultEmbedTextTooLarge, fallbackRunes, MaxToolResultEmbedFallbackRunes)
	}
	return nil
}

// ValidateToolResultContentPartCount bounds callback-controlled multipart fan-out even when the
// entries are nil or encode as empty protobuf messages.
func ValidateToolResultContentPartCount(count int) error {
	if count > MaxToolResultContentParts {
		return fmt.Errorf("%w: got %d, maximum %d", ErrToolResultTooManyContentParts, count, MaxToolResultContentParts)
	}
	return nil
}

// ValidateToolResultText applies PostgreSQL text-column invariants before a completed tool result
// reaches persistence. PostgreSQL rejects NUL bytes, and invalid UTF-8 cannot be represented
// consistently by either its text type or protobuf string fields.
func ValidateToolResultText(text string) error {
	if !utf8.ValidString(text) || strings.IndexByte(text, 0) >= 0 {
		return ErrToolResultTextInvalid
	}
	return nil
}

// ValidateToolResultEmbeds validates the service-layer embed collection without copying payloads.
func ValidateToolResultEmbeds(embeds []*ChatMessageEmbed) error {
	if err := ValidateToolResultEmbedCount(len(embeds)); err != nil {
		return err
	}
	for i, embed := range embeds {
		if embed == nil {
			continue
		}
		if err := ValidateToolResultEmbed(len(embed.EmbedData), embed.FallbackText); err != nil {
			return fmt.Errorf("embed %d: %w", i, err)
		}
	}
	return nil
}
