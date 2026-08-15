package hec

import (
	"errors"
	"math/big"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/indexname"
	"github.com/Suhaibinator/open-splunk/internal/jsonnumber"
)

const (
	MaximumMetadataValueBytes = 255
	MaximumEpochTextBytes     = 128
)

// OptionalString distinguishes absent metadata from an explicitly authored
// empty string. Precedence never treats empty as absence or trims a value.
type OptionalString struct {
	Present bool
	Value   string
}

// MetadataValues contains the dependency-neutral HEC routing metadata. Index
// canonicalization and host/source authorization stay in ingestion policy.
type MetadataValues struct {
	Host       OptionalString
	Source     OptionalString
	Sourcetype OptionalString
	Index      OptionalString
}

// DecodeEnvelopeMetadata validates JSON types and bounded UTF-8 storage while
// preserving exact authored values. Empty strings remain explicit values.
func DecodeEnvelopeMetadata(envelope Envelope) (MetadataValues, error) {
	result := MetadataValues{}
	items := []struct {
		name   string
		member PresentValue
		target *OptionalString
		kind   ErrorKind
	}{
		{"index", envelope.Index, &result.Index, ErrorIncorrectIndex},
		{"host", envelope.Host, &result.Host, ErrorInvalidDataFormat},
		{"source", envelope.Source, &result.Source, ErrorInvalidDataFormat},
		{"sourcetype", envelope.Sourcetype, &result.Sourcetype, ErrorInvalidDataFormat},
	}
	for _, item := range items {
		if !item.member.Present {
			continue
		}
		if item.member.Value.Kind != JSONString {
			return MetadataValues{}, NewEventError(item.kind, envelope.Number, errors.New("HEC metadata member has the wrong JSON type"))
		}
		value := item.member.Value.StringValue
		if item.name == "index" {
			if !indexname.ValidCanonical(value) {
				return MetadataValues{}, NewEventError(ErrorIncorrectIndex, envelope.Number, errors.New("HEC index is invalid"))
			}
		} else if !ValidTextMetadata(value) {
			return MetadataValues{}, NewEventError(ErrorInvalidDataFormat, envelope.Number, errors.New("HEC metadata member is invalid"))
		}
		item.target.Present = true
		item.target.Value = value
	}
	return result, nil
}

// ValidTextMetadata applies the common authored/default host, source, and
// sourcetype grammar without trimming or normalizing the accepted value.
func ValidTextMetadata(value string) bool {
	if value == "" || len(value) > MaximumMetadataValueBytes || !utf8.ValidString(value) ||
		strings.IndexByte(value, 0) >= 0 || asciiEdgeWhitespace(value[0]) || asciiEdgeWhitespace(value[len(value)-1]) {
		return false
	}
	return !strings.ContainsFunc(value, unicode.IsControl)
}

// DefaultMetadataFallbacks returns independent fixed fallbacks. Index remains
// absent because HEC v0.1 has no global index fallback.
func DefaultMetadataFallbacks() MetadataValues {
	return MetadataValues{
		Host:       OptionalString{Present: true, Value: "hec"},
		Source:     OptionalString{Present: true, Value: "http:hec"},
		Sourcetype: OptionalString{Present: true, Value: "httpevent"},
	}
}

func asciiEdgeWhitespace(value byte) bool {
	switch value {
	case '\t', '\n', '\v', '\f', '\r', ' ':
		return true
	default:
		return false
	}
}

// ResolveMetadata applies the field-specific v0.1 precedence. Only
// sourcetype consults the selected active index default; index, host, and
// source do not derive from index metadata. A present empty string wins;
// callers must apply any domain-specific empty or canonical-name policy after
// resolution.
func ResolveMetadata(event, token, index, fallback MetadataValues) MetadataValues {
	return MetadataValues{
		Host:       firstOptional(event.Host, token.Host, fallback.Host),
		Source:     firstOptional(event.Source, token.Source, fallback.Source),
		Sourcetype: firstOptional(event.Sourcetype, token.Sourcetype, index.Sourcetype, fallback.Sourcetype),
		Index:      firstOptional(event.Index, token.Index),
	}
}

func firstOptional(values ...OptionalString) OptionalString {
	for _, value := range values {
		if value.Present {
			return value
		}
	}
	return OptionalString{}
}

// ParseEnvelopeTime resolves an absent time to the one server-captured receive
// boundary and parses a present number or numeric string exactly.
func ParseEnvelopeTime(envelope Envelope, receivedAt time.Time) (time.Time, bool, error) {
	if !envelope.Time.Present {
		nanoseconds, err := timeToUnixNanoseconds(receivedAt)
		if err != nil {
			return time.Time{}, false, NewEventError(ErrorInvalidDataFormat, envelope.Number, err)
		}
		return time.Unix(0, nanoseconds).UTC(), false, nil
	}
	parsed, err := parsePresentEnvelopeTime(envelope)
	if err != nil {
		return time.Time{}, false, err
	}
	return parsed, true, nil
}

// parsePresentEnvelopeTime parses the present time member of an envelope.
func parsePresentEnvelopeTime(envelope Envelope) (time.Time, error) {
	var text string
	switch envelope.Time.Value.Kind {
	case JSONNumber:
		text = envelope.Time.Value.NumberValue.String()
	case JSONString:
		text = envelope.Time.Value.StringValue
	default:
		return time.Time{}, NewEventError(ErrorInvalidDataFormat, envelope.Number, errors.New("HEC time has the wrong JSON type"))
	}
	nanoseconds, err := ParseEpochNanoseconds(text)
	if err != nil {
		return time.Time{}, NewEventError(ErrorInvalidDataFormat, envelope.Number, err)
	}
	return time.Unix(0, nanoseconds).UTC(), nil
}

func validateExplicitTime(envelope Envelope) error {
	if !envelope.Time.Present {
		return nil
	}
	_, err := parsePresentEnvelopeTime(envelope)
	return err
}

// ParseEpochNanoseconds converts the complete JSON-number grammar from epoch
// seconds to an exact signed nanosecond count. Sub-nanosecond values, lexical
// extensions, and values outside int64 nanoseconds are rejected rather than
// rounded through float64.
func ParseEpochNanoseconds(text string) (int64, error) {
	if text == "" || len(text) > MaximumEpochTextBytes || !validEpochGrammar(text) {
		return 0, errors.New("HEC epoch time has invalid length")
	}
	if err := validateBoundedNumberLexeme(text); err != nil {
		return 0, errors.New("HEC epoch time is not a bounded JSON decimal")
	}
	seconds, err := jsonnumber.ParseDecimalRat(text)
	if err != nil {
		return 0, errors.New("HEC epoch time is not a bounded JSON decimal")
	}
	nanoseconds := new(big.Rat).Mul(seconds, big.NewRat(int64(time.Second), 1))
	if !nanoseconds.IsInt() {
		return 0, errors.New("HEC epoch time is more precise than nanoseconds")
	}
	integer := nanoseconds.Num()
	if !integer.IsInt64() {
		return 0, errors.New("HEC epoch time is outside the supported range")
	}
	return integer.Int64(), nil
}

func validEpochGrammar(text string) bool {
	position := 0
	if text[position] == '-' {
		position++
		if position == len(text) {
			return false
		}
	}
	if text[position] == '0' {
		position++
		if position < len(text) && asciiDigit(text[position]) {
			return false
		}
	} else {
		if text[position] < '1' || text[position] > '9' {
			return false
		}
		for position < len(text) && asciiDigit(text[position]) {
			position++
		}
	}
	if position < len(text) && text[position] == '.' {
		position++
		start := position
		for position < len(text) && asciiDigit(text[position]) {
			position++
		}
		if position == start {
			return false
		}
	}
	if position < len(text) && (text[position] == 'e' || text[position] == 'E') {
		position++
		if position < len(text) && (text[position] == '+' || text[position] == '-') {
			position++
		}
		if position == len(text) {
			return false
		}
		if text[position] == '0' {
			position++
			if position < len(text) && asciiDigit(text[position]) {
				return false
			}
		} else {
			if text[position] < '1' || text[position] > '9' {
				return false
			}
			for position < len(text) && asciiDigit(text[position]) {
				position++
			}
		}
	}
	return position == len(text)
}

func asciiDigit(value byte) bool { return value >= '0' && value <= '9' }

func timeToUnixNanoseconds(value time.Time) (int64, error) {
	value = value.UTC()
	result := new(big.Int).Mul(big.NewInt(value.Unix()), big.NewInt(int64(time.Second)))
	result.Add(result, big.NewInt(int64(value.Nanosecond())))
	if !result.IsInt64() {
		return 0, errors.New("HEC receive time is outside the supported nanosecond range")
	}
	return result.Int64(), nil
}
