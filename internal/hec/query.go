package hec

import (
	"errors"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/indexname"
)

// RawQuery is the completely decoded, validated raw-endpoint query. Channel
// remains separate so the handler can combine it with the header source under
// ParseRequestChannel's conflict rules.
type RawQuery struct {
	Time           OptionalString
	Metadata       MetadataValues
	Channel        Channel
	ChannelPresent bool
}

// ChannelValues returns the query source in the shape consumed by
// ParseRequestChannel.
func (query RawQuery) ChannelValues() []string {
	if !query.ChannelPresent {
		return nil
	}
	return []string{string(query.Channel)}
}

// ParseRawQuery decodes the exact raw metadata allowlist. URL decoding occurs
// once through net/url; malformed escapes, empty/unknown names, empty values,
// and duplicates fail closed. A token parameter receives the dedicated code
// 16 category without retaining its value.
func ParseRawQuery(rawQuery string, limits Limits) (RawQuery, error) {
	if err := limits.Validate(); err != nil {
		return RawQuery{}, NewProtocolError(ErrorInternal, err)
	}
	if len(rawQuery) > limits.MaximumRequestTargetBytes {
		return RawQuery{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC query exceeds its limit"))
	}
	// url.ParseQuery deliberately ignores empty ampersand-delimited segments,
	// while the HEC contract treats each as an empty parameter name. Close that
	// ambiguity before decoding names and values.
	if rawQuery != "" {
		if slices.Contains(strings.Split(rawQuery, "&"), "") {
			return RawQuery{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC raw query parameter name is empty"))
		}
	}
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return RawQuery{}, NewProtocolError(ErrorInvalidDataFormat, err)
	}
	if tokenValues, exists := values["token"]; exists {
		_ = tokenValues
		return RawQuery{}, NewProtocolError(ErrorQueryAuthorizationDisabled, nil)
	}
	allowed := map[string]struct{}{
		"time": {}, "host": {}, "source": {}, "sourcetype": {}, "index": {}, "channel": {},
	}
	for name, items := range values {
		_, supported := allowed[name]
		if name == "" || !utf8.ValidString(name) || !supported {
			return RawQuery{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC raw query parameter is unsupported"))
		}
		if len(items) != 1 {
			if name == "channel" {
				return RawQuery{}, NewProtocolError(ErrorChannelInvalid, errors.New("HEC channel query is repeated or invalid"))
			}
			return RawQuery{}, NewProtocolError(ErrorInvalidDataFormat, errors.New("HEC raw query name is invalid or repeated"))
		}
	}
	result := RawQuery{}
	// Channel validation precedes body and metadata diagnostics.
	if items, exists := values["channel"]; exists {
		value := items[0]
		if value == "" || !utf8.ValidString(value) {
			return RawQuery{}, NewProtocolError(ErrorChannelInvalid, nil)
		}
		channel, parseErr := ParseChannel(value, limits.MaximumChannelBytes)
		if parseErr != nil {
			return RawQuery{}, parseErr
		}
		result.Channel, result.ChannelPresent = channel, true
	}
	for _, name := range []string{"time", "index", "host", "source", "sourcetype"} {
		items, exists := values[name]
		if !exists {
			continue
		}
		value := items[0]
		if value == "" || !utf8.ValidString(value) {
			switch name {
			case "index":
				return RawQuery{}, NewProtocolError(ErrorIncorrectIndex, nil)
			default:
				return RawQuery{}, NewProtocolError(ErrorInvalidDataFormat, nil)
			}
		}
		switch name {
		case "time":
			if _, parseErr := ParseEpochNanoseconds(value); parseErr != nil {
				return RawQuery{}, NewProtocolError(ErrorInvalidDataFormat, parseErr)
			}
			result.Time = OptionalString{Present: true, Value: value}
		case "host":
			if !ValidTextMetadata(value) {
				return RawQuery{}, NewProtocolError(ErrorInvalidDataFormat, nil)
			}
			result.Metadata.Host = OptionalString{Present: true, Value: value}
		case "source":
			if !ValidTextMetadata(value) {
				return RawQuery{}, NewProtocolError(ErrorInvalidDataFormat, nil)
			}
			result.Metadata.Source = OptionalString{Present: true, Value: value}
		case "sourcetype":
			if !ValidTextMetadata(value) {
				return RawQuery{}, NewProtocolError(ErrorInvalidDataFormat, nil)
			}
			result.Metadata.Sourcetype = OptionalString{Present: true, Value: value}
		case "index":
			if !indexname.ValidCanonical(value) {
				return RawQuery{}, NewProtocolError(ErrorIncorrectIndex, nil)
			}
			result.Metadata.Index = OptionalString{Present: true, Value: value}
		}
	}
	return result, nil
}
