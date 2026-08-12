package hec

import "errors"

// Channel preserves the exact authored GUID spelling because channel identity
// is byte-for-byte and case-sensitive even though both ASCII hex cases are
// accepted by validation.
type Channel string

// ParseChannel validates the canonical hyphenated GUID grammar selected by
// HEC v0.1. UUID variant and version bits are deliberately not constrained.
func ParseChannel(value string, maximumBytes int) (Channel, error) {
	if maximumBytes <= 0 || maximumBytes > HardMaximumChannelBytes ||
		len(value) == 0 || len(value) > maximumBytes || len(value) != 36 {
		return "", NewProtocolError(ErrorChannelInvalid, nil)
	}
	for index := 0; index < len(value); index++ {
		switch index {
		case 8, 13, 18, 23:
			if value[index] != '-' {
				return "", NewProtocolError(ErrorChannelInvalid, nil)
			}
		default:
			if !asciiHex(value[index]) {
				return "", NewProtocolError(ErrorChannelInvalid, nil)
			}
		}
	}
	return Channel(value), nil
}

// ParseRequestChannel combines the one permitted header and query source.
// Repetition and use of both sources fail closed, even when both values match.
// Missing input is allowed only when the endpoint/token contract does not
// require a channel.
func ParseRequestChannel(
	headerValues []string,
	queryValues []string,
	required bool,
	maximumBytes int,
) (Channel, bool, error) {
	if len(headerValues) > 1 || len(queryValues) > 1 {
		return "", false, NewProtocolError(ErrorChannelInvalid, errors.New("channel source is repeated"))
	}
	if len(headerValues) == 0 && len(queryValues) == 0 {
		if required {
			return "", false, NewProtocolError(ErrorChannelMissing, nil)
		}
		return "", false, nil
	}
	value := ""
	if len(headerValues) == 1 {
		value = headerValues[0]
	}
	if len(queryValues) == 1 {
		if len(headerValues) == 1 {
			return "", false, NewProtocolError(ErrorChannelInvalid, errors.New("channel sources conflict"))
		}
		value = queryValues[0]
	}
	channel, err := ParseChannel(value, maximumBytes)
	if err != nil {
		return "", false, err
	}
	return channel, true, nil
}

func asciiHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}
