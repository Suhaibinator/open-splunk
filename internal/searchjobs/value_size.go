package searchjobs

import (
	"errors"
	"math"
	"unicode/utf8"
)

var (
	errValueSizeInvalidKind = errors.New("search result value kind is invalid")
	errValueSizeDepth       = errors.New("search result value exceeds maximum nesting depth")
	errValueSizeString      = errors.New("search result string is not valid UTF-8")
	errValueSizeDecimal     = errors.New("search result decimal is invalid")
	errValueSizeObjectField = errors.New("search result object field name is invalid")
)

// ProtoSizeLowerBound returns a conservative lower bound for the serialized
// open_splunk.v1.TypedValue produced from value. It reads immutable private
// storage directly: strings, bytes, lists, and objects are never cloned.
//
// When the lower bound exceeds stopAfter, size saturates at stopAfter+1 and
// exceeded is true. If stopAfter is math.MaxUint64, size saturates at that
// value and exceeded still distinguishes overflow. Message-length and varint
// widths are intentionally counted at their minimum, so this preflight can
// reject an oversized result without ever rejecting a value whose eventual
// protobuf could fit.
func (value Value) ProtoSizeLowerBound(stopAfter uint64) (size uint64, exceeded bool, err error) {
	preflight := valueSizePreflight{stopAfter: stopAfter}
	if err := preflight.value(value, 0); err != nil {
		return 0, false, err
	}
	return preflight.size, preflight.exceeded, nil
}

type valueSizePreflight struct {
	stopAfter uint64
	size      uint64
	exceeded  bool
}

func (preflight *valueSizePreflight) add(size uint64) {
	if preflight.exceeded {
		return
	}
	if size > preflight.stopAfter-preflight.size {
		preflight.exceeded = true
		if preflight.stopAfter == math.MaxUint64 {
			preflight.size = math.MaxUint64
		} else {
			preflight.size = preflight.stopAfter + 1
		}
		return
	}
	preflight.size += size
}

func (preflight *valueSizePreflight) value(value Value, depth int) error {
	if depth > maximumValueVisitDepth {
		return errValueSizeDepth
	}
	if preflight.exceeded {
		return nil
	}

	switch value.kind {
	case ValueKindNull:
		// oneof tag plus the nonzero NULL_VALUE_NULL enum.
		preflight.add(2)
	case ValueKindString:
		if !utf8.ValidString(value.stringValue) {
			return errValueSizeString
		}
		// oneof tag, at least one length byte, and the preserved payload.
		preflight.add(2)
		preflight.add(uint64(len(value.stringValue)))
	case ValueKindSigned, ValueKindUnsigned, ValueKindBool:
		// A selected scalar oneof retains presence even when its value is zero.
		preflight.add(2)
	case ValueKindDouble:
		preflight.add(9)
	case ValueKindBytes:
		preflight.add(2)
		preflight.add(uint64(len(value.bytesValue)))
	case ValueKindTime, ValueKindDuration:
		// The nested timestamp or duration may itself encode to zero bytes.
		preflight.add(2)
	case ValueKindDecimal:
		if !validDecimal(value.decimalValue) {
			return errValueSizeDecimal
		}
		// Decimal canonicalization may shorten the retained spelling. Count only
		// the two message envelopes and the one-byte minimum canonical value.
		preflight.add(5)
	case ValueKindList:
		// TypedValue.list_value envelope.
		preflight.add(2)
		for _, child := range value.listValue {
			// TypedValueList.values envelope around the child TypedValue.
			preflight.add(2)
			if err := preflight.value(child, depth+1); err != nil {
				return err
			}
		}
	case ValueKindObject:
		// TypedValue.object_value envelope.
		preflight.add(2)
		for _, field := range value.objectValue {
			if field.Name == "" || !utf8.ValidString(field.Name) {
				return errValueSizeObjectField
			}
			// TypedObject.fields envelope, name tag/length/payload, and the
			// TypedObjectField.value envelope around the child TypedValue.
			preflight.add(6)
			preflight.add(uint64(len(field.Name)))
			if err := preflight.value(field.Value, depth+1); err != nil {
				return err
			}
		}
	default:
		return errValueSizeInvalidKind
	}
	return nil
}
