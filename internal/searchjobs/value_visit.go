package searchjobs

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// ValueVisitTokenKind identifies one event in a depth-first Value traversal.
type ValueVisitTokenKind uint8

const (
	ValueVisitInvalid ValueVisitTokenKind = iota
	ValueVisitNull
	ValueVisitString
	ValueVisitSigned
	ValueVisitUnsigned
	ValueVisitDouble
	ValueVisitBool
	ValueVisitBytes
	ValueVisitTime
	ValueVisitDuration
	ValueVisitDecimal
	ValueVisitListBegin
	ValueVisitListEnd
	ValueVisitObjectBegin
	ValueVisitObjectField
	ValueVisitObjectEnd
)

// ValueVisitToken is one scalar, container boundary, or object-field event.
// Length is populated for container-begin events. StringValue is populated for
// string values and object field names; decimal values retain their exact
// spelling in StringValue. BytesValue is a clone owned by the callback.
type ValueVisitToken struct {
	Kind          ValueVisitTokenKind
	StringValue   string
	SignedValue   int64
	UnsignedValue uint64
	DoubleValue   float64
	BoolValue     bool
	BytesValue    []byte
	TimeValue     time.Time
	DurationValue time.Duration
	Length        int
}

const maximumValueVisitDepth = 32

// VisitDetached walks value in depth-first order without cloning recursive
// list or object payloads. The callback is invoked synchronously. Tokens may
// safely be retained: no list or object backing slices are exposed, and byte
// payloads are cloned once before their callback.
//
// VisitDetached is intended for converting a caller-owned detached result
// page. It validates the same value invariants and maximum depth as result
// admission. A callback error stops traversal and is returned unchanged.
func (value Value) VisitDetached(visit func(ValueVisitToken) error) error {
	if visit == nil {
		return errors.New("search result value visitor is nil")
	}
	return visitDetachedValue(value, 0, visit)
}

func visitDetachedValue(value Value, depth int, visit func(ValueVisitToken) error) error {
	if depth > maximumValueVisitDepth {
		return errors.New("search result value exceeds maximum nesting depth")
	}

	switch value.kind {
	case ValueKindNull:
		return visit(ValueVisitToken{Kind: ValueVisitNull})
	case ValueKindString:
		return visit(ValueVisitToken{Kind: ValueVisitString, StringValue: value.stringValue})
	case ValueKindSigned:
		return visit(ValueVisitToken{Kind: ValueVisitSigned, SignedValue: value.signedValue})
	case ValueKindUnsigned:
		return visit(ValueVisitToken{Kind: ValueVisitUnsigned, UnsignedValue: value.unsignedValue})
	case ValueKindDouble:
		return visit(ValueVisitToken{Kind: ValueVisitDouble, DoubleValue: value.doubleValue})
	case ValueKindBool:
		return visit(ValueVisitToken{Kind: ValueVisitBool, BoolValue: value.boolValue})
	case ValueKindBytes:
		return visit(ValueVisitToken{Kind: ValueVisitBytes, BytesValue: slices.Clone(value.bytesValue)})
	case ValueKindTime:
		return visit(ValueVisitToken{Kind: ValueVisitTime, TimeValue: value.timeValue})
	case ValueKindDuration:
		return visit(ValueVisitToken{Kind: ValueVisitDuration, DurationValue: value.durationValue})
	case ValueKindDecimal:
		if !validDecimal(value.decimalValue) {
			return errors.New("search result decimal is invalid")
		}
		return visit(ValueVisitToken{Kind: ValueVisitDecimal, StringValue: value.decimalValue})
	case ValueKindList:
		if err := visit(ValueVisitToken{Kind: ValueVisitListBegin, Length: len(value.listValue)}); err != nil {
			return err
		}
		for _, child := range value.listValue {
			if err := visitDetachedValue(child, depth+1, visit); err != nil {
				return err
			}
		}
		return visit(ValueVisitToken{Kind: ValueVisitListEnd})
	case ValueKindObject:
		if err := validateVisitObjectFields(value.objectValue); err != nil {
			return err
		}
		if err := visit(ValueVisitToken{Kind: ValueVisitObjectBegin, Length: len(value.objectValue)}); err != nil {
			return err
		}
		for _, field := range value.objectValue {
			if err := visit(ValueVisitToken{Kind: ValueVisitObjectField, StringValue: field.Name}); err != nil {
				return err
			}
			if err := visitDetachedValue(field.Value, depth+1, visit); err != nil {
				return err
			}
		}
		return visit(ValueVisitToken{Kind: ValueVisitObjectEnd})
	default:
		return errors.New("search result value kind is invalid")
	}
}

func validateVisitObjectFields(fields []ObjectField) error {
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field.Name == "" {
			return errors.New("search result object field name is empty")
		}
		if _, exists := seen[field.Name]; exists {
			return fmt.Errorf("search result object field %q is duplicated", field.Name)
		}
		seen[field.Name] = struct{}{}
	}
	return nil
}
