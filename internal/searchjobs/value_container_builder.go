package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ListValueFromItemsContext builds private list storage from immutable children
// and observes cancellation while constructing and validating nested values.
func ListValueFromItemsContext(ctx context.Context, length int, item func(int) (Value, error)) (Value, error) {
	if ctx == nil {
		return Value{}, errors.New("search result list builder context is nil")
	}
	return buildListValue(&valueMeasurement{ctx: ctx}, length, item)
}

func buildListValue(measurement *valueMeasurement, length int, item func(int) (Value, error)) (Value, error) {
	if length < 0 || item == nil {
		return Value{}, errors.New("search result list builder is invalid")
	}
	if err := measurement.checkContext(); err != nil {
		return Value{}, err
	}
	values := make([]Value, length)
	for index := range values {
		if err := measurement.pollContext(); err != nil {
			return Value{}, err
		}
		child, err := item(index)
		if err != nil {
			return Value{}, err
		}
		if _, _, err := measurement.measure(child, 1); err != nil {
			return Value{}, err
		}
		values[index] = child
	}
	if err := measurement.checkContext(); err != nil {
		return Value{}, err
	}
	return Value{kind: ValueKindList, listValue: values}, nil
}

// ObjectValueFromFields constructs ordered private object storage. Field values
// are immutable and safely shared; caller-owned field backing is never retained.
func ObjectValueFromFields(length int, item func(int) (ObjectField, error)) (Value, error) {
	return buildObjectValue(nil, length, item)
}

// ObjectValueFromFieldsContext also polls cancellation throughout construction
// and recursive validation. Only names need detaching from caller storage.
func ObjectValueFromFieldsContext(ctx context.Context, length int, item func(int) (ObjectField, error)) (Value, error) {
	if ctx == nil {
		return Value{}, errors.New("search result object builder context is nil")
	}
	return buildObjectValue(&valueMeasurement{ctx: ctx}, length, item)
}

func buildObjectValue(measurement *valueMeasurement, length int, item func(int) (ObjectField, error)) (Value, error) {
	if length < 0 || item == nil {
		return Value{}, errors.New("search result object builder is invalid")
	}
	if err := measurement.checkContext(); err != nil {
		return Value{}, err
	}
	fields := make([]ObjectField, length)
	if err := measurement.checkContext(); err != nil {
		return Value{}, err
	}
	seen := make(map[string]struct{}, length)
	for index := range fields {
		if err := measurement.pollContext(); err != nil {
			return Value{}, err
		}
		field, err := item(index)
		if err != nil {
			return Value{}, err
		}
		if field.Name == "" {
			return Value{}, errors.New("search result object field name is empty")
		}
		if _, ok := seen[field.Name]; ok {
			return Value{}, fmt.Errorf("search result object field %q is duplicated", field.Name)
		}
		seen[field.Name] = struct{}{}
		if _, _, err := measurement.measure(field.Value, 1); err != nil {
			return Value{}, err
		}
		if err := measurement.checkContext(); err != nil {
			return Value{}, err
		}
		fields[index] = ObjectField{Name: strings.Clone(field.Name), Value: field.Value}
	}
	if err := measurement.checkContext(); err != nil {
		return Value{}, err
	}
	return Value{kind: ValueKindObject, objectValue: fields}, nil
}

func (measurement *valueMeasurement) checkContext() error {
	if measurement == nil {
		return nil
	}
	return measurement.ctx.Err()
}
func (measurement *valueMeasurement) pollContext() error {
	if measurement == nil {
		return nil
	}
	if measurement.nodes&255 == 0 {
		if err := measurement.checkContext(); err != nil {
			return err
		}
	}
	measurement.nodes++
	return nil
}
