// Package protostrict provides shared helpers that reject protobuf messages
// carrying unknown fields, so wire-level forward compatibility never smuggles
// unvalidated data past a strict boundary.
package protostrict

import (
	"errors"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// ContainsUnknown reports whether message is invalid or carries unknown
// protobuf fields anywhere in its transitive message tree.
func ContainsUnknown(message protoreflect.Message) bool {
	if !message.IsValid() || len(message.GetUnknown()) != 0 {
		return true
	}
	containsUnknown := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsMap() {
			if field.MapValue().Message() == nil {
				return true
			}
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				if ContainsUnknown(item.Message()) {
					containsUnknown = true
					return false
				}
				return true
			})
			return !containsUnknown
		}
		if field.IsList() {
			if field.Message() == nil {
				return true
			}
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if ContainsUnknown(list.Get(index).Message()) {
					containsUnknown = true
					return false
				}
			}
			return true
		}
		if field.Message() != nil &&
			ContainsUnknown(value.Message()) {
			containsUnknown = true
			return false
		}
		return true
	})
	return containsUnknown
}

// RejectUnknownFields returns an error naming subject when message is invalid
// or carries unknown protobuf fields.
func RejectUnknownFields(message protoreflect.Message, subject string) error {
	if ContainsUnknown(message) {
		return errors.New(subject + " contains unknown protobuf fields")
	}
	return nil
}
