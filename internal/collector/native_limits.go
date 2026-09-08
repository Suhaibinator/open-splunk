package collector

import (
	"errors"
	"strings"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

// nativeFieldBudget validates the merged projection, including constants.
// Payload parsers independently count input fields before reserved filtering.
type nativeFieldBudget struct {
	fields, names, depth int
}

func (b *nativeFieldBudget) object(object *opensplunk.TypedObject, depth, prefix int) error {
	if depth > b.depth {
		return errors.New("native field nesting exceeds limit")
	}
	for _, field := range object.GetFields() {
		b.fields--
		if b.fields < 0 {
			return errors.New("native field count exceeds limit")
		}
		if !nativeValidName(field.GetName()) {
			return errors.New("invalid native field name")
		}
		name := field.GetName()
		pathBytes := prefix + len(name) + strings.Count(name, ".") + strings.Count(name, "\\")
		if prefix > 0 {
			pathBytes++
		}
		if pathBytes > eventfields.MaximumNormalizedFieldNameBytes {
			return errors.New("native field path exceeds limit")
		}
		b.names -= pathBytes
		if b.names < 0 {
			return errors.New("native field names exceed byte limit")
		}
		if err := b.value(field.GetValue(), depth, pathBytes); err != nil {
			return err
		}
	}
	return nil
}

func (b *nativeFieldBudget) value(value *opensplunk.TypedValue, depth, prefix int) error {
	if value == nil || value.GetKind() == nil {
		return errors.New("invalid native typed value")
	}
	switch kind := value.GetKind().(type) {
	case *opensplunk.TypedValue_ObjectValue:
		return b.object(kind.ObjectValue, depth+1, prefix)
	case *opensplunk.TypedValue_ListValue:
		if depth+1 > b.depth {
			return errors.New("native list nesting exceeds limit")
		}
		for _, item := range kind.ListValue.GetValues() {
			if err := b.value(item, depth+1, prefix); err != nil {
				return err
			}
		}
	case *opensplunk.TypedValue_StringValue:
		if !utf8.ValidString(kind.StringValue) {
			return errors.New("invalid native field text")
		}
	}
	return nil
}
