package clickhouse

import (
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// resolveConvertDescendant reads the current conversion output rather than
// falling through to a same-named immutable event path. Exact independently
// published fields are resolved first by resolveCompiledField.
func resolveConvertDescendant(field plan.FieldRef, state compileState) (fieldState, bool) {
	if len(field.Path) < 2 || !compiledDynamicFieldNameAllowed(state.dynamicFieldFilters, field.Name) {
		return fieldState{}, false
	}
	for prefixLength := len(field.Path) - 1; prefixLength > 0; prefixLength-- {
		prefix := eventfields.NormalizeDynamicPath(field.Path[:prefixLength])
		parent, exists := state.visible[prefix]
		if !exists || !parent.convertedContainer {
			continue
		}
		value := parent.valueSQL
		for _, segment := range field.Path[prefixLength:] {
			value = "dynamicElement(" + value + ", 'Map(String, Dynamic)')[" + quoteStringLiteral(eventfields.EncodePhysicalPathSegment(segment)) + "]"
		}
		relative := eventfields.NormalizeDynamicPath(field.Path[prefixLength:])
		key := quoteStringLiteral(relative)
		descendantPrefix := quoteStringLiteral(relative + ".")
		exact := "has(" + parent.relativeFieldNamesSQL + ", " + key + ")"
		descendant := "arrayExists(name -> startsWith(name, " + descendantPrefix + "), " + parent.relativeFieldNamesSQL + ")"
		names := "arrayMap(name -> substring(name, length(" + descendantPrefix + ")+1), arrayFilter(name -> startsWith(name, " + descendantPrefix + "), " + parent.relativeFieldNamesSQL + "))"
		types := "arrayMap(position -> arrayElement(" + parent.relativeFieldTypesSQL + ", position), arrayFilter(position -> startsWith(arrayElement(" + parent.relativeFieldNamesSQL + ", position), " + descendantPrefix + "), arrayEnumerate(" + parent.relativeFieldNamesSQL + ")))"
		return fieldState{
			valueSQL: value, dynamicTypeSQL: "dynamicType(" + value + ")", kind: fieldKindDynamic,
			existsSQL: "(" + exact + " OR " + descendant + ")", descendantSQL: descendant,
			storedTypeSQL:         "toUInt8(multiIf(" + exact + ", arrayElement(" + parent.relativeFieldTypesSQL + ", indexOf(" + parent.relativeFieldNamesSQL + ", " + key + ")), " + descendant + ", " + storedValueTypeSQL(eventfields.StoredValueTypeObject) + ", 0))",
			relativeFieldNamesSQL: names, relativeFieldTypesSQL: types,
			fieldMetadataVersionSQL: parent.fieldMetadataVersionSQL,
			maxStringBytes:          parent.maxStringBytes, convertedContainer: true,
			materializeForPredicate: true,
		}, true
	}
	return fieldState{}, false
}
