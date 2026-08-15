package knowledge

import (
	"slices"
	"sort"
)

// CanonicalFields returns a sorted, empty-free, duplicate-free clone of fields.
func CanonicalFields(fields []string) []string {
	result := slices.Clone(fields)
	sort.Strings(result)
	write := 0
	for _, field := range result {
		if field == "" || write > 0 && result[write-1] == field {
			continue
		}
		result[write] = field
		write++
	}
	return result[:write:write]
}

// FieldsIntersect reports whether two sorted canonical field sets share a
// member.
func FieldsIntersect(left, right []string) bool {
	for leftIndex, rightIndex := 0, 0; leftIndex < len(left) && rightIndex < len(right); {
		switch {
		case left[leftIndex] < right[rightIndex]:
			leftIndex++
		case left[leftIndex] > right[rightIndex]:
			rightIndex++
		default:
			return true
		}
	}
	return false
}
