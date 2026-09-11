package ingest

import opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"

const batchValueLimitViolation = "batch_value_limit"

// batchValueBudget performs an allocation-free preflight before any event is
// cloned. Unlike the semantic validator, it charges all submitted events to
// the same budget, including events which may later fail authorization or
// validation. Subtrees beyond the hard depth limit cannot reach cloning
// and are left to per-event validation so its error contract is preserved.
type batchValueBudget struct {
	remaining uint32
}

func (budget *batchValueBudget) consumeObject(object *opensplunk.TypedObject, depth uint32) bool {
	if uint64(len(object.GetFields())) > uint64(budget.remaining) {
		return false
	}
	for _, field := range object.GetFields() {
		if !budget.consumeValue(field.GetValue(), depth) {
			return false
		}
	}
	return true
}

func (budget *batchValueBudget) consumeValue(value *opensplunk.TypedValue, depth uint32) bool {
	if budget.remaining == 0 {
		return false
	}
	budget.remaining--
	if depth >= HardMaxNestingDepth {
		return true
	}
	switch kind := value.GetKind().(type) {
	case *opensplunk.TypedValue_ListValue:
		if uint64(len(kind.ListValue.GetValues())) > uint64(budget.remaining) {
			return false
		}
		for _, child := range kind.ListValue.GetValues() {
			if !budget.consumeValue(child, depth+1) {
				return false
			}
		}
	case *opensplunk.TypedValue_ObjectValue:
		return budget.consumeObject(kind.ObjectValue, depth+1)
	}
	return true
}
