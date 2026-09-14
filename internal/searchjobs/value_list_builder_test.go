package searchjobs

import "testing"

func TestListValueBuilderSharesImmutableChildrenWithoutExposingStorage(t *testing.T) {
	child := ListValue(StringValue("kept"))
	parent, err := ListValueFromItems(1, func(int) (Value, error) { return child, nil })
	if err != nil {
		t.Fatal(err)
	}
	if &parent.listValue[0].listValue[0] != &child.listValue[0] {
		t.Fatal("builder recopied immutable nested list")
	}
	opened, _ := parent.List()
	opened[0].listValue[0] = StringValue("changed")
	if parent.listValue[0].listValue[0].stringValue != "kept" {
		t.Fatal("public accessor exposed builder storage")
	}
	for range 40 {
		next, err := ListValueFromItems(1, func(int) (Value, error) { return parent, nil })
		if err != nil {
			return
		}
		parent = next
	}
	t.Fatal("builder accepted excessive list depth")
}
