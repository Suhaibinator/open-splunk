package knowledgecatalog

import (
	"reflect"
	"testing"
)

func TestNormalizeListRequestReturnsDetachedCanonicalPublicRequest(t *testing.T) {
	t.Parallel()

	appFilter := "  " + testApp + "\t"
	ownerFilter := "\n" + testOwner + " "
	textFilter := " needle\r"
	selectorFilter := "\tprod-* "
	request := ListRequest{
		PageToken:          "opaque-token",
		IncludeTotal:       true,
		AppIDFilter:        &appFilter,
		OwnerIDFilter:      &ownerFilter,
		TextFilter:         &textFilter,
		SelectorTextFilter: &selectorFilter,
		ObjectTypeFilters: []ObjectType{
			ObjectTypeFieldExtraction,
			ObjectTypeFieldAlias,
			ObjectTypeFieldExtraction,
		},
		StateFilters: []State{
			StateDisabled,
			StateDraft,
			StateDisabled,
		},
		SharingScopeFilters: []SharingScope{
			SharingScopePrivate,
			SharingScopeApp,
			SharingScopePrivate,
		},
	}

	got, err := NormalizeListRequest(testReadScope(), request)
	if err != nil {
		t.Fatalf("NormalizeListRequest: %v", err)
	}
	want := ListRequest{
		PageSize:           DefaultPageSize,
		PageToken:          "opaque-token",
		IncludeTotal:       true,
		AppIDFilter:        stringPointer(testApp),
		OwnerIDFilter:      stringPointer(testOwner),
		TextFilter:         stringPointer("needle"),
		SelectorTextFilter: stringPointer("prod-*"),
		ObjectTypeFilters: []ObjectType{
			ObjectTypeFieldAlias,
			ObjectTypeFieldExtraction,
		},
		StateFilters:        []State{StateDisabled, StateDraft},
		SharingScopeFilters: []SharingScope{SharingScopeApp, SharingScopePrivate},
		SortBy:              SortByName,
		SortDirection:       SortAscending,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeListRequest = %#v, want %#v", got, want)
	}

	appFilter = "attacker"
	ownerFilter = "attacker"
	textFilter = "attacker"
	selectorFilter = "attacker"
	request.ObjectTypeFilters[0] = ObjectTypeCalculatedField
	request.StateFilters[0] = StateActive
	request.SharingScopeFilters[0] = SharingScopeGlobal
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical request aliases caller mutation: %#v", got)
	}

	*got.AppIDFilter = "mutated"
	got.ObjectTypeFilters[0] = ObjectTypeCalculatedField
	again, err := NormalizeListRequest(testReadScope(), requestFromCanonicalWant(want))
	if err != nil {
		t.Fatalf("NormalizeListRequest(again): %v", err)
	}
	if !reflect.DeepEqual(again, want) {
		t.Fatalf("canonical results alias each other: %#v", again)
	}
}

func requestFromCanonicalWant(request ListRequest) ListRequest {
	return ListRequest{
		PageSize:            request.PageSize,
		PageToken:           request.PageToken,
		IncludeTotal:        request.IncludeTotal,
		AppIDFilter:         stringPointer(*request.AppIDFilter),
		OwnerIDFilter:       stringPointer(*request.OwnerIDFilter),
		TextFilter:          stringPointer(*request.TextFilter),
		ObjectTypeFilters:   append([]ObjectType(nil), request.ObjectTypeFilters...),
		StateFilters:        append([]State(nil), request.StateFilters...),
		SharingScopeFilters: append([]SharingScope(nil), request.SharingScopeFilters...),
		SelectorTextFilter:  stringPointer(*request.SelectorTextFilter),
		SortBy:              request.SortBy,
		SortDirection:       request.SortDirection,
	}
}
