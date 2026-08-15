package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestCatalogFilterBoundsApplyAfterASCIITrim(t *testing.T) {
	t.Parallel()

	for _, boundary := range []struct {
		kind    string
		maximum int
	}{
		{kind: "text", maximum: maximumFilterBytes},
		{kind: "selector", maximum: maximumFilterBytes},
		{kind: "app", maximum: maximumAppIDBytes},
		{kind: "owner", maximum: maximumOwnerIDBytes},
	} {
		exact := strings.Repeat("x", boundary.maximum)
		over := exact + "x"
		for _, test := range []struct {
			name        string
			value       string
			want        string
			wantInvalid bool
		}{
			{name: "exact", value: exact, want: exact},
			{name: "max-plus-one", value: over, wantInvalid: true},
			{
				name:  "padded-exact",
				value: "\t\n\v\f\r " + exact + " \r\f\v\n\t",
				want:  exact,
			},
			{
				name:        "padded-max-plus-one",
				value:       "\t " + over + " \r",
				wantInvalid: true,
			},
		} {
			t.Run(fmt.Sprintf("%s/%s", boundary.kind, test.name), func(t *testing.T) {
				t.Parallel()
				request := ListRequest{}
				privacyContractSetFilter(&request, boundary.kind, &test.value)
				normalized, err := normalizeListRequest(testReadScope(), request)
				if test.wantInvalid {
					if !errors.Is(err, control.ErrInvalidArgument) {
						t.Fatalf("normalizeListRequest(%d raw bytes) error = %v, want ErrInvalidArgument", len(test.value), err)
					}
					return
				}
				if err != nil {
					t.Fatalf("normalizeListRequest(%d raw bytes): %v", len(test.value), err)
				}
				got := privacyContractNormalizedFilter(normalized, boundary.kind)
				if got == nil || *got != test.want || len(*got) != boundary.maximum {
					t.Fatalf("normalized %s = %q (%d bytes), want %d-byte canonical value", boundary.kind, privacyContractString(got), len(privacyContractString(got)), boundary.maximum)
				}
			})
		}
	}
}

func TestCatalogFiltersRejectControlsRemainingAfterASCIITrim(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"text", "selector", "app", "owner"} {
		for _, test := range []struct {
			name  string
			value string
		}{
			{name: "empty-after-trim", value: "\t\n\v\f\r "},
			{name: "embedded-nul", value: "safe\x00unsafe"},
			{name: "embedded-c0", value: "safe\x1funsafe"},
			{name: "embedded-c1", value: "safe\u0085unsafe"},
			{name: "invalid-utf8", value: string([]byte{'s', 0xff})},
		} {
			t.Run(kind+"/"+test.name, func(t *testing.T) {
				t.Parallel()
				request := ListRequest{}
				privacyContractSetFilter(&request, kind, &test.value)
				if _, err := normalizeListRequest(testReadScope(), request); !errors.Is(err, control.ErrInvalidArgument) {
					t.Fatalf("normalizeListRequest(%q) error = %v, want ErrInvalidArgument", test.value, err)
				}
			})
		}
	}
}

func TestCatalogPaddedFiltersMatchCanonicalStoredValues(t *testing.T) {
	t.Parallel()

	database, store := newCatalogTestStore(t)
	description := "Needle café"
	insertFixtureObject(t, database, fixtureObject{id: "ko-padded-filter", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "padded_filter", SharingScopePrivate, &description, "Prod-Éast-*"),
		state:      StateActive, mutation: "create", timestamp: 10,
	}}})
	app, owner := "\t"+testApp+"\r ", " \v"+testOwner+"\f"
	text, selector := "\n Needle \r", "\tProd-É \f"
	page, err := store.List(context.Background(), testReadScope(), ListRequest{
		PageSize: 1, IncludeTotal: true,
		AppIDFilter: &app, OwnerIDFilter: &owner,
		TextFilter: &text, SelectorTextFilter: &selector,
	})
	if err != nil {
		t.Fatalf("List(padded canonical filters): %v", err)
	}
	if !slices.Equal(names(page.Objects), []string{"padded_filter"}) ||
		page.TotalSize == nil || *page.TotalSize != 1 || !page.TotalSizeExact {
		t.Fatalf("List(padded canonical filters) = %#v", page)
	}
}

func privacyContractSetFilter(request *ListRequest, kind string, value *string) {
	switch kind {
	case "text":
		request.TextFilter = value
	case "selector":
		request.SelectorTextFilter = value
	case "app":
		request.AppIDFilter = value
	case "owner":
		request.OwnerIDFilter = value
	default:
		panic("unknown privacy contract filter kind: " + kind)
	}
}

func privacyContractNormalizedFilter(request normalizedListRequest, kind string) *string {
	switch kind {
	case "text":
		return request.textFilter
	case "selector":
		return request.selectorTextFilter
	case "app":
		return request.appIDFilter
	case "owner":
		return request.ownerIDFilter
	default:
		panic("unknown privacy contract filter kind: " + kind)
	}
}

func privacyContractString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
