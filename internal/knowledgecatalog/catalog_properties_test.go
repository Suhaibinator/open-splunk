package knowledgecatalog

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestCatalogAuthorizationTruthTableAcrossGetAndList(t *testing.T) {
	t.Parallel()

	database, store := newCatalogTestStore(t)
	fixtures := []struct {
		id      string
		name    string
		appID   string
		ownerID string
		scope   SharingScope
		visible bool
	}{
		{id: "ko-private-own-readable", name: "a_private_own_readable", appID: testApp, ownerID: testOwner, scope: SharingScopePrivate, visible: true},
		{id: "ko-private-other-readable", name: "b_private_other_readable", appID: testApp, ownerID: "owner-b", scope: SharingScopePrivate},
		{id: "ko-private-own-unreadable", name: "c_private_own_unreadable", appID: testAppTwo, ownerID: testOwner, scope: SharingScopePrivate},
		{id: "ko-app-other-readable", name: "d_app_other_readable", appID: testApp, ownerID: "owner-b", scope: SharingScopeApp, visible: true},
		{id: "ko-app-own-unreadable", name: "e_app_own_unreadable", appID: testAppTwo, ownerID: testOwner, scope: SharingScopeApp},
		{id: "ko-global-other-unreadable", name: "f_global_other_unreadable", appID: testAppTwo, ownerID: "owner-b", scope: SharingScopeGlobal, visible: true},
	}
	for index, fixture := range fixtures {
		insertFixtureObject(t, database, fixtureObject{
			id: fixture.id, owner: fixture.ownerID,
			versions: []fixtureVersion{{
				definition: aliasDefinition(fixture.appID, fixture.name, fixture.scope, nil, fixture.name+"-*"),
				state:      StateActive, mutation: "create", timestamp: int64(100 + index),
			}},
		})
	}

	page, err := store.List(context.Background(), testReadScope(), ListRequest{})
	if err != nil {
		t.Fatalf("List(): %v", err)
	}
	wantNames := []string{"a_private_own_readable", "d_app_other_readable", "f_global_other_unreadable"}
	if !slices.Equal(names(page.Objects), wantNames) {
		t.Fatalf("visible list names = %v, want %v", names(page.Objects), wantNames)
	}
	for _, fixture := range fixtures {
		got, getErr := store.Get(context.Background(), testReadScope(), fixture.id, nil)
		if fixture.visible {
			if getErr != nil || got.KnowledgeObjectID != fixture.id {
				t.Errorf("Get(%s) = %#v, %v; want visible", fixture.id, got, getErr)
			}
		} else if !errors.Is(getErr, control.ErrNotFound) {
			t.Errorf("Get(%s) error = %v, want policy-neutral ErrNotFound", fixture.id, getErr)
		}
	}
}

func TestCatalogScopeAndFilterNormalizationAreCanonicalAndDetached(t *testing.T) {
	t.Parallel()

	appFilter := " \t" + testApp + "\r\n"
	ownerFilter := "\v" + testOwner + "\f "
	textFilter := " needle "
	selectorFilter := " prod-* "
	scope := ReadScope{
		TenantID: testTenant,
		OwnerID:  testOwner,
		ReadableAppIDs: []string{
			testAppTwo,
			testApp,
			testAppTwo,
		},
	}
	request := ListRequest{
		PageSize:            17,
		IncludeTotal:        true,
		AppIDFilter:         &appFilter,
		OwnerIDFilter:       &ownerFilter,
		TextFilter:          &textFilter,
		ObjectTypeFilters:   []ObjectType{ObjectTypeFieldAlias, ObjectTypeCalculatedField, ObjectTypeFieldAlias},
		StateFilters:        []State{StateDeleted, StateActive, StateDeleted},
		SharingScopeFilters: []SharingScope{SharingScopeGlobal, SharingScopePrivate, SharingScopeGlobal},
		SelectorTextFilter:  &selectorFilter,
		SortBy:              SortByUpdatedAt,
		SortDirection:       SortDescending,
	}
	normalized, err := normalizeListRequest(scope, request)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(normalized.scope.readableAppIDs, []string{testApp, testAppTwo}) ||
		!slices.Equal(normalized.objectTypeFilters, []ObjectType{ObjectTypeCalculatedField, ObjectTypeFieldAlias}) ||
		!slices.Equal(normalized.stateFilters, []State{StateActive, StateDeleted}) ||
		!slices.Equal(normalized.sharingScopeFilters, []SharingScope{SharingScopeGlobal, SharingScopePrivate}) ||
		normalized.textFilter == nil || *normalized.textFilter != "needle" ||
		normalized.selectorTextFilter == nil || *normalized.selectorTextFilter != "prod-*" {
		t.Fatalf("normalized request = %#v", normalized)
	}

	canonicalScope := ReadScope{TenantID: testTenant, OwnerID: testOwner, ReadableAppIDs: []string{testApp, testAppTwo}}
	canonicalAppFilter := testApp
	canonicalOwnerFilter := testOwner
	canonicalRequest := ListRequest{
		PageSize: 17, IncludeTotal: true,
		AppIDFilter: &canonicalAppFilter, OwnerIDFilter: &canonicalOwnerFilter,
		TextFilter: new("needle"), SelectorTextFilter: new("prod-*"),
		ObjectTypeFilters: []ObjectType{ObjectTypeCalculatedField, ObjectTypeFieldAlias},
		StateFilters:      []State{StateActive, StateDeleted},
		SharingScopeFilters: []SharingScope{
			SharingScopeGlobal, SharingScopePrivate,
		},
		SortBy: SortByUpdatedAt, SortDirection: SortDescending,
	}
	canonical, err := normalizeListRequest(canonicalScope, canonicalRequest)
	if err != nil {
		t.Fatal(err)
	}
	one, err := requestFingerprint(normalized)
	if err != nil {
		t.Fatal(err)
	}
	two, err := requestFingerprint(canonical)
	if err != nil {
		t.Fatal(err)
	}
	if one != two {
		t.Fatalf("semantically equal requests have fingerprints %q and %q", one, two)
	}

	// Mutating every caller-owned slice and source variable after normalization
	// cannot change the normalized authority or its fingerprint.
	scope.ReadableAppIDs[0] = "attacker-app"
	request.ObjectTypeFilters[0] = "attacker-type"
	request.StateFilters[0] = "attacker-state"
	request.SharingScopeFilters[0] = "attacker-scope"
	appFilter, ownerFilter, textFilter, selectorFilter = "attacker", "attacker", "attacker", "attacker"
	after, err := requestFingerprint(normalized)
	if err != nil {
		t.Fatal(err)
	}
	if after != one || *normalized.appIDFilter != testApp || *normalized.ownerIDFilter != testOwner ||
		*normalized.textFilter != "needle" || *normalized.selectorTextFilter != "prod-*" {
		t.Fatalf("normalized request retained caller authority: %#v", normalized)
	}
}

func TestPublicListRequestValidatorPinsStoreBoundsWithoutMutation(t *testing.T) {
	t.Parallel()

	appFilter := " \t" + testApp + "\r\n"
	request := ListRequest{
		PageSize:          MaximumPageSize,
		AppIDFilter:       &appFilter,
		ObjectTypeFilters: []ObjectType{ObjectTypeFieldAlias, ObjectTypeFieldAlias},
		StateFilters:      []State{StateDisabled, StateDraft},
	}
	beforeApps := slices.Clone(request.ObjectTypeFilters)
	beforeStates := slices.Clone(request.StateFilters)
	if err := ValidateListRequest(testReadScope(), request); err != nil {
		t.Fatalf("ValidateListRequest(valid): %v", err)
	}
	if !slices.Equal(request.ObjectTypeFilters, beforeApps) ||
		!slices.Equal(request.StateFilters, beforeStates) ||
		request.AppIDFilter == nil || *request.AppIDFilter != appFilter {
		t.Fatalf("ValidateListRequest mutated caller authority: %#v", request)
	}

	invalid := []ListRequest{
		{PageSize: MaximumPageSize + 1},
		{PageToken: " cursor-with-whitespace "},
		{AppIDFilter: new("")},
		{TextFilter: new("\x7f")},
		{ObjectTypeFilters: []ObjectType{
			ObjectTypeFieldAlias,
			ObjectTypeFieldExtraction,
			ObjectTypeCalculatedField,
			ObjectTypeFieldAlias,
		}},
	}
	for index, candidate := range invalid {
		if err := ValidateListRequest(testReadScope(), candidate); !errors.Is(
			err,
			control.ErrInvalidArgument,
		) {
			t.Errorf(
				"ValidateListRequest(invalid %d) = %v, want ErrInvalidArgument",
				index,
				err,
			)
		}
	}
}

func TestCatalogFingerprintBindsEverySemanticInputButNotToken(t *testing.T) {
	t.Parallel()

	baseScope := ReadScope{TenantID: "tenant-a", OwnerID: "owner-a", ReadableAppIDs: []string{"app-a", "app-b"}}
	base := ListRequest{
		PageSize: 7, IncludeTotal: true,
		AppIDFilter: new("app-a"), OwnerIDFilter: new("owner-a"),
		TextFilter: new("needle"), SelectorTextFilter: new("prod"),
		ObjectTypeFilters: []ObjectType{ObjectTypeFieldAlias},
		StateFilters:      []State{StateActive},
		SharingScopeFilters: []SharingScope{
			SharingScopePrivate,
		},
		SortBy: SortByName, SortDirection: SortAscending,
	}
	fingerprint := func(t *testing.T, scope ReadScope, request ListRequest) string {
		t.Helper()
		normalized, err := normalizeListRequest(scope, request)
		if err != nil {
			t.Fatalf("normalize: %v", err)
		}
		value, err := requestFingerprint(normalized)
		if err != nil {
			t.Fatalf("fingerprint: %v", err)
		}
		return value
	}
	want := fingerprint(t, baseScope, base)

	equivalentScope := baseScope
	equivalentScope.ReadableAppIDs = []string{"app-b", "app-a", "app-b"}
	equivalent := base
	equivalent.ObjectTypeFilters = []ObjectType{ObjectTypeFieldAlias, ObjectTypeFieldAlias}
	equivalent.StateFilters = []State{StateActive, StateActive}
	equivalent.SharingScopeFilters = []SharingScope{SharingScopePrivate, SharingScopePrivate}
	equivalent.PageToken = "opaque-token-is-not-request-semantics"
	if got := fingerprint(t, equivalentScope, equivalent); got != want {
		t.Fatalf("canonical permutation/token fingerprint = %q, want %q", got, want)
	}

	variants := []struct {
		name    string
		scope   ReadScope
		request ListRequest
	}{
		{name: "tenant", scope: ReadScope{TenantID: "tenant-b", OwnerID: "owner-a", ReadableAppIDs: []string{"app-a", "app-b"}}, request: base},
		{name: "owner", scope: ReadScope{TenantID: "tenant-a", OwnerID: "owner-b", ReadableAppIDs: []string{"app-a", "app-b"}}, request: base},
		{name: "readable apps", scope: ReadScope{TenantID: "tenant-a", OwnerID: "owner-a", ReadableAppIDs: []string{"app-a"}}, request: base},
	}
	appendVariant := func(name string, mutate func(*ListRequest)) {
		candidate := base
		candidate.ObjectTypeFilters = slices.Clone(base.ObjectTypeFilters)
		candidate.StateFilters = slices.Clone(base.StateFilters)
		candidate.SharingScopeFilters = slices.Clone(base.SharingScopeFilters)
		mutate(&candidate)
		variants = append(variants, struct {
			name    string
			scope   ReadScope
			request ListRequest
		}{name: name, scope: baseScope, request: candidate})
	}
	appendVariant("page size", func(r *ListRequest) { r.PageSize++ })
	appendVariant("include total", func(r *ListRequest) { r.IncludeTotal = false })
	appendVariant("app filter", func(r *ListRequest) { r.AppIDFilter = new("app-b") })
	appendVariant("owner filter", func(r *ListRequest) { r.OwnerIDFilter = new("owner-b") })
	appendVariant("text", func(r *ListRequest) { r.TextFilter = new("other") })
	appendVariant("selector", func(r *ListRequest) { r.SelectorTextFilter = new("other") })
	appendVariant("object types", func(r *ListRequest) { r.ObjectTypeFilters = []ObjectType{ObjectTypeCalculatedField} })
	appendVariant("states", func(r *ListRequest) { r.StateFilters = []State{StateDisabled} })
	appendVariant("sharing scopes", func(r *ListRequest) { r.SharingScopeFilters = []SharingScope{SharingScopeApp} })
	appendVariant("sort field", func(r *ListRequest) { r.SortBy = SortByCreatedAt })
	appendVariant("sort direction", func(r *ListRequest) { r.SortDirection = SortDescending })

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			t.Parallel()
			if got := fingerprint(t, variant.scope, variant.request); got == want {
				t.Fatalf("semantic variant retained fingerprint %q", got)
			}
		})
	}
}

func TestCatalogIdentityFilterAndVersionBoundaries(t *testing.T) {
	t.Parallel()

	valid255 := strings.Repeat("x", maximumTenantIDBytes)
	invalidUTF8 := string([]byte{0xff})
	for _, test := range []struct {
		name    string
		value   string
		maximum int
		valid   bool
	}{
		{name: "one", value: "x", maximum: 1, valid: true},
		{name: "exact maximum", value: valid255, maximum: maximumTenantIDBytes, valid: true},
		{name: "over maximum", value: valid255 + "x", maximum: maximumTenantIDBytes},
		{name: "empty", maximum: maximumTenantIDBytes},
		{name: "leading ascii space", value: " x", maximum: maximumTenantIDBytes},
		{name: "trailing ascii newline", value: "x\n", maximum: maximumTenantIDBytes},
		{name: "embedded nul", value: "x\x00y", maximum: maximumTenantIDBytes},
		{name: "embedded c0", value: "x\x1fy", maximum: maximumTenantIDBytes},
		{name: "embedded c1", value: "x\u0085y", maximum: maximumTenantIDBytes},
		{name: "invalid utf8", value: invalidUTF8, maximum: maximumTenantIDBytes},
		{name: "unicode", value: "ténant", maximum: maximumTenantIDBytes, valid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := validIdentity(test.value, test.maximum); got != test.valid {
				t.Fatalf("validIdentity(%q, %d) = %t, want %t", test.value, test.maximum, got, test.valid)
			}
		})
	}

	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-version-edge", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "version_edge", SharingScopePrivate, nil, "edge-*"),
		state:      StateActive, mutation: "create", timestamp: 10,
	}}})
	zero := uint64(0)
	overSigned := uint64(math.MaxInt64) + 1
	maximum := ^uint64(0)
	for _, version := range []*uint64{&zero, &overSigned, &maximum} {
		if _, err := store.Get(context.Background(), testReadScope(), "ko-version-edge", version); !errors.Is(err, control.ErrInvalidArgument) {
			t.Errorf("Get(version %d) error = %v, want ErrInvalidArgument", *version, err)
		}
	}
	next := uint64(2)
	if _, err := store.Get(context.Background(), testReadScope(), "ko-version-edge", &next); !errors.Is(err, control.ErrNotFound) {
		t.Errorf("Get(version beyond current) error = %v, want ErrNotFound", err)
	}
}

func TestCatalogTextAndSelectorFiltersUseExactBinaryContainment(t *testing.T) {
	t.Parallel()

	database, store := newCatalogTestStore(t)
	description := "Needle café"
	insertFixtureObject(t, database, fixtureObject{id: "ko-binary-filter", owner: testOwner, versions: []fixtureVersion{{
		definition: aliasDefinition(testApp, "binary_filter", SharingScopePrivate, &description, "Prod-Éast-*"),
		state:      StateActive, mutation: "create", timestamp: 10,
	}}})
	assertNames := func(label string, request ListRequest, want []string) {
		t.Helper()
		page, err := store.List(context.Background(), testReadScope(), request)
		if err != nil || !slices.Equal(names(page.Objects), want) {
			t.Fatalf("%s names = %v, error = %v, want %v", label, names(page.Objects), err, want)
		}
	}
	assertNames("text exact", ListRequest{TextFilter: new("Needle")}, []string{"binary_filter"})
	assertNames("text case", ListRequest{TextFilter: new("needle")}, nil)
	assertNames("text unicode bytes", ListRequest{TextFilter: new("café")}, []string{"binary_filter"})
	assertNames("text unicode case", ListRequest{TextFilter: new("CAFÉ")}, nil)
	assertNames("selector exact", ListRequest{SelectorTextFilter: new("Prod-É")}, []string{"binary_filter"})
	assertNames("selector case", ListRequest{SelectorTextFilter: new("prod-é")}, nil)
}

func TestCatalogCursorShapeMatrixAndAdversarialTokens(t *testing.T) {
	t.Parallel()

	fingerprint := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	integer := int64(42)
	tests := []struct {
		name   string
		sortBy SortBy
		cursor listCursor
	}{
		{name: "name", sortBy: SortByName, cursor: listCursor{PrimaryString: "name"}},
		{name: "created", sortBy: SortByCreatedAt, cursor: listCursor{PrimaryInteger: &integer}},
		{name: "updated", sortBy: SortByUpdatedAt, cursor: listCursor{PrimaryInteger: &integer}},
		{name: "object type", sortBy: SortByObjectType, cursor: listCursor{PrimaryString: "field_alias", SecondaryString: "name"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cursor := test.cursor
			cursor.Fingerprint = fingerprint
			cursor.CatalogRevision = 1
			cursor.CatalogState = fingerprint
			cursor.ObjectID = "ko-cursor"
			token, err := encodeCursor(testCursorKey, cursor)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeCursor(testCursorKey, token, fingerprint, test.sortBy)
			if err != nil || decoded.ObjectID != cursor.ObjectID || decoded.CatalogRevision != 1 {
				t.Fatalf("decodeCursor() = %#v, %v", decoded, err)
			}
			otherKey := append([]byte(nil), testCursorKey...)
			otherKey[0] ^= 0xff
			if _, err := decodeCursor(otherKey, token, fingerprint, test.sortBy); !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("wrong-key error = %v, want ErrInvalidCursor", err)
			}
			if _, err := decodeCursor(testCursorKey, token, "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", test.sortBy); !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("rebound fingerprint error = %v, want ErrInvalidCursor", err)
			}
			for index := range token {
				mutated := []byte(token)
				mutated[index] ^= 1
				if string(mutated) == token {
					continue
				}
				if _, err := decodeCursor(testCursorKey, string(mutated), fingerprint, test.sortBy); err == nil {
					t.Fatalf("single-byte mutation at %d authenticated", index)
				}
			}
		})
	}

	base := listCursor{Version: cursorVersion, Fingerprint: fingerprint, CatalogRevision: 1, CatalogState: fingerprint, PrimaryString: "name", ObjectID: "ko"}
	invalid := []listCursor{
		{Version: cursorVersion, Fingerprint: fingerprint, CatalogRevision: 0, CatalogState: fingerprint, PrimaryString: "name", ObjectID: "ko"},
		{Version: cursorVersion, Fingerprint: fingerprint, CatalogRevision: -1, CatalogState: fingerprint, PrimaryString: "name", ObjectID: "ko"},
		{Version: cursorVersion, Fingerprint: "not-base64", CatalogRevision: 1, CatalogState: fingerprint, PrimaryString: "name", ObjectID: "ko"},
		{Version: cursorVersion, Fingerprint: fingerprint + "=", CatalogRevision: 1, CatalogState: fingerprint, PrimaryString: "name", ObjectID: "ko"},
		{Version: cursorVersion, Fingerprint: fingerprint, CatalogRevision: 1, CatalogState: "not-base64", PrimaryString: "name", ObjectID: "ko"},
		{Version: cursorVersion, Fingerprint: fingerprint, CatalogRevision: 1, CatalogState: fingerprint + "=", PrimaryString: "name", ObjectID: "ko"},
		{Version: cursorVersion, Fingerprint: fingerprint, CatalogRevision: 1, CatalogState: fingerprint, PrimaryString: "name", ObjectID: ""},
		{Version: cursorVersion, Fingerprint: fingerprint, CatalogRevision: 1, CatalogState: fingerprint, PrimaryString: "name", ObjectID: " ko"},
	}
	for _, cursor := range invalid {
		if validCursor(cursor) {
			t.Errorf("validCursor(%#v) = true", cursor)
		}
	}
	if !validCursor(base) {
		t.Fatal("valid cursor rejected")
	}

	validToken, err := encodeCursor(testCursorKey, base)
	if err != nil {
		t.Fatal(err)
	}
	for _, wrongSort := range []SortBy{SortByCreatedAt, SortByUpdatedAt, SortByObjectType, "future"} {
		if _, err := decodeCursor(testCursorKey, validToken, fingerprint, wrongSort); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("decode name cursor as %q error = %v, want ErrInvalidCursor", wrongSort, err)
		}
	}
}
