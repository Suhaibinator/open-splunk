package knowledgecatalog

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
)

func FuzzCatalogFingerprintCanonicalizesSetLikeInputs(f *testing.F) {
	f.Add([]byte{}, uint8(0), uint8(0), uint8(0), uint8(0), false)
	f.Add([]byte{3, 1, 3, 2}, uint8(2), uint8(4), uint8(2), uint8(3), true)
	f.Add([]byte{255, 0, 1}, uint8(1), uint8(1), uint8(1), uint8(1), false)

	f.Fuzz(func(t *testing.T, appSeeds []byte, typeSeed, stateSeed, scopeSeed, sortSeed uint8, includeTotal bool) {
		if len(appSeeds) > maximumReadableApps {
			t.Skip()
		}
		apps := make([]string, 0, len(appSeeds)+1)
		for _, seed := range appSeeds {
			apps = append(apps, fmt.Sprintf("app-%03d", seed))
		}
		if len(apps) == 0 {
			apps = append(apps, "app-000")
		}
		objectTypes := [...]ObjectType{ObjectTypeFieldExtraction, ObjectTypeFieldAlias, ObjectTypeCalculatedField}
		states := [...]State{StateDraft, StateActive, StateDisabled, StateQuarantined, StateDeleted}
		scopes := [...]SharingScope{SharingScopePrivate, SharingScopeApp, SharingScopeGlobal}
		sorts := [...]SortBy{SortByName, SortByCreatedAt, SortByUpdatedAt, SortByObjectType}
		directions := [...]SortDirection{SortAscending, SortDescending}
		request := ListRequest{
			PageSize:            1 + uint32(sortSeed)%uint32(MaximumPageSize),
			IncludeTotal:        includeTotal,
			ObjectTypeFilters:   []ObjectType{objectTypes[int(typeSeed)%len(objectTypes)]},
			StateFilters:        []State{states[int(stateSeed)%len(states)]},
			SharingScopeFilters: []SharingScope{scopes[int(scopeSeed)%len(scopes)]},
			SortBy:              sorts[int(sortSeed)%len(sorts)],
			SortDirection:       directions[int(sortSeed)%len(directions)],
		}
		scope := ReadScope{TenantID: "tenant", OwnerID: "owner", ReadableAppIDs: apps}
		one, err := normalizeListRequest(scope, request)
		if err != nil {
			t.Fatalf("normalize original: %v", err)
		}
		oneFingerprint, err := requestFingerprint(one)
		if err != nil {
			t.Fatal(err)
		}

		permutedApps := slices.Clone(apps)
		slices.Reverse(permutedApps)
		permutedApps = append(permutedApps, apps[0])
		if len(permutedApps) > maximumReadableApps {
			permutedApps = permutedApps[:maximumReadableApps]
		}
		permuted := request
		permuted.ObjectTypeFilters = []ObjectType{request.ObjectTypeFilters[0], request.ObjectTypeFilters[0]}
		permuted.StateFilters = []State{request.StateFilters[0], request.StateFilters[0]}
		permuted.SharingScopeFilters = []SharingScope{request.SharingScopeFilters[0], request.SharingScopeFilters[0]}
		two, err := normalizeListRequest(ReadScope{
			TenantID: scope.TenantID, OwnerID: scope.OwnerID, ReadableAppIDs: permutedApps,
		}, permuted)
		if err != nil {
			t.Fatalf("normalize permutation: %v", err)
		}
		twoFingerprint, err := requestFingerprint(two)
		if err != nil {
			t.Fatal(err)
		}
		if oneFingerprint != twoFingerprint {
			t.Fatalf("set permutation changed fingerprint: %q != %q", oneFingerprint, twoFingerprint)
		}
	})
}

func FuzzCatalogCursorRoundTripPreservesTypedKeyset(f *testing.F) {
	f.Add(uint8(0), int64(1), []byte("name"), []byte("object"))
	f.Add(uint8(1), int64(-1), []byte{}, []byte{0, 1, 2})
	f.Add(uint8(2), int64(253402300799999999), []byte("updated"), []byte("id"))
	f.Add(uint8(3), int64(42), []byte("field_alias"), []byte("secondary"))

	f.Fuzz(func(t *testing.T, sortSeed uint8, primaryInteger int64, primarySeed, objectSeed []byte) {
		if len(primarySeed) > 1<<10 || len(objectSeed) > 1<<10 {
			t.Skip()
		}
		fingerprint := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		primaryDigest := sha256.Sum256(primarySeed)
		objectDigest := sha256.Sum256(objectSeed)
		primary := hex.EncodeToString(primaryDigest[:])
		objectID := "ko-" + hex.EncodeToString(objectDigest[:])
		cursor := listCursor{
			Fingerprint: fingerprint, CatalogRevision: 1, CatalogState: fingerprint, ObjectID: objectID,
		}
		sortBy := SortByName
		switch sortSeed % 4 {
		case 0:
			cursor.PrimaryString = primary
		case 1:
			sortBy = SortByCreatedAt
			cursor.PrimaryInteger = &primaryInteger
		case 2:
			sortBy = SortByUpdatedAt
			cursor.PrimaryInteger = &primaryInteger
		case 3:
			sortBy = SortByObjectType
			cursor.PrimaryString = string(ObjectTypeFieldAlias)
			cursor.SecondaryString = primary
		}
		token, err := encodeCursor(testCursorKey, cursor)
		if err != nil {
			t.Fatalf("encodeCursor: %v", err)
		}
		decoded, err := decodeCursor(testCursorKey, token, fingerprint, sortBy)
		if err != nil {
			t.Fatalf("decodeCursor: %v", err)
		}
		if decoded.CatalogRevision != cursor.CatalogRevision || decoded.CatalogState != cursor.CatalogState ||
			decoded.ObjectID != cursor.ObjectID ||
			decoded.PrimaryString != cursor.PrimaryString || decoded.SecondaryString != cursor.SecondaryString ||
			!equalCatalogCursorInteger(decoded.PrimaryInteger, cursor.PrimaryInteger) {
			t.Fatalf("cursor round trip = %#v, want %#v", decoded, cursor)
		}
		if len(token) != 0 {
			mutated := []byte(token)
			mutated[len(mutated)/2] ^= 1
			if _, err := decodeCursor(testCursorKey, string(mutated), fingerprint, sortBy); !errors.Is(err, ErrInvalidCursor) {
				t.Fatalf("mutated token error = %v, want ErrInvalidCursor", err)
			}
		}
	})
}

func FuzzBoundedListResponseRecordsMatchesReferenceModel(f *testing.F) {
	f.Add(uint8(1), []byte{})
	f.Add(uint8(2), []byte{2, 0, 2, 0})
	f.Add(uint8(2), []byte{3, 0, 3, 0})
	dependencyBoundary := make([]byte, 65*2)
	for index := 0; index < len(dependencyBoundary); index += 2 {
		dependencyBoundary[index] = 0
		dependencyBoundary[index+1] = 2
	}
	f.Add(uint8(255), dependencyBoundary)

	f.Fuzz(func(t *testing.T, pageSeed uint8, raw []byte) {
		if len(raw) > 2*(MaximumPageSize+1) {
			t.Skip()
		}
		pageSize := 1 + int(pageSeed)%MaximumPageSize
		count := min(len(raw)/2, pageSize+1)
		records := make([]projectionRecord, count)
		definitionCharges := [...]int64{
			1,
			maximumDefinitionBytes,
			MaximumListResponseCanonicalDefinitionBytes / 2,
			MaximumListResponseCanonicalDefinitionBytes/2 + 1,
			1024,
		}
		dependencyCharges := [...]int64{0, 1, maximumDependenciesPerVersion, maximumDependenciesPerVersion / 2}
		for index := range records {
			definitionSeed := raw[index*2]
			dependencySeed := raw[index*2+1]
			if definitionSeed%11 == 0 {
				records[index] = projectionRecord{State: StateQuarantined}
			} else {
				records[index] = projectionRecord{
					State:           StateActive,
					DefinitionBytes: definitionCharges[int(definitionSeed)%len(definitionCharges)],
				}
			}
			records[index].DependencyCount = dependencyCharges[int(dependencySeed)%len(dependencyCharges)]
		}

		wantCount := 0
		var definitions, dependencies int64
		for wantCount < len(records) && wantCount < pageSize {
			record := records[wantCount]
			charge := record.DefinitionBytes
			if record.State == StateQuarantined {
				charge = 0
			}
			if wantCount > 0 && (definitions > MaximumListResponseCanonicalDefinitionBytes-charge ||
				dependencies > MaximumListResponseDependencies-record.DependencyCount) {
				break
			}
			definitions += charge
			dependencies += record.DependencyCount
			wantCount++
		}

		returned, more, err := boundedListResponseRecords(records, pageSize)
		if err != nil {
			t.Fatalf("boundedListResponseRecords(valid model input): %v", err)
		}
		if len(returned) != wantCount || cap(returned) != wantCount || more != (wantCount < len(records)) {
			t.Fatalf("shape = len/cap %d/%d more=%t, want %d/%d more=%t",
				len(returned), cap(returned), more, wantCount, wantCount, wantCount < len(records))
		}
		for index := range returned {
			if !reflect.DeepEqual(returned[index], records[index]) {
				t.Fatalf("returned record %d is not the input prefix", index)
			}
		}
	})
}

func equalCatalogCursorInteger(left, right *int64) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
