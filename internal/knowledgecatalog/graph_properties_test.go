package knowledgecatalog

import (
	"encoding/base64"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestNormalizeDependencyListRequestReturnsDetachedCanonicalRequest(t *testing.T) {
	t.Parallel()
	version := uint64(7)
	request := DependencyListRequest{
		KnowledgeObjectID: "ko-normalized-graph",
		Version:           &version,
		PageToken:         "opaque-token",
		IncludeTotal:      true,
	}
	got, err := NormalizeDependencyListRequest(testReadScope(), request)
	if err != nil {
		t.Fatalf("NormalizeDependencyListRequest: %v", err)
	}
	want := DependencyListRequest{
		KnowledgeObjectID: "ko-normalized-graph",
		Version:           new(uint64(7)),
		PageSize:          DefaultPageSize,
		PageToken:         "opaque-token",
		IncludeTotal:      true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("NormalizeDependencyListRequest = %#v, want %#v", got, want)
	}
	version = 8
	if *got.Version != 7 {
		t.Fatal("canonical dependency request aliases caller version pointer")
	}
	*got.Version = 9
	again, err := NormalizeDependencyListRequest(testReadScope(), request)
	if err != nil || again.Version == got.Version || *again.Version != 8 {
		t.Fatalf("second canonical dependency request = %#v, %v", again, err)
	}

	maximum := uint64(math.MaxInt64) + 1
	for _, invalid := range []DependencyListRequest{
		{},
		{KnowledgeObjectID: " ko"},
		{KnowledgeObjectID: "ko", Version: new(uint64(0))},
		{KnowledgeObjectID: "ko", Version: &maximum},
		{KnowledgeObjectID: "ko", PageSize: MaximumPageSize + 1},
		{KnowledgeObjectID: "ko", PageToken: " cursor"},
		{KnowledgeObjectID: "ko", PageToken: strings.Repeat("x", maximumCursorBytes+1)},
	} {
		if _, err := NormalizeDependencyListRequest(testReadScope(), invalid); !errors.Is(err, control.ErrInvalidArgument) {
			t.Errorf("NormalizeDependencyListRequest(%#v) error = %v, want ErrInvalidArgument", invalid, err)
		}
	}
}

func TestGraphCursorPurposeFingerprintAndScalarBounds(t *testing.T) {
	t.Parallel()
	fingerprint := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	cursor := graphCursor{
		Fingerprint:        fingerprint,
		CatalogRevision:    9,
		CatalogState:       fingerprint,
		ResolvedObjectID:   "ko-root",
		ResolvedVersion:    2,
		LastSourceObjectID: "ko-source",
		LastSourceVersion:  3,
		LastTargetObjectID: "ko-target",
		LastTargetVersion:  4,
		LastDependencyRole: int32(DependencyRoleFieldInput),
	}
	token, err := encodeGraphCursor(testCursorKey, cursor)
	if err != nil {
		t.Fatalf("encodeGraphCursor: %v", err)
	}
	decoded, err := decodeGraphCursor(testCursorKey, token, fingerprint)
	if err != nil || decoded.ResolvedVersion != 2 || decoded.LastTargetVersion != 4 {
		t.Fatalf("decodeGraphCursor = %#v, %v", decoded, err)
	}
	if _, err := decodeGraphCursor(testCursorKey, token, base64.RawURLEncoding.EncodeToString(make([]byte, 31))); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("decodeGraphCursor(rebound) error = %v, want ErrInvalidCursor", err)
	}
	listToken, err := encodeCursor(testCursorKey, listCursor{
		Fingerprint:     fingerprint,
		CatalogRevision: 1,
		CatalogState:    fingerprint,
		PrimaryString:   "name",
		ObjectID:        "ko-list",
	})
	if err != nil {
		t.Fatalf("encode list cursor: %v", err)
	}
	if _, err := decodeGraphCursor(testCursorKey, listToken, fingerprint); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("decodeGraphCursor(list token) error = %v, want ErrInvalidCursor", err)
	}
	for _, mutate := range []func(*graphCursor){
		func(value *graphCursor) { value.ResolvedVersion = maximumVersionsPerTenant + 1 },
		func(value *graphCursor) { value.LastSourceVersion = maximumVersionsPerTenant + 1 },
		func(value *graphCursor) { value.LastTargetVersion = maximumVersionsPerTenant + 1 },
		func(value *graphCursor) { value.LastDependencyRole = 0 },
		func(value *graphCursor) { value.CatalogRevision = 0 },
	} {
		invalid := cursor
		invalid.Version = graphCursorVersion
		mutate(&invalid)
		if validGraphCursor(invalid) {
			t.Errorf("validGraphCursor accepted %#v", invalid)
		}
	}
}

func TestIncomingGraphQueryIsAuthorizationDrivenAndClipsPayloadScalars(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	normalized, err := normalizeDependencyListRequest(
		testReadScope(),
		DependencyListRequest{KnowledgeObjectID: "ko-plan-target", PageSize: 5},
		graphDirectionDependents,
	)
	if err != nil {
		t.Fatal(err)
	}
	root := versionRecord{
		TenantID: testTenant, KnowledgeObjectID: "ko-plan-target", ObjectVersion: 1,
	}
	compiled := incomingGraphCandidateReadQuery(
		database.GORMDB(),
		normalized,
		root,
		graphCursor{},
		6,
	).Session(&gorm.Session{DryRun: true}).Find(&[]graphDependencyReadRecord{})
	if compiled.Error != nil || compiled.Statement.SQL.Len() == 0 {
		t.Fatalf("compile incoming graph query: %v", compiled.Error)
	}
	query := compiled.Statement.SQL.String()
	for _, clipped := range []string{
		"CASE WHEN length(CAST(dependency.tenant_id AS BLOB))",
		"CASE WHEN length(CAST(dependency.source_object_id AS BLOB))",
		"CASE WHEN length(CAST(dependency.target_kind AS BLOB))",
		"CASE WHEN length(CAST(dependency.target_object_id AS BLOB))",
		"CASE WHEN length(CAST(dependency.dependency_role AS BLOB))",
	} {
		if !strings.Contains(query, clipped) {
			t.Fatalf("incoming graph SQL lacks clipped payload %q:\n%s", clipped, query)
		}
	}
	details := explainSQLiteQueryPlan(
		t,
		database.SQLDB(),
		query,
		append([]any(nil), compiled.Statement.Vars...),
	)
	joined := strings.Join(details, "\n")
	for _, required := range []string{
		"knowledge_objects_authorized_global_idx",
		"knowledge_objects_authorized_app_idx",
		"knowledge_objects_authorized_private_idx",
		"knowledge_object_dependencies_source_target_idx",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("incoming graph plan lacks %q:\n%s\nSQL:\n%s", required, joined, query)
		}
	}
	for _, forbidden := range []string{
		"SCAN dependency",
		"knowledge_object_dependencies_target_idx",
	} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("incoming graph plan contains forbidden %q:\n%s", forbidden, joined)
		}
	}
	sourceIndex := indexContaining(details, "authorized_source")
	dependencyIndex := indexContaining(details, "dependency USING")
	if sourceIndex < 0 || dependencyIndex <= sourceIndex {
		t.Fatalf("incoming graph plan is not authorization/current-source first:\n%s", joined)
	}
}
