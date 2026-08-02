package indexread

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestNormalizeScopeClonesSortsAndDeduplicates(t *testing.T) {
	t.Parallel()

	tenantStorage := []byte("tenant-a")
	tenantID := string(tenantStorage)
	indexStorage := []byte("index-b")
	indexName := string(indexStorage)
	input := []string{indexName, "index-a", indexName}

	scope, err := NormalizeScope(tenantID, input)
	if err != nil {
		t.Fatalf("NormalizeScope(): %v", err)
	}
	tenantStorage[0] = 'x'
	indexStorage[0] = 'x'
	input[1] = "index-c"

	if scope.TenantID != "tenant-a" {
		t.Fatalf("TenantID = %q, want tenant-a", scope.TenantID)
	}
	if want := []string{"index-a", "index-b"}; !slices.Equal(
		scope.IndexNames,
		want,
	) {
		t.Fatalf("IndexNames = %v, want %v", scope.IndexNames, want)
	}
}

func TestNormalizeScopeRejectsMalformedOrUnboundedInput(t *testing.T) {
	t.Parallel()

	tooMany := make([]string, MaximumIndexesPerScope+1)
	for index := range tooMany {
		tooMany[index] = "main"
	}
	for _, test := range []struct {
		name    string
		tenant  string
		indexes []string
	}{
		{name: "tenant", tenant: " tenant-a ", indexes: []string{"main"}},
		{name: "empty", tenant: "tenant-a"},
		{name: "noncanonical", tenant: "tenant-a", indexes: []string{"Main"}},
		{name: "too many", tenant: "tenant-a", indexes: tooMany},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if scope, err := NormalizeScope(
				test.tenant,
				test.indexes,
			); !errors.Is(err, ErrInvalidArgument) ||
				scope.TenantID != "" || scope.IndexNames != nil {
				t.Fatalf(
					"NormalizeScope() = (%#v, %v), want empty ErrInvalidArgument",
					scope,
					err,
				)
			}
		})
	}
}

func TestValidateTenantIDUsesTheSharedBoundedContract(t *testing.T) {
	t.Parallel()

	if err := ValidateTenantID("tenant-a"); err != nil {
		t.Fatalf("ValidateTenantID(valid): %v", err)
	}
	for _, value := range []string{
		"",
		" tenant-a",
		"tenant\na",
		string([]byte{0xff}),
		strings.Repeat("t", maximumTenantIDBytes+1),
	} {
		if err := ValidateTenantID(value); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf(
				"ValidateTenantID(%q) error = %v, want ErrInvalidArgument",
				value,
				err,
			)
		}
	}
}
