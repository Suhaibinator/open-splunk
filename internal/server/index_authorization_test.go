package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

type batchIndexAuthorizationCatalog struct {
	fakeIndexCatalog
	batchResult []control.Index
	batchErr    error
	batchCalls  [][]string
	singleCalls int
	mutateInput bool
}

func (catalog *batchIndexAuthorizationCatalog) GetIndexByName(
	ctx context.Context,
	name string,
) (control.Index, error) {
	catalog.singleCalls++
	return catalog.fakeIndexCatalog.GetIndexByName(ctx, name)
}

func (catalog *batchIndexAuthorizationCatalog) GetIndexesByNames(
	_ context.Context,
	names []string,
) ([]control.Index, error) {
	catalog.batchCalls = append(catalog.batchCalls, slices.Clone(names))
	if catalog.mutateInput && len(names) != 0 {
		names[0] = "private"
	}
	return slices.Clone(catalog.batchResult), catalog.batchErr
}

func TestAuthorizeRequestedIndexesUsesBatchCatalog(t *testing.T) {
	t.Parallel()

	requested := []string{"secondary", "main"}
	catalog := &batchIndexAuthorizationCatalog{
		batchResult: []control.Index{
			validationTestIndex("secondary"),
			validationTestIndex("main"),
		},
	}
	handler := &apiHandler{indexes: catalog}

	if err := handler.authorizeRequestedIndexes(context.Background(), requested); err != nil {
		t.Fatalf("authorizeRequestedIndexes() error = %v", err)
	}
	if catalog.singleCalls != 0 {
		t.Fatalf("GetIndexByName() calls = %d, want 0", catalog.singleCalls)
	}
	if len(catalog.batchCalls) != 1 || !slices.Equal(catalog.batchCalls[0], requested) {
		t.Fatalf("GetIndexesByNames() calls = %#v, want [%#v]", catalog.batchCalls, requested)
	}
}

func TestAuthorizeRequestedIndexesBatchFailsClosed(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		result  []control.Index
		err     error
		wantErr error
	}{
		{
			name:    "missing index",
			err:     control.ErrNotFound,
			wantErr: errIndexUnavailable,
		},
		{
			name:    "incomplete catalog result",
			result:  []control.Index{validationTestIndex("main")},
			wantErr: errInvalidIndexCatalogBatch,
		},
		{
			name: "wrong result order",
			result: []control.Index{
				validationTestIndex("secondary"),
				validationTestIndex("main"),
			},
			wantErr: errInvalidIndexCatalogBatch,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			catalog := &batchIndexAuthorizationCatalog{
				batchResult: test.result,
				batchErr:    test.err,
			}
			handler := &apiHandler{indexes: catalog}
			err := handler.authorizeRequestedIndexes(
				context.Background(),
				[]string{"main", "secondary"},
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("authorizeRequestedIndexes() error = %v, want %v", err, test.wantErr)
			}
			if catalog.singleCalls != 0 {
				t.Fatalf("GetIndexByName() calls = %d, want 0", catalog.singleCalls)
			}
			if errors.Is(test.wantErr, errInvalidIndexCatalogBatch) {
				_, externalErr := handler.resolveAuthorizedSearchIndexes(
					context.Background(),
					[]string{"main", "secondary"},
				)
				var httpErr *router.HTTPError
				if !errors.As(externalErr, &httpErr) ||
					httpErr.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf(
						"resolveAuthorizedSearchIndexes() error = %v, want HTTP %d",
						externalErr,
						http.StatusServiceUnavailable,
					)
				}
			}
		})
	}
}

func TestAuthorizeRequestedIndexesDetachesBatchInput(t *testing.T) {
	t.Parallel()

	requested := []string{"main"}
	catalog := &batchIndexAuthorizationCatalog{
		batchResult: []control.Index{validationTestIndex("private")},
		mutateInput: true,
	}
	handler := &apiHandler{indexes: catalog}

	err := handler.authorizeRequestedIndexes(context.Background(), requested)
	if !errors.Is(err, errInvalidIndexCatalogBatch) {
		t.Fatalf("authorizeRequestedIndexes() error = %v, want %v", err, errInvalidIndexCatalogBatch)
	}
	if !slices.Equal(requested, []string{"main"}) {
		t.Fatalf("requested indexes mutated to %#v", requested)
	}
	if catalog.singleCalls != 0 {
		t.Fatalf("GetIndexByName() calls = %d, want 0", catalog.singleCalls)
	}
}
