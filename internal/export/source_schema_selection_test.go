package export

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func wideSelectionSchema(count int) searchjobs.Schema {
	columns := make([]searchjobs.Column, count)
	for index := range columns {
		columns[index] = searchjobs.Column{Name: fmt.Sprintf("series_%06d", index), Kind: searchjobs.ValueKindUnsigned}
	}
	return searchjobs.Schema{Columns: columns}
}

func TestTrustedSchemaSelectionAttestationBounds(t *testing.T) {
	schema := wideSelectionSchema(maximumColumns + 1)
	measured, valid, err := measureTrustedSourceSchema(context.Background(), schema)
	if err != nil || !valid {
		t.Fatalf("measure=%d valid=%t err=%v", measured, valid, err)
	}
	for _, bytes := range []uint64{0, measured, maximumTrustedSourceSchemaBytes, maximumTrustedSourceSchemaBytes + 1} {
		t.Run(fmt.Sprintf("bytes_%d", bytes), func(t *testing.T) {
			lease := &continuationResultLease{schema: schema, schemaBytes: bytes}
			selected, trusted := trustedSchemaForSelection(lease)
			want := bytes > 0 && bytes <= maximumTrustedSourceSchemaBytes
			if trusted != want {
				t.Fatalf("trusted=%t want%t", trusted, want)
			}
			if want && !slices.Equal(selected.Columns, schema.Columns) {
				t.Fatal("attested schema changed")
			}
			if !want && len(selected.Columns) != 0 {
				t.Fatal("invalid attestation exposed schema")
			}
		})
	}
	if selected, trusted := trustedSchemaForSelection(&continuationResultLease{schemaBytes: measured}); trusted || len(selected.Columns) != 0 {
		t.Fatal("empty attested schema accepted")
	}
	if selected, trusted := trustedSchemaForSelection(&exportTestLease{schema: schema}); trusted || !slices.Equal(selected.Columns, schema.Columns) {
		t.Fatal("untrusted lease gained attestation or lost schema")
	}
}

func TestWideSelectionKeepsCompleteNameAndRequestedColumnValidation(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		t.Run(fmt.Sprintf("trusted_%t", trusted), func(t *testing.T) {
			for _, kind := range []string{"valid", "unselected empty name", "unselected invalid UTF8", "duplicate selected source", "duplicate request", "unknown request", "empty request", "too many requests"} {
				t.Run(kind, func(t *testing.T) {
					schema := wideSelectionSchema(maximumColumns + 1)
					requested := []string{schema.Columns[maximumColumns-1].Name, schema.Columns[0].Name}
					var want error
					switch kind {
					case "unselected empty name":
						schema.Columns[maximumColumns].Name = ""
						want = ErrSourceUnavailable
					case "unselected invalid UTF8":
						schema.Columns[maximumColumns].Name = string([]byte{0xff})
						want = ErrSourceUnavailable
					case "duplicate selected source":
						schema.Columns[maximumColumns].Name = schema.Columns[0].Name
						want = ErrSourceUnavailable
					case "duplicate request":
						requested[1] = requested[0]
						want = ErrInvalidColumns
					case "unknown request":
						requested[1] = "missing"
						want = ErrInvalidColumns
					case "empty request":
						requested = nil
						want = ErrInvalidColumns
					case "too many requests":
						requested = make([]string, maximumColumns+1)
						want = ErrInvalidColumns
					}
					selection, err := selectColumnsContext(context.Background(), schema, requested, trusted)
					if !errors.Is(err, want) {
						t.Fatalf("selection error=%v want%v", err, want)
					}
					if want == nil && (!slices.Equal(selection.indexes, []int{maximumColumns - 1, 0}) || selection.columns[0].Name != requested[0] || selection.columns[1].Name != requested[1]) {
						t.Fatal("requested order or indexes changed")
					}
				})
			}
		})
	}
}

func TestGenericWideSelectionStillMeasuresMetadata(t *testing.T) {
	schema := wideSelectionSchema(maximumColumns + 1)
	// Share a small backing string: logical metadata exceeds its cap without
	// allocating a source-sized payload or changing generic validation rules.
	delimiter := strings.Repeat("x", 64<<10)
	for index := range schema.Columns {
		schema.Columns[index].FlatMultivalueDelimiter = delimiter
	}
	requested := []string{schema.Columns[0].Name}
	if _, err := selectColumns(schema, requested); !errors.Is(err, ErrInvalidColumns) {
		t.Fatalf("generic oversized metadata=%v", err)
	}
	if _, err := selectColumnsContext(context.Background(), schema, requested, false); !errors.Is(err, ErrInvalidColumns) {
		t.Fatalf("context-aware oversized metadata=%v", err)
	}
}

func TestWideSelectionCancellationAcrossAdmissionAndScan(t *testing.T) {
	schema := wideSelectionSchema(4*maximumColumns + 1)
	requested := []string{schema.Columns[0].Name, schema.Columns[len(schema.Columns)-1].Name}
	completedChecks := make(map[bool]int)
	for _, trusted := range []bool{false, true} {
		t.Run(fmt.Sprintf("trusted_%t", trusted), func(t *testing.T) {
			completed := &exportListCountingContext{after: 1 << 20}
			if _, err := selectColumnsContext(completed, schema, requested, trusted); err != nil {
				t.Fatal(err)
			}
			completedChecks[trusted] = completed.checks
			// Cancel at each observable admission/scan phase, including the final
			// check after selection is complete. No timer races or sleeps are needed.
			for after := 1; after <= completed.checks; after++ {
				canceled := &exportListCountingContext{after: after}
				selection, err := selectColumnsContext(canceled, schema, requested, trusted)
				if !errors.Is(err, context.Canceled) || len(selection.columns) != 0 || len(selection.indexes) != 0 {
					t.Fatalf("cancel check%d/%d: err=%v selected=%d", after, completed.checks, err, len(selection.columns))
				}
			}
		})
	}
	if completedChecks[false] <= completedChecks[true] {
		t.Fatalf("trusted selection repeated metadata traversal: trusted checks=%d untrusted checks=%d", completedChecks[true], completedChecks[false])
	}
}

type selectionShutdownLease struct {
	*continuationResultLease
	beforeSelect func()
}

func (lease *selectionShutdownLease) trustedResolvedSchema() (searchjobs.Schema, uint64, bool) {
	lease.beforeSelect()
	return lease.continuationResultLease.trustedResolvedSchema()
}

func TestManagerPreservesShutdownClassificationDuringColumnSelection(t *testing.T) {
	schema := wideSelectionSchema(maximumColumns + 1)
	bytes, valid, err := measureTrustedSourceSchema(context.Background(), schema)
	if err != nil || !valid {
		t.Fatalf("schema measurement: valid=%t err=%v", valid, err)
	}
	pin := &exportTestLease{schema: schema, closedSignal: make(chan struct{})}
	lease := &selectionShutdownLease{continuationResultLease: &continuationResultLease{
		ResultLease: pin, schema: schema, schemaBytes: bytes,
	}}
	manager := newExportTestManager(t, &hardeningStaticSource{lease: lease}, nil)
	lease.beforeSelect = func() {
		manager.mu.Lock()
		manager.closed = true
		manager.cancel()
		manager.mu.Unlock()
	}
	if _, err := manager.Create(context.Background(), testAccess, CreateRequest{
		SearchJobID: "wide", Format: FormatCSV, Columns: []string{schema.Columns[0].Name},
	}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Create during selection shutdown=%v, want ErrClosed", err)
	}
	if pin.closeCount.Load() != 1 {
		t.Fatalf("source close count=%d, want1", pin.closeCount.Load())
	}
}
