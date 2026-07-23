package queryexec

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func queryIntegrationTestFieldCatalog(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	executor *Executor,
) {
	t.Helper()
	indexTime := queryIntegrationInsertFieldCatalogEvents(t, ctx, connection)

	t.Run("field catalog compiler and executor", func(t *testing.T) {
		t.Run("empty event relation retains known schema", func(t *testing.T) {
			compiled := queryIntegrationCompileFieldCatalog(t, "field-catalog-empty", indexTime, `index=field-catalog-empty`)
			catalog, err := executor.ExecuteFieldCatalog(ctx, compiled)
			if err != nil {
				t.Fatal(err)
			}
			if catalog.TotalEvents != 0 || len(catalog.Fields) == 0 {
				t.Fatalf("empty catalog = %#v, want zero events and known schema", catalog)
			}
			for _, profile := range catalog.Fields {
				if profile.EventCount != 0 || profile.NullCount != 0 || profile.MissingCount != 0 || len(profile.ObservedTypes) != 0 {
					t.Fatalf("empty profile %q = %#v", profile.FieldName, profile)
				}
			}
		})

		t.Run("v1 metadata preserves exact counts and semantic types", func(t *testing.T) {
			compiled := queryIntegrationCompileFieldCatalog(t, "field-catalog-v1", indexTime, `index=field-catalog-v1`)
			catalog, err := executor.ExecuteFieldCatalog(ctx, compiled)
			if err != nil {
				t.Fatal(err)
			}
			if catalog.TotalEvents != 3 {
				t.Fatalf("total events = %d, want 3", catalog.TotalEvents)
			}
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "status", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeString,
					eventfields.StoredValueTypeSint64,
				}, EventCount: 2, MissingCount: 1,
			})
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "nullable", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeNull,
				}, EventCount: 1, NullCount: 1, MissingCount: 2,
			})
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "list", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeList,
				}, EventCount: 1, MissingCount: 2,
			})
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "object", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeObject,
				}, EventCount: 1, MissingCount: 2,
			})
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "typed_bytes", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeBytes,
				}, EventCount: 1, MissingCount: 2,
			})
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "host", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeString,
				}, EventCount: 3,
			})
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "service", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeNull,
					eventfields.StoredValueTypeString,
				}, EventCount: 3, NullCount: 1,
			})
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "_raw", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeString,
					eventfields.StoredValueTypeBytes,
				}, EventCount: 3,
			})
		})

		t.Run("dynamic eval preserves types and materializes missing as null", func(t *testing.T) {
			compiled := queryIntegrationCompileFieldCatalog(
				t, "field-catalog-v1", indexTime,
				`index=field-catalog-v1 | eval copied=status,parent_copy=parent | table copied,parent_copy,typed_bytes`,
			)
			catalog, err := executor.ExecuteFieldCatalog(ctx, compiled)
			if err != nil {
				t.Fatal(err)
			}
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "copied", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeNull,
					eventfields.StoredValueTypeString,
					eventfields.StoredValueTypeSint64,
				}, EventCount: 3, NullCount: 1,
			})
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "typed_bytes", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeBytes,
				}, EventCount: 1, MissingCount: 2,
			})
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "parent_copy", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeNull,
					eventfields.StoredValueTypeObject,
				}, EventCount: 3, NullCount: 2,
			})
		})

		t.Run("projected object parent uses descendant proof", func(t *testing.T) {
			compiled := queryIntegrationCompileFieldCatalog(
				t, "field-catalog-v1", indexTime,
				`index=field-catalog-v1 | table parent`,
			)
			catalog, err := executor.ExecuteFieldCatalog(ctx, compiled)
			if err != nil {
				t.Fatal(err)
			}
			assertFieldCatalogProfile(t, catalog, FieldProfileRow{
				FieldName: "parent", ObservedTypes: []eventfields.StoredValueType{
					eventfields.StoredValueTypeObject,
				}, EventCount: 1, MissingCount: 2,
			})
		})

		for _, indexName := range []string{
			"field-catalog-legacy",
			"field-catalog-invalid",
			"field-catalog-invalid-name",
			"field-catalog-oversized",
		} {
			indexName := indexName
			t.Run(indexName+" fails closed", func(t *testing.T) {
				compiled := queryIntegrationCompileFieldCatalog(t, indexName, indexTime, "index="+indexName)
				catalog, err := executor.ExecuteFieldCatalog(ctx, compiled)
				if !errors.Is(err, ErrFieldMetadataUnavailable) || catalog.TotalEvents != 0 || len(catalog.Fields) != 0 {
					t.Fatalf("catalog = %#v, error = %v, want ErrFieldMetadataUnavailable atomically", catalog, err)
				}
			})
		}
	})
}

func queryIntegrationInsertFieldCatalogEvents(t *testing.T, ctx context.Context, connection clickhousedriver.Conn) time.Time {
	t.Helper()
	query := "INSERT INTO open_splunk.events (event_id, tenant_id, index_name, event_time, index_time, " +
		"collected_at, event_time_source, host, source, sourcetype, service, severity, level, body, raw, " +
		"raw_encoding, trace_id, span_id, fields, field_names, field_types, field_metadata_version, collector_id, batch_id, batch_sequence, " +
		"expires_at, visibility_seq)"
	batch, err := connection.PrepareBatch(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	indexTime := time.Now().UTC().Add(-30 * time.Second).Truncate(time.Millisecond)
	service := "catalog"
	type fixture struct {
		id       string
		index    string
		fields   *clickhousedriver.JSON
		names    []string
		types    []uint8
		version  uint8
		service  *string
		raw      []byte
		encoding uint8
	}
	newDocument := func(values map[string]any) *clickhousedriver.JSON {
		document := clickhousedriver.NewJSON()
		for name, value := range values {
			document.SetValueAtPath(name, clickhousedriver.NewDynamic(value))
		}
		return document
	}
	fixtures := []fixture{
		{
			id: "v1-a", index: "field-catalog-v1", service: &service,
			fields: func() *clickhousedriver.JSON {
				document := newDocument(map[string]any{
					"list": []string{"x"}, "nullable": nil, "parent.child": "leaf", "status": int64(503),
				})
				document.SetValueAtPath("typed_bytes", queryIntegrationExtendedValue("bytes/v1", "AP8"))
				return document
			}(),
			names: []string{"list", "nullable", "parent.child", "status", "typed_bytes"},
			types: []uint8{
				uint8(eventfields.StoredValueTypeList),
				uint8(eventfields.StoredValueTypeNull),
				uint8(eventfields.StoredValueTypeString),
				uint8(eventfields.StoredValueTypeSint64),
				uint8(eventfields.StoredValueTypeBytes),
			}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			id: "v1-b", index: "field-catalog-v1", service: &service,
			fields: newDocument(map[string]any{
				"object": map[string]string{}, "status": "ok",
			}),
			names: []string{"object", "status"},
			types: []uint8{
				uint8(eventfields.StoredValueTypeObject),
				uint8(eventfields.StoredValueTypeString),
			}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			id: "v1-c", index: "field-catalog-v1", fields: clickhousedriver.NewJSON(),
			names: []string{}, types: []uint8{}, version: eventfields.CurrentFieldMetadataVersion,
			raw:      []byte{0xff, 0x00, 'x'},
			encoding: 2,
		},
		{
			id: "legacy", index: "field-catalog-legacy", fields: newDocument(map[string]any{"status": "legacy"}),
			names: []string{"status"}, types: []uint8{}, version: 0,
		},
		{
			id: "unsorted", index: "field-catalog-invalid", fields: newDocument(map[string]any{"a": "a", "z": "z"}),
			names:   []string{"z", "a"},
			types:   []uint8{uint8(eventfields.StoredValueTypeString), uint8(eventfields.StoredValueTypeString)},
			version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			id: "duplicate", index: "field-catalog-invalid", fields: newDocument(map[string]any{"a": "a"}),
			names:   []string{"a", "a"},
			types:   []uint8{uint8(eventfields.StoredValueTypeString), uint8(eventfields.StoredValueTypeString)},
			version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			id: "invalid-name", index: "field-catalog-invalid-name", fields: clickhousedriver.NewJSON(),
			names: []string{string([]byte{0xff})}, types: []uint8{uint8(eventfields.StoredValueTypeString)},
			version: eventfields.CurrentFieldMetadataVersion,
		},
	}
	oversizedNames := make([]string, eventfields.MaximumStoredFieldsPerEvent+1)
	oversizedTypes := make([]uint8, len(oversizedNames))
	for index := range oversizedNames {
		oversizedNames[index] = fmt.Sprintf("field-%04d", index)
		oversizedTypes[index] = uint8(eventfields.StoredValueTypeString)
	}
	fixtures = append(fixtures, fixture{
		id: "oversized", index: "field-catalog-oversized", fields: clickhousedriver.NewJSON(),
		names: oversizedNames, types: oversizedTypes, version: eventfields.CurrentFieldMetadataVersion,
	})
	fixtures = append(fixtures, fixture{
		id: "long-name", index: "field-catalog-invalid-name", fields: clickhousedriver.NewJSON(),
		names: []string{strings.Repeat("x", eventfields.MaximumNormalizedFieldNameBytes+1)},
		types: []uint8{uint8(eventfields.StoredValueTypeString)}, version: eventfields.CurrentFieldMetadataVersion,
	})
	for sequence, event := range fixtures {
		message := "field catalog " + event.id
		raw := event.raw
		if raw == nil {
			raw = []byte(message)
		}
		encoding := event.encoding
		if encoding == 0 {
			encoding = 1
		}
		eventTime := indexTime.Add(time.Duration(sequence) * time.Millisecond)
		if err := batch.Append(
			"queryexec-field-catalog-"+event.id, "tenant", event.index, eventTime, indexTime,
			nil, uint8(1), "catalog-host", "catalog.log", "test", event.service, uint8(1), nil, &message, raw,
			encoding, nil, nil, event.fields, event.names, event.types, event.version, "collector", "field-catalog-batch", uint64(sequence+1),
			indexTime.Add(24*time.Hour), uint64(1),
		); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	return indexTime
}

func queryIntegrationCompileFieldCatalog(
	t *testing.T,
	indexName string,
	indexTime time.Time,
	source string,
) clickhouse.CompiledFieldCatalog {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant",
		AuthorizedIndexes: []string{indexName},
		RequestedIndexes:  []string{indexName},
		Earliest:          indexTime.Add(-time.Minute),
		Latest:            indexTime.Add(time.Minute),
		IndexTimeCutoff:   indexTime.Add(time.Second),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).CompileFieldCatalog(logical, clickhouse.FieldCatalogSpec{MaximumFields: 100})
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func assertFieldCatalogProfile(t *testing.T, catalog FieldCatalogResult, want FieldProfileRow) {
	t.Helper()
	for _, profile := range catalog.Fields {
		if profile.FieldName != want.FieldName {
			continue
		}
		if !reflect.DeepEqual(profile, want) {
			t.Fatalf("profile %q = %#v, want %#v", want.FieldName, profile, want)
		}
		return
	}
	t.Fatalf("profile %q is missing from %#v", want.FieldName, catalog.Fields)
}
