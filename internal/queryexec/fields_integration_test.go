package queryexec

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
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

	t.Run("field summary compiler and executor", func(t *testing.T) {
		t.Run("mixed dynamic values preserve typed identity and missing counts", func(t *testing.T) {
			compiled := queryIntegrationCompileFieldSummary(
				t, "field-catalog-v1", indexTime, `index=field-catalog-v1`, "status",
			)
			summary, err := executor.ExecuteFieldSummary(ctx, compiled)
			if err != nil {
				t.Fatal(err)
			}
			if summary.FieldName != "status" || summary.EventCount != 2 || summary.NullCount != 0 ||
				summary.MissingCount != 1 || summary.DistinctCount != 2 ||
				!reflect.DeepEqual(summary.ObservedTypes, []eventfields.StoredValueType{
					eventfields.StoredValueTypeString, eventfields.StoredValueTypeSint64,
				}) || len(summary.TopValues) != 2 {
				t.Fatalf("status summary = %#v", summary)
			}
			if value, ok := summary.TopValues[0].Value.Signed(); !ok || value != 503 || summary.TopValues[0].Count != 1 {
				t.Fatalf("first status value = %#v", summary.TopValues[0])
			}
			if value, ok := summary.TopValues[1].Value.String(); !ok || value != "ok" || summary.TopValues[1].Count != 1 {
				t.Fatalf("second status value = %#v", summary.TopValues[1])
			}
		})

		t.Run("explicit null is not missing or distinct", func(t *testing.T) {
			compiled := queryIntegrationCompileFieldSummary(
				t, "field-catalog-v1", indexTime, `index=field-catalog-v1`, "nullable",
			)
			summary, err := executor.ExecuteFieldSummary(ctx, compiled)
			if err != nil {
				t.Fatal(err)
			}
			if summary.EventCount != 1 || summary.NullCount != 1 || summary.MissingCount != 2 ||
				summary.DistinctCount != 0 || len(summary.TopValues) != 0 ||
				!reflect.DeepEqual(summary.ObservedTypes, []eventfields.StoredValueType{eventfields.StoredValueTypeNull}) {
				t.Fatalf("nullable summary = %#v", summary)
			}
		})

		t.Run("fixed and extended scalar encodings round trip", func(t *testing.T) {
			for _, test := range []struct {
				name      string
				fieldName string
				assert    func(*testing.T, searchjobs.Value)
			}{
				{
					name: "fixed string", fieldName: "host",
					assert: func(t *testing.T, value searchjobs.Value) {
						if got, ok := value.String(); !ok || got != "catalog-host" {
							t.Fatalf("host value = %#v", value)
						}
					},
				},
				{
					name: "extended bytes", fieldName: "typed_bytes",
					assert: func(t *testing.T, value searchjobs.Value) {
						if got, ok := value.Bytes(); !ok || !reflect.DeepEqual(got, []byte{0x00, 0xff}) {
							t.Fatalf("typed_bytes value = %#v", value)
						}
					},
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					compiled := queryIntegrationCompileFieldSummary(
						t, "field-catalog-v1", indexTime, `index=field-catalog-v1`, test.fieldName,
					)
					summary, err := executor.ExecuteFieldSummary(ctx, compiled)
					if err != nil {
						t.Fatal(err)
					}
					if summary.DistinctCount != 1 || len(summary.TopValues) != 1 {
						t.Fatalf("%s summary = %#v", test.fieldName, summary)
					}
					test.assert(t, summary.TopValues[0].Value)
				})
			}
		})

		t.Run("fixed timestamps preserve sub-microsecond identity", func(t *testing.T) {
			summary, err := executor.ExecuteFieldSummary(ctx, queryIntegrationCompileFieldSummary(
				t, "field-summary-fixed-time", indexTime, `index=field-summary-fixed-time`, "_time",
			))
			if err != nil {
				t.Fatal(err)
			}
			if summary.EventCount != 2 || summary.NullCount != 0 || summary.MissingCount != 0 ||
				summary.DistinctCount != 2 || len(summary.TopValues) != 2 ||
				!reflect.DeepEqual(summary.ObservedTypes, []eventfields.StoredValueType{
					eventfields.StoredValueTypeTimestamp,
				}) {
				t.Fatalf("_time summary = %#v", summary)
			}
			first := queryIntegrationFixedTimestampBase(indexTime)
			gotTimes := make([]time.Time, len(summary.TopValues))
			for position := range summary.TopValues {
				got, ok := summary.TopValues[position].Value.Time()
				if !ok || got.Location() != time.UTC || summary.TopValues[position].Count != 1 {
					t.Fatalf("_time value %d = %#v, want a UTC timestamp once", position, summary.TopValues[position])
				}
				gotTimes[position] = got
			}
			if gotTimes[1].Before(gotTimes[0]) {
				gotTimes[0], gotTimes[1] = gotTimes[1], gotTimes[0]
			}
			if !gotTimes[0].Equal(first) || !gotTimes[1].Equal(first.Add(89*time.Nanosecond)) {
				t.Fatalf("_time values = %v, want %s and %s", gotTimes, first, first.Add(89*time.Nanosecond))
			}
		})

		t.Run("extended scalars canonicalize without losing typed identity", func(t *testing.T) {
			decimalSummary, err := executor.ExecuteFieldSummary(ctx, queryIntegrationCompileFieldSummary(
				t, "field-summary-scalars", indexTime, `index=field-summary-scalars`, "amount",
			))
			if err != nil {
				t.Fatal(err)
			}
			if decimalSummary.DistinctCount != 2 || len(decimalSummary.TopValues) != 2 ||
				decimalSummary.TopValues[0].Count != 2 || decimalSummary.TopValues[1].Count != 1 {
				t.Fatalf("decimal summary = %#v", decimalSummary)
			}
			if value, ok := decimalSummary.TopValues[0].Value.Decimal(); !ok || value != "1.23" {
				t.Fatalf("canonical decimal = (%q, %v)", value, ok)
			}

			identitySummary, err := executor.ExecuteFieldSummary(ctx, queryIntegrationCompileFieldSummary(
				t, "field-summary-scalars", indexTime, `index=field-summary-scalars`, "typed_identity",
			))
			if err != nil {
				t.Fatal(err)
			}
			if identitySummary.DistinctCount != 2 || identitySummary.MissingCount != 1 || len(identitySummary.TopValues) != 2 ||
				identitySummary.TopValues[0].Value.Kind() != searchjobs.ValueKindString ||
				identitySummary.TopValues[1].Value.Kind() != searchjobs.ValueKindSigned {
				t.Fatalf("typed identity summary = %#v", identitySummary)
			}
			if value, _ := identitySummary.TopValues[0].Value.String(); value != "500" {
				t.Fatalf("string identity = %q", value)
			}
			if value, _ := identitySummary.TopValues[1].Value.Signed(); value != 500 {
				t.Fatalf("signed identity = %d", value)
			}

			instantSummary, err := executor.ExecuteFieldSummary(ctx, queryIntegrationCompileFieldSummary(
				t, "field-summary-scalars", indexTime, `index=field-summary-scalars`, "instant",
			))
			if err != nil {
				t.Fatal(err)
			}
			instant, ok := instantSummary.TopValues[0].Value.Time()
			if !ok || instant.Year() != 9999 || instant.Location() != time.UTC || instantSummary.TopValues[0].Count != 2 {
				t.Fatalf("far-future timestamp summary = %#v", instantSummary)
			}

			ratioSummary, err := executor.ExecuteFieldSummary(ctx, queryIntegrationCompileFieldSummary(
				t, "field-summary-scalars", indexTime, `index=field-summary-scalars`, "ratio",
			))
			if err != nil {
				t.Fatal(err)
			}
			ratio, ok := ratioSummary.TopValues[0].Value.Double()
			if !ok || ratio != 0 || math.Signbit(ratio) || ratioSummary.DistinctCount != 1 || ratioSummary.TopValues[0].Count != 2 {
				t.Fatalf("canonical zero summary = %#v", ratioSummary)
			}
		})

		t.Run("final projected relation is analyzed", func(t *testing.T) {
			compiled := queryIntegrationCompileFieldSummary(
				t, "field-catalog-v1", indexTime,
				`index=field-catalog-v1 | eval copied=status | table copied`, "copied",
			)
			summary, err := executor.ExecuteFieldSummary(ctx, compiled)
			if err != nil {
				t.Fatal(err)
			}
			if summary.EventCount != 3 || summary.NullCount != 1 || summary.MissingCount != 0 ||
				summary.DistinctCount != 2 || len(summary.TopValues) != 2 {
				t.Fatalf("projected summary = %#v", summary)
			}

			_, err = (clickhouse.Compiler{}).CompileFieldSummary(
				queryIntegrationFieldPlan(t, "field-catalog-v1", indexTime, `index=field-catalog-v1 | table message`),
				clickhouse.FieldSummarySpec{
					FieldName:             "status",
					MaximumValues:         10,
					MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues,
					MaximumValueBytes:     clickhouse.MaximumFieldSummaryValueBytes,
				},
			)
			if !errors.Is(err, clickhouse.ErrFieldSummaryNotFound) {
				t.Fatalf("removed field compile error = %v, want ErrFieldSummaryNotFound", err)
			}
		})

		for _, fieldName := range []string{"list", "object"} {
			fieldName := fieldName
			t.Run(fieldName+" fails closed as unsupported", func(t *testing.T) {
				compiled := queryIntegrationCompileFieldSummary(
					t, "field-catalog-v1", indexTime, `index=field-catalog-v1`, fieldName,
				)
				summary, err := executor.ExecuteFieldSummary(ctx, compiled)
				if !errors.Is(err, searchjobs.ErrUnsupportedValue) || summary.FieldName != "" {
					t.Fatalf("summary = %#v, error = %v, want atomic ErrUnsupportedValue", summary, err)
				}
			})
		}

		t.Run("invalid metadata fails closed", func(t *testing.T) {
			compiled := queryIntegrationCompileFieldSummary(
				t, "field-catalog-legacy", indexTime, `index=field-catalog-legacy`, "status",
			)
			summary, err := executor.ExecuteFieldSummary(ctx, compiled)
			if !errors.Is(err, ErrFieldMetadataUnavailable) || summary.FieldName != "" {
				t.Fatalf("summary = %#v, error = %v, want atomic ErrFieldMetadataUnavailable", summary, err)
			}
		})
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
		id        string
		index     string
		fields    *clickhousedriver.JSON
		names     []string
		types     []uint8
		version   uint8
		service   *string
		raw       []byte
		encoding  uint8
		eventTime time.Time
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
			id: "scalar-a", index: "field-summary-scalars",
			fields: func() *clickhousedriver.JSON {
				document := newDocument(map[string]any{
					"flag": true, "ratio": math.Copysign(0, -1), "typed_identity": int64(500),
				})
				document.SetValueAtPath("amount", queryIntegrationExtendedValue("decimal/v1", "1.2300"))
				document.SetValueAtPath("binary", queryIntegrationExtendedValue("bytes/v1", "AP8"))
				document.SetValueAtPath("elapsed", queryIntegrationExtendedValue("duration/v1", "-12:-345000000"))
				document.SetValueAtPath("instant", queryIntegrationExtendedValue("timestamp/v1", "9999-12-31T23:59:59.999999999Z"))
				return document
			}(),
			names: []string{"amount", "binary", "elapsed", "flag", "instant", "ratio", "typed_identity"},
			types: []uint8{
				uint8(eventfields.StoredValueTypeDecimal),
				uint8(eventfields.StoredValueTypeBytes),
				uint8(eventfields.StoredValueTypeDuration),
				uint8(eventfields.StoredValueTypeBool),
				uint8(eventfields.StoredValueTypeTimestamp),
				uint8(eventfields.StoredValueTypeDouble),
				uint8(eventfields.StoredValueTypeSint64),
			}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			id: "scalar-b", index: "field-summary-scalars",
			fields: func() *clickhousedriver.JSON {
				document := newDocument(map[string]any{
					"flag": false, "ratio": float64(0), "typed_identity": "500",
				})
				document.SetValueAtPath("amount", queryIntegrationExtendedValue("decimal/v1", "1.23e0"))
				document.SetValueAtPath("binary", queryIntegrationExtendedValue("bytes/v1", "AP8"))
				document.SetValueAtPath("elapsed", queryIntegrationExtendedValue("duration/v1", "-12:-345000000"))
				document.SetValueAtPath("instant", queryIntegrationExtendedValue("timestamp/v1", "9999-12-31T23:59:59.999999999Z"))
				return document
			}(),
			names: []string{"amount", "binary", "elapsed", "flag", "instant", "ratio", "typed_identity"},
			types: []uint8{
				uint8(eventfields.StoredValueTypeDecimal),
				uint8(eventfields.StoredValueTypeBytes),
				uint8(eventfields.StoredValueTypeDuration),
				uint8(eventfields.StoredValueTypeBool),
				uint8(eventfields.StoredValueTypeTimestamp),
				uint8(eventfields.StoredValueTypeDouble),
				uint8(eventfields.StoredValueTypeString),
			}, version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			id: "scalar-c", index: "field-summary-scalars",
			fields: func() *clickhousedriver.JSON {
				document := clickhousedriver.NewJSON()
				document.SetValueAtPath("amount", queryIntegrationExtendedValue("decimal/v1", "2"))
				return document
			}(),
			names:   []string{"amount"},
			types:   []uint8{uint8(eventfields.StoredValueTypeDecimal)},
			version: eventfields.CurrentFieldMetadataVersion,
		},
		{
			id: "fixed-time-a", index: "field-summary-fixed-time",
			fields: clickhousedriver.NewJSON(), names: []string{}, types: []uint8{},
			version:   eventfields.CurrentFieldMetadataVersion,
			eventTime: queryIntegrationFixedTimestampBase(indexTime),
		},
		{
			id: "fixed-time-b", index: "field-summary-fixed-time",
			fields: clickhousedriver.NewJSON(), names: []string{}, types: []uint8{},
			version:   eventfields.CurrentFieldMetadataVersion,
			eventTime: queryIntegrationFixedTimestampBase(indexTime).Add(89 * time.Nanosecond),
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
		if !event.eventTime.IsZero() {
			eventTime = event.eventTime
		}
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

func queryIntegrationFixedTimestampBase(indexTime time.Time) time.Time {
	return indexTime.Add(-10 * time.Second).Add(123_456_700 * time.Nanosecond)
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

func queryIntegrationCompileFieldSummary(
	t *testing.T,
	indexName string,
	indexTime time.Time,
	source string,
	fieldName string,
) clickhouse.CompiledFieldSummary {
	t.Helper()
	logical := queryIntegrationFieldPlan(t, indexName, indexTime, source)
	compiled, err := (clickhouse.Compiler{}).CompileFieldSummary(logical, clickhouse.FieldSummarySpec{
		FieldName:             fieldName,
		MaximumValues:         10,
		MaximumDistinctValues: clickhouse.MaximumFieldSummaryDistinctValues,
		MaximumValueBytes:     clickhouse.MaximumFieldSummaryValueBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func queryIntegrationFieldPlan(
	t *testing.T,
	indexName string,
	indexTime time.Time,
	source string,
) *plan.Query {
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
	return logical
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
