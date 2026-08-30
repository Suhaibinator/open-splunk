package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/alerts"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type alertArtifactStore interface {
	Get(context.Context, searchjobs.AccessScope, string, searchartifacts.AccessMode) (searchartifacts.Record, error)
	Acquire(context.Context, searchjobs.AccessScope, string) (searchartifacts.ResultLease, error)
	UpdateSettingsExpected(context.Context, searchjobs.AccessScope, string, searchartifacts.Settings, uint64) (searchartifacts.Record, error)
}

// runtimeAlertArtifacts is the alert coordinator's durable job boundary. Job
// polling uses AccessInspect so background observation never slides expiry;
// result acquisition remains a real access after triggered retention is set.
type runtimeAlertArtifacts struct {
	store    alertArtifactStore
	tenantID string
}

func newRuntimeAlertArtifacts(store alertArtifactStore, tenantID string) (*runtimeAlertArtifacts, error) {
	if store == nil || strings.TrimSpace(tenantID) == "" {
		return nil, errors.New("alert artifact runtime requires a store and tenant")
	}
	return &runtimeAlertArtifacts{store: store, tenantID: tenantID}, nil
}

func (runtime *runtimeAlertArtifacts) ReadAlertSearchJob(ctx context.Context, ownerID, jobID string) (alerts.SearchJobSnapshot, error) {
	record, err := runtime.store.Get(ctx, runtime.access(ownerID), jobID, searchartifacts.AccessInspect)
	if errors.Is(err, searchartifacts.ErrExpired) {
		return alerts.SearchJobSnapshot{ID: jobID, State: alerts.SearchJobExpired}, nil
	}
	if err != nil {
		return alerts.SearchJobSnapshot{}, err
	}
	state, err := alertSearchJobState(record.State)
	if err != nil {
		return alerts.SearchJobSnapshot{}, err
	}
	failure := ""
	if state == alerts.SearchJobFailed {
		failure = "SEARCH_FAILED"
	}
	return alerts.SearchJobSnapshot{
		ID: record.Job.ID, State: state, StartedAt: record.Job.StartedAt, FinishedAt: record.Job.FinishedAt,
		ExpiresAt: record.ExpiresAt, ResultCount: record.Job.RowCount,
		ResultsTruncated: record.Job.ResultsTruncated, FailureCategory: failure,
	}, nil
}

func (runtime *runtimeAlertArtifacts) ReadAlertSearchResults(ctx context.Context, ownerID, jobID string, limit int) (result alerts.SearchResults, returnedErr error) {
	if limit < 0 || limit > alerts.MaximumSampleRows {
		return alerts.SearchResults{}, errors.New("alert result sample limit is invalid")
	}
	lease, err := runtime.store.Acquire(ctx, runtime.access(ownerID), jobID)
	if err != nil {
		return alerts.SearchResults{}, err
	}
	defer func() {
		if closeErr := lease.Close(); returnedErr == nil && closeErr != nil {
			returnedErr = closeErr
		}
	}()
	schema := lease.Schema()
	result.Schema = make([]alerts.ResultField, len(schema.Columns))
	for index, column := range schema.Columns {
		kind, kindErr := alertResultFieldType(column.Kind)
		if kindErr != nil {
			return alerts.SearchResults{}, kindErr
		}
		result.Schema[index] = alerts.ResultField{Name: column.Name, Type: kind}
	}
	result.Rows = make([]map[string]any, 0, limit)
	result.More = lease.RowCount() > uint64(limit)
	for len(result.Rows) < limit {
		row, ok, nextErr := lease.Next(ctx)
		if nextErr != nil {
			return alerts.SearchResults{}, nextErr
		}
		if !ok {
			break
		}
		if len(row.Values) != len(schema.Columns) {
			return alerts.SearchResults{}, errors.New("alert result row does not match its schema")
		}
		converted := make(map[string]any, len(row.Values))
		for index, value := range row.Values {
			convertedValue, valueErr := alertJSONValue(value, 0)
			if valueErr != nil {
				return alerts.SearchResults{}, valueErr
			}
			converted[schema.Columns[index].Name] = convertedValue
		}
		result.Rows = append(result.Rows, converted)
	}
	return result, nil
}

func (runtime *runtimeAlertArtifacts) ExtendAlertSearchJob(ctx context.Context, ownerID, jobID string, lifetime time.Duration) (time.Time, error) {
	if lifetime <= 0 {
		return time.Time{}, errors.New("alert retention lifetime must be positive")
	}
	access := runtime.access(ownerID)
	for range 4 {
		current, err := runtime.store.Get(ctx, access, jobID, searchartifacts.AccessInspect)
		if err != nil {
			return time.Time{}, err
		}
		settings := searchartifacts.Settings{
			Visibility:     current.Visibility,
			RetentionClass: searchartifacts.RetentionTriggeredWebhook,
			Lifetime:       lifetime,
		}
		if current.Lifetime > lifetime {
			settings.RetentionClass = current.RetentionClass
			settings.Lifetime = current.Lifetime
		}
		updated, err := runtime.store.UpdateSettingsExpected(ctx, access, jobID, settings, current.Job.Version)
		if errors.Is(err, searchartifacts.ErrConflict) {
			continue
		}
		if err != nil {
			return time.Time{}, err
		}
		return updated.ExpiresAt, nil
	}
	return time.Time{}, searchartifacts.ErrConflict
}

func (runtime *runtimeAlertArtifacts) access(ownerID string) searchjobs.AccessScope {
	return searchjobs.AccessScope{TenantID: runtime.tenantID, OwnerID: ownerID}
}

func alertSearchJobState(state searchartifacts.State) (alerts.SearchJobState, error) {
	switch state {
	case searchartifacts.StateQueued:
		return alerts.SearchJobQueued, nil
	case searchartifacts.StateParsing, searchartifacts.StatePlanning, searchartifacts.StateRunning:
		return alerts.SearchJobRunning, nil
	case searchartifacts.StateCompleted:
		return alerts.SearchJobCompleted, nil
	case searchartifacts.StateFailed:
		return alerts.SearchJobFailed, nil
	case searchartifacts.StateCanceled:
		return alerts.SearchJobCanceled, nil
	case searchartifacts.StateExpired:
		return alerts.SearchJobExpired, nil
	case searchartifacts.StateInterrupted:
		return alerts.SearchJobInterrupted, nil
	default:
		return "", errors.New("alert search artifact returned an invalid state")
	}
}

func alertResultFieldType(kind searchjobs.ValueKind) (string, error) {
	switch kind {
	case searchjobs.ValueKindNull:
		return "null", nil
	case searchjobs.ValueKindMissing:
		return "missing", nil
	case searchjobs.ValueKindString:
		return "string", nil
	case searchjobs.ValueKindSigned:
		return "sint64", nil
	case searchjobs.ValueKindUnsigned:
		return "uint64", nil
	case searchjobs.ValueKindDouble:
		return "double", nil
	case searchjobs.ValueKindBool:
		return "bool", nil
	case searchjobs.ValueKindBytes:
		return "bytes", nil
	case searchjobs.ValueKindTime:
		return "timestamp", nil
	case searchjobs.ValueKindDuration:
		return "duration", nil
	case searchjobs.ValueKindList:
		return "list", nil
	case searchjobs.ValueKindObject:
		return "object", nil
	case searchjobs.ValueKindDecimal:
		return "decimal", nil
	case searchjobs.ValueKindMixed:
		return "mixed", nil
	default:
		return "", errors.New("alert result schema contains an invalid type")
	}
}

func alertJSONValue(value searchjobs.Value, depth int) (any, error) {
	if depth > 32 {
		return nil, errors.New("alert result value exceeds maximum nesting")
	}
	switch value.Kind() {
	case searchjobs.ValueKindNull, searchjobs.ValueKindMissing:
		return nil, nil
	case searchjobs.ValueKindString:
		result, ok := value.String()
		return requiredAlertValue(result, ok)
	case searchjobs.ValueKindSigned:
		result, ok := value.Signed()
		return requiredAlertValue(result, ok)
	case searchjobs.ValueKindUnsigned:
		result, ok := value.Unsigned()
		return requiredAlertValue(result, ok)
	case searchjobs.ValueKindDouble:
		result, ok := value.Double()
		if !ok || math.IsNaN(result) || math.IsInf(result, 0) {
			return nil, errors.New("alert result contains a non-JSON number")
		}
		return result, nil
	case searchjobs.ValueKindBool:
		result, ok := value.Bool()
		return requiredAlertValue(result, ok)
	case searchjobs.ValueKindBytes:
		result, ok := value.Bytes()
		if !ok {
			return nil, errors.New("alert result value kind does not match its payload")
		}
		return base64.StdEncoding.EncodeToString(result), nil
	case searchjobs.ValueKindTime:
		result, ok := value.Time()
		if !ok {
			return nil, errors.New("alert result value kind does not match its payload")
		}
		return result.UTC().Format(time.RFC3339Nano), nil
	case searchjobs.ValueKindDuration:
		result, ok := value.Duration()
		if !ok {
			return nil, errors.New("alert result value kind does not match its payload")
		}
		return result.String(), nil
	case searchjobs.ValueKindDecimal:
		result, ok := value.Decimal()
		return requiredAlertValue(result, ok)
	case searchjobs.ValueKindList:
		values, ok := value.List()
		if !ok {
			return nil, errors.New("alert result value kind does not match its payload")
		}
		result := make([]any, len(values))
		for index, child := range values {
			converted, err := alertJSONValue(child, depth+1)
			if err != nil {
				return nil, err
			}
			result[index] = converted
		}
		return result, nil
	case searchjobs.ValueKindObject:
		fields, ok := value.Object()
		if !ok {
			return nil, errors.New("alert result value kind does not match its payload")
		}
		result := make(map[string]any, len(fields))
		for _, field := range fields {
			converted, err := alertJSONValue(field.Value, depth+1)
			if err != nil {
				return nil, err
			}
			result[field.Name] = converted
		}
		return result, nil
	default:
		return nil, fmt.Errorf("alert result contains unsupported value kind %d", value.Kind())
	}
}

func requiredAlertValue[T any](value T, ok bool) (any, error) {
	if !ok {
		return nil, errors.New("alert result value kind does not match its payload")
	}
	return value, nil
}

var _ alerts.SearchJobReader = (*runtimeAlertArtifacts)(nil)
var _ alerts.SearchResultReader = (*runtimeAlertArtifacts)(nil)
var _ alerts.DurableRetentionUpdater = (*runtimeAlertArtifacts)(nil)
