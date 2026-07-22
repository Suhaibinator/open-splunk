package searchhistory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type indexedEntry struct {
	jobID         string
	appID         string
	savedSearchID string
	state         int64
	searchText    string
	createdAt     int64
	finishedAt    int64
	duration      int64
	matchedEvents int64
	encoded       []byte
	checksum      [sha256.Size]byte
}

func normalizeScope(scope AccessScope) (AccessScope, error) {
	scope.TenantID = strings.TrimSpace(scope.TenantID)
	scope.OwnerID = strings.TrimSpace(scope.OwnerID)
	if err := validateText("tenant ID", scope.TenantID, maximumTenantIDBytes, false); err != nil {
		return AccessScope{}, err
	}
	if err := validateText("owner ID", scope.OwnerID, maximumOwnerIDBytes, false); err != nil {
		return AccessScope{}, err
	}
	return scope, nil
}

func normalizeEntry(input *opensplunkv1.SearchHistoryEntry) (*opensplunkv1.SearchHistoryEntry, indexedEntry, error) {
	if input == nil {
		return nil, indexedEntry{}, invalid("search-history entry is required")
	}
	entry := proto.Clone(input).(*opensplunkv1.SearchHistoryEntry)
	if err := rejectUnknownFields(entry.ProtoReflect()); err != nil {
		return nil, indexedEntry{}, invalid(err.Error())
	}
	entry.SearchJobId = strings.TrimSpace(entry.SearchJobId)
	if err := validateText("search job ID", entry.SearchJobId, maximumSearchJobIDBytes, false); err != nil {
		return nil, indexedEntry{}, err
	}
	if entry.Definition == nil {
		return nil, indexedEntry{}, invalid("search definition is required")
	}
	if err := normalizeSearchDefinition(entry.Definition); err != nil {
		return nil, indexedEntry{}, err
	}
	if err := normalizeSource(entry); err != nil {
		return nil, indexedEntry{}, err
	}
	if !terminalState(entry.FinalState) {
		return nil, indexedEntry{}, invalid("final state must be completed, failed, canceled, or expired")
	}
	if err := normalizeEffectiveScope(entry); err != nil {
		return nil, indexedEntry{}, err
	}
	if err := validateResolvedRange(entry.ResolvedTimeRange); err != nil {
		return nil, indexedEntry{}, err
	}
	created, normalizedCreated, err := normalizedTimestamp("created_at", entry.CreatedAt)
	if err != nil {
		return nil, indexedEntry{}, err
	}
	entry.CreatedAt = normalizedCreated
	finished, normalizedFinished, err := normalizedTimestamp("finished_at", entry.FinishedAt)
	if err != nil {
		return nil, indexedEntry{}, err
	}
	entry.FinishedAt = normalizedFinished
	if finished.Before(created) {
		return nil, indexedEntry{}, invalid("finished_at cannot precede created_at")
	}
	if entry.StartedAt != nil {
		started, normalizedStarted, err := normalizedTimestamp("started_at", entry.StartedAt)
		if err != nil {
			return nil, indexedEntry{}, err
		}
		entry.StartedAt = normalizedStarted
		if started.Before(created) || started.After(finished) {
			return nil, indexedEntry{}, invalid("started_at must fall between created_at and finished_at")
		}
	}
	duration := time.Duration(0)
	if entry.Duration != nil {
		duration, err = checkedDuration(entry.Duration)
		if err != nil {
			return nil, indexedEntry{}, err
		}
	} else {
		entry.Duration = durationpb.New(0)
	}
	if entry.MatchedEvents > math.MaxInt64 {
		return nil, indexedEntry{}, invalid("matched event count is outside the supported range")
	}
	entry.CompilerVersion = strings.TrimSpace(entry.CompilerVersion)
	if err := validateText("compiler version", entry.CompilerVersion, maximumCompilerVersionBytes, false); err != nil {
		return nil, indexedEntry{}, err
	}
	if err := validateWarnings(entry.Warnings); err != nil {
		return nil, indexedEntry{}, err
	}
	if err := validateFailure(entry.FinalState, entry.Failure); err != nil {
		return nil, indexedEntry{}, err
	}

	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(entry)
	if err != nil {
		return nil, indexedEntry{}, fmt.Errorf("encode search-history entry: %w", err)
	}
	if len(encoded) == 0 || len(encoded) > maximumEntryBytes {
		return nil, indexedEntry{}, invalid(fmt.Sprintf("search-history entry cannot exceed %d bytes", maximumEntryBytes))
	}
	appID := entry.Definition.GetAppId()
	savedSearchID := entry.Source.GetSavedSearchId()
	indexed := indexedEntry{
		jobID: entry.SearchJobId, appID: appID, savedSearchID: savedSearchID,
		state: int64(entry.FinalState), searchText: entry.Definition.Spl,
		createdAt: created.UnixMicro(), finishedAt: finished.UnixMicro(),
		duration: int64(duration), matchedEvents: int64(entry.MatchedEvents),
		encoded: encoded, checksum: sha256.Sum256(encoded),
	}
	return entry, indexed, nil
}

func normalizeSearchDefinition(definition *opensplunkv1.SearchDefinition) error {
	// SPL is reusable user intent. Validate it without trimming or otherwise
	// rewriting bytes so "Open in Search" restores exactly what was entered.
	if strings.TrimSpace(definition.Spl) == "" {
		return invalid("SPL must contain a non-whitespace search")
	}
	if err := validateText("SPL", definition.Spl, maximumSPLBytes, false); err != nil {
		return err
	}
	if definition.AppId != nil {
		appID := strings.TrimSpace(*definition.AppId)
		if err := validateText("app ID", appID, maximumAppIDBytes, true); err != nil {
			return err
		}
		definition.AppId = &appID
	}
	if len(definition.IndexScope) > maximumIndexScope {
		return invalid(fmt.Sprintf("index scope cannot contain more than %d indexes", maximumIndexScope))
	}
	seenIndexes := make(map[string]struct{}, len(definition.IndexScope))
	normalizedIndexes := make([]string, 0, len(definition.IndexScope))
	for _, indexName := range definition.IndexScope {
		indexName = strings.TrimSpace(indexName)
		if err := validateText("requested index", indexName, 255, false); err != nil {
			return err
		}
		if _, exists := seenIndexes[indexName]; exists {
			continue
		}
		seenIndexes[indexName] = struct{}{}
		normalizedIndexes = append(normalizedIndexes, indexName)
	}
	definition.IndexScope = normalizedIndexes
	if len(definition.SelectedFields) > maximumIndexScope {
		return invalid(fmt.Sprintf("selected fields cannot contain more than %d values", maximumIndexScope))
	}
	for index, field := range definition.SelectedFields {
		field = strings.TrimSpace(field)
		if err := validateText("selected field", field, 1024, false); err != nil {
			return err
		}
		definition.SelectedFields[index] = field
	}
	if definition.TimeRange != nil {
		for _, field := range []struct {
			name  string
			value *string
		}{
			{name: "earliest time expression", value: definition.TimeRange.Earliest},
			{name: "latest time expression", value: definition.TimeRange.Latest},
			{name: "time zone", value: definition.TimeRange.Timezone},
		} {
			name, value := field.name, field.value
			if value == nil {
				continue
			}
			trimmed := strings.TrimSpace(*value)
			if err := validateText(name, trimmed, 1024, true); err != nil {
				return err
			}
			*value = trimmed
		}
	}
	if definition.PreferredResultTab < opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_UNSPECIFIED || definition.PreferredResultTab > opensplunkv1.SearchResultTab_SEARCH_RESULT_TAB_VISUALIZATION {
		return invalid("preferred result tab is invalid")
	}
	return nil
}

func normalizeSource(entry *opensplunkv1.SearchHistoryEntry) error {
	if entry.Source == nil {
		entry.Source = &opensplunkv1.SearchJobSource{Origin: opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC}
	}
	source := entry.Source
	if source.Origin < opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC || source.Origin > opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_API {
		return invalid("search origin is invalid")
	}
	for _, field := range []struct {
		name  string
		value *string
	}{
		{name: "saved-search ID", value: source.SavedSearchId},
		{name: "history search ID", value: source.HistorySearchId},
		{name: "dashboard ID", value: source.DashboardId},
	} {
		name, value := field.name, field.value
		if value == nil {
			continue
		}
		trimmed := strings.TrimSpace(*value)
		if err := validateText(name, trimmed, maximumSavedSearchIDBytes, false); err != nil {
			return err
		}
		*value = trimmed
	}
	switch source.Origin {
	case opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH:
		if source.SavedSearchId == nil || source.HistorySearchId != nil || source.DashboardId != nil {
			return invalid("saved-search origin requires a saved-search ID")
		}
	case opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN:
		if source.HistorySearchId == nil || source.SavedSearchId != nil || source.DashboardId != nil {
			return invalid("history-rerun origin requires a history search ID")
		}
	case opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_DASHBOARD:
		if source.DashboardId == nil || source.SavedSearchId != nil || source.HistorySearchId != nil {
			return invalid("dashboard origin requires a dashboard ID")
		}
	default:
		if source.SavedSearchId != nil || source.HistorySearchId != nil || source.DashboardId != nil {
			return invalid("search source IDs do not match the search origin")
		}
	}
	return nil
}

func normalizeEffectiveScope(entry *opensplunkv1.SearchHistoryEntry) error {
	if len(entry.EffectiveIndexScope) > maximumIndexScope {
		return invalid(fmt.Sprintf("effective index scope cannot contain more than %d indexes", maximumIndexScope))
	}
	normalized := make([]string, 0, len(entry.EffectiveIndexScope))
	seen := make(map[string]struct{}, len(entry.EffectiveIndexScope))
	for _, indexName := range entry.EffectiveIndexScope {
		name, err := control.NormalizeIndexName(indexName)
		if err != nil {
			return invalid("effective index scope contains an invalid index name")
		}
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	slices.Sort(normalized)
	entry.EffectiveIndexScope = normalized
	return nil
}

func validateResolvedRange(value *opensplunkv1.ResolvedTimeRange) error {
	if value == nil {
		return nil
	}
	earliest, normalizedEarliest, err := normalizedTimestamp("resolved earliest", value.Earliest)
	if err != nil {
		return err
	}
	value.Earliest = normalizedEarliest
	latest, normalizedLatest, err := normalizedTimestamp("resolved latest", value.Latest)
	if err != nil {
		return err
	}
	value.Latest = normalizedLatest
	if !earliest.Before(latest) {
		return invalid("resolved earliest must precede latest")
	}
	value.Timezone = strings.TrimSpace(value.Timezone)
	return validateText("resolved time zone", value.Timezone, 1024, false)
}

func validateWarnings(warnings []*opensplunkv1.ApiWarning) error {
	if len(warnings) > maximumWarnings {
		return invalid(fmt.Sprintf("warnings cannot contain more than %d values", maximumWarnings))
	}
	for _, warning := range warnings {
		if warning == nil {
			return invalid("warnings cannot contain an empty value")
		}
		if err := validateText("warning code", strings.TrimSpace(warning.Code), 128, false); err != nil {
			return err
		}
		if err := validateText("warning message", warning.Message, maximumFailureMessageBytes, false); err != nil {
			return err
		}
		if warning.OccurredAt != nil {
			_, normalized, err := normalizedTimestamp("warning occurrence", warning.OccurredAt)
			if err != nil {
				return err
			}
			warning.OccurredAt = normalized
		}
	}
	return nil
}

func validateFailure(state opensplunkv1.SearchJobState, failure *opensplunkv1.SearchFailure) error {
	if failure == nil {
		if state == opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED {
			return invalid("failed history entries require a safe failure summary")
		}
		return nil
	}
	if state != opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED {
		return invalid("only failed history entries may contain a failure summary")
	}
	if failure.Code < opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INVALID_SPL || failure.Code > opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_RESULT_EXPIRED {
		return invalid("failure code is invalid")
	}
	if err := validateText("failure message", failure.Message, maximumFailureMessageBytes, false); err != nil {
		return err
	}
	if len(failure.Diagnostics) > maximumDiagnostics {
		return invalid(fmt.Sprintf("failure diagnostics cannot contain more than %d values", maximumDiagnostics))
	}
	for _, diagnostic := range failure.Diagnostics {
		if diagnostic == nil {
			return invalid("failure diagnostics cannot contain an empty value")
		}
		if err := validateText("diagnostic code", strings.TrimSpace(diagnostic.Code), 128, false); err != nil {
			return err
		}
		if err := validateText("diagnostic message", diagnostic.Message, maximumFailureMessageBytes, false); err != nil {
			return err
		}
		if len(diagnostic.Suggestions) > 32 {
			return invalid("diagnostic suggestions cannot contain more than 32 values")
		}
	}
	return nil
}

func normalizedTimestamp(name string, value *timestamppb.Timestamp) (time.Time, *timestamppb.Timestamp, error) {
	if value == nil || value.CheckValid() != nil {
		return time.Time{}, nil, invalid(name + " is required and must be a valid timestamp")
	}
	normalized := time.UnixMicro(value.AsTime().UnixMicro()).UTC()
	return normalized, timestamppb.New(normalized), nil
}

func checkedDuration(value *durationpb.Duration) (time.Duration, error) {
	if value == nil || value.CheckValid() != nil || value.Seconds < 0 || value.Nanos < 0 {
		return 0, invalid("duration must be a valid non-negative duration")
	}
	const (
		maxSeconds = int64(math.MaxInt64) / int64(time.Second)
		maxNanos   = int32(int64(math.MaxInt64) % int64(time.Second))
	)
	if value.Seconds > maxSeconds || (value.Seconds == maxSeconds && value.Nanos > maxNanos) {
		return 0, invalid("duration is outside the supported nanosecond range")
	}
	return time.Duration(value.Seconds)*time.Second + time.Duration(value.Nanos), nil
}

func terminalState(state opensplunkv1.SearchJobState) bool {
	return state >= opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED && state <= opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED
}

func validateText(name, value string, maximum int, allowEmpty bool) error {
	if (!allowEmpty && value == "") || len(value) > maximum || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		qualifier := fmt.Sprintf("must be valid UTF-8 without NUL bytes and contain at most %d bytes", maximum)
		if !allowEmpty {
			qualifier = fmt.Sprintf("must be non-empty valid UTF-8 without NUL bytes and contain at most %d bytes", maximum)
		}
		return invalid(name + " " + qualifier)
	}
	return nil
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return invalid("context is required")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("search history: %w", err)
	}
	return nil
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", control.ErrInvalidArgument, message)
}

func decodeEntry(encoded, expectedChecksum []byte) (*opensplunkv1.SearchHistoryEntry, indexedEntry, error) {
	checksum := sha256.Sum256(encoded)
	if len(expectedChecksum) != sha256.Size || !bytes.Equal(checksum[:], expectedChecksum) {
		return nil, indexedEntry{}, errors.New("search-history entry checksum mismatch")
	}
	entry := new(opensplunkv1.SearchHistoryEntry)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, entry); err != nil {
		return nil, indexedEntry{}, fmt.Errorf("decode search-history entry: %w", err)
	}
	normalizedEntry, normalized, err := normalizeEntry(entry)
	if err != nil {
		// Persisted corruption is an availability failure, never caller input.
		return nil, indexedEntry{}, fmt.Errorf("validate persisted search-history entry: %v", err)
	}
	if !bytes.Equal(normalized.encoded, encoded) {
		return nil, indexedEntry{}, errors.New("persisted search-history entry is not canonical")
	}
	return normalizedEntry, normalized, nil
}

func rejectUnknownFields(message protoreflect.Message) error {
	if len(message.GetUnknown()) != 0 {
		return errors.New("search-history entry contains unknown protobuf fields")
	}
	var visitErr error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsMap() {
			if field.MapValue().Kind() != protoreflect.MessageKind {
				return true
			}
			value.Map().Range(func(_ protoreflect.MapKey, mapValue protoreflect.Value) bool {
				visitErr = rejectUnknownFields(mapValue.Message())
				return visitErr == nil
			})
			return visitErr == nil
		}
		if field.IsList() {
			if field.Kind() != protoreflect.MessageKind {
				return true
			}
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if visitErr = rejectUnknownFields(list.Get(index).Message()); visitErr != nil {
					return false
				}
			}
			return true
		}
		if field.Kind() == protoreflect.MessageKind {
			visitErr = rejectUnknownFields(value.Message())
			return visitErr == nil
		}
		return true
	})
	return visitErr
}
