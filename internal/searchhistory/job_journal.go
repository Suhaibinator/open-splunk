package searchhistory

import (
	"context"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// JobJournal adapts detached search-job lifecycle snapshots to the durable
// history store. It deliberately records only reusable intent and safe
// execution metadata; generated SQL and result rows never cross this seam.
type JobJournal struct {
	store *Store
}

// NewJobJournal constructs a searchjobs.JobJournal backed by store.
func NewJobJournal(store *Store) (*JobJournal, error) {
	if store == nil || store.orm == nil {
		return nil, invalid("search-history store is required")
	}
	return &JobJournal{store: store}, nil
}

// Admit persists the queued attempt before the manager exposes or executes it.
func (journal *JobJournal) Admit(ctx context.Context, job searchjobs.Job) error {
	if journal == nil || journal.store == nil {
		return invalid("search-history job journal is unavailable")
	}
	entry, err := journal.entry(job, false)
	if err != nil {
		return err
	}
	_, err = journal.store.BeginAttempt(ctx, jobScope(job), entry)
	return err
}

// Finalize atomically publishes the first terminal snapshot for an admitted
// attempt. The manager and store both make duplicate delivery harmless.
func (journal *JobJournal) Finalize(ctx context.Context, job searchjobs.Job) error {
	if journal == nil || journal.store == nil {
		return invalid("search-history job journal is unavailable")
	}
	entry, err := journal.entry(job, true)
	if err != nil {
		return err
	}
	_, err = journal.store.CompleteAttempt(ctx, jobScope(job), entry)
	return err
}

func (journal *JobJournal) entry(job searchjobs.Job, terminal bool) (*opensplunkv1.SearchHistoryEntry, error) {
	if !searchjobs.ValidCompilerVersion(job.CompilerVersion) {
		return nil, invalid("search job compiler version is invalid")
	}
	if job.CompilerVersion != spl.CompatibilityVersion {
		return nil, invalid("search job compiler version does not match the current SPL compatibility identity")
	}
	knowledgeSnapshot, err := cloneKnowledgeSnapshotSummary(job.KnowledgeSnapshot)
	if err != nil {
		return nil, err
	}
	state, err := historyState(job.State)
	if err != nil {
		return nil, err
	}
	if terminal != job.State.Terminal() || (!terminal && job.State != searchjobs.StateQueued) {
		if terminal {
			return nil, invalid("search job must be terminal before history finalization")
		}
		return nil, invalid("search job must be queued at history admission")
	}

	timeRange, timezone, err := searchjobproto.TimeRange(job)
	if err != nil {
		return nil, invalid("search job time-range intent is invalid")
	}
	definition := &opensplunkv1.SearchDefinition{
		Spl:        job.SPL,
		IndexScope: slices.Clone(job.RequestedIndexes),
		TimeRange:  timeRange,
	}
	if job.AppID != "" {
		appID := job.AppID
		definition.AppId = &appID
	}
	source, err := searchjobproto.Source(job.Source)
	if err != nil {
		return nil, invalid("search job source metadata is invalid")
	}
	entry := &opensplunkv1.SearchHistoryEntry{
		SearchJobId: job.ID,
		Definition:  definition,
		Source:      source,
		ResolvedTimeRange: &opensplunkv1.ResolvedTimeRange{
			Earliest: timestamppb.New(job.Earliest),
			Latest:   timestamppb.New(job.Latest),
			Timezone: timezone,
		},
		FinalState:        state,
		CompilerVersion:   job.CompilerVersion,
		CreatedAt:         timestamppb.New(job.CreatedAt),
		KnowledgeSnapshot: knowledgeSnapshot,
	}
	if !terminal {
		return entry, nil
	}

	entry.EffectiveIndexScope = slices.Clone(job.EffectiveIndexes)
	entry.ScannedRows = job.ScannedRows
	entry.ScannedBytes = job.ScannedBytes
	entry.ProducedRows = job.RowCount
	entry.FinishedAt = timestamppb.New(job.FinishedAt)
	duration := time.Duration(0)
	if !job.StartedAt.IsZero() {
		entry.StartedAt = timestamppb.New(job.StartedAt)
		if job.FinishedAt.After(job.StartedAt) {
			duration = job.FinishedAt.Sub(job.StartedAt)
		}
	}
	entry.Duration = durationpb.New(duration)
	if state == opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED {
		entry.Failure = failureToHistory(job.Failure)
	}
	return entry, nil
}

func jobScope(job searchjobs.Job) AccessScope {
	return AccessScope{TenantID: job.TenantID, OwnerID: job.OwnerID}
}

func historyState(state searchjobs.State) (opensplunkv1.SearchJobState, error) {
	switch state {
	case searchjobs.StateQueued:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_QUEUED, nil
	case searchjobs.StateCompleted:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED, nil
	case searchjobs.StateFailed:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_FAILED, nil
	case searchjobs.StateCanceled:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_CANCELED, nil
	case searchjobs.StateExpired:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_EXPIRED, nil
	default:
		return opensplunkv1.SearchJobState_SEARCH_JOB_STATE_UNSPECIFIED, invalid("search job state cannot be recorded in history")
	}
}

func failureToHistory(failure *searchjobs.Failure) *opensplunkv1.SearchFailure {
	if failure == nil {
		return &opensplunkv1.SearchFailure{
			Code:    opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL,
			Message: "search failed without a safe failure summary",
		}
	}
	message := boundedUTF8(failure.Message, maximumFailureMessageBytes)
	if strings.TrimSpace(message) == "" {
		message = "search failed"
	}
	result := &opensplunkv1.SearchFailure{
		Code:        failureCodeToHistory(failure.Code),
		Message:     message,
		Retryable:   failure.Retryable,
		Diagnostics: make([]*opensplunkv1.Diagnostic, 0, min(len(failure.Diagnostics), maximumDiagnostics)),
	}
	for _, diagnostic := range failure.Diagnostics[:min(len(failure.Diagnostics), maximumDiagnostics)] {
		code := boundedUTF8(strings.TrimSpace(diagnostic.Code), 128)
		if code == "" {
			code = "SPL_DIAGNOSTIC"
		}
		message := boundedUTF8(diagnostic.Message, maximumFailureMessageBytes)
		if strings.TrimSpace(message) == "" {
			message = "search diagnostic"
		}
		converted := &opensplunkv1.Diagnostic{
			Code:        code,
			Severity:    opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
			Message:     message,
			Suggestions: make([]string, 0, min(len(diagnostic.Suggestions), 32)),
		}
		for _, suggestion := range diagnostic.Suggestions[:min(len(diagnostic.Suggestions), 32)] {
			converted.Suggestions = append(converted.Suggestions, boundedUTF8(suggestion, 1024))
		}
		converted.SourceRange = searchjobproto.SourceRange(diagnostic)
		result.Diagnostics = append(result.Diagnostics, converted)
	}
	return result
}

func failureCodeToHistory(code searchjobs.FailureCode) opensplunkv1.SearchFailureCode {
	if projected := searchjobproto.FailureCode(code); projected != opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_UNSPECIFIED {
		return projected
	}
	return opensplunkv1.SearchFailureCode_SEARCH_FAILURE_CODE_INTERNAL
}

func boundedUTF8(value string, maximum int) string {
	value = strings.ReplaceAll(value, "\x00", "�")
	if len(value) <= maximum && utf8.ValidString(value) {
		return value
	}
	if !utf8.ValidString(value) {
		value = strings.ToValidUTF8(value, "�")
	}
	if len(value) <= maximum {
		return value
	}
	value = value[:maximum]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

var _ searchjobs.JobJournal = (*JobJournal)(nil)
