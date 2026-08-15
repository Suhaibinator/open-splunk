package server

import (
	"errors"
	"slices"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func searchJobToProto(job searchjobs.Job, now time.Time) (*opensplunkv1.SearchJob, error) {
	if !searchjobs.ValidCompilerVersion(job.CompilerVersion) {
		return nil, errors.New("search job contains an invalid compiler version")
	}
	knowledgeSnapshot, err := projectKnowledgeSnapshotSummary(job.KnowledgeSnapshot)
	if err != nil {
		return nil, err
	}
	resultShape := searchjobproto.ResultShapeForSPL(job.SPL)
	earliest, err := validTimestamp(job.Earliest)
	if err != nil {
		return nil, err
	}
	latest, err := validTimestamp(job.Latest)
	if err != nil {
		return nil, err
	}
	indexTimeCutoff, err := validTimestamp(job.IndexTimeCutoff)
	if err != nil {
		return nil, err
	}
	createdAt, err := validTimestamp(job.CreatedAt)
	if err != nil {
		return nil, err
	}
	timeRange, timezone, err := searchjobproto.TimeRange(job)
	if err != nil {
		return nil, errors.New("search job contains invalid time-range intent")
	}
	source, err := searchjobproto.Source(job.Source)
	if err != nil {
		return nil, errors.New("search job contains invalid source metadata")
	}
	progress, err := searchjobproto.Progress(job, now)
	if err != nil {
		return nil, errors.New("search job contains invalid progress metadata")
	}
	if err := validateBoundedIdentifier(job.AppID, maximumSavedSearchAppIDBytes, true); err != nil {
		return nil, errors.New("search job contains an invalid app ID")
	}
	definition := &opensplunkv1.SearchDefinition{
		Spl:        job.SPL,
		TimeRange:  timeRange,
		IndexScope: slices.Clone(job.RequestedIndexes),
	}
	if job.AppID != "" {
		definition.AppId = new(job.AppID)
	}
	result := &opensplunkv1.SearchJob{
		SearchJobId:         job.ID,
		StateVersion:        job.Version,
		Definition:          definition,
		Source:              source,
		NormalizedSpl:       optionalString(job.NormalizedSPL),
		CompilerVersion:     job.CompilerVersion,
		EffectiveIndexScope: slices.Clone(job.EffectiveIndexes),
		ResolvedTimeRange: &opensplunkv1.ResolvedTimeRange{
			Earliest: earliest,
			Latest:   latest,
			Timezone: timezone,
		},
		IndexTimeCutoff:   indexTimeCutoff,
		State:             searchjobproto.State(job.State),
		ResultKind:        resultShape.Kind,
		ResultsTruncated:  job.ResultsTruncated,
		Progress:          progress,
		CreatedAt:         createdAt,
		KnowledgeSnapshot: knowledgeSnapshot,
	}
	if job.Schema != nil {
		result.ResultSchema, err = searchjobproto.Schema(job.ID, *job.Schema, resultShape)
		if err != nil {
			return nil, err
		}
	}
	if job.Failure != nil {
		result.Failure = searchjobproto.Failure(*job.Failure)
		result.Diagnostics = searchjobproto.Diagnostics(job.Failure.Diagnostics)
	}
	if job.ResultsTruncated {
		occurredAt := job.FinishedAt
		if occurredAt.IsZero() {
			occurredAt = now.Round(0).UTC()
		}
		warningTime, timestampErr := validTimestamp(occurredAt)
		if timestampErr != nil {
			return nil, timestampErr
		}
		result.Warnings = append(result.Warnings, &opensplunkv1.ApiWarning{
			Code:       "RESULTS_TRUNCATED",
			Message:    "Retained search results reached the server row boundary; a bounded export can re-execute the same scoped query.",
			OccurredAt: warningTime,
		})
	}
	if !job.StartedAt.IsZero() {
		result.StartedAt, err = validTimestamp(job.StartedAt)
		if err != nil {
			return nil, err
		}
	}
	if !job.FinishedAt.IsZero() {
		result.FinishedAt, err = validTimestamp(job.FinishedAt)
		if err != nil {
			return nil, err
		}
	}
	if !job.ExpiresAt.IsZero() {
		result.ExpiresAt, err = validTimestamp(job.ExpiresAt)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func indexSummaryToProto(index control.Index) *opensplunkv1.IndexSummary {
	return &opensplunkv1.IndexSummary{
		IndexId:         index.ID,
		Name:            index.Definition.Name,
		DisplayName:     index.Definition.DisplayName,
		State:           indexStateToProto(index.State),
		IngestionAccess: accessState(index.Definition.IngestionEnabled),
		SearchAccess:    accessState(index.Definition.SearchEnabled),
	}
}

func indexStateToProto(state control.IndexState) opensplunkv1.IndexState {
	switch state {
	case control.IndexStateActive:
		return opensplunkv1.IndexState_INDEX_STATE_ACTIVE
	case control.IndexStateArchived:
		return opensplunkv1.IndexState_INDEX_STATE_ARCHIVED
	case control.IndexStateDeleting:
		return opensplunkv1.IndexState_INDEX_STATE_DELETING
	default:
		return opensplunkv1.IndexState_INDEX_STATE_UNSPECIFIED
	}
}

func accessState(enabled bool) opensplunkv1.IndexAccessState {
	if enabled {
		return opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED
	}
	return opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_DISABLED
}

func validTimestamp(input time.Time) (*timestamppb.Timestamp, error) {
	if input.IsZero() {
		return nil, errors.New("required timestamp is zero")
	}
	return timestampToProto(input)
}

// timestampToProto accepts the protobuf minimum instant. In Go that instant is
// time.Time's zero value, which is invalid only for required metadata fields,
// not for a typed search-result cell.
func timestampToProto(input time.Time) (*timestamppb.Timestamp, error) {
	result := timestamppb.New(input.Round(0).UTC())
	if err := result.CheckValid(); err != nil {
		return nil, errors.New("timestamp is outside protobuf range")
	}
	return result, nil
}

func cloneApps(input []*opensplunkv1.AppSummary) []*opensplunkv1.AppSummary {
	result := make([]*opensplunkv1.AppSummary, len(input))
	for index, app := range input {
		result[index] = proto.Clone(app).(*opensplunkv1.AppSummary)
	}
	return result
}

func appExists(apps []*opensplunkv1.AppSummary, id string) bool {
	for _, app := range apps {
		if app.GetAppId() == id {
			return true
		}
	}
	return false
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return new(value)
}

// protobufRouteDefinition keeps protobuf routes behind the constructor that
// installs the version-skew boundary. The unexported wrapper prevents another
// package from supplying an untracked SRouter definition to the protobuf route
// set.
type protobufRouteDefinition struct {
	definition router.RouteDefinition
}

func newForwardCompatibleProtoRoute[Request proto.Message, Response any](
	config router.RouteConfig[Request, Response],
) protobufRouteDefinition {
	config.Sanitizer = forwardCompatibleProtoSanitizer[Request]
	return protobufRouteDefinition{
		definition: router.NewGenericRouteDefinition[Request, Response, string, struct{}](config),
	}
}

func unwrapProtobufRoutes(routes []protobufRouteDefinition) []router.RouteDefinition {
	definitions := make([]router.RouteDefinition, len(routes))
	for index := range routes {
		definitions[index] = routes[index].definition
	}
	return definitions
}

// forwardCompatibleProtoSanitizer discards fields unknown to this server before
// request validation or persistence. Create and update knowledge requests are
// the exception: unknown fields inside their persisted definitions are rejected
// before ordinary unknown fields are cleared. SRouter has already enforced the
// raw body limit, so discarded bytes still consume the caller's request budget.
func forwardCompatibleProtoSanitizer[T proto.Message](request T) (T, error) {
	if isNilDependency(request) {
		return request, nil
	}
	switch knowledgeRequest := any(request).(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		if err := rejectUnknownKnowledgeDefinition(knowledgeRequest.GetDefinition()); err != nil {
			return request, err
		}
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		if err := rejectUnknownKnowledgeDefinition(knowledgeRequest.GetDefinition()); err != nil {
			return request, err
		}
	case *opensplunkv1.ValidateKnowledgeObjectRequest:
		// Validate distinguishes unknown request and mask fields (envelope
		// errors) from unknown applied-definition fields (in-band candidate
		// invalidity). Its dedicated decoder has already projected updates and
		// bounded dangerous repetitions, so no generic clearing is safe here.
		return request, nil
	case *opensplunkv1.PreviewKnowledgeObjectRequest:
		// Preview shares Validate's candidate-envelope unknown authority. Its
		// request-only decoder applies the same bounded update projection before
		// this sanitizer can ever be used by a future route.
		return request, nil
	case *opensplunkv1.CreateLookupRequest:
		if err := rejectUnknownLookupDefinition(knowledgeRequest.GetDefinition()); err != nil {
			return request, err
		}
	case *opensplunkv1.ReplaceLookupRequest:
		if err := rejectUnknownLookupDefinition(knowledgeRequest.GetDefinition()); err != nil {
			return request, err
		}
	case *opensplunkv1.PreviewLookupRequest:
		if err := rejectUnknownLookupDefinition(knowledgeRequest.GetDefinition()); err != nil {
			return request, err
		}
	}

	pending := []protoreflect.Message{request.ProtoReflect()}
	for len(pending) != 0 {
		last := len(pending) - 1
		message := pending[last]
		pending = pending[:last]
		if !message.IsValid() {
			continue
		}

		message.SetUnknown(nil)
		message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			switch {
			case field.IsMap():
				if field.MapValue().Message() == nil {
					return true
				}
				value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
					pending = append(pending, item.Message())
					return true
				})
			case field.IsList():
				if field.Message() == nil {
					return true
				}
				list := value.List()
				for index := 0; index < list.Len(); index++ {
					pending = append(pending, list.Get(index).Message())
				}
			case field.Message() != nil:
				pending = append(pending, value.Message())
			}
			return true
		})
	}
	return request, nil
}
