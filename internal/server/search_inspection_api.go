package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchinspection"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

const maximumSearchInspectionResponseBytes = 8 << 20

func (handler *apiHandler) searchInspectionRoutes(
	noAuth router.AuthLevel,
	smallRequestBytes int64,
) []protobufRouteDefinition {
	return []protobufRouteDefinition{
		newForwardCompatibleProtoRoute[
			*opensplunkv1.InspectSearchJobRequest,
			*serializedSearchInspectionResponse](router.RouteConfig[
			*opensplunkv1.InspectSearchJobRequest,
			*serializedSearchInspectionResponse,
		]{
			Path:       searchInspectionRoute,
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedSearchInspectionCodec(),
			Handler:    handler.inspectSearchJob,
			SourceType: router.Body,
			Overrides: sroutercommon.RouteOverrides{
				MaxBodySize: smallRequestBytes,
			},
		}),
	}
}

func (handler *apiHandler) inspectSearchJob(
	request *http.Request,
	input *opensplunkv1.InspectSearchJobRequest,
) (*serializedSearchInspectionResponse, error) {
	access, err := handler.searchInspectionAccess(request)
	if err != nil {
		return nil, err
	}
	if input == nil ||
		len(input.ProtoReflect().GetUnknown()) != 0 ||
		!validSearchInspectionJobID(input.GetSearchJobId()) {
		return nil, badRequestError("search inspection request is invalid")
	}
	if handler.searchInspections == nil {
		return nil, unavailableError("search inspection service is unavailable")
	}
	if err := searchInspectionRequestContextError(request.Context()); err != nil {
		return nil, err
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError(
			"search inspection response capacity is exhausted",
		)
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	jobID := strings.Clone(input.GetSearchJobId())
	result, operationErr := handler.searchInspections.Inspect(
		request.Context(),
		access,
		searchinspection.Request{SearchJobID: jobID},
	)
	if mapped := mapSearchInspectionCallError(
		request.Context(),
		operationErr,
	); mapped != nil {
		return nil, mapped
	}
	if err := searchInspectionRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	if err := searchinspection.ValidateResult(result); err != nil {
		return nil, internalError()
	}
	if err := searchInspectionRequestContextError(request.Context()); err != nil {
		return nil, err
	}

	message, err := searchInspectionResultToProto(jobID, result)
	if err != nil || proto.Size(message) > maximumSearchInspectionResponseBytes {
		return nil, internalError()
	}
	if err := searchInspectionRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedSearchInspectionResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) searchInspectionAccess(
	request *http.Request,
) (searchjobs.AccessScope, error) {
	principal, err := handler.administratorPrincipal(request)
	if err != nil {
		return searchjobs.AccessScope{}, err
	}
	return searchjobs.AccessScope{
		TenantID: principal.TenantID(),
		OwnerID:  principal.OwnerID(),
	}, nil
}

func validSearchInspectionJobID(value string) bool {
	return strings.TrimSpace(value) == value &&
		validateBoundedIdentifier(value, searchjobs.MaximumJobIDBytes, false) == nil
}

func searchInspectionResultToProto(
	jobID string,
	result searchinspection.Result,
) (*opensplunkv1.InspectSearchJobResponse, error) {
	outputKind, err := searchInspectionOutputKindToProto(result.Plan.Output.Kind)
	if err != nil {
		return nil, err
	}
	knowledgeSnapshot, err := projectKnowledgeSnapshotSummary(
		result.KnowledgeSnapshot,
	)
	if err != nil {
		return nil, err
	}

	stages := make(
		[]*opensplunkv1.SearchInspectionLogicalStage,
		len(result.Plan.Stages),
	)
	for index, stage := range result.Plan.Stages {
		var sourceRange *opensplunkv1.SourceRange
		if stage.SourceRange != nil {
			sourceRange = &opensplunkv1.SourceRange{
				Start: searchInspectionSourcePositionToProto(
					stage.SourceRange.Start,
				),
				End: searchInspectionSourcePositionToProto(
					stage.SourceRange.End,
				),
			}
		}
		operatorProvenance := make(
			[]*opensplunkv1.KnowledgeProvenance,
			len(stage.KnowledgeObjects),
		)
		for provenanceIndex, provenance := range stage.KnowledgeObjects {
			operatorProvenance[provenanceIndex] =
				searchInspectionRedactedProvenanceToProto(provenance)
		}
		outputProvenance := make(
			[]*opensplunkv1.SearchInspectionOutputProvenance,
			len(stage.OutputProvenance),
		)
		for provenanceIndex, provenance := range stage.OutputProvenance {
			object, ok := searchInspectionRedactedProvenanceByOrdinal(
				stage.KnowledgeObjects,
				provenance.ObjectOrdinal,
			)
			if !ok {
				return nil, fmt.Errorf(
					"project search inspection result: output provenance is invalid",
				)
			}
			outputProvenance[provenanceIndex] =
				&opensplunkv1.SearchInspectionOutputProvenance{
					OutputField: strings.Clone(provenance.Field),
					Provenance: searchInspectionRedactedProvenanceToProto(
						object,
					),
				}
		}
		stages[index] = &opensplunkv1.SearchInspectionLogicalStage{
			StageIndex:         stage.Index,
			Operator:           strings.Clone(stage.Operator),
			InputFields:        slices.Clone(stage.InputFields),
			OutputFields:       slices.Clone(stage.OutputFields),
			SourceRange:        sourceRange,
			OperatorProvenance: operatorProvenance,
			OutputProvenance:   outputProvenance,
		}
	}
	reads := make(
		[]*opensplunkv1.SearchInspectionPhysicalRead,
		len(result.PhysicalPlan.Reads),
	)
	for readIndex, read := range result.PhysicalPlan.Reads {
		indexes := make(
			[]*opensplunkv1.SearchInspectionPhysicalIndex,
			len(read.Indexes),
		)
		for index, physicalIndex := range read.Indexes {
			indexes[index] = searchInspectionPhysicalIndexToProto(
				physicalIndex,
			)
		}
		reads[readIndex] = &opensplunkv1.SearchInspectionPhysicalRead{
			Columns: slices.Clone(read.Columns),
			Indexes: indexes,
		}
	}

	return &opensplunkv1.InspectSearchJobResponse{
		SearchJobId: strings.Clone(jobID),
		LogicalPlan: &opensplunkv1.SearchInspectionLogicalPlan{
			Stages:           stages,
			ReferencedFields: slices.Clone(result.Plan.ReferencedFields),
			Output: &opensplunkv1.SearchInspectionOutputShape{
				Kind:             outputKind,
				Fields:           slices.Clone(result.Plan.Output.Fields),
				MaxDynamicFields: uint32(result.Plan.Output.MaxDynamicFields),
			},
		},
		PhysicalPlan: &opensplunkv1.SearchInspectionPhysicalPlan{
			NodeTypes: slices.Clone(result.PhysicalPlan.NodeTypes),
			Reads:     reads,
		},
		GeneratedSql:      strings.Clone(result.GeneratedSQL),
		ExplainText:       strings.Clone(result.ExplainText),
		DiagnosticQueryId: strings.Clone(result.DiagnosticQueryID),
		KnowledgeSnapshot: knowledgeSnapshot,
	}, nil
}

func searchInspectionRedactedProvenanceToProto(
	value searchinspection.RedactedObjectProvenance,
) *opensplunkv1.KnowledgeProvenance {
	return &opensplunkv1.KnowledgeProvenance{
		Source: &opensplunkv1.KnowledgeProvenance_RedactedObject{
			RedactedObject: &opensplunkv1.KnowledgeRedactedObjectProvenance{
				RedactedObjectOrdinal: value.Ordinal,
				ObjectType:            value.ObjectType,
				Stage:                 value.Stage,
			},
		},
	}
}

func searchInspectionRedactedProvenanceByOrdinal(
	objects []searchinspection.RedactedObjectProvenance,
	ordinal uint32,
) (searchinspection.RedactedObjectProvenance, bool) {
	if len(objects) == 1 {
		return objects[0], objects[0].Ordinal == ordinal
	}
	index, found := slices.BinarySearchFunc(
		objects,
		ordinal,
		func(
			object searchinspection.RedactedObjectProvenance,
			want uint32,
		) int {
			if object.Ordinal < want {
				return -1
			}
			if object.Ordinal > want {
				return 1
			}
			return 0
		},
	)
	if !found {
		return searchinspection.RedactedObjectProvenance{}, false
	}
	return objects[index], true
}

func searchInspectionSourcePositionToProto(
	position searchinspection.SourcePosition,
) *opensplunkv1.SourcePosition {
	return &opensplunkv1.SourcePosition{
		ByteOffset: position.ByteOffset,
		Line:       position.Line,
		Column:     position.Column,
	}
}

func searchInspectionPhysicalIndexToProto(
	index queryexec.ExplainIndex,
) *opensplunkv1.SearchInspectionPhysicalIndex {
	return &opensplunkv1.SearchInspectionPhysicalIndex{
		Type:             strings.Clone(index.Type),
		Name:             strings.Clone(index.Name),
		Keys:             slices.Clone(index.Keys),
		InitialParts:     index.InitialParts,
		SelectedParts:    index.SelectedParts,
		InitialGranules:  index.InitialGranules,
		SelectedGranules: index.SelectedGranules,
	}
}

func searchInspectionOutputKindToProto(
	kind searchinspection.OutputKind,
) (opensplunkv1.SearchInspectionOutputKind, error) {
	switch kind {
	case searchinspection.OutputKindOpen:
		return opensplunkv1.SearchInspectionOutputKind_SEARCH_INSPECTION_OUTPUT_KIND_OPEN, nil
	case searchinspection.OutputKindStatic:
		return opensplunkv1.SearchInspectionOutputKind_SEARCH_INSPECTION_OUTPUT_KIND_STATIC, nil
	case searchinspection.OutputKindDynamic:
		return opensplunkv1.SearchInspectionOutputKind_SEARCH_INSPECTION_OUTPUT_KIND_DYNAMIC, nil
	default:
		return opensplunkv1.SearchInspectionOutputKind_SEARCH_INSPECTION_OUTPUT_KIND_UNSPECIFIED,
			errors.New("invalid search inspection output kind")
	}
}

func mapSearchInspectionCallError(
	ctx context.Context,
	operationErr error,
) error {
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"search inspection request was canceled",
		)
	}
	switch {
	case errors.Is(operationErr, searchinspection.ErrInvalidRequest):
		return badRequestError("search inspection request is invalid")
	case errors.Is(operationErr, searchjobs.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "search job not found")
	case errors.Is(operationErr, searchjobs.ErrExpired):
		return router.NewHTTPError(
			http.StatusGone,
			"search job results expired",
		)
	case errors.Is(operationErr, searchjobs.ErrResultsNotReady):
		return router.NewHTTPError(
			http.StatusConflict,
			"search results are not ready",
		)
	case errors.Is(operationErr, searchjobs.ErrResultsUnavailable):
		return router.NewHTTPError(
			http.StatusConflict,
			"search results are unavailable",
		)
	case errors.Is(operationErr, searchjobs.ErrUnsupportedValue),
		errors.Is(operationErr, searchjobs.ErrExecutionLimit):
		return router.NewHTTPError(
			http.StatusUnprocessableEntity,
			"search inspection is unsupported or exceeded its execution limit",
		)
	case errors.Is(operationErr, searchjobs.ErrCapacity):
		return router.NewHTTPError(
			http.StatusTooManyRequests,
			"search inspection capacity is exhausted",
		)
	case errors.Is(operationErr, searchjobs.ErrClosed),
		errors.Is(operationErr, searchjobs.ErrStorageUnavailable):
		return unavailableError("search inspection service is unavailable")
	default:
		return internalError()
	}
}

func searchInspectionRequestContextError(ctx context.Context) error {
	return canceledRequestError(ctx, "search inspection request was canceled")
}

type serializedSearchInspectionResponse = boundedProtoResponse[*opensplunkv1.InspectSearchJobResponse]

type serializedSearchInspectionCodec = boundedProtoCodec[
	*opensplunkv1.InspectSearchJobRequest,
	*opensplunkv1.InspectSearchJobResponse,
]

func newSerializedSearchInspectionCodec() *serializedSearchInspectionCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.InspectSearchJobRequest,
			*opensplunkv1.InspectSearchJobResponse,
		](),
		boundedProtoCodecOptions{
			stateError:   "search inspection serialization state is invalid",
			messageError: "search inspection response is missing",
			contextError: searchInspectionRequestContextError,
			maximumBytes: maximumSearchInspectionResponseBytes,
			sizeError:    "search inspection response exceeds its byte limit",
		},
	)
}
