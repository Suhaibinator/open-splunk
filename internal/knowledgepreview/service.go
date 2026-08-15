// Package knowledgepreview owns the bounded, transport-independent advisory
// comparison of one active-publication candidate against a retained immutable
// search execution. It has no catalog resolver, route, capability, or feature
// gate authority.
package knowledgepreview

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultMaximumRows   = uint32(100)
	MaximumRows          = uint32(1_000)
	MaximumColumns       = 1_024
	MaximumResponseBytes = 8 << 20
)

var (
	ErrInvalidRequest      = errors.New("knowledge preview request is invalid")
	ErrNotFoundOrForbidden = errors.New("knowledge preview execution was not found or is forbidden")
	ErrUnavailable         = errors.New("knowledge preview is unavailable")
	ErrResourceLimit       = errors.New("knowledge preview resource limit exceeded")
	ErrResponseTooLarge    = errors.New("knowledge preview response exceeds limit")
	ErrInvariant           = errors.New("knowledge preview authority is invalid")
	errRowLimit            = errors.New("knowledge preview row limit reached")
)

// RetainedExecutionSource is the sole job seam. Manager satisfies it directly
// and returns one lease plus the execution snapshot sealed to that exact result
// generation.
type RetainedExecutionSource interface {
	AcquireExecutionFor(
		context.Context,
		searchjobs.AccessScope,
		string,
	) (searchjobs.ResultLease, searchjobs.ExecutionSnapshot, error)
}

// Compiler lowers a complete prospective program for one Manager-minted
// execution. Implementations must return ordinary compiler-sealed authority.
type Compiler interface {
	CompilePreview(
		context.Context,
		searchjobs.ExecutionSnapshot,
		knowledgeprogram.Program,
	) (clickhouse.CompiledQuery, error)
}

// ProductionCompilerAdapter calls the ordinary compiler without a tag, bypass,
// or alternate sealing seam.
type ProductionCompilerAdapter struct {
	Compiler clickhouse.Compiler
}

func (adapter ProductionCompilerAdapter) CompilePreview(
	ctx context.Context,
	execution searchjobs.ExecutionSnapshot,
	program knowledgeprogram.Program,
) (clickhouse.CompiledQuery, error) {
	if ctx == nil {
		return clickhouse.CompiledQuery{}, ErrInvariant
	}
	if err := ctx.Err(); err != nil {
		return clickhouse.CompiledQuery{}, err
	}
	logical, err := searchsnapshot.BuildExecutionPlanWithKnowledgePrelude(
		execution,
		program,
	)
	if err != nil {
		return clickhouse.CompiledQuery{}, err
	}
	if err := ctx.Err(); err != nil {
		return clickhouse.CompiledQuery{}, err
	}
	return adapter.Compiler.Compile(logical)
}

// Config supplies every authority required by Preview. No dependency can be
// inferred from a catalog resolver or current search admission service.
type Config struct {
	Searches RetainedExecutionSource
	Writer   *knowledgecatalog.Writer
	Compiler Compiler
	Executor searchjobs.Executor
}

// Service is immutable after construction and safe for concurrent use. The
// catalog Writer and retained Manager retain their own bounded gates.
type Service struct {
	searches RetainedExecutionSource
	writer   *knowledgecatalog.Writer
	compiler Compiler
	executor searchjobs.Executor
}

func NewService(config Config) (*Service, error) {
	if nilcheck.IsNil(config.Searches) || config.Writer == nil ||
		!config.Writer.ReadyForManagement() || nilcheck.IsNil(config.Compiler) ||
		nilcheck.IsNil(config.Executor) {
		return nil, errors.New("knowledge preview dependencies are incomplete")
	}
	return &Service{
		searches: config.Searches,
		writer:   config.Writer,
		compiler: config.Compiler,
		executor: config.Executor,
	}, nil
}

// Ready reports only complete service construction. It does not advertise a
// capability or imply that the production compiler accepts nonempty programs.
func (service *Service) Ready() bool {
	return service != nil && !nilcheck.IsNil(service.searches) &&
		service.writer != nil && service.writer.ReadyForManagement() &&
		!nilcheck.IsNil(service.compiler) && !nilcheck.IsNil(service.executor)
}

// Preview validates, derives, compiles, and executes one all-or-nothing
// comparison while holding the exact retained result lease open. Candidate
// invalidity is the sole successful validation-only outcome.
func (service *Service) Preview(
	ctx context.Context,
	access searchjobs.AccessScope,
	validationScope knowledgecatalog.ValidationScope,
	request *opensplunkv1.PreviewKnowledgeObjectRequest,
) (SealedResponse, error) {
	if ctx == nil || !service.Ready() || request == nil {
		return SealedResponse{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return SealedResponse{}, err
	}
	maximumRows, err := previewMaximumRows(request.MaximumRows)
	if err != nil {
		return SealedResponse{}, err
	}
	if access.TenantID == "" || access.OwnerID == "" ||
		validationScope.Read.TenantID != access.TenantID ||
		validationScope.Read.OwnerID != access.OwnerID ||
		validationScope.Write.TenantID != access.TenantID ||
		validationScope.Write.OwnerID != access.OwnerID {
		return SealedResponse{}, ErrNotFoundOrForbidden
	}

	lease, execution, err := service.searches.AcquireExecutionFor(
		ctx,
		searchjobs.AccessScope{
			TenantID: strings.Clone(access.TenantID),
			OwnerID:  strings.Clone(access.OwnerID),
		},
		request.GetRetainedSearchJobId(),
	)
	if err != nil {
		return SealedResponse{}, classifyAcquireError(err)
	}
	if lease == nil {
		return SealedResponse{}, ErrUnavailable
	}
	defer lease.Close()
	if execution.ID != request.GetRetainedSearchJobId() ||
		execution.TenantID != access.TenantID || execution.OwnerID != access.OwnerID {
		return SealedResponse{}, ErrNotFoundOrForbidden
	}
	if _, valid := execution.ValidatedResultLease(lease); !valid {
		return SealedResponse{}, ErrUnavailable
	}
	retained, err := execution.OpenRetainedKnowledgeExecution()
	if err != nil || retained == nil {
		return SealedResponse{}, ErrUnavailable
	}

	validationRequest := activeValidationView(request)
	sealedValidation, err := service.writer.Validate(
		ctx,
		cloneValidationScope(validationScope),
		validationRequest,
	)
	if err != nil {
		return SealedResponse{}, err
	}
	validation, err := sealedValidation.Proto(ctx)
	if err != nil || validation == nil || validation.GetResult() == nil {
		return SealedResponse{}, ErrInvariant
	}
	if !validation.GetResult().GetValid() {
		return SealResponse(ctx, ResponseInput{
			Validation:  sealedValidation,
			JobID:       execution.ID,
			MaximumRows: maximumRows,
		})
	}

	program, err := prospectiveProgram(
		ctx,
		execution,
		request,
		validation.GetResult(),
	)
	if err != nil {
		return SealedResponse{}, err
	}
	after, err := service.compiler.CompilePreview(ctx, execution, program)
	if err != nil {
		return SealedResponse{}, classifyExecutionError(err)
	}
	if err := validateCompiledPreview(after, execution, program); err != nil {
		return SealedResponse{}, err
	}
	before := retained.CompiledQuery
	if err := validateRetainedCompiled(before, execution, retained.KnowledgePrelude); err != nil {
		return SealedResponse{}, err
	}

	shape := searchjobproto.ResultShapeForSPL(execution.SPL)
	beforeProjection, err := executeProjection(
		ctx, service.executor, before, execution.ID, shape, maximumRows,
	)
	if err != nil {
		return SealedResponse{}, err
	}
	afterProjection, err := executeProjection(
		ctx, service.executor, after, execution.ID, shape, maximumRows,
	)
	if err != nil {
		return SealedResponse{}, err
	}
	if err := ctx.Err(); err != nil {
		return SealedResponse{}, err
	}
	return SealResponse(ctx, ResponseInput{
		Validation:   sealedValidation,
		BeforeSchema: beforeProjection.schema,
		AfterSchema:  afterProjection.schema,
		BeforeRows:   beforeProjection.rows,
		AfterRows:    afterProjection.rows,
		Truncated:    beforeProjection.truncated || afterProjection.truncated,
		JobID:        execution.ID,
		MaximumRows:  maximumRows,
	})
}

func previewMaximumRows(value *uint32) (uint32, error) {
	if value == nil {
		return DefaultMaximumRows, nil
	}
	if *value == 0 || *value > MaximumRows {
		return 0, fmt.Errorf("%w: maximum_rows must be between 1 and %d", ErrInvalidRequest, MaximumRows)
	}
	return *value, nil
}

func activeValidationView(
	request *opensplunkv1.PreviewKnowledgeObjectRequest,
) *opensplunkv1.ValidateKnowledgeObjectRequest {
	return &opensplunkv1.ValidateKnowledgeObjectRequest{
		Definition:        request.Definition,
		KnowledgeObjectId: request.KnowledgeObjectId,
		ExpectedVersion:   request.ExpectedVersion,
		UpdateMask:        request.UpdateMask,
		Intent:            opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	}
}

func cloneValidationScope(scope knowledgecatalog.ValidationScope) knowledgecatalog.ValidationScope {
	return knowledgecatalog.ValidationScope{
		Read: knowledgecatalog.ReadScope{
			TenantID:       strings.Clone(scope.Read.TenantID),
			OwnerID:        strings.Clone(scope.Read.OwnerID),
			ReadableAppIDs: slices.Clone(scope.Read.ReadableAppIDs),
		},
		Write: knowledgecatalog.WriteScope{
			TenantID:       strings.Clone(scope.Write.TenantID),
			OwnerID:        strings.Clone(scope.Write.OwnerID),
			WritableAppIDs: slices.Clone(scope.Write.WritableAppIDs),
		},
	}
}

func prospectiveProgram(
	ctx context.Context,
	execution searchjobs.ExecutionSnapshot,
	request *opensplunkv1.PreviewKnowledgeObjectRequest,
	validation *opensplunkv1.KnowledgeValidationResult,
) (knowledgeprogram.Program, error) {
	if err := ctx.Err(); err != nil {
		return knowledgeprogram.Program{}, err
	}
	if validation == nil || !validation.GetValid() ||
		validation.GetNormalizedDefinition() == nil ||
		len(validation.GetDefinitionSha256()) != sha256.Size {
		return knowledgeprogram.Program{}, ErrInvariant
	}
	snapshot := execution.KnowledgeSnapshot.Proto()
	if snapshot == nil {
		return knowledgeprogram.Program{}, ErrUnavailable
	}
	objects := make([]*opensplunkv1.KnowledgeSnapshotObject, 0, len(snapshot.GetObjects())+1)
	byID := make(map[string]*opensplunkv1.KnowledgeSnapshotObject, len(snapshot.GetObjects())+1)
	for _, object := range snapshot.GetObjects() {
		cloned, ok := proto.Clone(object).(*opensplunkv1.KnowledgeSnapshotObject)
		if !ok || cloned == nil || cloned.GetKnowledgeObjectId() == "" {
			return knowledgeprogram.Program{}, ErrUnavailable
		}
		if _, exists := byID[cloned.GetKnowledgeObjectId()]; exists {
			return knowledgeprogram.Program{}, ErrUnavailable
		}
		objects = append(objects, cloned)
		byID[cloned.GetKnowledgeObjectId()] = cloned
	}

	normalized, ok := proto.Clone(validation.GetNormalizedDefinition()).(*opensplunkv1.KnowledgeObjectDefinition)
	if !ok || normalized == nil {
		return knowledgeprogram.Program{}, ErrInvariant
	}
	objectType, stage, rank, ok := candidateTypeAndStage(normalized)
	if !ok || objectType != validation.GetObjectType() {
		return knowledgeprogram.Program{}, ErrInvariant
	}
	candidateID := request.GetKnowledgeObjectId()
	candidateVersion := uint64(1)
	candidateOwner := execution.OwnerID
	if request.KnowledgeObjectId != nil {
		retained := byID[candidateID]
		if retained == nil || retained.GetVersion() != request.GetExpectedVersion() ||
			request.GetExpectedVersion() == math.MaxInt64 {
			return knowledgeprogram.Program{}, ErrUnavailable
		}
		candidateVersion = request.GetExpectedVersion() + 1
		candidateOwner = retained.GetOwnerId()
		for index, object := range objects {
			if object.GetKnowledgeObjectId() == candidateID {
				objects = append(objects[:index], objects[index+1:]...)
				break
			}
		}
	} else {
		candidateID = previewObjectID(execution.ID, validation.GetDefinitionSha256(), byID)
		if candidateID == "" {
			return knowledgeprogram.Program{}, ErrResourceLimit
		}
	}
	candidate := &opensplunkv1.KnowledgeSnapshotObject{
		Stage:             stage,
		KnowledgeObjectId: candidateID,
		Version:           candidateVersion,
		ObjectType:        objectType,
		Name:              strings.Clone(normalized.GetName()),
		AppId:             strings.Clone(normalized.GetAppId()),
		OwnerId:           strings.Clone(candidateOwner),
		SharingScope:      normalized.GetSharingScope(),
		Definition:        normalized,
		DefinitionSha256:  bytes.Clone(validation.GetDefinitionSha256()),
	}
	objects = append(objects, candidate)
	slices.SortFunc(objects, func(left, right *opensplunkv1.KnowledgeSnapshotObject) int {
		_, _, leftRank, leftOK := candidateTypeAndStage(left.GetDefinition())
		_, _, rightRank, rightOK := candidateTypeAndStage(right.GetDefinition())
		if !leftOK || !rightOK {
			return strings.Compare(left.GetKnowledgeObjectId(), right.GetKnowledgeObjectId())
		}
		if leftRank != rightRank {
			return int(leftRank) - int(rightRank)
		}
		if compared := strings.Compare(left.GetName(), right.GetName()); compared != 0 {
			return compared
		}
		return strings.Compare(left.GetKnowledgeObjectId(), right.GetKnowledgeObjectId())
	})
	stageOrdinals := make(map[opensplunkv1.KnowledgeSearchStage]uint32, 3)
	for index, object := range objects {
		object.ResolutionOrdinal = uint32(index)
		object.StageOrdinal = stageOrdinals[object.GetStage()]
		stageOrdinals[object.GetStage()]++
	}
	program, err := knowledgeprogram.Compile(objects)
	if err != nil || program.IsZero() {
		return knowledgeprogram.Program{}, ErrUnavailable
	}
	if err := candidateDependenciesMatch(
		candidateID,
		candidateVersion,
		program.Dependencies(),
		validation.GetDependencies(),
	); err != nil {
		return knowledgeprogram.Program{}, err
	}
	_ = rank
	return program, nil
}

func previewObjectID(
	jobID string,
	digest []byte,
	existing map[string]*opensplunkv1.KnowledgeSnapshotObject,
) string {
	for nonce := uint32(0); nonce <= knowledgeprogram.MaximumObjects; nonce++ {
		hash := sha256.New()
		_, _ = hash.Write([]byte("open-splunk-knowledge-preview-object-v1\x00"))
		_, _ = hash.Write([]byte(jobID))
		_, _ = hash.Write(digest)
		var nonceBytes [4]byte
		binary.BigEndian.PutUint32(nonceBytes[:], nonce)
		_, _ = hash.Write(nonceBytes[:])
		candidate := "preview-" + hex.EncodeToString(hash.Sum(nil))
		if _, collision := existing[candidate]; !collision {
			return candidate
		}
	}
	return ""
}

func candidateTypeAndStage(
	definition *opensplunkv1.KnowledgeObjectDefinition,
) (opensplunkv1.KnowledgeObjectType, opensplunkv1.KnowledgeSearchStage, uint8, bool) {
	if definition == nil {
		return 0, 0, 0, false
	}
	switch definition.GetBody().(type) {
	case *opensplunkv1.KnowledgeObjectDefinition_FieldExtraction:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
			opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION, 1, true
	case *opensplunkv1.KnowledgeObjectDefinition_FieldAlias:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
			opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS, 2, true
	case *opensplunkv1.KnowledgeObjectDefinition_CalculatedField:
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
			opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD, 3, true
	default:
		return 0, 0, 0, false
	}
}

type directDependency struct {
	id      string
	version uint64
	role    opensplunkv1.KnowledgeDependencyRole
}

func candidateDependenciesMatch(
	candidateID string,
	candidateVersion uint64,
	programDependencies []*opensplunkv1.KnowledgeObjectDependency,
	validated []*opensplunkv1.KnowledgeValidationDependency,
) error {
	derived := make([]directDependency, 0, len(validated))
	for _, dependency := range programDependencies {
		if dependency.GetSource().GetKnowledgeObjectId() != candidateID ||
			dependency.GetSource().GetVersion() != candidateVersion {
			continue
		}
		target := dependency.GetTarget().GetObject()
		if target == nil {
			return ErrUnavailable
		}
		derived = append(derived, directDependency{
			id: target.GetKnowledgeObjectId(), version: target.GetVersion(), role: dependency.GetRole(),
		})
	}
	want := make([]directDependency, len(validated))
	for index, dependency := range validated {
		if dependency == nil || dependency.GetTarget() == nil {
			return ErrInvariant
		}
		want[index] = directDependency{
			id:      dependency.GetTarget().GetKnowledgeObjectId(),
			version: dependency.GetTarget().GetVersion(),
			role:    dependency.GetRole(),
		}
	}
	compare := func(left, right directDependency) int {
		if value := strings.Compare(left.id, right.id); value != 0 {
			return value
		}
		if left.version < right.version {
			return -1
		}
		if left.version > right.version {
			return 1
		}
		return int(left.role) - int(right.role)
	}
	slices.SortFunc(derived, compare)
	slices.SortFunc(want, compare)
	if !slices.Equal(derived, want) {
		return ErrUnavailable
	}
	return nil
}

func validateRetainedCompiled(
	compiled clickhouse.CompiledQuery,
	execution searchjobs.ExecutionSnapshot,
	program knowledgeprogram.Program,
) error {
	if !compiled.HasValidExecutionSeal() {
		return ErrUnavailable
	}
	return validateCompiledPreview(compiled, execution, program)
}

func validateCompiledPreview(
	compiled clickhouse.CompiledQuery,
	execution searchjobs.ExecutionSnapshot,
	program knowledgeprogram.Program,
) error {
	if !compiled.HasValidExecutionSeal() {
		return ErrUnavailable
	}
	tenantID, indexes, ok := compiled.ReadScope()
	if !ok || tenantID != execution.TenantID ||
		!slices.Equal(indexes, execution.EffectiveIndexes) {
		return ErrUnavailable
	}
	evidence, ok := compiled.KnowledgeSnapshotEvidenceFor(program)
	if !ok || evidence.TenantID() != execution.TenantID ||
		!slices.Equal(evidence.EffectiveIndexes(), execution.EffectiveIndexes) {
		return ErrUnavailable
	}
	return nil
}

type projection struct {
	schema    *opensplunkv1.ResultSchema
	rows      []*opensplunkv1.ResultRow
	truncated bool
}

type projectionSink struct {
	ctx          context.Context
	jobID        string
	shape        searchjobproto.ResultShape
	maximumRows  uint32
	maximumBytes int
	convertRows  func(
		context.Context,
		string,
		searchjobs.Schema,
		[]searchjobs.ResultRow,
		int,
	) ([]*opensplunkv1.ResultRow, error)
	schema    searchjobs.Schema
	public    *opensplunkv1.ResultSchema
	rows      []*opensplunkv1.ResultRow
	bytes     int
	set       bool
	truncated bool
}

func (sink *projectionSink) SetSchema(schema searchjobs.Schema) error {
	if sink == nil || sink.ctx == nil || sink.set || len(schema.Columns) == 0 ||
		len(schema.Columns) > MaximumColumns || sink.maximumBytes <= 0 ||
		sink.maximumBytes > MaximumResponseBytes || sink.convertRows == nil {
		return ErrInvariant
	}
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	public, err := searchjobproto.Schema(sink.jobID, schema, sink.shape)
	if err != nil {
		return ErrInvariant
	}
	size := proto.Size(public)
	if size <= 0 || size > sink.maximumBytes {
		return ErrResponseTooLarge
	}
	sink.schema = searchjobs.Schema{Columns: slices.Clone(schema.Columns)}
	sink.public = public
	sink.bytes = size
	sink.set = true
	return nil
}

func (sink *projectionSink) AddRow(values []searchjobs.Value) error {
	if sink == nil || !sink.set || sink.public == nil || len(values) != len(sink.schema.Columns) {
		return ErrInvariant
	}
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	if uint64(len(sink.rows)) >= uint64(sink.maximumRows) {
		sink.truncated = true
		return errRowLimit
	}
	remaining := sink.maximumBytes - sink.bytes
	if remaining <= 0 {
		return ErrResponseTooLarge
	}
	lowerBound := uint64(0)
	for _, value := range values {
		valueBound, exceeded, err := value.ProtoSizeLowerBound(uint64(remaining) - lowerBound)
		if err != nil {
			return ErrInvariant
		}
		if exceeded {
			return ErrResponseTooLarge
		}
		lowerBound += valueBound
	}
	projected, err := sink.convertRows(
		sink.ctx,
		sink.jobID,
		sink.schema,
		[]searchjobs.ResultRow{{Ordinal: uint64(len(sink.rows)), Values: values}},
		1,
	)
	if err != nil || len(projected) != 1 || projected[0] == nil {
		return ErrInvariant
	}
	size := proto.Size(projected[0])
	if size <= 0 || size > remaining {
		return ErrResponseTooLarge
	}
	sink.bytes += size
	sink.rows = append(sink.rows, projected[0])
	return nil
}

func executeProjection(
	ctx context.Context,
	executor searchjobs.Executor,
	compiled clickhouse.CompiledQuery,
	jobID string,
	shape searchjobproto.ResultShape,
	maximumRows uint32,
) (projection, error) {
	sink := &projectionSink{
		ctx: ctx, jobID: jobID, shape: shape, maximumRows: maximumRows,
		maximumBytes: MaximumResponseBytes,
		convertRows:  searchjobproto.Rows,
		rows:         make([]*opensplunkv1.ResultRow, 0, maximumRows),
	}
	err := executor.Execute(ctx, compiled, sink)
	if err != nil && (!errors.Is(err, errRowLimit) || !sink.truncated) {
		return projection{}, classifyExecutionError(err)
	}
	if err := ctx.Err(); err != nil {
		return projection{}, err
	}
	if !sink.set || sink.public == nil {
		return projection{}, ErrInvariant
	}
	return projection{schema: sink.public, rows: sink.rows, truncated: sink.truncated}, nil
}

func classifyAcquireError(err error) error {
	if err == nil {
		return ErrUnavailable
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case errors.Is(err, searchjobs.ErrNotFound),
		errors.Is(err, searchjobs.ErrExpired),
		errors.Is(err, searchjobs.ErrResultsNotReady),
		errors.Is(err, searchjobs.ErrResultsUnavailable):
		return ErrNotFoundOrForbidden
	case errors.Is(err, searchjobs.ErrCapacity):
		return ErrResourceLimit
	default:
		return ErrUnavailable
	}
}

func classifyExecutionError(err error) error {
	if err == nil {
		return ErrUnavailable
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case errors.Is(err, ErrResponseTooLarge):
		return ErrResponseTooLarge
	case errors.Is(err, ErrInvariant):
		return ErrInvariant
	case errors.Is(err, searchjobs.ErrExecutionLimit),
		errors.Is(err, searchjobs.ErrCapacity),
		errors.Is(err, control.ErrCapacityExceeded),
		errors.Is(err, knowledgevalidation.ErrResponseTooLarge):
		return ErrResourceLimit
	default:
		return ErrUnavailable
	}
}
