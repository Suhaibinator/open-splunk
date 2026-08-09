package server

import (
	"errors"
	"math"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestPreviewKnowledgeObjectEnvelopeRejectsNilRequest(t *testing.T) {
	if err := validatePreviewKnowledgeObjectRequestEnvelope(nil); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("nil Preview request error = %v", err)
	}
}

func TestPreviewKnowledgeObjectEnvelopeMatchesActiveValidation(t *testing.T) {
	objectID := "ko-preview"
	version := uint64(7)
	zeroVersion := uint64(0)
	overVersion := uint64(math.MaxInt64) + 1

	tests := []struct {
		name    string
		request *opensplunkv1.PreviewKnowledgeObjectRequest
	}{
		{
			name: "create",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-create",
				Definition:          previewEnvelopeTestDefinition(),
			},
		},
		{
			name: "update",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-update",
				Definition:          previewEnvelopeTestDefinition(),
				KnowledgeObjectId:   &objectID,
				ExpectedVersion:     &version,
				UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"field_alias"}},
			},
		},
		{
			name: "definition absent",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-definition-absent",
			},
		},
		{
			name: "create carries expected version",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-create-version",
				Definition:          previewEnvelopeTestDefinition(),
				ExpectedVersion:     &version,
			},
		},
		{
			name: "create carries mask",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-create-mask",
				Definition:          previewEnvelopeTestDefinition(),
				UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"name"}},
			},
		},
		{
			name: "update empty object ID",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-empty-object",
				Definition:          previewEnvelopeTestDefinition(),
				KnowledgeObjectId:   new(string),
				ExpectedVersion:     &version,
				UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"name"}},
			},
		},
		{
			name: "update version absent",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-version-absent",
				Definition:          previewEnvelopeTestDefinition(),
				KnowledgeObjectId:   &objectID,
				UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"name"}},
			},
		},
		{
			name: "update version zero",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-version-zero",
				Definition:          previewEnvelopeTestDefinition(),
				KnowledgeObjectId:   &objectID,
				ExpectedVersion:     &zeroVersion,
				UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"name"}},
			},
		},
		{
			name: "update version over MaxInt64",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-version-over",
				Definition:          previewEnvelopeTestDefinition(),
				KnowledgeObjectId:   &objectID,
				ExpectedVersion:     &overVersion,
				UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"name"}},
			},
		},
		{
			name: "update mask absent",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-mask-absent",
				Definition:          previewEnvelopeTestDefinition(),
				KnowledgeObjectId:   &objectID,
				ExpectedVersion:     &version,
			},
		},
		{
			name: "update mask empty",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-mask-empty",
				Definition:          previewEnvelopeTestDefinition(),
				KnowledgeObjectId:   &objectID,
				ExpectedVersion:     &version,
				UpdateMask:          &fieldmaskpb.FieldMask{},
			},
		},
		{
			name: "update mask unsupported",
			request: &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-mask-unsupported",
				Definition:          previewEnvelopeTestDefinition(),
				KnowledgeObjectId:   &objectID,
				ExpectedVersion:     &version,
				UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"future"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := knowledgecatalog.ValidateKnowledgeObjectRequest(
				previewEnvelopeTestActiveValidationView(test.request),
			)
			got := validatePreviewKnowledgeObjectRequestEnvelope(test.request)
			if (got == nil) != (want == nil) {
				t.Fatalf("Preview/Validate errors = (%v, %v)", got, want)
			}
			if got != nil && got.Error() != want.Error() {
				t.Fatalf("Preview error = %q, Validate error = %q", got, want)
			}
		})
	}
}

func TestPreviewKnowledgeObjectEnvelopeForcesActiveValidation(t *testing.T) {
	objectID := "ko-preview-active"
	version := uint64(9)
	request := &opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-preview-active",
		Definition:          previewEnvelopeTestDefinition(),
		KnowledgeObjectId:   &objectID,
		ExpectedVersion:     &version,
		UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"field_alias"}},
	}
	view := previewKnowledgeObjectActiveValidationView(request)
	if view.GetIntent() != opensplunkv1.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION ||
		view.Definition != request.Definition || view.KnowledgeObjectId != request.KnowledgeObjectId ||
		view.ExpectedVersion != request.ExpectedVersion || view.UpdateMask != request.UpdateMask {
		t.Fatalf("Preview active validation view = %+v", view)
	}
}

func TestPreviewKnowledgeObjectEnvelopeRequiresCanonicalRetainedJobID(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{name: "ordinary", value: "job-preview", valid: true},
		{name: "interior space", value: "job preview", valid: true},
		{name: "unicode", value: "job-π", valid: true},
		{name: "exact byte limit", value: strings.Repeat("j", searchjobs.MaximumJobIDBytes), valid: true},
		{name: "empty", value: ""},
		{name: "over byte limit", value: strings.Repeat("j", searchjobs.MaximumJobIDBytes+1)},
		{name: "leading space", value: " job"},
		{name: "trailing space", value: "job "},
		{name: "leading unicode space", value: "\u00a0job"},
		{name: "control", value: "job\x00preview"},
		{name: "invalid UTF-8", value: string([]byte{0xff})},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: test.value,
				Definition:          previewEnvelopeTestDefinition(),
			}
			err := validatePreviewKnowledgeObjectRequestEnvelope(request)
			if test.valid {
				if err != nil {
					t.Fatalf("validate canonical ID: %v", err)
				}
				return
			}
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("validate noncanonical ID error = %v", err)
			}
		})
	}
}

func TestPreviewKnowledgeObjectEnvelopeUnknownAuthoritySplit(t *testing.T) {
	unknown := protowire.AppendVarint(
		protowire.AppendTag(nil, 100, protowire.VarintType),
		1,
	)

	outer := &opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-outer-unknown",
		Definition:          previewEnvelopeTestDefinition(),
	}
	outer.ProtoReflect().SetUnknown(unknown)
	if err := validatePreviewKnowledgeObjectRequestEnvelope(outer); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("outer unknown error = %v", err)
	}

	objectID := "ko-mask-unknown"
	version := uint64(1)
	maskUnknown := &opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-mask-unknown",
		Definition:          previewEnvelopeTestDefinition(),
		KnowledgeObjectId:   &objectID,
		ExpectedVersion:     &version,
		UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"field_alias"}},
	}
	maskUnknown.UpdateMask.ProtoReflect().SetUnknown(unknown)
	if err := validatePreviewKnowledgeObjectRequestEnvelope(maskUnknown); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("mask unknown error = %v", err)
	}

	createCandidateUnknown := &opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-create-candidate-unknown",
		Definition:          previewEnvelopeTestDefinition(),
	}
	createCandidateUnknown.Definition.ProtoReflect().SetUnknown(unknown)
	createBefore := proto.Clone(createCandidateUnknown).(*opensplunkv1.PreviewKnowledgeObjectRequest)
	if err := validatePreviewKnowledgeObjectRequestEnvelope(createCandidateUnknown); err != nil {
		t.Fatalf("create candidate unknown was rejected by the envelope: %v", err)
	}
	if !proto.Equal(createCandidateUnknown, createBefore) {
		t.Fatalf("create candidate unknown authority was mutated: %v", createCandidateUnknown)
	}

	updateCandidateUnknown := &opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-update-candidate-unknown",
		Definition:          previewEnvelopeTestDefinition(),
		KnowledgeObjectId:   &objectID,
		ExpectedVersion:     &version,
		UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"field_alias"}},
	}
	updateCandidateUnknown.Definition.GetFieldAlias().ProtoReflect().SetUnknown(unknown)
	updateBefore := proto.Clone(updateCandidateUnknown).(*opensplunkv1.PreviewKnowledgeObjectRequest)
	if err := validatePreviewKnowledgeObjectRequestEnvelope(updateCandidateUnknown); err != nil {
		t.Fatalf("selected update candidate unknown was rejected by the envelope: %v", err)
	}
	if !proto.Equal(updateCandidateUnknown, updateBefore) {
		t.Fatalf("update candidate unknown authority was mutated: %v", updateCandidateUnknown)
	}
}

func TestPreviewKnowledgeObjectEnvelopeLeavesMaximumRowsUntouched(t *testing.T) {
	zero := uint32(0)
	one := uint32(1)
	maximum := uint32(math.MaxUint32)
	tests := []struct {
		name string
		rows *uint32
	}{
		{name: "absent"},
		{name: "explicit zero", rows: &zero},
		{name: "one", rows: &one},
		{name: "maximum", rows: &maximum},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := &opensplunkv1.PreviewKnowledgeObjectRequest{
				RetainedSearchJobId: "job-rows-" + strings.ReplaceAll(test.name, " ", "-"),
				Definition:          previewEnvelopeTestDefinition(),
				MaximumRows:         test.rows,
			}
			before, ok := proto.Clone(request).(*opensplunkv1.PreviewKnowledgeObjectRequest)
			if !ok {
				t.Fatal("clone Preview request failed")
			}
			if err := validatePreviewKnowledgeObjectRequestEnvelope(request); err != nil {
				t.Fatalf("validate structural envelope: %v", err)
			}
			if !proto.Equal(request, before) || (request.MaximumRows == nil) != (test.rows == nil) {
				t.Fatalf("maximum_rows or request authority changed: before=%+v after=%+v", before, request)
			}
			if test.rows != nil && request.GetMaximumRows() != *test.rows {
				t.Fatalf("maximum_rows = %d, want %d", request.GetMaximumRows(), *test.rows)
			}
		})
	}
}

func TestPreviewKnowledgeObjectEnvelopeRejectsDecodedWrongWireKnownFields(t *testing.T) {
	base, err := proto.Marshal(&opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-wrong-wire",
		Definition:          previewEnvelopeTestDefinition(),
	})
	if err != nil {
		t.Fatalf("marshal base Preview request: %v", err)
	}

	tests := []struct {
		name     string
		number   protowire.Number
		wireType protowire.Type
	}{
		{name: "retained job ID", number: 1, wireType: protowire.VarintType},
		{name: "definition", number: 2, wireType: protowire.VarintType},
		{name: "object ID", number: 3, wireType: protowire.VarintType},
		{name: "expected version", number: 4, wireType: protowire.BytesType},
		{name: "update mask", number: 5, wireType: protowire.VarintType},
		{name: "maximum rows", number: 6, wireType: protowire.BytesType},
	}

	codec := newPreviewKnowledgeObjectRequestCodec()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wire := append([]byte(nil), base...)
			wire = protowire.AppendTag(wire, test.number, test.wireType)
			if test.wireType == protowire.BytesType {
				wire = protowire.AppendBytes(wire, []byte("wrong"))
			} else {
				wire = protowire.AppendVarint(wire, 1)
			}
			decoded, decodeErr := codec.DecodeBytes(wire)
			if decodeErr != nil {
				t.Fatalf("DecodeBytes wrong wire: %v", decodeErr)
			}
			if len(decoded.ProtoReflect().GetUnknown()) == 0 {
				t.Fatal("wrong-wire known field was not retained as outer unknown authority")
			}
			if validateErr := validatePreviewKnowledgeObjectRequestEnvelope(decoded); !errors.Is(validateErr, control.ErrInvalidArgument) {
				t.Fatalf("validate decoded wrong wire error = %v", validateErr)
			}
		})
	}
}

func previewEnvelopeTestDefinition() *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId: "app-preview",
		Name:  "preview-alias",
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField:      "source",
				DestinationField: "destination",
			},
		},
	}
}

func previewEnvelopeTestActiveValidationView(
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
