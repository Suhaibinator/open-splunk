package server

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestPreviewKnowledgeObjectRequestCodecDifferentialMergeAndProjection(t *testing.T) {
	codec := newPreviewKnowledgeObjectRequestCodec()
	definitionA := &opensplunk.KnowledgeObjectDefinition{
		AppId: "app-a",
		Name:  "first",
		Selector: &opensplunk.KnowledgeSelector{
			IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: "index-a"}},
		},
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunk.FieldAliasDefinition{SourceField: "source-a"},
		},
	}
	definitionB := &opensplunk.KnowledgeObjectDefinition{
		Name: "last",
		Selector: &opensplunk.KnowledgeSelector{
			HostPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: "host-b"}},
		},
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunk.FieldAliasDefinition{DestinationField: "destination-b"},
		},
	}

	createWire := validateTestBytesField(1, []byte("job-first"))
	createWire = append(createWire, validateTestBytesField(1, []byte("job-last"))...)
	createWire = append(createWire, validateTestBytesField(2, validateTestMarshal(t, definitionA))...)
	createWire = append(createWire, validateTestBytesField(2, validateTestMarshal(t, definitionB))...)
	createWire = append(createWire, validateTestVarintField(6, 3)...)
	createWire = append(createWire, validateTestVarintField(6, uint64(1<<32+7))...)
	createWire = append(createWire, validateTestVarintField(99, 8)...)
	var ordinaryCreate opensplunk.PreviewKnowledgeObjectRequest
	if err := proto.Unmarshal(createWire, &ordinaryCreate); err != nil {
		t.Fatal(err)
	}
	decodedCreate, err := codec.DecodeBytes(createWire)
	if err != nil || !proto.Equal(decodedCreate, &ordinaryCreate) {
		t.Fatalf("create differential mismatch\n got: %v\nwant: %v\nerr: %v", decodedCreate, &ordinaryCreate, err)
	}

	definitionUnknown := proto.Clone(definitionB).(*opensplunk.KnowledgeObjectDefinition)
	definitionUnknown.ProtoReflect().SetUnknown(validateTestVarintField(100, 1))
	definitionUnknown.GetSelector().ProtoReflect().SetUnknown(validateTestVarintField(101, 2))
	maskA := &fieldmaskpb.FieldMask{Paths: []string{"name"}}
	maskB := &fieldmaskpb.FieldMask{Paths: []string{"selector"}}
	maskB.ProtoReflect().SetUnknown(validateTestVarintField(102, 3))
	updateWire := validateTestBytesField(1, []byte("job-update"))
	updateWire = append(updateWire, validateTestBytesField(2, validateTestMarshal(t, definitionA))...)
	updateWire = append(updateWire, validateTestBytesField(2, validateTestMarshal(t, definitionUnknown))...)
	updateWire = append(updateWire, validateTestBytesField(3, []byte("ko-a"))...)
	updateWire = append(updateWire, validateTestVarintField(4, 9)...)
	updateWire = append(updateWire, validateTestBytesField(5, validateTestMarshal(t, maskA))...)
	updateWire = append(updateWire, validateTestBytesField(5, validateTestMarshal(t, maskB))...)
	updateWire = append(updateWire, validateTestVarintField(6, 0)...)
	updateWire = append(updateWire, validateTestVarintField(99, 9)...)
	var ordinaryUpdate opensplunk.PreviewKnowledgeObjectRequest
	if err := proto.Unmarshal(updateWire, &ordinaryUpdate); err != nil {
		t.Fatal(err)
	}
	expectedUpdate := proto.Clone(&ordinaryUpdate).(*opensplunk.PreviewKnowledgeObjectRequest)
	expectedUpdate.Definition = &opensplunk.KnowledgeObjectDefinition{
		Name:     ordinaryUpdate.GetDefinition().GetName(),
		Selector: proto.Clone(ordinaryUpdate.GetDefinition().GetSelector()).(*opensplunk.KnowledgeSelector),
	}
	decodedUpdate, err := codec.DecodeBytes(updateWire)
	if err != nil || !proto.Equal(decodedUpdate, expectedUpdate) || decodedUpdate.MaximumRows == nil {
		t.Fatalf("update differential mismatch\n got: %v\nwant: %v\nerr: %v", decodedUpdate, expectedUpdate, err)
	}
}

func TestPreviewKnowledgeObjectRequestCodecBoundsSearchJobID(t *testing.T) {
	codec := newPreviewKnowledgeObjectRequestCodec()
	maximum := strings.Repeat("j", searchjobs.MaximumJobIDBytes)
	decoded, err := codec.DecodeBytes(validateTestBytesField(1, []byte(maximum)))
	if err != nil || decoded.GetRetainedSearchJobId() != maximum {
		t.Fatalf("maximum job ID = %q / %v", decoded.GetRetainedSearchJobId(), err)
	}

	oversized := strings.Repeat("é", searchjobs.MaximumJobIDBytes/2+1)
	decoded, err = codec.DecodeBytes(validateTestBytesField(1, []byte(oversized)))
	if err != nil || decoded.GetRetainedSearchJobId() != previewOversizedSearchJobIDWitness ||
		len(decoded.GetRetainedSearchJobId()) != searchjobs.MaximumJobIDBytes+1 ||
		!utf8.ValidString(decoded.GetRetainedSearchJobId()) {
		t.Fatalf("oversized job witness = %q / %v", decoded.GetRetainedSearchJobId(), err)
	}

	oversizedThenValid := validateTestBytesField(1, []byte(oversized))
	oversizedThenValid = append(oversizedThenValid, validateTestBytesField(1, []byte("job-final"))...)
	decoded, err = codec.DecodeBytes(oversizedThenValid)
	if err != nil || decoded.GetRetainedSearchJobId() != "job-final" {
		t.Fatalf("oversized then valid = %q / %v", decoded.GetRetainedSearchJobId(), err)
	}
	validThenOversized := validateTestBytesField(1, []byte("job-first"))
	validThenOversized = append(validThenOversized, validateTestBytesField(1, []byte(oversized))...)
	decoded, err = codec.DecodeBytes(validThenOversized)
	if err != nil || decoded.GetRetainedSearchJobId() != previewOversizedSearchJobIDWitness {
		t.Fatalf("valid then oversized = %q / %v", decoded.GetRetainedSearchJobId(), err)
	}

	invalidThenValid := validateTestBytesField(1, []byte{0xff})
	invalidThenValid = append(invalidThenValid, validateTestBytesField(1, []byte("job-final"))...)
	if _, err := codec.DecodeBytes(invalidThenValid); err == nil {
		t.Fatal("invalid UTF-8 in an overwritten job ID was accepted")
	}
}

func TestPreviewKnowledgeObjectRequestCodecPreservesEmptyUpdatePresence(t *testing.T) {
	wire := validateTestBytesField(2, validateTestBytesField(2, []byte("projected-away")))
	wire = append(wire, validateTestBytesField(3, nil)...)
	wire = append(wire, validateTestVarintField(4, 0)...)
	wire = append(wire, validateTestBytesField(5, nil)...)
	wire = append(wire, validateTestVarintField(6, 0)...)
	decoded, err := newPreviewKnowledgeObjectRequestCodec().DecodeBytes(wire)
	if err != nil || decoded.KnowledgeObjectId == nil || decoded.GetKnowledgeObjectId() != "" ||
		decoded.ExpectedVersion == nil || decoded.GetExpectedVersion() != 0 ||
		decoded.UpdateMask == nil || len(decoded.GetUpdateMask().GetPaths()) != 0 ||
		decoded.MaximumRows == nil || decoded.GetMaximumRows() != 0 ||
		decoded.Definition == nil || decoded.GetDefinition().GetName() != "" {
		t.Fatalf("empty update presence/projection = %v / %v", decoded, err)
	}
}

func TestPreviewKnowledgeObjectRequestCodecRawLimitBodyCloseAndDetachment(t *testing.T) {
	codec := newPreviewKnowledgeObjectRequestCodec()
	limit := int(maximumKnowledgeMutationRequestBytes)
	tag := protowire.AppendTag(nil, 100, protowire.BytesType)
	payloadLength := limit - len(tag) - protowire.SizeVarint(uint64(limit))
	exact := append([]byte(nil), tag...)
	exact = protowire.AppendBytes(exact, make([]byte, payloadLength))
	if len(exact) != limit {
		t.Fatalf("exact fixture bytes = %d, want %d", len(exact), limit)
	}
	if _, err := codec.DecodeBytes(exact); err != nil {
		t.Fatalf("DecodeBytes exact limit: %v", err)
	}
	if _, err := codec.Decode(httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", bytes.NewReader(exact))); err != nil {
		t.Fatalf("Decode exact limit: %v", err)
	}
	over := append(exact, 0)
	for name, decode := range map[string]func() error{
		"bytes": func() error { _, err := codec.DecodeBytes(over); return err },
		"http": func() error {
			_, err := codec.Decode(httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", bytes.NewReader(over)))
			return err
		},
	} {
		err := decode()
		var maximum *http.MaxBytesError
		if !errors.As(err, &maximum) || maximum.Limit != maximumKnowledgeMutationRequestBytes {
			t.Fatalf("%s over-limit error = %T %v", name, err, err)
		}
	}

	body := &validateTrackingReadCloser{Reader: bytes.NewReader(nil)}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
	request.Body = body
	if _, err := codec.Decode(request); err != nil || !body.closed {
		t.Fatalf("body close = %v / %t", err, body.closed)
	}
	if _, err := codec.Decode(nil); err == nil {
		t.Fatal("nil request was accepted")
	}
	requestWithoutBody := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
	requestWithoutBody.Body = nil
	if _, err := codec.Decode(requestWithoutBody); err == nil {
		t.Fatal("request without a body was accepted")
	}
	if request := codec.NewRequest(); request == nil {
		t.Fatal("NewRequest returned nil")
	}

	input := &opensplunk.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-detached",
		Definition:          validateTestDefinition("detached"),
	}
	input.ProtoReflect().SetUnknown(validateTestVarintField(100, 9))
	wire := validateTestMarshal(t, input)
	decoded, err := codec.DecodeBytes(wire)
	if err != nil {
		t.Fatal(err)
	}
	for index := range wire {
		wire[index] = 0
	}
	if decoded.GetRetainedSearchJobId() != "job-detached" ||
		decoded.GetDefinition().GetName() != "detached" || len(decoded.ProtoReflect().GetUnknown()) == 0 {
		t.Fatalf("decoded Preview aliases input: %v", decoded)
	}
}

func TestPreviewKnowledgeObjectRequestSanitizerPreservesUnknownAuthorities(t *testing.T) {
	request := &opensplunk.PreviewKnowledgeObjectRequest{
		Definition: validateTestDefinition("unknowns"),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"field_alias"}},
	}
	request.ProtoReflect().SetUnknown(validateTestVarintField(100, 1))
	request.UpdateMask.ProtoReflect().SetUnknown(validateTestVarintField(101, 2))
	request.Definition.GetFieldAlias().ProtoReflect().SetUnknown(validateTestVarintField(102, 3))
	got, err := forwardCompatibleProtoSanitizer(request)
	if err != nil || got != request || len(got.ProtoReflect().GetUnknown()) == 0 ||
		len(got.GetUpdateMask().ProtoReflect().GetUnknown()) == 0 ||
		len(got.GetDefinition().GetFieldAlias().ProtoReflect().GetUnknown()) == 0 {
		t.Fatalf("Preview sanitizer changed unknown authorities: %v / %v", got, err)
	}
}

func TestPreviewKnowledgeObjectRequestCodecCapsAndSkipsRepeatedCandidateFields(t *testing.T) {
	codec := newPreviewKnowledgeObjectRequestCodec()
	selectorPayload := bytes.Repeat(validateTestBytesField(1, nil), 1_000_000)
	definitionPayload := append(validateTestBytesField(5, selectorPayload), validateTestBytesField(11, nil)...)
	decoded, err := codec.DecodeBytes(validateTestBytesField(2, definitionPayload))
	if err != nil {
		t.Fatalf("million selected patterns: %v", err)
	}
	if got := len(decoded.GetDefinition().GetSelector().GetIndexPatterns()); got != maximumValidateRetainedSelectorPatterns {
		t.Fatalf("retained selector patterns = %d, want %d", got, maximumValidateRetainedSelectorPatterns)
	}

	updateWire := validateTestBytesField(2, append(
		validateTestBytesField(2, []byte("selected-name")),
		validateTestBytesField(5, selectorPayload)...,
	))
	updateWire = append(updateWire, validateTestBytesField(3, []byte("ko-a"))...)
	updateWire = append(updateWire, validateTestBytesField(5, validateTestMarshal(t, &fieldmaskpb.FieldMask{Paths: []string{"name"}}))...)
	decoded, err = codec.DecodeBytes(updateWire)
	if err != nil || decoded.GetDefinition().GetName() != "selected-name" || decoded.GetDefinition().GetSelector() != nil {
		t.Fatalf("million unselected patterns = %v / %v", decoded, err)
	}

	maskPayload := bytes.Repeat(validateTestBytesField(1, nil), 1_000_000)
	maskWire := validateTestBytesField(2, nil)
	maskWire = append(maskWire, validateTestBytesField(3, []byte("ko-a"))...)
	maskWire = append(maskWire, validateTestBytesField(5, maskPayload)...)
	decoded, err = codec.DecodeBytes(maskWire)
	if err != nil {
		t.Fatalf("million mask paths: %v", err)
	}
	if got := len(decoded.GetUpdateMask().GetPaths()); got != maximumValidateRetainedMaskPaths {
		t.Fatalf("retained mask paths = %d, want %d", got, maximumValidateRetainedMaskPaths)
	}

	regexPayload := bytes.Repeat(validateTestBytesField(2, nil), 1_000_000)
	regexDefinition := validateTestBytesField(10, validateTestBytesField(2, regexPayload))
	decoded, err = codec.DecodeBytes(validateTestBytesField(2, regexDefinition))
	if err != nil {
		t.Fatalf("million selected regex outputs: %v", err)
	}
	if got := len(decoded.GetDefinition().GetFieldExtraction().GetRegex().GetOutputFields()); got != maximumValidateRetainedExtractionOutputs {
		t.Fatalf("retained regex outputs = %d, want %d", got, maximumValidateRetainedExtractionOutputs)
	}
	regexUpdate := validateTestBytesField(2, append(
		validateTestBytesField(2, []byte("selected-name")),
		regexDefinition...,
	))
	regexUpdate = append(regexUpdate, validateTestBytesField(3, []byte("ko-a"))...)
	regexUpdate = append(regexUpdate, validateTestBytesField(5, validateTestMarshal(t, &fieldmaskpb.FieldMask{Paths: []string{"name"}}))...)
	decoded, err = codec.DecodeBytes(regexUpdate)
	if err != nil || decoded.GetDefinition().GetName() != "selected-name" || decoded.GetDefinition().GetBody() != nil {
		t.Fatalf("million unselected regex outputs = %v / %v", decoded, err)
	}

	droppedBadPattern := bytes.Repeat(
		validateTestBytesField(1, nil),
		maximumValidateRetainedSelectorPatterns,
	)
	droppedBadPattern = append(
		droppedBadPattern,
		validateTestBytesField(1, validateTestBytesField(2, []byte{0xff}))...,
	)
	badDefinition := validateTestBytesField(5, droppedBadPattern)
	if _, err := codec.DecodeBytes(validateTestBytesField(2, badDefinition)); err == nil {
		t.Fatal("invalid UTF-8 after the selector retention cap was accepted")
	}

	unselectedBad := validateTestBytesField(2, []byte("selected-name"))
	unselectedBad = append(
		unselectedBad,
		validateTestBytesField(5, validateTestBytesField(1, validateTestBytesField(2, []byte{0xff})))...,
	)
	badUpdate := validateTestBytesField(2, unselectedBad)
	badUpdate = append(badUpdate, validateTestBytesField(3, []byte("ko-a"))...)
	badUpdate = append(badUpdate, validateTestBytesField(5, validateTestMarshal(t, &fieldmaskpb.FieldMask{Paths: []string{"name"}}))...)
	if _, err := codec.DecodeBytes(badUpdate); err == nil {
		t.Fatal("invalid UTF-8 in an unselected selector was accepted")
	}
}

func TestPreviewKnowledgeObjectRequestCodecAlternationAndUnknownCopiesAreAllocationBounded(t *testing.T) {
	codec := newPreviewKnowledgeObjectRequestCodec()
	emptyJobs := bytes.Repeat(validateTestBytesField(1, nil), 1_000_000)
	allocations := testing.AllocsPerRun(1, func() {
		if _, err := codec.DecodeBytes(emptyJobs); err != nil {
			panic(err)
		}
	})
	if allocations > 50 {
		t.Fatalf("million job-ID scalar allocations = %.0f, want bounded", allocations)
	}

	regexWithOutput := validateTestBytesField(2, validateTestBytesField(2, nil))
	aliasWithUnknown := validateTestVarintField(100, 1)
	bodyAlternation := make([]byte, 0, 80_000)
	for range 10_000 {
		bodyAlternation = append(bodyAlternation, validateTestBytesField(10, regexWithOutput)...)
		bodyAlternation = append(bodyAlternation, validateTestBytesField(11, aliasWithUnknown)...)
	}
	alternatingWire := validateTestBytesField(2, bodyAlternation)
	allocations = testing.AllocsPerRun(1, func() {
		decoded, err := codec.DecodeBytes(alternatingWire)
		if err != nil || decoded.GetDefinition().GetFieldAlias() == nil {
			panic(err)
		}
	})
	if allocations > 100 {
		t.Fatalf("Preview oneof alternation allocations = %.0f, want bounded", allocations)
	}

	regexWithOutputAndUnknown := append(
		validateTestBytesField(2, nil),
		validateTestVarintField(100, 1)...,
	)
	jsonWithUnknown := validateTestVarintField(101, 2)
	nestedAlternation := make([]byte, 0, 120_000)
	for range 10_000 {
		nestedAlternation = append(nestedAlternation, validateTestBytesField(2, regexWithOutputAndUnknown)...)
		nestedAlternation = append(nestedAlternation, validateTestBytesField(3, jsonWithUnknown)...)
	}
	nestedWire := validateTestBytesField(2, validateTestBytesField(10, nestedAlternation))
	allocations = testing.AllocsPerRun(1, func() {
		decoded, err := codec.DecodeBytes(nestedWire)
		if err != nil || decoded.GetDefinition().GetFieldExtraction().GetJson() == nil {
			panic(err)
		}
	})
	if allocations > 100 {
		t.Fatalf("Preview nested oneof alternation allocations = %.0f, want bounded", allocations)
	}

	unknownWire := validateTestBytesField(100, make([]byte, 512<<10))
	benchmark := testing.Benchmark(func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			if _, err := codec.DecodeBytes(unknownWire); err != nil {
				b.Fatal(err)
			}
		}
	})
	maximumAllocatedBytes := int64(len(unknownWire)*5/2 + 64<<10)
	if got := benchmark.AllocedBytesPerOp(); got > maximumAllocatedBytes {
		t.Fatalf("outer-unknown allocated bytes/op = %d, want <= %d", got, maximumAllocatedBytes)
	}
}

func TestPreviewKnowledgeObjectRequestCodecUnknownAuthorityAndWrongWire(t *testing.T) {
	codec := newPreviewKnowledgeObjectRequestCodec()
	definition := validateTestDefinition("create-unknown")
	definition.ProtoReflect().SetUnknown(validateTestVarintField(100, 1))
	definition.GetFieldAlias().ProtoReflect().SetUnknown(validateTestVarintField(101, 2))
	createWire := validateTestBytesField(2, validateTestMarshal(t, definition))
	createWire = append(createWire, validateTestVarintField(1, 7)...)
	createWire = append(createWire, validateTestVarintField(2, 8)...)
	createWire = append(createWire, validateTestVarintField(3, 9)...)
	createWire = append(createWire, validateTestBytesField(4, nil)...)
	createWire = append(createWire, validateTestVarintField(5, 10)...)
	createWire = append(createWire, validateTestBytesField(6, nil)...)
	createWire = append(createWire, validateTestVarintField(99, 11)...)
	var ordinary opensplunk.PreviewKnowledgeObjectRequest
	if err := proto.Unmarshal(createWire, &ordinary); err != nil {
		t.Fatal(err)
	}
	create, err := codec.DecodeBytes(createWire)
	if err != nil || !proto.Equal(create, &ordinary) ||
		len(create.GetDefinition().ProtoReflect().GetUnknown()) == 0 ||
		len(create.GetDefinition().GetFieldAlias().ProtoReflect().GetUnknown()) == 0 ||
		len(create.ProtoReflect().GetUnknown()) == 0 {
		t.Fatalf("create unknown/wrong-wire semantics = %v / ordinary %v / %v", create, &ordinary, err)
	}

	selector := &opensplunk.KnowledgeSelector{
		IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: "main"}},
	}
	selector.ProtoReflect().SetUnknown(validateTestVarintField(102, 3))
	updateDefinition := validateTestDefinition("unselected")
	updateDefinition.Selector = selector
	updateDefinition.ProtoReflect().SetUnknown(validateTestVarintField(103, 4))
	updateDefinition.GetFieldAlias().ProtoReflect().SetUnknown(validateTestVarintField(104, 5))
	mask := &fieldmaskpb.FieldMask{Paths: []string{"selector"}}
	mask.ProtoReflect().SetUnknown(validateTestVarintField(105, 6))
	updateWire := validateTestBytesField(2, validateTestMarshal(t, updateDefinition))
	updateWire = append(updateWire, validateTestBytesField(3, []byte("ko-a"))...)
	updateWire = append(updateWire, validateTestVarintField(4, 7)...)
	updateWire = append(updateWire, validateTestBytesField(5, validateTestMarshal(t, mask))...)
	updateWire = append(updateWire, validateTestBytesField(6, nil)...)
	updateWire = append(updateWire, validateTestVarintField(99, 8)...)
	update, err := codec.DecodeBytes(updateWire)
	if err != nil || update.GetDefinition().GetSelector() == nil || update.GetDefinition().GetBody() != nil ||
		len(update.GetDefinition().ProtoReflect().GetUnknown()) != 0 ||
		len(update.GetDefinition().GetSelector().ProtoReflect().GetUnknown()) == 0 ||
		len(update.GetUpdateMask().ProtoReflect().GetUnknown()) == 0 ||
		len(update.ProtoReflect().GetUnknown()) == 0 {
		t.Fatalf("update unknown authority = %v / %v", update, err)
	}
}

func previewTestUnknownGroup(depth int, mismatch bool) []byte {
	var result []byte
	for index := range depth {
		result = protowire.AppendTag(result, protowire.Number(100+index), protowire.StartGroupType)
	}
	for index := depth - 1; index >= 0; index-- {
		number := protowire.Number(100 + index)
		if mismatch && index == depth-1 {
			number++
		}
		result = protowire.AppendTag(result, number, protowire.EndGroupType)
	}
	return result
}

func TestPreviewKnowledgeObjectRequestCodecMalformedAndGroupDepth(t *testing.T) {
	codec := newPreviewKnowledgeObjectRequestCodec()
	for _, malformed := range [][]byte{{0x12, 0xff}, {0x0c}} {
		if _, err := codec.DecodeBytes(malformed); err == nil {
			t.Fatalf("malformed wire %x was accepted", malformed)
		}
	}
	for _, depth := range []int{31, 32} {
		if _, err := codec.DecodeBytes(previewTestUnknownGroup(depth, false)); err != nil {
			t.Fatalf("group depth %d: %v", depth, err)
		}
	}
	if _, err := codec.DecodeBytes(previewTestUnknownGroup(33, false)); err == nil {
		t.Fatal("group depth 33 was accepted")
	}
	if _, err := codec.DecodeBytes(previewTestUnknownGroup(2, true)); err == nil {
		t.Fatal("mismatched group was accepted")
	}
}

func TestPreviewKnowledgeObjectRequestCodecRemainsUnregistered(t *testing.T) {
	attempts := &knowledgeBoundaryAppender{}
	handler, httpHandler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		&knowledgeHTTPCatalog{},
		&knowledgeHTTPWriter{},
		knowledgeHTTPApps(),
		attempts,
	)
	if routes := handler.knowledgeManagementRoutes(0); len(routes) != 9 {
		t.Fatalf("management routes = %d, want unchanged nine", len(routes))
	}
	body := newKnowledgeBoundaryObservedBody("unread Preview body", nil)
	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/api/knowledge/objects/preview",
		body,
	)
	request.Host = "example.com"
	request.Header.Set("Authorization", "Bearer "+knowledgeBoundaryToken)
	request.Header.Set("Content-Type", "application/x-protobuf")
	response := httptest.NewRecorder()
	httpHandler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || body.reads() != 0 {
		t.Fatalf("Preview route status = %d / reads = %d", response.Code, body.reads())
	}
	authenticator := handler.browserAuthenticator.(*knowledgeBoundaryAuthenticator)
	if authenticator.callCount() != 0 || len(attempts.snapshot()) != 0 {
		t.Fatalf("unregistered Preview auth = %d / attempts = %+v", authenticator.callCount(), attempts.snapshot())
	}
}

func FuzzPreviewKnowledgeObjectRequestCodec(f *testing.F) {
	objectID := "ko-fuzz"
	version := uint64(7)
	rows := uint32(9)
	create := &opensplunk.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-create",
		Definition:          validateTestDefinition("fuzz-create"),
		MaximumRows:         &rows,
	}
	update := &opensplunk.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: "job-update",
		Definition:          validateTestDefinition("fuzz-update"),
		KnowledgeObjectId:   &objectID,
		ExpectedVersion:     &version,
		UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"name", "selector"}},
		MaximumRows:         &rows,
	}
	for _, seed := range [][]byte{
		validateTestMarshal(f, create),
		validateTestMarshal(f, update),
		append(validateTestBytesField(2, validateTestBytesField(11, nil)), validateTestBytesField(2, validateTestBytesField(12, nil))...),
		append(validateTestBytesField(1, []byte(strings.Repeat("j", searchjobs.MaximumJobIDBytes+1))), validateTestVarintField(6, 1<<32+3)...),
		{0x12, 0xff},
		{0x0c},
		{0xa0, 0x86, 0x00, 0x00},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			return
		}
		owned := append([]byte(nil), data...)
		var ordinary opensplunk.PreviewKnowledgeObjectRequest
		ordinaryErr := proto.Unmarshal(owned, &ordinary)
		codec := newPreviewKnowledgeObjectRequestCodec()
		decoded, err := codec.DecodeBytes(owned)
		if err != nil {
			// The dedicated 32-level unknown-group policy may reject protobuf
			// accepted by the general decoder.
			return
		}
		if ordinaryErr != nil {
			t.Fatalf("custom decoder accepted general-protobuf failure: %v", ordinaryErr)
		}
		again, err := codec.DecodeBytes(owned)
		if err != nil || !proto.Equal(decoded, again) {
			t.Fatalf("repeated decode mismatch: %v / %v / %v", decoded, again, err)
		}
		if !previewFuzzCardinalityBounded(decoded) {
			t.Fatalf("decoded request exceeded retained bounds: %v", decoded)
		}
		wire, err := proto.Marshal(decoded)
		if err != nil {
			t.Fatalf("decoded request cannot marshal: %v", err)
		}
		var roundTrip opensplunk.PreviewKnowledgeObjectRequest
		if err := proto.Unmarshal(wire, &roundTrip); err != nil || !proto.Equal(decoded, &roundTrip) {
			t.Fatalf("decoded request cannot round trip: %v / %v", &roundTrip, err)
		}
		beforeMutation := proto.Clone(decoded).(*opensplunk.PreviewKnowledgeObjectRequest)
		for index := range owned {
			owned[index] ^= 0xff
		}
		if !proto.Equal(decoded, beforeMutation) {
			t.Fatal("decoded request aliases fuzz input")
		}

		if len(ordinary.GetUpdateMask().GetPaths()) > len(validateDefinitionProjectionFields) {
			return
		}
		expected := previewFuzzExpected(&ordinary)
		if previewFuzzCardinalityBounded(expected) &&
			!validateFuzzHasUnknown(expected.ProtoReflect()) &&
			!proto.Equal(decoded, expected) {
			t.Fatalf("bounded Preview differs\n got: %v\nwant: %v", decoded, expected)
		}
	})
}

func previewFuzzCardinalityBounded(request *opensplunk.PreviewKnowledgeObjectRequest) bool {
	if request == nil || len(request.GetRetainedSearchJobId()) > maximumPreviewRetainedSearchJobIDBytes ||
		len(request.GetUpdateMask().GetPaths()) > maximumValidateRetainedMaskPaths {
		return false
	}
	definition := request.GetDefinition()
	if definition == nil {
		return true
	}
	selector := definition.GetSelector()
	if selector != nil && (len(selector.GetIndexPatterns()) > maximumValidateRetainedSelectorPatterns ||
		len(selector.GetHostPatterns()) > maximumValidateRetainedSelectorPatterns ||
		len(selector.GetSourcePatterns()) > maximumValidateRetainedSelectorPatterns ||
		len(selector.GetSourcetypePatterns()) > maximumValidateRetainedSelectorPatterns) {
		return false
	}
	extraction := definition.GetFieldExtraction()
	return extraction == nil || extraction.GetRegex() == nil ||
		len(extraction.GetRegex().GetOutputFields()) <= maximumValidateRetainedExtractionOutputs
}

func previewFuzzExpected(
	request *opensplunk.PreviewKnowledgeObjectRequest,
) *opensplunk.PreviewKnowledgeObjectRequest {
	result := proto.Clone(request).(*opensplunk.PreviewKnowledgeObjectRequest)
	if len(result.GetRetainedSearchJobId()) > searchjobs.MaximumJobIDBytes {
		result.RetainedSearchJobId = previewOversizedSearchJobIDWitness
	}
	if request.KnowledgeObjectId == nil || request.GetDefinition() == nil {
		return result
	}
	selected := &opensplunk.KnowledgeObjectDefinition{}
	for _, path := range request.GetUpdateMask().GetPaths() {
		switch path {
		case "app_id":
			selected.AppId = request.GetDefinition().GetAppId()
		case "name":
			selected.Name = request.GetDefinition().GetName()
		case "description":
			if request.GetDefinition().Description != nil {
				value := request.GetDefinition().GetDescription()
				selected.Description = &value
			}
		case "sharing_scope":
			selected.SharingScope = request.GetDefinition().GetSharingScope()
		case "selector":
			if request.GetDefinition().GetSelector() != nil {
				selected.Selector = proto.Clone(request.GetDefinition().GetSelector()).(*opensplunk.KnowledgeSelector)
			}
		case "field_extraction":
			if body := request.GetDefinition().GetFieldExtraction(); body != nil {
				selected.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: proto.Clone(body).(*opensplunk.FieldExtractionDefinition)}
			}
		case "field_alias":
			if body := request.GetDefinition().GetFieldAlias(); body != nil {
				selected.Body = &opensplunk.KnowledgeObjectDefinition_FieldAlias{FieldAlias: proto.Clone(body).(*opensplunk.FieldAliasDefinition)}
			}
		case "calculated_field":
			if body := request.GetDefinition().GetCalculatedField(); body != nil {
				selected.Body = &opensplunk.KnowledgeObjectDefinition_CalculatedField{CalculatedField: proto.Clone(body).(*opensplunk.CalculatedFieldDefinition)}
			}
		}
	}
	result.Definition = selected
	return result
}
