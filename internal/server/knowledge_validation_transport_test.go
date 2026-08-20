package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func validateTestBytesField(number protowire.Number, payload []byte) []byte {
	result := protowire.AppendTag(nil, number, protowire.BytesType)
	return protowire.AppendBytes(result, payload)
}

func validateTestVarintField(number protowire.Number, value uint64) []byte {
	result := protowire.AppendTag(nil, number, protowire.VarintType)
	return protowire.AppendVarint(result, value)
}

func validateTestMarshal(t testing.TB, message proto.Message) []byte {
	t.Helper()
	result, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func validateTestDefinition(name string) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId:        "app-a",
		Name:         name,
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunk.FieldAliasDefinition{
				SourceField:      "source_value",
				DestinationField: "derived_value",
			},
		},
	}
}

func TestValidateKnowledgeObjectCodecDifferentialMergeAndProjection(t *testing.T) {
	codec := newValidateKnowledgeObjectCodec()

	selectorA := &opensplunk.KnowledgeSelector{IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: "index-a"}}}
	selectorB := &opensplunk.KnowledgeSelector{HostPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: "host-b"}}}
	definitionA := &opensplunk.KnowledgeObjectDefinition{
		AppId: "app-a", Name: "first", Selector: selectorA,
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunk.FieldAliasDefinition{SourceField: "source-a"}},
	}
	definitionB := &opensplunk.KnowledgeObjectDefinition{
		Name: "last", Selector: selectorB,
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunk.FieldAliasDefinition{DestinationField: "destination-b"}},
	}
	definitionC := &opensplunk.KnowledgeObjectDefinition{
		Body: &opensplunk.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunk.CalculatedFieldDefinition{Expression: "host"}},
	}
	definitionD := &opensplunk.KnowledgeObjectDefinition{
		Body: &opensplunk.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunk.CalculatedFieldDefinition{DestinationField: "calculated"}},
	}

	var createWire []byte
	for _, definition := range []*opensplunk.KnowledgeObjectDefinition{definitionA, definitionB, definitionC, definitionD} {
		createWire = append(createWire, validateTestBytesField(1, validateTestMarshal(t, definition))...)
	}
	createWire = append(createWire, validateTestVarintField(5, uint64(opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE))...)
	createWire = append(createWire, validateTestVarintField(99, 7)...)
	var ordinaryCreate opensplunk.ValidateKnowledgeObjectRequest
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
	maskB.ProtoReflect().SetUnknown(validateTestVarintField(100, 3))
	var updateWire []byte
	updateWire = append(updateWire, validateTestBytesField(1, validateTestMarshal(t, definitionA))...)
	updateWire = append(updateWire, validateTestBytesField(1, validateTestMarshal(t, definitionUnknown))...)
	updateWire = append(updateWire, validateTestBytesField(2, []byte("ko-a"))...)
	updateWire = append(updateWire, validateTestVarintField(3, 9)...)
	updateWire = append(updateWire, validateTestBytesField(4, validateTestMarshal(t, maskA))...)
	updateWire = append(updateWire, validateTestBytesField(4, validateTestMarshal(t, maskB))...)
	updateWire = append(updateWire, validateTestVarintField(5, uint64(opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION))...)
	updateWire = append(updateWire, validateTestVarintField(99, 8)...)
	var ordinaryUpdate opensplunk.ValidateKnowledgeObjectRequest
	if err := proto.Unmarshal(updateWire, &ordinaryUpdate); err != nil {
		t.Fatal(err)
	}
	expectedUpdate := proto.Clone(&ordinaryUpdate).(*opensplunk.ValidateKnowledgeObjectRequest)
	expectedUpdate.Definition = &opensplunk.KnowledgeObjectDefinition{
		Name:     ordinaryUpdate.GetDefinition().GetName(),
		Selector: proto.Clone(ordinaryUpdate.GetDefinition().GetSelector()).(*opensplunk.KnowledgeSelector),
	}
	decodedUpdate, err := codec.DecodeBytes(updateWire)
	if err != nil || !proto.Equal(decodedUpdate, expectedUpdate) {
		t.Fatalf("update differential mismatch\n got: %v\nwant: %v\nerr: %v", decodedUpdate, expectedUpdate, err)
	}
}

func TestValidateKnowledgeObjectCodecCapsRepeatedCandidateFields(t *testing.T) {
	codec := newValidateKnowledgeObjectCodec()

	selectorPayload := bytes.Repeat(validateTestBytesField(1, nil), 1_000_000)
	definitionPayload := append(validateTestBytesField(5, selectorPayload), validateTestBytesField(11, nil)...)
	createWire := validateTestBytesField(1, definitionPayload)
	createWire = append(createWire, validateTestVarintField(5, 1)...)
	decoded, err := codec.DecodeBytes(createWire)
	if err != nil {
		t.Fatalf("million selected patterns: %v", err)
	}
	if got := len(decoded.GetDefinition().GetSelector().GetIndexPatterns()); got != maximumValidateRetainedSelectorPatterns {
		t.Fatalf("retained selector patterns = %d, want %d", got, maximumValidateRetainedSelectorPatterns)
	}

	regexPayload := bytes.Repeat(validateTestBytesField(2, nil), 1_000_000)
	extractionPayload := validateTestBytesField(2, regexPayload)
	definitionPayload = validateTestBytesField(10, extractionPayload)
	createWire = validateTestBytesField(1, definitionPayload)
	createWire = append(createWire, validateTestVarintField(5, 1)...)
	decoded, err = codec.DecodeBytes(createWire)
	if err != nil {
		t.Fatalf("million selected outputs: %v", err)
	}
	if got := len(decoded.GetDefinition().GetFieldExtraction().GetRegex().GetOutputFields()); got != maximumValidateRetainedExtractionOutputs {
		t.Fatalf("retained outputs = %d, want %d", got, maximumValidateRetainedExtractionOutputs)
	}

	maskPayload := bytes.Repeat(validateTestBytesField(1, nil), 1_000_000)
	updateWire := validateTestBytesField(1, nil)
	updateWire = append(updateWire, validateTestBytesField(2, []byte("ko-a"))...)
	updateWire = append(updateWire, validateTestBytesField(4, maskPayload)...)
	decoded, err = codec.DecodeBytes(updateWire)
	if err != nil {
		t.Fatalf("million mask paths: %v", err)
	}
	if got := len(decoded.GetUpdateMask().GetPaths()); got != maximumValidateRetainedMaskPaths {
		t.Fatalf("retained mask paths = %d, want %d", got, maximumValidateRetainedMaskPaths)
	}
}

func TestValidateKnowledgeObjectCodecSkipsUnselectedWithoutSkippingValidation(t *testing.T) {
	codec := newValidateKnowledgeObjectCodec()
	selectorPayload := bytes.Repeat(validateTestBytesField(1, nil), 1_000_000)
	definitionPayload := append(validateTestBytesField(2, []byte("selected-name")), validateTestBytesField(5, selectorPayload)...)
	definitionPayload = append(definitionPayload, validateTestBytesField(10, validateTestBytesField(2, validateTestBytesField(2, nil)))...)
	updateWire := validateTestBytesField(1, definitionPayload)
	updateWire = append(updateWire, validateTestBytesField(2, []byte("ko-a"))...)
	updateWire = append(updateWire, validateTestVarintField(3, 1)...)
	updateWire = append(updateWire, validateTestBytesField(4, validateTestMarshal(t, &fieldmaskpb.FieldMask{Paths: []string{"name"}}))...)
	decoded, err := codec.DecodeBytes(updateWire)
	if err != nil {
		t.Fatalf("unselected million-entry field: %v", err)
	}
	if decoded.GetDefinition().GetName() != "selected-name" || decoded.GetDefinition().GetSelector() != nil || decoded.GetDefinition().GetBody() != nil {
		t.Fatalf("selected update projection = %v", decoded.GetDefinition())
	}

	badPattern := validateTestBytesField(2, []byte{0xff})
	badSelector := validateTestBytesField(1, badPattern)
	badDefinition := append(validateTestBytesField(2, []byte("selected-name")), validateTestBytesField(5, badSelector)...)
	badWire := validateTestBytesField(1, badDefinition)
	badWire = append(badWire, validateTestBytesField(2, []byte("ko-a"))...)
	badWire = append(badWire, validateTestBytesField(4, validateTestMarshal(t, &fieldmaskpb.FieldMask{Paths: []string{"name"}}))...)
	if _, err := codec.DecodeBytes(badWire); err == nil {
		t.Fatal("invalid UTF-8 in an unselected selector was accepted")
	}
	selectorWithDroppedBadPattern := bytes.Repeat(validateTestBytesField(1, nil), maximumValidateRetainedSelectorPatterns)
	selectorWithDroppedBadPattern = append(
		selectorWithDroppedBadPattern,
		validateTestBytesField(1, validateTestBytesField(2, []byte{0xff}))...,
	)
	if _, err := codec.DecodeBytes(validateTestBytesField(1, validateTestBytesField(5, selectorWithDroppedBadPattern))); err == nil {
		t.Fatal("invalid UTF-8 in a dropped selector pattern was accepted")
	}
	outputsWithDroppedBadValue := bytes.Repeat(validateTestBytesField(2, nil), maximumValidateRetainedExtractionOutputs)
	outputsWithDroppedBadValue = append(outputsWithDroppedBadValue, validateTestBytesField(2, []byte{0xff})...)
	droppedOutputDefinition := validateTestBytesField(10, validateTestBytesField(2, outputsWithDroppedBadValue))
	if _, err := codec.DecodeBytes(validateTestBytesField(1, droppedOutputDefinition)); err == nil {
		t.Fatal("invalid UTF-8 in a dropped regex output was accepted")
	}

	badAlias := validateTestBytesField(1, []byte{0xff})
	clearedBody := append(validateTestBytesField(11, badAlias), validateTestBytesField(12, nil)...)
	if _, err := codec.DecodeBytes(validateTestBytesField(1, clearedBody)); err == nil {
		t.Fatal("invalid UTF-8 in a cleared oneof member was accepted")
	}
}

func TestValidateKnowledgeObjectCodecOneofAlternationIsAllocationBounded(t *testing.T) {
	codec := newValidateKnowledgeObjectCodec()
	emptyAlternation := make([]byte, 0, 2_000_000)
	for index := range 1_000_000 {
		number := protowire.Number(10)
		if index%2 != 0 {
			number = 11
		}
		emptyAlternation = append(emptyAlternation, validateTestBytesField(number, nil)...)
	}
	decoded, err := codec.DecodeBytes(validateTestBytesField(1, emptyAlternation))
	if err != nil || decoded.GetDefinition().GetFieldAlias() == nil {
		t.Fatalf("million body alternations = %v / %v", decoded, err)
	}

	regexWithOutput := validateTestBytesField(2, validateTestBytesField(2, nil))
	aliasWithUnknown := validateTestVarintField(100, 1)
	contentAlternation := make([]byte, 0, 80_000)
	for range 10_000 {
		contentAlternation = append(contentAlternation, validateTestBytesField(10, regexWithOutput)...)
		contentAlternation = append(contentAlternation, validateTestBytesField(11, aliasWithUnknown)...)
	}
	contentWire := validateTestBytesField(1, contentAlternation)
	allocations := testing.AllocsPerRun(1, func() {
		if _, decodeErr := codec.DecodeBytes(contentWire); decodeErr != nil {
			panic(decodeErr)
		}
	})
	if allocations > 100 {
		t.Fatalf("oneof alternation allocations = %.0f, want bounded", allocations)
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
	nestedWire := validateTestBytesField(1, validateTestBytesField(10, nestedAlternation))
	allocations = testing.AllocsPerRun(1, func() {
		if _, decodeErr := codec.DecodeBytes(nestedWire); decodeErr != nil {
			panic(decodeErr)
		}
	})
	if allocations > 100 {
		t.Fatalf("nested oneof alternation allocations = %.0f, want bounded", allocations)
	}
}

func TestValidateKnowledgeObjectCodecWrongWireUnknownProjection(t *testing.T) {
	codec := newValidateKnowledgeObjectCodec()
	aliasPayload := append(
		validateTestBytesField(1, []byte("source")),
		validateTestVarintField(1, 7)...,
	)
	definitionPayload := append(validateTestBytesField(11, aliasPayload), validateTestVarintField(10, 1)...)
	createWire := validateTestBytesField(1, definitionPayload)
	var ordinary opensplunk.ValidateKnowledgeObjectRequest
	if err := proto.Unmarshal(createWire, &ordinary); err != nil {
		t.Fatal(err)
	}
	create, err := codec.DecodeBytes(createWire)
	if err != nil || !proto.Equal(create, &ordinary) || create.GetDefinition().GetFieldAlias() == nil ||
		len(create.GetDefinition().ProtoReflect().GetUnknown()) == 0 || len(create.GetDefinition().GetFieldAlias().ProtoReflect().GetUnknown()) == 0 {
		t.Fatalf("create wrong-wire semantics = %v / ordinary %v / %v", create, &ordinary, err)
	}

	updateWire := append([]byte(nil), createWire...)
	updateWire = append(updateWire, validateTestBytesField(2, []byte("ko-a"))...)
	updateWire = append(updateWire, validateTestBytesField(4, validateTestMarshal(t, &fieldmaskpb.FieldMask{Paths: []string{"field_alias"}}))...)
	update, err := codec.DecodeBytes(updateWire)
	if err != nil || update.GetDefinition().GetFieldAlias() == nil ||
		len(update.GetDefinition().ProtoReflect().GetUnknown()) != 0 ||
		len(update.GetDefinition().GetFieldAlias().ProtoReflect().GetUnknown()) == 0 {
		t.Fatalf("update wrong-wire projection = %v / %v", update, err)
	}
}

func TestValidateKnowledgeObjectCodecBoundsMalformedGroupsAndDetaches(t *testing.T) {
	codec := newValidateKnowledgeObjectCodec()
	if _, err := codec.DecodeBytes([]byte{0x0a, 0xff}); err == nil {
		t.Fatal("malformed length-delimited field was accepted")
	}

	group := func(depth int, mismatch bool) []byte {
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
	for _, depth := range []int{31, 32} {
		if _, err := codec.DecodeBytes(group(depth, false)); err != nil {
			t.Fatalf("group depth %d: %v", depth, err)
		}
	}
	if _, err := codec.DecodeBytes(group(33, false)); err == nil {
		t.Fatal("group depth 33 was accepted")
	}
	if _, err := codec.DecodeBytes(group(2, true)); err == nil {
		t.Fatal("mismatched group was accepted")
	}

	request := &opensplunk.ValidateKnowledgeObjectRequest{Definition: validateTestDefinition("detached"), Intent: 1}
	request.ProtoReflect().SetUnknown(validateTestVarintField(100, 9))
	wire := validateTestMarshal(t, request)
	decoded, err := codec.DecodeBytes(wire)
	if err != nil {
		t.Fatal(err)
	}
	for index := range wire {
		wire[index] = 0
	}
	if decoded.GetDefinition().GetName() != "detached" || len(decoded.ProtoReflect().GetUnknown()) == 0 {
		t.Fatalf("decoded request aliases input: %v", decoded)
	}
}

func TestValidateKnowledgeObjectCodecRawLimitBothDecodeForms(t *testing.T) {
	codec := newValidateKnowledgeObjectCodec()
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
}

type validateTrackingReadCloser struct {
	io.Reader
	closed bool
}

func (reader *validateTrackingReadCloser) Close() error {
	reader.closed = true
	return nil
}

func TestValidateKnowledgeObjectCodecClosesRequestBody(t *testing.T) {
	body := &validateTrackingReadCloser{Reader: bytes.NewReader(nil)}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
	request.Body = body
	if _, err := newValidateKnowledgeObjectCodec().Decode(request); err != nil {
		t.Fatal(err)
	}
	if !body.closed {
		t.Fatal("Decode did not close the request body")
	}
}

func TestValidateKnowledgeObjectSanitizerPreservesUnknownAuthorities(t *testing.T) {
	request := &opensplunk.ValidateKnowledgeObjectRequest{
		Definition: validateTestDefinition("unknowns"),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
	}
	request.ProtoReflect().SetUnknown(validateTestVarintField(100, 1))
	request.UpdateMask.ProtoReflect().SetUnknown(validateTestVarintField(101, 2))
	request.Definition.GetFieldAlias().ProtoReflect().SetUnknown(validateTestVarintField(102, 3))
	got, err := forwardCompatibleProtoSanitizer(request)
	if err != nil || got != request || len(got.ProtoReflect().GetUnknown()) == 0 ||
		len(got.GetUpdateMask().ProtoReflect().GetUnknown()) == 0 ||
		len(got.GetDefinition().GetFieldAlias().ProtoReflect().GetUnknown()) == 0 {
		t.Fatalf("Validate sanitizer changed unknown authorities: %v / %v", got, err)
	}
}

func validateTestSeal(t *testing.T) knowledgevalidation.SealedValidateResponse {
	t.Helper()
	result, err := knowledgevalidation.BuildInactive(t.Context(), validateTestDefinition("response"))
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := knowledgevalidation.SealValidateResponse(t.Context(), result, 7)
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

func TestValidateKnowledgeObjectCodecWritesExactSealAndReleasesPermit(t *testing.T) {
	codec := newValidateKnowledgeObjectCodec()
	sealed := validateTestSeal(t)
	released := 0
	response := httptest.NewRecorder()
	err := codec.Encode(response, newSerializedValidateKnowledgeObjectResponse(
		context.Background(),
		sealed,
		func() { released++ },
	))
	if err != nil || released != 1 || !bytes.Equal(response.Body.Bytes(), sealed.DeterministicBytes()) || response.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("Encode exact seal = %v / released %d / bytes %t / type %q", err, released, bytes.Equal(response.Body.Bytes(), sealed.DeterministicBytes()), response.Header().Get("Content-Type"))
	}

	for _, test := range []struct {
		name   string
		sealed knowledgevalidation.SealedValidateResponse
		ctx    context.Context
	}{
		{name: "zero seal", ctx: context.Background()},
		{name: "nil context", sealed: sealed},
		{name: "canceled", sealed: sealed, ctx: canceledValidateTestContext()},
	} {
		t.Run(test.name, func(t *testing.T) {
			innerReleased := 0
			innerResponse := httptest.NewRecorder()
			err := codec.Encode(innerResponse, newSerializedValidateKnowledgeObjectResponse(test.ctx, test.sealed, func() { innerReleased++ }))
			if err == nil || innerReleased != 1 || innerResponse.Body.Len() != 0 {
				t.Fatalf("Encode invalid state = %v / released %d / bytes %d", err, innerReleased, innerResponse.Body.Len())
			}
		})
	}

	if err := codec.Encode(httptest.NewRecorder(), nil); err == nil {
		t.Fatal("nil serialized response was accepted")
	}
	if err := codec.Encode(httptest.NewRecorder(), newSerializedValidateKnowledgeObjectResponse(context.Background(), sealed, nil)); err == nil {
		t.Fatal("nil serialization release was accepted")
	}
	released = 0
	if err := codec.Encode(nil, newSerializedValidateKnowledgeObjectResponse(context.Background(), sealed, func() { released++ })); err == nil || released != 1 {
		t.Fatalf("nil response writer = %v / released %d", err, released)
	}

	copyBytes := sealed.DeterministicBytes()
	copyBytes[0] ^= 0xff
	response = httptest.NewRecorder()
	released = 0
	if err := codec.Encode(response, newSerializedValidateKnowledgeObjectResponse(context.Background(), sealed, func() { released++ })); err != nil || bytes.Equal(response.Body.Bytes(), copyBytes) || released != 1 {
		t.Fatalf("mutated seal accessor affected encoding: %v / %d", err, released)
	}
}

func canceledValidateTestContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

type validateFailingResponseWriter struct{ header http.Header }

func (writer *validateFailingResponseWriter) Header() http.Header { return writer.header }
func (*validateFailingResponseWriter) Write([]byte) (int, error)  { return 0, io.ErrClosedPipe }
func (*validateFailingResponseWriter) WriteHeader(int)            {}

func TestValidateKnowledgeObjectCodecReleasesPermitOnWriteFailure(t *testing.T) {
	released := 0
	writer := &validateFailingResponseWriter{header: make(http.Header)}
	err := newValidateKnowledgeObjectCodec().Encode(writer, newSerializedValidateKnowledgeObjectResponse(
		context.Background(), validateTestSeal(t), func() { released++ },
	))
	if !errors.Is(err, io.ErrClosedPipe) || released != 1 {
		t.Fatalf("write failure = %v / released %d", err, released)
	}
}

func FuzzValidateKnowledgeObjectCodec(f *testing.F) {
	create := &opensplunk.ValidateKnowledgeObjectRequest{
		Definition: validateTestDefinition("fuzz-create"),
		Intent:     opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_INACTIVE_STORAGE,
	}
	objectID := "ko-fuzz"
	version := uint64(7)
	update := &opensplunk.ValidateKnowledgeObjectRequest{
		Definition:        validateTestDefinition("fuzz-update"),
		KnowledgeObjectId: &objectID,
		ExpectedVersion:   &version,
		UpdateMask:        &fieldmaskpb.FieldMask{Paths: []string{"name", "selector"}},
		Intent:            opensplunk.KnowledgeValidationIntent_KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
	}
	for _, seed := range [][]byte{
		validateTestMarshal(f, create),
		validateTestMarshal(f, update),
		append(validateTestBytesField(1, validateTestBytesField(11, nil)), validateTestBytesField(1, validateTestBytesField(12, nil))...),
		append(validateTestBytesField(1, validateTestBytesField(11, nil)), validateTestVarintField(10, 1)...),
		{0x0a, 0xff},
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
		var ordinary opensplunk.ValidateKnowledgeObjectRequest
		ordinaryErr := proto.Unmarshal(owned, &ordinary)
		codec := newValidateKnowledgeObjectCodec()
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
		if !validateFuzzCardinalityBounded(decoded) {
			t.Fatalf("decoded request exceeded retained cardinality: %v", decoded)
		}
		wire, err := proto.Marshal(decoded)
		if err != nil {
			t.Fatalf("decoded request cannot marshal: %v", err)
		}
		var roundTrip opensplunk.ValidateKnowledgeObjectRequest
		if err := proto.Unmarshal(wire, &roundTrip); err != nil || !proto.Equal(decoded, &roundTrip) {
			t.Fatalf("decoded request cannot round trip: %v / %v", &roundTrip, err)
		}
		beforeMutation := proto.Clone(decoded).(*opensplunk.ValidateKnowledgeObjectRequest)
		for index := range owned {
			owned[index] ^= 0xff
		}
		if !proto.Equal(decoded, beforeMutation) {
			t.Fatal("decoded request aliases fuzz input")
		}

		if ordinary.KnowledgeObjectId == nil {
			if validateFuzzCardinalityBounded(&ordinary) && !validateFuzzHasUnknown(ordinary.ProtoReflect()) && !proto.Equal(decoded, &ordinary) {
				t.Fatalf("bounded create differs from ordinary decode\n got: %v\nwant: %v", decoded, &ordinary)
			}
			return
		}
		if len(ordinary.GetUpdateMask().GetPaths()) > len(validateDefinitionProjectionFields) {
			return
		}
		expected := validateFuzzSelectedUpdate(&ordinary)
		if validateFuzzCardinalityBounded(expected) && !proto.Equal(decoded, expected) {
			t.Fatalf("bounded update projection differs\n got: %v\nwant: %v", decoded, expected)
		}
	})
}

func validateFuzzCardinalityBounded(request *opensplunk.ValidateKnowledgeObjectRequest) bool {
	if request == nil || len(request.GetUpdateMask().GetPaths()) > maximumValidateRetainedMaskPaths {
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

func validateFuzzSelectedUpdate(
	request *opensplunk.ValidateKnowledgeObjectRequest,
) *opensplunk.ValidateKnowledgeObjectRequest {
	result := proto.Clone(request).(*opensplunk.ValidateKnowledgeObjectRequest)
	if request.GetDefinition() == nil {
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

func validateFuzzHasUnknown(message protoreflect.Message) bool {
	if !message.IsValid() {
		return false
	}
	if len(message.GetUnknown()) != 0 {
		return true
	}
	found := false
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch {
		case field.IsList() && field.Message() != nil:
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if validateFuzzHasUnknown(list.Get(index).Message()) {
					found = true
					return false
				}
			}
		case field.Message() != nil:
			found = validateFuzzHasUnknown(value.Message())
		}
		return !found
	})
	return found
}
