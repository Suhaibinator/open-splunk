package server

import (
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

func TestLookupDefinitionSanitizerRejectsUnknownSemanticsBeforePersistence(t *testing.T) {
	wrappers := []struct {
		name string
		run  func(*opensplunk.LookupDefinition) error
	}{
		{"create", func(definition *opensplunk.LookupDefinition) error {
			_, err := sanitizeCreateLookupRequest(t.Context(), &opensplunk.CreateLookupRequest{Definition: definition})
			return err
		}},
		{"replace", func(definition *opensplunk.LookupDefinition) error {
			_, err := sanitizeReplaceLookupRequest(t.Context(), &opensplunk.ReplaceLookupRequest{Definition: definition})
			return err
		}},
		{"preview", func(definition *opensplunk.LookupDefinition) error {
			_, err := sanitizePreviewLookupRequest(t.Context(), &opensplunk.PreviewLookupRequest{Definition: definition})
			return err
		}},
	}
	for _, wrapper := range wrappers {
		t.Run(wrapper.name, func(t *testing.T) {
			definition := lookupHTTPDefinition("catalog")
			addKnowledgeHTTPUnknown(definition.GetOutputMappings()[0])
			if err := wrapper.run(definition); err == nil || !strings.Contains(err.Error(), "unsupported fields") {
				t.Fatalf("sanitize unknown lookup definition error = %v", err)
			}
			if len(definition.GetOutputMappings()[0].ProtoReflect().GetUnknown()) == 0 {
				t.Fatal("rejected lookup definition was mutated")
			}
		})
	}

	oversized := lookupHTTPDefinition("catalog")
	oversized.KeyMappings = make([]*opensplunk.LookupFieldMapping, 5)
	oversized.KeyMappings[0] = &opensplunk.LookupFieldMapping{}
	addKnowledgeHTTPUnknown(oversized.KeyMappings[0])
	if _, err := sanitizeCreateLookupRequest(t.Context(), &opensplunk.CreateLookupRequest{Definition: oversized}); err == nil ||
		!strings.Contains(err.Error(), "entry limit") {
		t.Fatalf("sanitize oversized definition error = %v", err)
	}
	if len(oversized.KeyMappings[0].ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("shape preflight traversed the oversized definition")
	}

	envelope := &opensplunk.CreateLookupRequest{Definition: lookupHTTPDefinition("catalog"), CsvData: []byte("id,value\n")}
	addKnowledgeHTTPUnknown(envelope)
	if _, err := sanitizeCreateLookupRequest(t.Context(), envelope); err != nil {
		t.Fatalf("sanitize future request envelope: %v", err)
	}
	// An unknown envelope field is neither stripped nor rejected; lookupservice
	// never digests the request envelope deterministically.
	if len(envelope.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("tolerated request envelope field did not survive the sanitizer")
	}
}

// lookupSanitizerCase adapts one typed lookup route sanitizer to a shared
// table. build returns a fresh request so a case can mutate it freely.
type lookupSanitizerCase struct {
	name     string
	build    func() proto.Message
	sanitize func(*testing.T, proto.Message) (proto.Message, error)
}

func lookupSanitizerCases() []lookupSanitizerCase {
	definition := func() *opensplunk.LookupDefinition { return lookupHTTPDefinition("catalog") }
	return []lookupSanitizerCase{
		{
			name: "create",
			build: func() proto.Message {
				return &opensplunk.CreateLookupRequest{Definition: definition(), CsvData: []byte("id,value\n")}
			},
			sanitize: func(t *testing.T, request proto.Message) (proto.Message, error) {
				return sanitizeCreateLookupRequest(t.Context(), request.(*opensplunk.CreateLookupRequest))
			},
		},
		{
			name:  "get",
			build: func() proto.Message { return &opensplunk.GetLookupRequest{LookupId: "lookup-1"} },
			sanitize: func(t *testing.T, request proto.Message) (proto.Message, error) {
				return sanitizeGetLookupRequest(t.Context(), request.(*opensplunk.GetLookupRequest))
			},
		},
		{
			name:  "list",
			build: func() proto.Message { return &opensplunk.ListLookupsRequest{} },
			sanitize: func(t *testing.T, request proto.Message) (proto.Message, error) {
				return sanitizeListLookupsRequest(t.Context(), request.(*opensplunk.ListLookupsRequest))
			},
		},
		{
			name: "replace",
			build: func() proto.Message {
				return &opensplunk.ReplaceLookupRequest{LookupId: "lookup-1", ExpectedVersion: 1, Definition: definition()}
			},
			sanitize: func(t *testing.T, request proto.Message) (proto.Message, error) {
				return sanitizeReplaceLookupRequest(t.Context(), request.(*opensplunk.ReplaceLookupRequest))
			},
		},
		{
			name: "set state",
			build: func() proto.Message {
				return &opensplunk.SetLookupStateRequest{
					LookupId:        "lookup-1",
					ExpectedVersion: 1,
					State:           opensplunk.LookupState_LOOKUP_STATE_DISABLED,
				}
			},
			sanitize: func(t *testing.T, request proto.Message) (proto.Message, error) {
				return sanitizeSetLookupStateRequest(t.Context(), request.(*opensplunk.SetLookupStateRequest))
			},
		},
		{
			name: "delete",
			build: func() proto.Message {
				return &opensplunk.DeleteLookupRequest{LookupId: "lookup-1", ExpectedVersion: 1}
			},
			sanitize: func(t *testing.T, request proto.Message) (proto.Message, error) {
				return sanitizeDeleteLookupRequest(t.Context(), request.(*opensplunk.DeleteLookupRequest))
			},
		},
		{
			name: "preview",
			build: func() proto.Message {
				return &opensplunk.PreviewLookupRequest{Definition: definition(), CsvData: []byte("id,value\n")}
			},
			sanitize: func(t *testing.T, request proto.Message) (proto.Message, error) {
				return sanitizePreviewLookupRequest(t.Context(), request.(*opensplunk.PreviewLookupRequest))
			},
		},
	}
}

func TestLookupSanitizersReturnValidRequestsUnchanged(t *testing.T) {
	for _, test := range lookupSanitizerCases() {
		t.Run(test.name, func(t *testing.T) {
			request := test.build()
			before := proto.Clone(request)
			got, err := test.sanitize(t, request)
			if err != nil {
				t.Fatalf("sanitize valid %s request: %v", test.name, err)
			}
			if got != request {
				t.Fatalf("%s sanitizer returned a different pointer", test.name)
			}
			if !proto.Equal(got, before) {
				t.Fatalf("%s sanitizer changed a valid request: %v", test.name, got)
			}
		})
	}
}

func TestLookupSanitizersTolerateUnknownEnvelopeFields(t *testing.T) {
	for _, test := range lookupSanitizerCases() {
		t.Run(test.name, func(t *testing.T) {
			request := test.build()
			addKnowledgeHTTPUnknown(request)
			got, err := test.sanitize(t, request)
			if err != nil {
				t.Fatalf("sanitize %s request with unknown envelope field: %v", test.name, err)
			}
			if len(got.ProtoReflect().GetUnknown()) == 0 {
				t.Fatalf("%s sanitizer did not leave a tolerated envelope field as-is", test.name)
			}
		})
	}
}
