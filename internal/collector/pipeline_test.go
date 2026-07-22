package collector

import (
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// --- test builders (names prefixed to avoid clashes with decoder_test.go) ---

func plField(name string, value *opensplunkv1.TypedValue) *opensplunkv1.TypedObjectField {
	return &opensplunkv1.TypedObjectField{Name: name, Value: value}
}

func plInt(n int64) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_Sint64Value{Sint64Value: n}}
}

func plObj(fields ...*opensplunkv1.TypedObjectField) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_ObjectValue{
		ObjectValue: &opensplunkv1.TypedObject{Fields: fields},
	}}
}

func plList(values ...*opensplunkv1.TypedValue) *opensplunkv1.TypedValue {
	return &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_ListValue{
		ListValue: &opensplunkv1.TypedValueList{Values: values},
	}}
}

// plEvent builds a LogEvent with canonical metadata populated (so tests can
// assert processors never touch it) and the given dynamic fields.
func plEvent(fields ...*opensplunkv1.TypedObjectField) *opensplunkv1.LogEvent {
	msg := "canonical message with token=should-not-be-touched"
	lvl := "INFO"
	return &opensplunkv1.LogEvent{
		EventId:    "evt-1",
		IndexName:  "main",
		Host:       "host-1",
		Source:     "/var/log/app.log",
		Sourcetype: "app",
		Level:      &lvl,
		Message:    &msg,
		Raw:        []byte(`{"token":"raw-secret"}`),
		Fields:     &opensplunkv1.TypedObject{Fields: fields},
	}
}

// plFieldNames returns the ordered top-level dynamic field names.
func plFieldNames(e *opensplunkv1.LogEvent) []string {
	if e == nil || e.Fields == nil {
		return nil
	}
	names := make([]string, 0, len(e.Fields.Fields))
	for _, f := range e.Fields.Fields {
		names = append(names, f.Name)
	}
	return names
}

func plLookup(e *opensplunkv1.LogEvent, name string) *opensplunkv1.TypedValue {
	if e == nil || e.Fields == nil {
		return nil
	}
	for _, f := range e.Fields.Fields {
		if f.Name == name {
			return f.Value
		}
	}
	return nil
}

// mustProcess runs p and fails the test on error.
func mustProcess(t *testing.T, p Processor, e *opensplunkv1.LogEvent) *opensplunkv1.LogEvent {
	t.Helper()
	out, err := p.Process(e)
	if err != nil {
		t.Fatalf("%s.Process: unexpected error: %v", p.Name(), err)
	}
	return out
}

// assertUnchanged asserts the original event was not mutated by comparing it to a
// snapshot taken before processing.
func assertUnchanged(t *testing.T, original, snapshot *opensplunkv1.LogEvent) {
	t.Helper()
	if !proto.Equal(original, snapshot) {
		t.Fatalf("input event was mutated in place:\n got  %v\n want %v", original, snapshot)
	}
}

// --- Allow ---

func TestNewAllowProcessorRejectsEmptyList(t *testing.T) {
	t.Parallel()
	if _, err := NewAllowProcessor(nil); err == nil {
		t.Fatal("NewAllowProcessor(nil): want error, got nil")
	}
	if _, err := NewAllowProcessor([]string{}); err == nil {
		t.Fatal("NewAllowProcessor([]): want error, got nil")
	}
}

func TestAllowProcessor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		allow    []string
		fields   []*opensplunkv1.TypedObjectField
		want     []string // surviving field names, in order
		wantNoop bool     // same pointer returned
	}{
		{
			name:   "keeps only listed and preserves order",
			allow:  []string{"a", "c"},
			fields: []*opensplunkv1.TypedObjectField{plField("a", plInt(1)), plField("b", plInt(2)), plField("c", plInt(3))},
			want:   []string{"a", "c"},
		},
		{
			name:  "nested object under kept field is kept whole",
			allow: []string{"keep"},
			fields: []*opensplunkv1.TypedObjectField{
				plField("keep", plObj(plField("inner", plInt(9)))),
				plField("drop", plInt(1)),
			},
			want: []string{"keep"},
		},
		{
			name:   "case sensitive exact match",
			allow:  []string{"Token"},
			fields: []*opensplunkv1.TypedObjectField{plField("token", plInt(1)), plField("Token", plInt(2))},
			want:   []string{"Token"},
		},
		{
			name:     "all kept is a no-op",
			allow:    []string{"a", "b"},
			fields:   []*opensplunkv1.TypedObjectField{plField("a", plInt(1)), plField("b", plInt(2))},
			want:     []string{"a", "b"},
			wantNoop: true,
		},
		{
			name:     "no dynamic fields is a no-op",
			allow:    []string{"a"},
			fields:   nil,
			want:     nil,
			wantNoop: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := NewAllowProcessor(tt.allow)
			if err != nil {
				t.Fatalf("NewAllowProcessor: %v", err)
			}
			in := plEvent(tt.fields...)
			snap := proto.Clone(in).(*opensplunkv1.LogEvent)
			out := mustProcess(t, p, in)
			assertUnchanged(t, in, snap)
			if tt.wantNoop && out != in {
				t.Fatalf("expected no-op (same pointer), got clone")
			}
			if !tt.wantNoop && out == in {
				t.Fatalf("expected clone, got same pointer")
			}
			if got := plFieldNames(out); !equalStrings(got, tt.want) {
				t.Fatalf("surviving fields = %v, want %v", got, tt.want)
			}
			// Nested-whole check for the relevant case.
			if tt.name == "nested object under kept field is kept whole" {
				kept := plLookup(out, "keep").GetObjectValue()
				if kept == nil || len(kept.Fields) != 1 || kept.Fields[0].Name != "inner" {
					t.Fatalf("nested object not preserved whole: %v", kept)
				}
			}
		})
	}
}

// --- Deny ---

func TestDenyProcessor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		deny     []string
		fields   []*opensplunkv1.TypedObjectField
		want     []string
		wantNoop bool
	}{
		{
			name:   "removes listed",
			deny:   []string{"b"},
			fields: []*opensplunkv1.TypedObjectField{plField("a", plInt(1)), plField("b", plInt(2)), plField("c", plInt(3))},
			want:   []string{"a", "c"},
		},
		{
			name:     "empty deny is identity no-op",
			deny:     nil,
			fields:   []*opensplunkv1.TypedObjectField{plField("a", plInt(1))},
			want:     []string{"a"},
			wantNoop: true,
		},
		{
			name:     "no listed field present is a no-op",
			deny:     []string{"x"},
			fields:   []*opensplunkv1.TypedObjectField{plField("a", plInt(1))},
			want:     []string{"a"},
			wantNoop: true,
		},
		{
			name:     "case sensitive",
			deny:     []string{"Token"},
			fields:   []*opensplunkv1.TypedObjectField{plField("token", plInt(1))},
			want:     []string{"token"},
			wantNoop: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := NewDenyProcessor(tt.deny)
			if err != nil {
				t.Fatalf("NewDenyProcessor: %v", err)
			}
			in := plEvent(tt.fields...)
			snap := proto.Clone(in).(*opensplunkv1.LogEvent)
			out := mustProcess(t, p, in)
			assertUnchanged(t, in, snap)
			if tt.wantNoop && out != in {
				t.Fatalf("expected no-op (same pointer), got clone")
			}
			if !tt.wantNoop && out == in {
				t.Fatalf("expected clone, got same pointer")
			}
			if got := plFieldNames(out); !equalStrings(got, tt.want) {
				t.Fatalf("surviving fields = %v, want %v", got, tt.want)
			}
		})
	}
}

// --- Rename ---

func TestNewRenameProcessorConstructionErrors(t *testing.T) {
	t.Parallel()
	cases := []struct{ from, to string }{
		{"", "b"},
		{"a", ""},
		{"a", "a"},
		{"", ""},
	}
	for _, c := range cases {
		if _, err := NewRenameProcessor(c.from, c.to); err == nil {
			t.Fatalf("NewRenameProcessor(%q,%q): want error, got nil", c.from, c.to)
		}
	}
}

func TestRenameProcessor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		from, to string
		fields   []*opensplunkv1.TypedObjectField
		want     []string
		wantNoop bool
		wantVal  int64 // value expected at `to` (via GetSint64Value)
	}{
		{
			name: "renames in place preserving position",
			from: "old", to: "new",
			fields:  []*opensplunkv1.TypedObjectField{plField("a", plInt(1)), plField("old", plInt(7)), plField("b", plInt(2))},
			want:    []string{"a", "new", "b"},
			wantVal: 7,
		},
		{
			name: "renamed field replaces pre-existing target",
			from: "old", to: "dest",
			fields:  []*opensplunkv1.TypedObjectField{plField("dest", plInt(100)), plField("old", plInt(7)), plField("z", plInt(2))},
			want:    []string{"old_becomes_dest_here_placeholder"}, // replaced below
			wantVal: 7,
		},
		{
			name: "missing from is a no-op",
			from: "absent", to: "new",
			fields:   []*opensplunkv1.TypedObjectField{plField("a", plInt(1))},
			want:     []string{"a"},
			wantNoop: true,
		},
		{
			name: "case sensitive from",
			from: "Old", to: "new",
			fields:   []*opensplunkv1.TypedObjectField{plField("old", plInt(1))},
			want:     []string{"old"},
			wantNoop: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			p, err := NewRenameProcessor(tt.from, tt.to)
			if err != nil {
				t.Fatalf("NewRenameProcessor: %v", err)
			}
			in := plEvent(tt.fields...)
			snap := proto.Clone(in).(*opensplunkv1.LogEvent)
			out := mustProcess(t, p, in)
			assertUnchanged(t, in, snap)
			if tt.wantNoop && out != in {
				t.Fatalf("expected no-op (same pointer), got clone")
			}
			if !tt.wantNoop && out == in {
				t.Fatalf("expected clone, got same pointer")
			}
			if tt.name == "renamed field replaces pre-existing target" {
				// Position: `dest` is removed, `old` renamed to `dest` in place.
				if got := plFieldNames(out); !equalStrings(got, []string{"dest", "z"}) {
					t.Fatalf("fields = %v, want [dest z]", got)
				}
				if v := plLookup(out, "dest"); v.GetSint64Value() != 7 {
					t.Fatalf("dest value = %d, want 7 (renamed value wins)", v.GetSint64Value())
				}
				return
			}
			if got := plFieldNames(out); !equalStrings(got, tt.want) {
				t.Fatalf("fields = %v, want %v", got, tt.want)
			}
			if !tt.wantNoop {
				if v := plLookup(out, tt.to); v == nil || v.GetSint64Value() != tt.wantVal {
					t.Fatalf("%s value = %v, want %d", tt.to, v, tt.wantVal)
				}
			}
		})
	}
}

// --- Redact ---

func TestNewRedactProcessorConstructionErrors(t *testing.T) {
	t.Parallel()
	if _, err := NewRedactProcessor(nil, "X"); err == nil {
		t.Fatal("empty fields: want error")
	}
	if _, err := NewRedactProcessor([]string{}, "X"); err == nil {
		t.Fatal("empty fields slice: want error")
	}
	if _, err := NewRedactProcessor([]string{"token"}, ""); err == nil {
		t.Fatal("empty replacement: want error")
	}
}

func TestRedactProcessor(t *testing.T) {
	t.Parallel()

	t.Run("replaces scalar object and list values at top level with a string", func(t *testing.T) {
		t.Parallel()
		p, _ := NewRedactProcessor([]string{"secretScalar", "secretObj", "secretList"}, "[REDACTED]")
		in := plEvent(
			plField("keep", plInt(1)),
			plField("secretScalar", plInt(42)),
			plField("secretObj", plObj(plField("inner", plInt(9)))),
			plField("secretList", plList(plInt(1), plInt(2))),
		)
		snap := proto.Clone(in).(*opensplunkv1.LogEvent)
		out := mustProcess(t, p, in)
		assertUnchanged(t, in, snap)
		if out == in {
			t.Fatal("expected clone, got same pointer")
		}
		if v := plLookup(out, "secretScalar"); v.GetStringValue() != "[REDACTED]" {
			t.Fatalf("secretScalar = %v, want redacted string", v)
		}
		// Type change: object -> string.
		if v := plLookup(out, "secretObj"); v.GetStringValue() != "[REDACTED]" || v.GetObjectValue() != nil {
			t.Fatalf("secretObj not replaced with string: %v", v)
		}
		// Type change: list -> string.
		if v := plLookup(out, "secretList"); v.GetStringValue() != "[REDACTED]" || v.GetListValue() != nil {
			t.Fatalf("secretList not replaced with string: %v", v)
		}
		if v := plLookup(out, "keep"); v.GetSint64Value() != 1 {
			t.Fatalf("keep field damaged: %v", v)
		}
	})

	t.Run("redacts recursively in nested objects and objects inside lists", func(t *testing.T) {
		t.Parallel()
		p, _ := NewRedactProcessor([]string{"password"}, "***")
		in := plEvent(
			plField("outer", plObj(
				plField("mid", plObj(
					plField("password", stringValue("deep-secret")),
					plField("ok", plInt(1)),
				)),
			)),
			plField("users", plList(
				plObj(plField("name", stringValue("alice")), plField("password", stringValue("pw1"))),
				plObj(plField("name", stringValue("bob")), plField("password", stringValue("pw2"))),
			)),
		)
		snap := proto.Clone(in).(*opensplunkv1.LogEvent)
		out := mustProcess(t, p, in)
		assertUnchanged(t, in, snap)

		deep := plLookup(out, "outer").GetObjectValue().Fields[0].Value.GetObjectValue()
		if got := deep.Fields[0].Value.GetStringValue(); got != "***" {
			t.Fatalf("deep password = %q, want ***", got)
		}
		if got := deep.Fields[1].Value.GetSint64Value(); got != 1 {
			t.Fatalf("sibling ok damaged: %d", got)
		}
		list := plLookup(out, "users").GetListValue()
		for i, item := range list.Values {
			obj := item.GetObjectValue()
			if got := obj.Fields[1].Value.GetStringValue(); got != "***" {
				t.Fatalf("users[%d].password = %q, want ***", i, got)
			}
		}
	})

	t.Run("no match is a no-op", func(t *testing.T) {
		t.Parallel()
		p, _ := NewRedactProcessor([]string{"token"}, "X")
		in := plEvent(plField("a", plObj(plField("b", plInt(1)))))
		out := mustProcess(t, p, in)
		if out != in {
			t.Fatal("expected no-op (same pointer)")
		}
	})

	t.Run("does not touch canonical raw or message", func(t *testing.T) {
		t.Parallel()
		p, _ := NewRedactProcessor([]string{"token"}, "X")
		in := plEvent(plField("token", stringValue("dyn-secret")))
		out := mustProcess(t, p, in)
		if got := out.GetMessage(); !strings.Contains(got, "token=should-not-be-touched") {
			t.Fatalf("message was altered: %q", got)
		}
		if got := string(out.GetRaw()); !strings.Contains(got, "raw-secret") {
			t.Fatalf("raw was altered: %q", got)
		}
		if v := plLookup(out, "token"); v.GetStringValue() != "X" {
			t.Fatalf("dynamic token not redacted: %v", v)
		}
	})
}

// --- Pipeline ---

// pipeFunc is a test Processor built from a closure.
type pipeFunc struct {
	name string
	fn   func(*opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error)
}

func (p pipeFunc) Name() string { return p.name }
func (p pipeFunc) Process(e *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
	return p.fn(e)
}

func TestPipelineProcess(t *testing.T) {
	t.Parallel()

	t.Run("nil receiver passes through", func(t *testing.T) {
		t.Parallel()
		var p *Pipeline
		in := plEvent(plField("a", plInt(1)))
		out, err := p.Process(in)
		if err != nil || out != in {
			t.Fatalf("nil pipeline: out=%v err=%v", out, err)
		}
	})

	t.Run("empty chain passes through", func(t *testing.T) {
		t.Parallel()
		in := plEvent(plField("a", plInt(1)))
		out, err := NewPipeline().Process(in)
		if err != nil || out != in {
			t.Fatalf("empty pipeline: out=%v err=%v", out, err)
		}
	})

	t.Run("runs in order", func(t *testing.T) {
		t.Parallel()
		allow, _ := NewAllowProcessor([]string{"a", "b"})
		deny, _ := NewDenyProcessor([]string{"b"})
		in := plEvent(plField("a", plInt(1)), plField("b", plInt(2)), plField("c", plInt(3)))
		out, err := NewPipeline(allow, deny).Process(in)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if got := plFieldNames(out); !equalStrings(got, []string{"a"}) {
			t.Fatalf("fields = %v, want [a]", got)
		}
	})

	t.Run("stops on drop", func(t *testing.T) {
		t.Parallel()
		reached := false
		drop := pipeFunc{name: "drop", fn: func(*opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
			return nil, nil
		}}
		after := pipeFunc{name: "after", fn: func(e *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
			reached = true
			return e, nil
		}}
		out, err := NewPipeline(drop, after).Process(plEvent())
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if out != nil {
			t.Fatalf("out = %v, want nil (dropped)", out)
		}
		if reached {
			t.Fatal("processor after drop must not run")
		}
	})

	t.Run("stops on error", func(t *testing.T) {
		t.Parallel()
		reached := false
		boom := pipeFunc{name: "boom", fn: func(*opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
			return nil, errNotImplemented
		}}
		after := pipeFunc{name: "after", fn: func(e *opensplunkv1.LogEvent) (*opensplunkv1.LogEvent, error) {
			reached = true
			return e, nil
		}}
		out, err := NewPipeline(boom, after).Process(plEvent())
		if err == nil {
			t.Fatal("want error, got nil")
		}
		if out != nil {
			t.Fatalf("out = %v, want nil on error", out)
		}
		if reached {
			t.Fatal("processor after error must not run")
		}
	})

	t.Run("order dependence rename then redact", func(t *testing.T) {
		t.Parallel()
		// rename user_password -> password, then redact password.
		rename, _ := NewRenameProcessor("user_password", "password")
		redact, _ := NewRedactProcessor([]string{"password"}, "***")

		in := plEvent(plField("user_password", stringValue("hunter2")))
		snap := proto.Clone(in).(*opensplunkv1.LogEvent)

		// rename -> redact: field is renamed first, so redact catches it.
		out, err := NewPipeline(rename, redact).Process(in)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		assertUnchanged(t, in, snap)
		if v := plLookup(out, "password"); v.GetStringValue() != "***" {
			t.Fatalf("rename-then-redact: password = %v, want ***", v)
		}

		// redact -> rename: redact runs first on the pre-rename name, misses it,
		// so the secret survives under the new name (order matters).
		in2 := plEvent(plField("user_password", stringValue("hunter2")))
		out2, err := NewPipeline(redact, rename).Process(in2)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if v := plLookup(out2, "password"); v.GetStringValue() != "hunter2" {
			t.Fatalf("redact-then-rename: password = %v, want hunter2 (secret not caught)", v)
		}
	})
}

// --- Redaction invariant ---

func TestRedactionInvariantNoSecretSurvivesMarshaling(t *testing.T) {
	t.Parallel()
	const secret = "SUPER-SECRET-PLANTED-VALUE-9f3a"

	in := plEvent(
		plField("token", stringValue(secret)),
		plField("nested", plObj(
			plField("password", stringValue(secret)),
			plField("keep", stringValue("visible")),
		)),
		plField("accounts", plList(
			plObj(plField("token", stringValue(secret))),
			plObj(plField("deep", plObj(plField("password", stringValue(secret))))),
		)),
	)

	p, err := NewRedactProcessor([]string{"token", "password"}, "[REDACTED]")
	if err != nil {
		t.Fatalf("NewRedactProcessor: %v", err)
	}
	out := mustProcess(t, p, in)

	blob, err := protojson.Marshal(out)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}
	if strings.Contains(string(blob), secret) {
		t.Fatalf("planted secret survived redaction in marshaled event:\n%s", blob)
	}
	// Sanity: non-secret content is still present.
	if !strings.Contains(string(blob), "visible") {
		t.Fatalf("redaction removed too much; 'visible' missing:\n%s", blob)
	}
}

// equalStrings compares two string slices for order-sensitive equality.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
