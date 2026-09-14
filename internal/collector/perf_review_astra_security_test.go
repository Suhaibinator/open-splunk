package collector

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/jsonnumber"
	"google.golang.org/protobuf/proto"
)

func TestPerfReviewAstraSecurityDifferential(t *testing.T) {
	d := nativeTextDecoder(t, "logfmt", nil)
	count := 0
	check := func(raw string) {
		t.Helper()
		a, b := &opensplunk.LogEvent{Fields: &opensplunk.TypedObject{}}, &opensplunk.LogEvent{Fields: &opensplunk.TypedObject{}}
		ea, eb := d.decodeLogfmt(a, []byte(raw)), d.perfSecurityHistoricalLogfmt(b, []byte(raw))
		if fmt.Sprint(ea) != fmt.Sprint(eb) || !proto.Equal(a, b) {
			t.Fatalf("raw=%q new=%v old=%v newEvent=%v oldEvent=%v", raw, ea, eb, a, b)
		}
		count++
	}
	atoms := []string{"a", " ", "\t", "\n", "\x00", "\x1f", "\x7f", "é", "🔐", "\xff", `\`, `"`, `\"`, `\\`, `\n`, `\x20`, `\u0000`, `\ud800`, `\udfff`, `\ud83d\udd10`, `\udd10\ud83d`, `\uZZZZ`}
	for _, a := range atoms {
		for _, b := range atoms {
			for _, c := range atoms {
				for _, suffix := range []string{`"`, `"tail`, "", `" next=1`} {
					check(`value="` + a + b + c + suffix)
				}
			}
		}
	}
	for _, raw := range []string{`host="secret" source="spoof" service="x"`, `message="token=password" password="secret"`, `value="ok" value="bad\ud800"`, `message="\ud800\n` + "\x00" + `"`, `timestamp="1"`} {
		check(raw)
	}
	alphabet := "01-+.eE9x \t"
	var visit func(string, int)
	visit = func(s string, remaining int) {
		oracle := s != "" && !strings.ContainsAny(s, " \t\r\n") && (s[0] == '-' || s[0] >= '0' && s[0] <= '9') && json.Valid([]byte(s))
		if jsonnumber.Valid(s) != oracle {
			t.Fatalf("number grammar mismatch %q", s)
		}
		if remaining == 0 {
			return
		}
		for _, r := range alphabet {
			visit(s+string(r), remaining-1)
		}
	}
	visit("", 5)
	t.Logf("Matched historical error/event behavior for %d logfmt records and exhaustive numeric alphabet through length 5", count)
}
func (d *Decoder) perfSecurityHistoricalLogfmt(event *opensplunk.LogEvent, raw []byte) error {
	if !utf8.Valid(raw) {
		return errors.New("logfmt requires UTF-8")
	}
	seen := make(map[string]struct{})
	fields := make([]*opensplunk.TypedObjectField, 0, 8)
	for at := 0; at < len(raw); {
		for at < len(raw) && nativeHorizontal(raw[at]) {
			at++
		}
		if at == len(raw) {
			break
		}
		start := at
		for at < len(raw) && raw[at] != '=' && !nativeHorizontal(raw[at]) {
			at++
		}
		if at == len(raw) || raw[at] != '=' {
			return errors.New("logfmt token requires equals")
		}
		name := string(raw[start:at])
		if !nativeValidName(name) || strings.ContainsAny(name, "\"\\") {
			return errors.New("invalid logfmt field name")
		}
		if _, exists := seen[name]; exists {
			return errors.New("duplicate logfmt field")
		}
		seen[name] = struct{}{}
		if len(seen) > d.cfg.MaxJSONFields {
			return errors.New("logfmt field count exceeds limit")
		}
		at++
		start = at
		var value any
		if at < len(raw) && raw[at] == '"' {
			at++
			closed := false
			for at < len(raw) {
				c := raw[at]
				at++
				if c == '\\' {
					if at < len(raw) {
						at++
					}
					continue
				}
				if c == '"' {
					closed = true
					break
				}
			}
			if !closed || (at < len(raw) && !nativeHorizontal(raw[at])) {
				return errors.New("invalid quoted logfmt value")
			}
			var decoded string
			if err := json.Unmarshal(raw[start:at], &decoded); err != nil {
				return errors.New("invalid logfmt string escape")
			}
			if !nativeJSONSurrogatesValid(raw[start:at]) {
				return errors.New("logfmt string contains an unpaired Unicode surrogate")
			}
			value = decoded
		} else {
			for at < len(raw) && !nativeHorizontal(raw[at]) {
				if raw[at] < ' ' || raw[at] == 0x7f || raw[at] == '"' {
					return errors.New("invalid unquoted logfmt value")
				}
				at++
			}
			token := raw[start:at]
			text := string(token)
			switch {
			case text == "true":
				value = true
			case text == "false":
				value = false
			case len(token) > 0 && (token[0] == '-' || (token[0] >= '0' && token[0] <= '9')) && json.Valid(token):
				value = json.Number(text)
			default:
				value = text
			}
		}
		role := d.parser.CanonicalField(name)
		if role != "" {
			if err := d.nativeCanonical(event, role, value); err != nil {
				return err
			}
			continue
		}
		if eventfields.IsCollectorReservedRoot(name) {
			continue
		}
		converted, err := typedJSONValue(value)
		if err != nil {
			return errors.New("invalid logfmt typed value")
		}
		fields = append(fields, &opensplunk.TypedObjectField{Name: name, Value: converted})
	}
	if len(seen) == 0 {
		return errors.New("logfmt requires a field")
	}
	event.Fields.Fields = fields
	return nil
}
