package collector

import (
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func TestNativeFixedDockerStrictTimestampAndText(t *testing.T) {
	t.Parallel()
	d := &Decoder{cfg: DecodeConfig{MaxJSONDepth: defaultMaxJSONDepth, MaxJSONFields: defaultMaxJSONFields}}
	base := `{"log":"ok","stream":"stdout","time":"2024-02-29T20:30:12Z"}`
	for _, stamp := range []string{"2024-02-29T20:30:12.1234567891Z", "2024-02-29T20:30:12,123Z", "2024-02-29T1:30:12Z", "2024-02-29T20:30:12+24:00", "2024-02-29T20:30:12+01:60", "0000-01-01T00:00:00Z"} {
		if err := d.decodeDocker(&opensplunk.LogEvent{}, []byte(strings.Replace(base, "2024-02-29T20:30:12Z", stamp, 1))); err == nil {
			t.Errorf("accepted timestamp %q", stamp)
		}
	}
	for _, log := range []string{`\ud800`, `\udc00`, `\ud800x`, `\ud800\u0041`} {
		if err := d.decodeDocker(&opensplunk.LogEvent{}, []byte(strings.Replace(base, "ok", log, 1))); err == nil {
			t.Errorf("accepted malformed surrogate %q", log)
		}
	}
	for _, log := range []string{`\ud83d\ude00`, `literal \\ud800`, "replacement �"} {
		if err := d.decodeDocker(&opensplunk.LogEvent{}, []byte(strings.Replace(base, "ok", log, 1))); err != nil {
			t.Errorf("rejected valid text %q: %v", log, err)
		}
	}
}

func TestNativeFixedAccessZeroOffsetSigns(t *testing.T) {
	t.Parallel()
	d := &Decoder{cfg: DecodeConfig{Format: "apache-common"}}
	for _, zone := range []string{"+0000", "-0000"} {
		event := &opensplunk.LogEvent{}
		raw := []byte(`host - - [29/Feb/2024:20:30:12 ` + zone + `] "GET / HTTP/1.1" 200 0`)
		if err := d.decodeAccess(event, raw); err != nil {
			t.Errorf("offset %s rejected: %v", zone, err)
		}
	}
}

// Apache log_remote_user emits a literal pair of quotes for an authenticated
// empty user, distinct from its dash for a missing user:
// https://github.com/apache/httpd/blob/trunk/modules/loggers/mod_log_config.c
func TestNativeFixedApacheEmptyUserSentinel(t *testing.T) {
	t.Parallel()
	for _, format := range []InputFormat{"apache-common", "apache-combined", "nginx-combined"} {
		d := &Decoder{cfg: DecodeConfig{Format: format}}
		base := `host - USER [29/Feb/2024:20:30:12 +0000] "GET / HTTP/1.1" 200 0`
		if format != "apache-common" {
			base += ` "-" "agent"`
		}
		for _, user := range []string{`""`, `"alice"`, `""suffix`, `prefix""`} {
			event := &opensplunk.LogEvent{}
			err := d.decodeAccess(event, []byte(strings.Replace(base, "USER", user, 1)))
			if format != "nginx-combined" && user == `""` {
				if err != nil {
					t.Errorf("%s empty user: %v", format, err)
					continue
				}
				value := fieldValue(event, "user")
				if _, ok := value.GetKind().(*opensplunk.TypedValue_StringValue); !ok || value.GetStringValue() != "" {
					t.Errorf("%s empty user projection = %v", format, value)
				}
			} else if err == nil {
				t.Errorf("%s accepted unsupported unquoted user %q", format, user)
			}
		}
		if err := d.decodeAccess(&opensplunk.LogEvent{}, []byte(strings.Replace(base, "host - USER", `"" - alice`, 1))); err == nil {
			t.Errorf("%s accepted empty user sentinel as a client address", format)
		}
		if err := d.decodeAccess(&opensplunk.LogEvent{}, []byte(strings.Replace(base, "host - USER", `host "" alice`, 1))); err == nil {
			t.Errorf("%s accepted empty user sentinel as ident", format)
		}
	}
}

// ap_escape_logitem uses hex for form feed; its C escapes are b/n/r/t/v,
// not f: https://github.com/apache/httpd/blob/trunk/server/util.c.
func TestNativeFixedApacheFormFeedEscaping(t *testing.T) {
	t.Parallel()
	d := &Decoder{cfg: DecodeConfig{Format: "apache-combined"}}
	base := `host - alice [29/Feb/2024:20:30:12 +0000] "GET / HTTP/1.1" 200 0 "-" "AGENT"`
	event := &opensplunk.LogEvent{}
	if err := d.decodeAccess(event, []byte(strings.Replace(base, "AGENT", `a\x0cb`, 1))); err != nil {
		t.Fatal(err)
	}
	if value := fieldValue(event, "user_agent").GetStringValue(); value != "a\fb" {
		t.Errorf("form feed decoded as %q", value)
	}
	if err := d.decodeAccess(&opensplunk.LogEvent{}, []byte(strings.Replace(base, "AGENT", `a\fb`, 1))); err == nil {
		t.Fatal("accepted unsupported Apache form-feed C escape")
	}
}
