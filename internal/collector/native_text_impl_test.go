package collector

import "testing"

func TestNativeLogfmtRejectsUnpairedSurrogates(t *testing.T) {
	decoder := nativeTextDecoder(t, "logfmt", nil)
	for _, raw := range []string{`message="\ud800"`, `message="\udc00"`, `message="\ud800x"`, `message="\ud800\u0041"`} {
		if _, err := decoder.Decode([]byte(raw), SourcePosition{}, nativeTextNow); err == nil {
			t.Errorf("accepted unpaired surrogate in %s", raw)
		}
	}
	for _, raw := range []string{`message="\ud83d\ude00"`, `message="\\ud800"`} {
		nativeTextDecode(t, decoder, raw)
	}
}
