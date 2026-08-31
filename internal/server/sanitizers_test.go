package server

import (
	"bytes"
	"errors"
	"net/http"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/proto"
)

// assertUnknownFieldTolerated states what an ordinary route sanitizer does with
// a field this binary does not define: unknown fields are neither stripped nor
// rejected on this route, because protobuf decoding ignores them and the
// sanitizer leaves the decoded message as-is.
func assertUnknownFieldTolerated(t *testing.T, message proto.Message, want []byte) {
	t.Helper()
	if got := message.ProtoReflect().GetUnknown(); !bytes.Equal(got, want) {
		t.Fatalf("unknown fields = %x, want %x", got, want)
	}
}

// assertSanitizerRejection is the single rejection assertion every route
// sanitizer test shares: the sanitizer answered 400 carrying exactly message,
// because those messages are the endpoint's public contract.
func assertSanitizerRejection(t *testing.T, err error, message string) {
	t.Helper()
	assertSanitizerHTTPError(t, err, http.StatusBadRequest, message)
}

// assertSanitizerHTTPError is the status-taking variant, for the few sanitizers
// that reject with something other than 400.
func assertSanitizerHTTPError(t *testing.T, err error, status int, message string) {
	t.Helper()
	var httpErr *router.HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("error = %T %v, want *router.HTTPError", err, err)
	}
	if httpErr.StatusCode != status || httpErr.Message != message {
		t.Fatalf(
			"error = %d %q, want %d %q",
			httpErr.StatusCode,
			httpErr.Message,
			status,
			message,
		)
	}
}
