package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

func TestBoundedProtoCodecEnforcesMarshalByteLimitAndReleasesPermit(t *testing.T) {
	released := 0
	bounded := newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunkv1.GetSystemBootstrapRequest, *opensplunkv1.GetSystemBootstrapResponse](),
		boundedProtoCodecOptions{maximumBytes: 1, sizeError: "test response is too large"},
	)
	response := httptest.NewRecorder()
	err := bounded.Encode(response, &boundedProtoResponse[*opensplunkv1.GetSystemBootstrapResponse]{
		message: &opensplunkv1.GetSystemBootstrapResponse{ServerVersion: "larger than one byte"},
		ctx:     context.Background(),
		release: func() { released++ },
	})
	if err == nil || !strings.Contains(err.Error(), "too large") || released != 1 || response.Body.Len() != 0 {
		t.Fatalf("Encode error/released/body = %v/%d/%d", err, released, response.Body.Len())
	}
}
