package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func TestBoundedProtoCodecEnforcesMarshalByteLimitAndReleasesPermit(t *testing.T) {
	released := 0
	bounded := newBoundedProtoCodec(
		codec.NewProtoCodec[*opensplunk.GetSystemBootstrapRequest, *opensplunk.GetSystemBootstrapResponse](),
		boundedProtoCodecOptions{maximumBytes: 1, sizeError: "test response is too large"},
	)
	response := httptest.NewRecorder()
	err := bounded.Encode(response, &boundedProtoResponse[*opensplunk.GetSystemBootstrapResponse]{
		message: &opensplunk.GetSystemBootstrapResponse{Features: []opensplunk.ServerFeature{
			opensplunk.ServerFeature_SERVER_FEATURE_SEARCH,
		}},
		ctx:     context.Background(),
		release: func() { released++ },
	})
	if err == nil || !strings.Contains(err.Error(), "too large") || released != 1 || response.Body.Len() != 0 {
		t.Fatalf("Encode error/released/body = %v/%d/%d", err, released, response.Body.Len())
	}
}
