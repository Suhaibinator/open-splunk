package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

type failingStateTokenAdministration struct {
	mutableTokenAdministration
	err error
}

func (administration *failingStateTokenAdministration) SetCollectorTokenEnabled(context.Context, string, uint64, bool) (auth.CollectorToken, error) {
	return auth.CollectorToken{}, administration.err
}

type privateDatabaseError struct{}

func (privateDatabaseError) Error() string { return "private token or SQL parameter" }
func (privateDatabaseError) Code() int     { return 19 }

func TestTokenStateFailureLogsDatabaseCodeWithoutPrivateDetails(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		err    error
		status int
		logs   int
	}{
		{name: "database failure", err: fmt.Errorf("private wrapper: %w", privateDatabaseError{}), status: http.StatusServiceUnavailable, logs: 1},
		{name: "version conflict", err: control.ErrVersionConflict, status: http.StatusConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			core, observed := observer.New(zapcore.WarnLevel)
			handler := &apiHandler{
				logger:          zap.New(core),
				ingestionTokens: &failingStateTokenAdministration{err: test.err},
			}
			_, err := handler.setIngestionTokenEnabled(
				httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/ingestion-tokens/state/set", nil),
				&opensplunk.SetIngestionTokenEnabledRequest{
					IngestionTokenId: "private-id",
					ExpectedVersion:  1,
				},
			)
			assertHTTPErrorStatus(t, err, test.status)
			entries := observed.All()
			if len(entries) != test.logs {
				t.Fatalf("diagnostic entries = %d, want %d", len(entries), test.logs)
			}
			if test.logs == 0 {
				return
			}
			fields := entries[0].ContextMap()
			if entries[0].Message != "ingestion token state update unavailable" ||
				len(fields) != 2 ||
				fields["database_error_code"] != int64(19) ||
				fields["database_contention"] != false {
				t.Fatalf("unexpected diagnostic (must contain only safe classification): %#v", entries[0])
			}
		})
	}
}
