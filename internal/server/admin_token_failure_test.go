package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
	privateFailure := privateDatabaseError{}
	for _, test := range []struct {
		name   string
		err    error
		status int
		logs   int
		phase  string
	}{
		{
			name: "begin",
			err: fmt.Errorf(
				"begin collector token state update: private-id secret-prefix: %w",
				privateFailure,
			),
			status: http.StatusServiceUnavailable,
			logs:   1,
			phase:  "begin",
		},
		{
			name: "read before",
			err: fmt.Errorf(
				"read collector token for state update: private-id secret-prefix: %w",
				privateFailure,
			),
			status: http.StatusServiceUnavailable,
			logs:   1,
			phase:  "read_before",
		},
		{
			name: "update",
			err: fmt.Errorf(
				"set collector token enabled state: private-id secret-prefix: %w",
				privateFailure,
			),
			status: http.StatusServiceUnavailable,
			logs:   1,
			phase:  "update",
		},
		{
			name: "read after",
			err: fmt.Errorf(
				"read state-updated collector token: private-id secret-prefix: %w",
				privateFailure,
			),
			status: http.StatusServiceUnavailable,
			logs:   1,
			phase:  "read_after",
		},
		{
			name: "audit",
			err: fmt.Errorf(
				"append collector token state update audit event: private-id secret-prefix: %w",
				privateFailure,
			),
			status: http.StatusServiceUnavailable,
			logs:   1,
			phase:  "audit",
		},
		{
			name: "commit",
			err: fmt.Errorf(
				"commit collector token state update: private-id secret-prefix: %w",
				privateFailure,
			),
			status: http.StatusServiceUnavailable,
			logs:   1,
			phase:  "commit",
		},
		{
			name:   "other",
			err:    fmt.Errorf("private wrapper for private-id secret-prefix: %w", privateFailure),
			status: http.StatusServiceUnavailable,
			logs:   1,
			phase:  "other",
		},
		{
			name: "recognized prefix after private wrapper",
			err: fmt.Errorf(
				"private wrapper: begin collector token state update: private-id: %w",
				privateFailure,
			),
			status: http.StatusServiceUnavailable,
			logs:   1,
			phase:  "other",
		},
		{
			name: "joined rollback suffix",
			err: errors.Join(
				fmt.Errorf("begin collector token state update: private-id: %w", privateFailure),
				errors.New("roll back transaction: private rollback token"),
			),
			status: http.StatusServiceUnavailable,
			logs:   1,
			phase:  "begin",
		},
		{name: "version conflict", err: control.ErrVersionConflict, status: http.StatusConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			core, observed := observer.New(zapcore.WarnLevel)
			handler := &apiHandler{
				logger:          zap.New(core),
				ingestionTokens: &failingStateTokenAdministration{err: test.err},
			}
			response, err := handler.setIngestionTokenEnabled(
				httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/ingestion-tokens/state/set", nil),
				&opensplunk.SetIngestionTokenEnabledRequest{
					IngestionTokenId: "private-id",
					ExpectedVersion:  1,
				},
			)
			assertHTTPErrorStatus(t, err, test.status)
			if response != nil {
				t.Fatalf("failure response = %#v, want nil", response)
			}
			if test.status == http.StatusServiceUnavailable &&
				err.Error() != "503: ingestion token service is unavailable" {
				t.Fatalf("failure error = %q, want generic unavailable response", err)
			}
			entries := observed.All()
			if len(entries) != test.logs {
				t.Fatalf("diagnostic entries = %d, want %d", len(entries), test.logs)
			}
			if test.logs == 0 {
				return
			}
			fields := entries[0].ContextMap()
			if entries[0].Message != "ingestion token state update unavailable" ||
				len(fields) != 3 ||
				fields["mutation_phase"] != test.phase ||
				fields["database_error_code"] != int64(19) ||
				fields["database_contention"] != false {
				t.Fatalf("unexpected diagnostic (must contain only safe classification): %#v", entries[0])
			}
			formatted := fmt.Sprintf("%#v", entries[0])
			for _, sensitive := range []string{
				"private-id",
				"secret-prefix",
				"private token or SQL parameter",
				"private rollback token",
			} {
				if strings.Contains(formatted, sensitive) {
					t.Fatalf("diagnostic contains private value %q: %s", sensitive, formatted)
				}
			}
		})
	}
}
