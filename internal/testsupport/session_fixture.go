package testsupport

// SessionAuthenticator is a test fixture for an authenticated user. Production
// composition uses identity.Runtime and never imports this helper.
import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/authorization"
)

type SessionAuthenticator struct {
	subject    authorization.Subject
	credential []byte
}

func NewSessionAuthenticator(userID uint64, userType int32, credential []byte) (*SessionAuthenticator, error) {
	subject := authorization.Subject{UserID: userID, UserType: userType}
	if !subject.Valid() {
		return nil, authorization.ErrUnauthorized
	}
	return &SessionAuthenticator{subject: subject, credential: append([]byte(nil), credential...)}, nil
}
func (s *SessionAuthenticator) AuthenticateRequest(request *http.Request) (*http.Request, authorization.Subject, error) {
	if s == nil || request == nil {
		return request, authorization.Subject{}, authorization.ErrUnauthorized
	}
	if err := request.Context().Err(); err != nil {
		return request, authorization.Subject{}, err
	}
	if len(s.credential) > 0 {
		var candidate string
		if cookie, err := request.Cookie("session_token"); err == nil {
			candidate = cookie.Value
		} else {
			candidate = strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
		}
		if subtle.ConstantTimeCompare([]byte(candidate), s.credential) != 1 {
			return request, authorization.Subject{}, authorization.ErrUnauthorized
		}
	}
	return request, s.subject, nil
}

type fixturePolicies struct{ role int32 }

func (fixturePolicies) OrganizationForIndex(context.Context, string) (int64, error) { return 1, nil }
func (fixturePolicies) Rules(context.Context, string) ([]authorization.Rule, error) { return nil, nil }
func (fixturePolicies) SetOrganization(context.Context, string, int64, int64) error { return nil }
func (fixturePolicies) SetRule(context.Context, authorization.Rule, int64) error    { return nil }
func (fixturePolicies) DeleteRule(context.Context, authorization.Rule, int64) error { return nil }
func (policies fixturePolicies) EffectiveRole(context.Context, uint64, int64) (int32, bool, error) {
	role := policies.role
	if role == 0 {
		role = authorization.ViewerRole
	}
	return role, true, nil
}

func AuthorizationFixtureRole(role int32, physicalTenant string) *authorization.Service {
	policies := fixturePolicies{role: role}
	service, err := authorization.NewService(policies, policies, physicalTenant)
	if err != nil {
		panic(err)
	}
	return service
}
func AuthorizationFixture(physicalTenant ...string) *authorization.Service {
	tenant := "default"
	if len(physicalTenant) > 0 && physicalTenant[0] != "" {
		tenant = physicalTenant[0]
	}
	service, _ := authorization.NewService(fixturePolicies{}, fixturePolicies{}, tenant)
	return service
}
