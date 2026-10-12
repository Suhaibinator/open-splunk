// Package testmocks provides mock implementations for testing purposes.
// This package centralizes mock implementations to avoid duplication across test files.
package testmocks

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/stretchr/testify/mock"
	"gorm.io/gorm"

	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// MockPersistenceHandler provides a unified mock implementation of CommonPersistenceInterface.
// It embeds testify's mock.Mock for expectation-based testing while providing sensible defaults.
//
// Usage:
//   - For stub behavior (default returns): just create the mock and use it directly
//   - For expectations: use mock.On("MethodName", args...).Return(results...)
//
// Example with expectations:
//
//	mockPersistence := &testmocks.MockPersistenceHandler{}
//	mockPersistence.On("GetUserByID", ctx, uint64(1)).Return(testUser, nil)
//	// ... use mockPersistence in your test
//	mockPersistence.AssertExpectations(t)
//
// Example with default stub behavior:
//
//	mockPersistence := &testmocks.MockPersistenceHandler{}
//	// GetUserByID will return (nil, ErrUserNotFound) by default
type MockPersistenceHandler struct {
	mock.Mock

	// Optional hooks for special behavior (used by some tests)
	UpdateOrganizationHook func(ctx context.Context, organization *common_persistence_models.Organization) error
	DeleteOrganizationHook func(ctx context.Context, id int64) error

	// PublicStorageDisabled makes PublicStorageEnabled report false. The zero
	// value keeps public object storage enabled so tests exercise the public
	// bucket paths unless they opt out.
	PublicStorageDisabled bool
}

// NewMockPersistenceHandler creates a new mock persistence handler.
func NewMockPersistenceHandler() *MockPersistenceHandler {
	return &MockPersistenceHandler{}
}

// Compile-time check to ensure MockPersistenceHandler implements CommonPersistenceInterface.
var _ common_persistence.CommonPersistenceInterface = (*MockPersistenceHandler)(nil)
var _ common_persistence.PersonalWorkspacePersistence = (*MockPersistenceHandler)(nil)

// hasExpectation checks if a method has an expectation set via On().
func (m *MockPersistenceHandler) hasExpectation(methodName string) bool {
	for _, call := range m.ExpectedCalls {
		if call.Method == methodName {
			return true
		}
	}
	return false
}

// ============================================================================
// METRICS & TRANSACTION MANAGEMENT
// ============================================================================

func (m *MockPersistenceHandler) GetMetricsHandler() *common_metrics.CommonMetricsHandler {
	return nil
}

// BeginTransaction requires an explicit expectation to return a usable transaction.
// An uninitialized gorm.DB cannot execute queries or commit and must never be
// returned as a successful default.
func (m *MockPersistenceHandler) BeginTransaction(ctx context.Context) (*gorm.DB, error) {
	if m.hasExpectation("BeginTransaction") {
		args := m.Called(ctx)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*gorm.DB), args.Error(1)
	}
	return nil, errors.New("mock BeginTransaction requires an explicit expectation")
}

// RunInTransaction requires an explicit expectation. Tests can use the expectation's
// Run callback to invoke fn; the mock does not provide a database transaction.
func (m *MockPersistenceHandler) RunInTransaction(ctx context.Context, fn func(context.Context) error) error {
	if m.hasExpectation("RunInTransaction") {
		return m.Called(ctx, fn).Error(0)
	}
	return errors.New("mock RunInTransaction requires an explicit expectation")
}

// ============================================================================
// USER MANAGEMENT METHODS
// ============================================================================

func (m *MockPersistenceHandler) CreateUser(ctx context.Context, user *common_persistence_models.User) error {
	if m.hasExpectation("CreateUser") {
		args := m.Called(ctx, user)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetUserByID(ctx context.Context, id uint64) (*common_persistence_models.User, error) {
	if m.hasExpectation("GetUserByID") {
		args := m.Called(ctx, id)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.User), args.Error(1)
	}
	return nil, common_persistence.ErrUserNotFound
}

func (m *MockPersistenceHandler) GetUserByEmail(ctx context.Context, email string) (*common_persistence_models.User, error) {
	if m.hasExpectation("GetUserByEmail") {
		args := m.Called(ctx, email)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.User), args.Error(1)
	}
	return nil, common_persistence.ErrUserNotFound
}

func (m *MockPersistenceHandler) GetUserByUsername(ctx context.Context, username string) (*common_persistence_models.User, error) {
	if m.hasExpectation("GetUserByUsername") {
		args := m.Called(ctx, username)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.User), args.Error(1)
	}
	return nil, common_persistence.ErrUserNotFound
}

func (m *MockPersistenceHandler) ListChatMessageEditHistory(
	ctx context.Context,
	messageID uint64,
	pagination *proto_models.PaginationRequest,
) ([]*common_persistence_models.ChatMessageEditHistory, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListChatMessageEditHistory") {
		args := m.Called(ctx, messageID, pagination)
		if args.Get(0) == nil {
			return nil, nil, args.Error(2)
		}
		return args.Get(0).([]*common_persistence_models.ChatMessageEditHistory), args.Get(1).(*proto_models.PaginationResponse), args.Error(2)
	}
	return []*common_persistence_models.ChatMessageEditHistory{}, &proto_models.PaginationResponse{}, nil
}

func (m *MockPersistenceHandler) SearchPublicUsers(
	ctx context.Context,
	query string,
	scope proto_models.UserSearchScope,
	organizationID *uint64,
	excludeUserID *uint64,
	allowEmail bool,
	allowUsername bool,
	allowRealName bool,
	pagination *proto_models.PaginationRequest,
	suggestionsOnly bool,
) ([]*common_persistence_models.User, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("SearchPublicUsers") {
		args := m.Called(ctx, query, scope, organizationID, excludeUserID, allowEmail, allowUsername, allowRealName, pagination, suggestionsOnly)
		if args.Get(0) == nil {
			return nil, nil, args.Error(2)
		}
		return args.Get(0).([]*common_persistence_models.User), args.Get(1).(*proto_models.PaginationResponse), args.Error(2)
	}
	return []*common_persistence_models.User{}, &proto_models.PaginationResponse{}, nil
}

func (m *MockPersistenceHandler) CheckUsernameExists(ctx context.Context, username string) (bool, error) {
	if m.hasExpectation("CheckUsernameExists") {
		args := m.Called(ctx, username)
		return args.Bool(0), args.Error(1)
	}
	return false, nil
}

func (m *MockPersistenceHandler) UpdateUser(ctx context.Context, user *common_persistence_models.User) error {
	if m.hasExpectation("UpdateUser") {
		args := m.Called(ctx, user)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateUsernameWithAudit(
	ctx context.Context,
	targetUserID uint64,
	newUsername string,
	actorUserID *uint64,
	reason string,
) error {
	if m.hasExpectation("UpdateUsernameWithAudit") {
		args := m.Called(ctx, targetUserID, newUsername, actorUserID, reason)
		return args.Error(0)
	}
	return nil
}

// SetUserCacheInvalidator records the call via testify only when an expectation
// was set; otherwise it is a no-op.
func (m *MockPersistenceHandler) SetUserCacheInvalidator(invalidator func(userID uint64) error) {
	if m.hasExpectation("SetUserCacheInvalidator") {
		m.Called(invalidator)
	}
}

// ============================================================================
// SESSION TOKEN MANAGEMENT
// ============================================================================

func (m *MockPersistenceHandler) CreateSessionToken(ctx context.Context, token *common_persistence_models.SessionToken) error {
	if m.hasExpectation("CreateSessionToken") {
		args := m.Called(ctx, token)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetValidSessionToken(ctx context.Context, token string) (*common_persistence_models.SessionToken, error) {
	if m.hasExpectation("GetValidSessionToken") {
		args := m.Called(ctx, token)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.SessionToken), args.Error(1)
	}
	return nil, common_persistence.ErrSessionTokenNotFound
}

func (m *MockPersistenceHandler) RevokeSessionTokenAt(ctx context.Context, token string, revokeAt time.Time) error {
	if m.hasExpectation("RevokeSessionTokenAt") {
		args := m.Called(ctx, token, revokeAt)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ConsumeSecurityToken(ctx context.Context, token string, tokenType proto_models.TokenType) (*common_persistence_models.SessionToken, error) {
	if m.hasExpectation("ConsumeSecurityToken") {
		args := m.Called(ctx, token, tokenType)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.SessionToken), args.Error(1)
	}
	return nil, common_persistence.ErrSessionTokenNotFound
}

func (m *MockPersistenceHandler) RevokeAllUserTokens(ctx context.Context, userID uint64, tokenType proto_models.TokenType) error {
	if m.hasExpectation("RevokeAllUserTokens") {
		args := m.Called(ctx, userID, tokenType)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) DeleteExpiredSessionTokens(ctx context.Context) error {
	if m.hasExpectation("DeleteExpiredSessionTokens") {
		args := m.Called(ctx)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) RevokeSessionToken(ctx context.Context, token string) error {
	if m.hasExpectation("RevokeSessionToken") {
		args := m.Called(ctx, token)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) RevokeAllSessionTokensForUser(ctx context.Context, userID uint64) error {
	if m.hasExpectation("RevokeAllSessionTokensForUser") {
		args := m.Called(ctx, userID)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) RevokeAllSessionTokensForUserAndReturnTokens(ctx context.Context, userID uint64) ([]string, error) {
	if m.hasExpectation("RevokeAllSessionTokensForUserAndReturnTokens") {
		args := m.Called(ctx, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]string), args.Error(1)
	}
	return []string{}, nil
}

func (m *MockPersistenceHandler) GetActiveTokensByUserID(ctx context.Context, userID uint64) ([]*common_persistence_models.SessionToken, error) {
	if m.hasExpectation("GetActiveTokensByUserID") {
		args := m.Called(ctx, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.SessionToken), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetActiveSessionTokensByUserID(ctx context.Context, userID uint64) ([]*common_persistence_models.SessionToken, error) {
	if m.hasExpectation("GetActiveSessionTokensByUserID") {
		args := m.Called(ctx, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.SessionToken), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ListActiveSessionTokensByUserID(ctx context.Context, userID uint64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.SessionToken, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListActiveSessionTokensByUserID") {
		args := m.Called(ctx, userID, pagination)
		var tokens []*common_persistence_models.SessionToken
		if args.Get(0) != nil {
			tokens = args.Get(0).([]*common_persistence_models.SessionToken)
		}
		var paginationResponse *proto_models.PaginationResponse
		if args.Get(1) != nil {
			paginationResponse = args.Get(1).(*proto_models.PaginationResponse)
		}
		return tokens, paginationResponse, args.Error(2)
	}
	return nil, nil, nil
}

func (m *MockPersistenceHandler) RevokeSessionTokenByIDForUser(ctx context.Context, userID uint64, sessionID uint64) (string, error) {
	if m.hasExpectation("RevokeSessionTokenByIDForUser") {
		args := m.Called(ctx, userID, sessionID)
		return args.String(0), args.Error(1)
	}
	return "", nil
}

func (m *MockPersistenceHandler) RevokeOtherSessionTokensForUser(ctx context.Context, userID uint64, currentToken string) ([]string, error) {
	if m.hasExpectation("RevokeOtherSessionTokensForUser") {
		args := m.Called(ctx, userID, currentToken)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]string), args.Error(1)
	}
	return []string{}, nil
}

// ============================================================================
// EMAIL VERIFICATION MANAGEMENT
// ============================================================================

func (m *MockPersistenceHandler) CreateEmailVerification(ctx context.Context, ev *common_persistence_models.EmailVerification) error {
	if m.hasExpectation("CreateEmailVerification") {
		args := m.Called(ctx, ev)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetEmailVerificationByUserID(ctx context.Context, userID uint64) (*common_persistence_models.EmailVerification, error) {
	if m.hasExpectation("GetEmailVerificationByUserID") {
		args := m.Called(ctx, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.EmailVerification), args.Error(1)
	}
	return nil, common_persistence.ErrEmailVerificationNotFound
}

func (m *MockPersistenceHandler) MarkEmailVerified(ctx context.Context, userID uint64) (string, error) {
	if m.hasExpectation("MarkEmailVerified") {
		args := m.Called(ctx, userID)
		return args.String(0), args.Error(1)
	}
	return "", nil
}

func (m *MockPersistenceHandler) DiscardUnverifiedSignupCredentials(ctx context.Context, userID uint64) (common_persistence.DiscardedSignupCredentials, error) {
	if m.hasExpectation("DiscardUnverifiedSignupCredentials") {
		args := m.Called(ctx, userID)
		return args.Get(0).(common_persistence.DiscardedSignupCredentials), args.Error(1)
	}
	return common_persistence.DiscardedSignupCredentials{}, nil
}

func (m *MockPersistenceHandler) ListUnverifiedUserIDs(ctx context.Context, cutoff time.Time, limit int) ([]uint64, error) {
	if m.hasExpectation("ListUnverifiedUserIDs") {
		args := m.Called(ctx, cutoff, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]uint64), args.Error(1)
	}
	return []uint64{}, nil
}

// ============================================================================
// PHONE VERIFICATION MANAGEMENT
// ============================================================================

func (m *MockPersistenceHandler) CreatePhoneVerification(ctx context.Context, pv *common_persistence_models.PhoneVerification) error {
	if m.hasExpectation("CreatePhoneVerification") {
		args := m.Called(ctx, pv)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetPhoneVerificationByUserID(ctx context.Context, userID uint64) (*common_persistence_models.PhoneVerification, error) {
	if m.hasExpectation("GetPhoneVerificationByUserID") {
		args := m.Called(ctx, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.PhoneVerification), args.Error(1)
	}
	return nil, common_persistence.ErrPhoneVerificationNotFound
}

func (m *MockPersistenceHandler) UpdatePhoneVerification(ctx context.Context, pv *common_persistence_models.PhoneVerification) error {
	if m.hasExpectation("UpdatePhoneVerification") {
		args := m.Called(ctx, pv)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) DeletePhoneVerification(ctx context.Context, id uint64) error {
	if m.hasExpectation("DeletePhoneVerification") {
		args := m.Called(ctx, id)
		return args.Error(0)
	}
	return nil
}

// ============================================================================
// MULTI-AUTH SUPPORT
// ============================================================================

func (m *MockPersistenceHandler) GetUserAuthMethods(ctx context.Context, userID uint64) (*common_persistence_models.UserAuthMethods, error) {
	if m.hasExpectation("GetUserAuthMethods") {
		args := m.Called(ctx, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.UserAuthMethods), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) CreateUserPassword(ctx context.Context, userID uint64, passwordHash, passwordSalt string) error {
	if m.hasExpectation("CreateUserPassword") {
		args := m.Called(ctx, userID, passwordHash, passwordSalt)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetUserPassword(ctx context.Context, userID uint64) (*common_persistence_models.UserPassword, error) {
	if m.hasExpectation("GetUserPassword") {
		args := m.Called(ctx, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.UserPassword), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) UpdateUserPassword(ctx context.Context, userID uint64, passwordHash, passwordSalt string) error {
	if m.hasExpectation("UpdateUserPassword") {
		args := m.Called(ctx, userID, passwordHash, passwordSalt)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateUserOTPSecret(ctx context.Context, userID uint64, otpSecret string) error {
	if m.hasExpectation("UpdateUserOTPSecret") {
		args := m.Called(ctx, userID, otpSecret)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) EnableUserOTP(ctx context.Context, userID uint64, verifiedAt *time.Time) error {
	if m.hasExpectation("EnableUserOTP") {
		args := m.Called(ctx, userID, verifiedAt)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) DisableUserOTP(ctx context.Context, userID uint64) error {
	if m.hasExpectation("DisableUserOTP") {
		args := m.Called(ctx, userID)
		return args.Error(0)
	}
	return nil
}

// ============================================================================
// OAUTH IDENTITIES
// ============================================================================

func (m *MockPersistenceHandler) FindOAuthIdentity(ctx context.Context, providerSlug, subject string) (*common_persistence_models.OAuthLogin, error) {
	if m.hasExpectation("FindOAuthIdentity") {
		args := m.Called(ctx, providerSlug, subject)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.OAuthLogin), args.Error(1)
	}
	return nil, common_persistence.ErrOAuthIdentityNotFound
}

func (m *MockPersistenceHandler) FindUserOAuthIdentity(ctx context.Context, userID uint64, providerSlug string) (*common_persistence_models.OAuthLogin, error) {
	if m.hasExpectation("FindUserOAuthIdentity") {
		args := m.Called(ctx, userID, providerSlug)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.OAuthLogin), args.Error(1)
	}
	return nil, common_persistence.ErrOAuthIdentityNotFound
}

func (m *MockPersistenceHandler) LinkOAuthIdentity(ctx context.Context, userID uint64, providerSlug, subject, email string) error {
	if m.hasExpectation("LinkOAuthIdentity") {
		args := m.Called(ctx, userID, providerSlug, subject, email)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) TouchOAuthIdentity(ctx context.Context, identityID uint64, subject, email string) error {
	if m.hasExpectation("TouchOAuthIdentity") {
		args := m.Called(ctx, identityID, subject, email)
		return args.Error(0)
	}
	return nil
}

// ============================================================================
// USER SETTINGS MANAGEMENT
// ============================================================================

func (m *MockPersistenceHandler) GetUserSettingsByUserID(ctx context.Context, userID uint64) (*common_persistence_models.UserSettings, error) {
	if m.hasExpectation("GetUserSettingsByUserID") {
		args := m.Called(ctx, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.UserSettings), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) UpsertUserSettings(ctx context.Context, settings *common_persistence_models.UserSettings, columns []string) error {
	if m.hasExpectation("UpsertUserSettings") {
		args := m.Called(ctx, settings, columns)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CreateUserSettings(ctx context.Context, settings *common_persistence_models.UserSettings) error {
	if m.hasExpectation("CreateUserSettings") {
		args := m.Called(ctx, settings)
		return args.Error(0)
	}
	return nil
}

// ============================================================================
// ATTACHMENT MANAGEMENT
// ============================================================================

func (m *MockPersistenceHandler) CreateAttachment(ctx context.Context, attachment *common_persistence_models.Attachment) error {
	if m.hasExpectation("CreateAttachment") {
		args := m.Called(ctx, attachment)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CreateInternalAttachment(ctx context.Context, attachment *common_persistence_models.Attachment) error {
	if m.hasExpectation("CreateInternalAttachment") {
		args := m.Called(ctx, attachment)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetAttachmentByID(ctx context.Context, id uint64) (*common_persistence_models.Attachment, error) {
	if m.hasExpectation("GetAttachmentByID") {
		args := m.Called(ctx, id)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) UpdateAttachment(ctx context.Context, attachment *common_persistence_models.Attachment) error {
	if m.hasExpectation("UpdateAttachment") {
		args := m.Called(ctx, attachment)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateAttachmentIfStatus(ctx context.Context, attachment *common_persistence_models.Attachment, expectedStatus common_persistence_models.UploadStatus) (bool, error) {
	if m.hasExpectation("UpdateAttachmentIfStatus") {
		args := m.Called(ctx, attachment, expectedStatus)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) ListSettledTemporaryObjects(ctx context.Context, now time.Time, limit int) ([]*common_persistence_models.Attachment, error) {
	if m.hasExpectation("ListSettledTemporaryObjects") {
		args := m.Called(ctx, now, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ClearTemporaryObjectName(ctx context.Context, attachmentID uint64, objectName string) error {
	if m.hasExpectation("ClearTemporaryObjectName") {
		return m.Called(ctx, attachmentID, objectName).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ClaimExpiredPublicAttachments(ctx context.Context, cutoffTime, processingEndTime time.Time) ([]*common_persistence_models.Attachment, error) {
	if m.hasExpectation("ClaimExpiredPublicAttachments") {
		args := m.Called(ctx, cutoffTime, processingEndTime)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) RenewPendingUploads(ctx context.Context, attachmentIDs []uint64, expiresAt time.Time) ([]*common_persistence_models.Attachment, error) {
	if m.hasExpectation("RenewPendingUploads") {
		args := m.Called(ctx, attachmentIDs, expiresAt)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) RenewPendingUploadsInScope(ctx context.Context, attachmentIDs []uint64, userID uint64, objectType, entityID string, expiresAt time.Time) ([]*common_persistence_models.Attachment, error) {
	if m.hasExpectation("RenewPendingUploadsInScope") {
		args := m.Called(ctx, attachmentIDs, userID, objectType, entityID, expiresAt)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) UpdateExpiredPendingUpload(ctx context.Context, attachment *common_persistence_models.Attachment, cutoffTime time.Time) (bool, error) {
	if m.hasExpectation("UpdateExpiredPendingUpload") {
		args := m.Called(ctx, attachment, cutoffTime)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) UpdateAttachmentObjectTypes(ctx context.Context, attachmentIDs []uint64, objectType string) error {
	if m.hasExpectation("UpdateAttachmentObjectTypes") {
		args := m.Called(ctx, attachmentIDs, objectType)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetAttachmentsByStatus(ctx context.Context, status common_persistence_models.UploadStatus, limit int) ([]*common_persistence_models.Attachment, error) {
	if m.hasExpectation("GetAttachmentsByStatus") {
		args := m.Called(ctx, status, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) DeleteAttachmentsByIDs(ctx context.Context, ids []uint64) error {
	if m.hasExpectation("DeleteAttachmentsByIDs") {
		args := m.Called(ctx, ids)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) FindAttachmentByHash(ctx context.Context, fileHash string, visibility proto_models.AttachmentVisibility) (*common_persistence_models.Attachment, error) {
	if m.hasExpectation("FindAttachmentByHash") {
		args := m.Called(ctx, fileHash, visibility)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetInternalAttachmentByEntity(ctx context.Context, objectType string, entityID string) (*common_persistence_models.Attachment, error) {
	if m.hasExpectation("GetInternalAttachmentByEntity") {
		args := m.Called(ctx, objectType, entityID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) PublishInternalAttachmentObject(ctx context.Context, attachmentID uint64, objectName string, data []byte, contentType string) (*common_persistence_models.Attachment, error) {
	if m.hasExpectation("PublishInternalAttachmentObject") {
		args := m.Called(ctx, attachmentID, objectName, data, contentType)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) PublishPublicAttachmentObject(ctx context.Context, attachment *common_persistence_models.Attachment, source common_persistence.ObjectPrecondition) error {
	if m.hasExpectation("PublishPublicAttachmentObject") {
		return m.Called(ctx, attachment, source).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ListInternalAttachmentReclaimCandidates(ctx context.Context, before time.Time, limit int) ([]*common_persistence_models.Attachment, error) {
	if m.hasExpectation("ListInternalAttachmentReclaimCandidates") {
		args := m.Called(ctx, before, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) TombstoneInternalAttachmentIfUnreferenced(ctx context.Context, id uint64, inactiveBefore time.Time) (bool, error) {
	if m.hasExpectation("TombstoneInternalAttachmentIfUnreferenced") {
		args := m.Called(ctx, id, inactiveBefore)
		return args.Bool(0), args.Error(1)
	}
	return false, nil
}

func (m *MockPersistenceHandler) GetAttachmentsByIDs(ctx context.Context, attachmentIDs []uint64) ([]*common_persistence_models.Attachment, error) {
	if m.hasExpectation("GetAttachmentsByIDs") {
		args := m.Called(ctx, attachmentIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetAttachmentsByIDsAndUser(ctx context.Context, attachmentIDs []uint64, userID uint64) ([]*common_persistence_models.Attachment, error) {
	if m.hasExpectation("GetAttachmentsByIDsAndUser") {
		args := m.Called(ctx, attachmentIDs, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetChatMessageReferencedAttachmentIDs(ctx context.Context, ids []uint64) (map[uint64]bool, error) {
	return m.referencedAttachmentIDs("GetChatMessageReferencedAttachmentIDs", ctx, ids)
}

func (m *MockPersistenceHandler) GetChatHistoryReferencedAttachmentIDs(ctx context.Context, ids []uint64) (map[uint64]bool, error) {
	return m.referencedAttachmentIDs("GetChatHistoryReferencedAttachmentIDs", ctx, ids)
}

func (m *MockPersistenceHandler) referencedAttachmentIDs(method string, ctx context.Context, ids []uint64) (map[uint64]bool, error) {
	if m.hasExpectation(method) {
		args := m.MethodCalled(method, ctx, ids)
		referenced, _ := args.Get(0).(map[uint64]bool)
		return referenced, args.Error(1)
	}
	return map[uint64]bool{}, nil
}

func (m *MockPersistenceHandler) CreateAttachments(ctx context.Context, attachments []*common_persistence_models.Attachment) error {
	if m.hasExpectation("CreateAttachments") {
		args := m.Called(ctx, attachments)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateAttachmentEntityIDs(ctx context.Context, attachmentIDs []uint64, entityID string) error {
	if m.hasExpectation("UpdateAttachmentEntityIDs") {
		args := m.Called(ctx, attachmentIDs, entityID)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateUserProfilePicture(ctx context.Context, userID uint64, attachmentID *uint64) error {
	if m.hasExpectation("UpdateUserProfilePicture") {
		args := m.Called(ctx, userID, attachmentID)
		return args.Error(0)
	}
	return nil
}

// ============================================================================
// MINIO OBJECT STORAGE OPERATIONS
// ============================================================================

func (m *MockPersistenceHandler) GetPresignedURL(ctx context.Context, objectName string, expiry time.Duration) (string, error) {
	if m.hasExpectation("GetPresignedURL") {
		args := m.Called(ctx, objectName, expiry)
		return args.String(0), args.Error(1)
	}
	return "", nil
}

// PresignHostScopedDownloadURL returns a recognizable fake presigned URL by default
// so lifecycle tests can assert that the host-scoped presign path was dispatched
// (host, object name, and signing time all surface in the URL) without recomputing
// real SigV4 HMACs. Set an expectation to control the return value.
func (m *MockPersistenceHandler) PresignHostScopedDownloadURL(ctx context.Context, objectName string, publicOrigin string, region string, expiry time.Duration, signingTime time.Time, accessKey string, secretKey config.Secret) (string, error) {
	if m.hasExpectation("PresignHostScopedDownloadURL") {
		args := m.Called(ctx, objectName, publicOrigin, region, expiry, signingTime, accessKey, secretKey)
		return args.String(0), args.Error(1)
	}
	scheme := "https"
	host := publicOrigin
	if idx := strings.Index(host, "://"); idx >= 0 {
		scheme = host[:idx]
		host = host[idx+len("://"):]
	}
	credentialSuffix := ""
	if accessKey != "" {
		credentialSuffix = "&X-Amz-Credential=" + accessKey
	}
	return scheme + "://" + host + "/mock-bucket/" + objectName +
		"?presigned=1&X-Amz-Date=" + signingTime.UTC().Format("20060102T150405Z") + credentialSuffix, nil
}

func (m *MockPersistenceHandler) GeneratePresignedPostURL(ctx context.Context, objectName string, expiresAt time.Time, minSize, maxSize int64, contentType string) (url string, formData map[string]string, err error) {
	if m.hasExpectation("GeneratePresignedPostURL") {
		args := m.Called(ctx, objectName, expiresAt, minSize, maxSize, contentType)
		if args.Get(1) == nil {
			return args.String(0), nil, args.Error(2)
		}
		return args.String(0), args.Get(1).(map[string]string), args.Error(2)
	}
	return "", nil, nil
}

func (m *MockPersistenceHandler) GetObjectReader(ctx context.Context, objectName string, precondition common_persistence.ObjectPrecondition) (io.ReadCloser, error) {
	if m.hasExpectation("GetObjectReader") {
		args := m.Called(ctx, objectName, precondition)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(io.ReadCloser), args.Error(1)
	}
	// Every upload now crosses the mandatory MIME boundary, including tests of
	// unrelated lifecycle transitions. Supply a seekable benign default;
	// security/content tests must still configure their exact bytes explicitly.
	return mockObjectReadSeeker{strings.NewReader("hello world")}, nil
}

type mockObjectReadSeeker struct{ *strings.Reader }

func (mockObjectReadSeeker) Close() error { return nil }

func (m *MockPersistenceHandler) PutObjectBytes(ctx context.Context, objectName string, data []byte, contentType string) error {
	if m.hasExpectation("PutObjectBytes") {
		args := m.Called(ctx, objectName, data, contentType)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) PutObjectStream(ctx context.Context, objectName string, reader io.Reader, size int64, opts common_persistence.PutObjectOptions) error {
	if m.hasExpectation("PutObjectStream") {
		args := m.Called(ctx, objectName, reader, size, opts)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CopyObject(ctx context.Context, sourceObject, destObject string, source common_persistence.ObjectPrecondition) error {
	if m.hasExpectation("CopyObject") {
		args := m.Called(ctx, sourceObject, destObject, source)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CopyObjectToPublicBucket(ctx context.Context, sourceObject, destObject string, source common_persistence.ObjectPrecondition) error {
	if m.hasExpectation("CopyObjectToPublicBucket") {
		args := m.Called(ctx, sourceObject, destObject, source)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) RemoveObject(ctx context.Context, objectName string) error {
	if m.hasExpectation("RemoveObject") {
		args := m.Called(ctx, objectName)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ObjectExists(ctx context.Context, objectName string) (bool, error) {
	if m.hasExpectation("ObjectExists") {
		args := m.Called(ctx, objectName)
		return args.Bool(0), args.Error(1)
	}
	return false, nil
}

// PublicStorageEnabled reports whether the mock pretends a public bucket is
// configured. It defaults to true and can be turned off with the
// PublicStorageDisabled field or overridden with an expectation.
func (m *MockPersistenceHandler) PublicStorageEnabled() bool {
	if m.hasExpectation("PublicStorageEnabled") {
		args := m.Called()
		return args.Bool(0)
	}
	return !m.PublicStorageDisabled
}

func (m *MockPersistenceHandler) ObjectExistsInPublicBucket(ctx context.Context, objectName string) (bool, error) {
	if m.hasExpectation("ObjectExistsInPublicBucket") {
		args := m.Called(ctx, objectName)
		return args.Bool(0), args.Error(1)
	}
	return false, nil
}

func (m *MockPersistenceHandler) GetObjectInfo(ctx context.Context, objectName string) (*common_persistence.MinioObjectInfo, error) {
	if m.hasExpectation("GetObjectInfo") {
		args := m.Called(ctx, objectName)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.MinioObjectInfo), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) CalculateDualHashesStream(ctx context.Context, objectName string, precondition common_persistence.ObjectPrecondition) (*common_persistence.DualHashes, error) {
	if m.hasExpectation("CalculateDualHashesStream") {
		args := m.Called(ctx, objectName, precondition)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.DualHashes), args.Error(1)
	}
	return nil, nil
}

// ============================================================================
// CONTACT MESSAGE MANAGEMENT
// ============================================================================

func (m *MockPersistenceHandler) CreateContactMessage(ctx context.Context, message *common_persistence_models.ContactMessage) error {
	if m.hasExpectation("CreateContactMessage") {
		args := m.Called(ctx, message)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ListCareerJobPostings(ctx context.Context, req *proto_models.ListCareerJobPostingsRequest) ([]*common_persistence_models.CareerJobPosting, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListCareerJobPostings") {
		args := m.Called(ctx, req)
		var jobs []*common_persistence_models.CareerJobPosting
		var paginationResp *proto_models.PaginationResponse
		if args.Get(0) != nil {
			jobs = args.Get(0).([]*common_persistence_models.CareerJobPosting)
		}
		if args.Get(1) != nil {
			paginationResp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return jobs, paginationResp, args.Error(2)
	}
	return []*common_persistence_models.CareerJobPosting{}, &proto_models.PaginationResponse{}, nil
}

func (m *MockPersistenceHandler) GetCareerJobPostingByID(ctx context.Context, id uint64) (*common_persistence_models.CareerJobPosting, error) {
	if m.hasExpectation("GetCareerJobPostingByID") {
		args := m.Called(ctx, id)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.CareerJobPosting), args.Error(1)
	}
	return nil, common_persistence.ErrCareerJobPostingNotFound
}

func (m *MockPersistenceHandler) GetCareerJobPostingBySlug(ctx context.Context, slug string) (*common_persistence_models.CareerJobPosting, error) {
	if m.hasExpectation("GetCareerJobPostingBySlug") {
		args := m.Called(ctx, slug)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.CareerJobPosting), args.Error(1)
	}
	return nil, common_persistence.ErrCareerJobPostingNotFound
}

func (m *MockPersistenceHandler) CreateCareerJobPosting(ctx context.Context, job *common_persistence_models.CareerJobPosting) error {
	if m.hasExpectation("CreateCareerJobPosting") {
		args := m.Called(ctx, job)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateCareerJobPosting(ctx context.Context, job *common_persistence_models.CareerJobPosting) error {
	if m.hasExpectation("UpdateCareerJobPosting") {
		args := m.Called(ctx, job)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CreateCareerApplication(ctx context.Context, application *common_persistence_models.CareerApplication) error {
	if m.hasExpectation("CreateCareerApplication") {
		args := m.Called(ctx, application)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetCareerApplicationByID(ctx context.Context, id uint64) (*common_persistence_models.CareerApplication, error) {
	if m.hasExpectation("GetCareerApplicationByID") {
		args := m.Called(ctx, id)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.CareerApplication), args.Error(1)
	}
	return nil, common_persistence.ErrCareerApplicationNotFound
}

func (m *MockPersistenceHandler) GetCareerApplicationByReferenceID(ctx context.Context, referenceID string) (*common_persistence_models.CareerApplication, error) {
	if m.hasExpectation("GetCareerApplicationByReferenceID") {
		args := m.Called(ctx, referenceID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.CareerApplication), args.Error(1)
	}
	return nil, common_persistence.ErrCareerApplicationNotFound
}

func (m *MockPersistenceHandler) ListCareerApplicationsByApplicantUserID(ctx context.Context, applicantUserID uint64, req *proto_models.ListMyCareerApplicationsRequest) ([]*common_persistence_models.CareerApplication, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListCareerApplicationsByApplicantUserID") {
		args := m.Called(ctx, applicantUserID, req)
		if args.Get(0) == nil {
			return nil, nil, args.Error(2)
		}
		return args.Get(0).([]*common_persistence_models.CareerApplication), args.Get(1).(*proto_models.PaginationResponse), args.Error(2)
	}
	return nil, nil, nil
}

func (m *MockPersistenceHandler) GetCareerApplicationByReferenceIDAndApplicantUserID(ctx context.Context, referenceID string, applicantUserID uint64) (*common_persistence_models.CareerApplication, error) {
	if m.hasExpectation("GetCareerApplicationByReferenceIDAndApplicantUserID") {
		args := m.Called(ctx, referenceID, applicantUserID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.CareerApplication), args.Error(1)
	}
	return nil, common_persistence.ErrCareerApplicationNotFound
}

func (m *MockPersistenceHandler) UpdateCareerApplicationStatus(ctx context.Context, applicationID uint64, status proto_models.CareerApplicationStatus, adminNotes string) error {
	if m.hasExpectation("UpdateCareerApplicationStatus") {
		args := m.Called(ctx, applicationID, status, adminNotes)
		return args.Error(0)
	}
	return nil
}

// ============================================================================
// LIFECYCLE MANAGEMENT
// ============================================================================

func (m *MockPersistenceHandler) Close() error {
	if m.hasExpectation("Close") {
		args := m.Called()
		return args.Error(0)
	}
	return nil
}

// ============================================================================
// ADMIN MANAGEMENT METHODS
// ============================================================================

func (m *MockPersistenceHandler) AdminListUsers(ctx context.Context, req *proto_models.AdminListUsersRequest) ([]*common_service_models.AdminUserDetails, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("AdminListUsers") {
		args := m.Called(ctx, req)
		var users []*common_service_models.AdminUserDetails
		var pagination *proto_models.PaginationResponse
		if args.Get(0) != nil {
			users = args.Get(0).([]*common_service_models.AdminUserDetails)
		}
		if args.Get(1) != nil {
			pagination = args.Get(1).(*proto_models.PaginationResponse)
		}
		return users, pagination, args.Error(2)
	}
	return nil, nil, nil
}

func (m *MockPersistenceHandler) AdminGetUser(ctx context.Context, userID uint64) (*common_service_models.AdminUserDetails, error) {
	if m.hasExpectation("AdminGetUser") {
		args := m.Called(ctx, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_service_models.AdminUserDetails), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AdminListUsernameChangeHistory(ctx context.Context, req *proto_models.AdminListUsernameChangeHistoryRequest) ([]*common_persistence_models.UsernameChangeHistory, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("AdminListUsernameChangeHistory") {
		args := m.Called(ctx, req)
		var entries []*common_persistence_models.UsernameChangeHistory
		var pagination *proto_models.PaginationResponse
		if args.Get(0) != nil {
			entries = args.Get(0).([]*common_persistence_models.UsernameChangeHistory)
		}
		if args.Get(1) != nil {
			pagination = args.Get(1).(*proto_models.PaginationResponse)
		}
		return entries, pagination, args.Error(2)
	}
	return nil, nil, nil
}

func (m *MockPersistenceHandler) AdminUpdateUserBanStatus(ctx context.Context, userID uint64, banned bool, reason string) error {
	if m.hasExpectation("AdminUpdateUserBanStatus") {
		args := m.Called(ctx, userID, banned, reason)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) AdminUpdateUserType(ctx context.Context, userID uint64, newUserType proto_models.UserType, reason string) error {
	if m.hasExpectation("AdminUpdateUserType") {
		args := m.Called(ctx, userID, newUserType, reason)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) AdminDeleteUser(ctx context.Context, userID uint64, deleteAttachments bool, reason string) error {
	if m.hasExpectation("AdminDeleteUser") {
		args := m.Called(ctx, userID, deleteAttachments, reason)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) DeleteUserIfUnverified(ctx context.Context, userID uint64, deleteAttachments bool, reason string) (bool, error) {
	if m.hasExpectation("DeleteUserIfUnverified") {
		args := m.Called(ctx, userID, deleteAttachments, reason)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) AdminListContactMessages(ctx context.Context, pagination *proto_models.PaginationRequest, statusFilter, emailFilter string, sort *proto_models.SortRequest, sortBy proto_models.AdminListContactMessagesSortBy) ([]*proto_models.AdminContactMessage, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("AdminListContactMessages") {
		args := m.Called(ctx, pagination, statusFilter, emailFilter, sort, sortBy)
		var messages []*proto_models.AdminContactMessage
		var paginationResp *proto_models.PaginationResponse
		if args.Get(0) != nil {
			messages = args.Get(0).([]*proto_models.AdminContactMessage)
		}
		if args.Get(1) != nil {
			paginationResp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return messages, paginationResp, args.Error(2)
	}
	return nil, nil, nil
}

func (m *MockPersistenceHandler) AdminGetContactMessageDetails(ctx context.Context, messageID uint64) (*proto_models.AdminContactMessage, error) {
	if m.hasExpectation("AdminGetContactMessageDetails") {
		args := m.Called(ctx, messageID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*proto_models.AdminContactMessage), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AdminGetContactMessageByReferenceID(ctx context.Context, referenceID string) (*proto_models.AdminContactMessage, error) {
	if m.hasExpectation("AdminGetContactMessageByReferenceID") {
		args := m.Called(ctx, referenceID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*proto_models.AdminContactMessage), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AdminUpdateContactMessageStatus(ctx context.Context, messageID uint64, status, adminNotes string) error {
	if m.hasExpectation("AdminUpdateContactMessageStatus") {
		args := m.Called(ctx, messageID, status, adminNotes)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) AdminDeleteContactMessage(ctx context.Context, messageID uint64, reason string) error {
	if m.hasExpectation("AdminDeleteContactMessage") {
		args := m.Called(ctx, messageID, reason)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) AdminListCareerJobPostings(ctx context.Context, req *proto_models.AdminListCareerJobPostingsRequest) ([]*proto_models.CareerJobPosting, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("AdminListCareerJobPostings") {
		args := m.Called(ctx, req)
		var jobs []*proto_models.CareerJobPosting
		var paginationResp *proto_models.PaginationResponse
		if args.Get(0) != nil {
			jobs = args.Get(0).([]*proto_models.CareerJobPosting)
		}
		if args.Get(1) != nil {
			paginationResp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return jobs, paginationResp, args.Error(2)
	}
	return []*proto_models.CareerJobPosting{}, &proto_models.PaginationResponse{}, nil
}

func (m *MockPersistenceHandler) AdminGetCareerJobPosting(ctx context.Context, jobPostingID uint64) (*proto_models.CareerJobPosting, error) {
	if m.hasExpectation("AdminGetCareerJobPosting") {
		args := m.Called(ctx, jobPostingID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*proto_models.CareerJobPosting), args.Error(1)
	}
	return nil, common_persistence.ErrCareerJobPostingNotFound
}

func (m *MockPersistenceHandler) AdminListCareerApplications(ctx context.Context, req *proto_models.AdminListCareerApplicationsRequest) ([]*common_persistence_models.CareerApplication, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("AdminListCareerApplications") {
		args := m.Called(ctx, req)
		var applications []*common_persistence_models.CareerApplication
		var paginationResp *proto_models.PaginationResponse
		if args.Get(0) != nil {
			applications = args.Get(0).([]*common_persistence_models.CareerApplication)
		}
		if args.Get(1) != nil {
			paginationResp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return applications, paginationResp, args.Error(2)
	}
	return []*common_persistence_models.CareerApplication{}, &proto_models.PaginationResponse{}, nil
}

func (m *MockPersistenceHandler) AdminListAttachments(ctx context.Context, pagination *proto_models.PaginationRequest, userIDFilter *uint64, statusFilter, contentTypeFilter string, minSizeBytes, maxSizeBytes uint64, sort *proto_models.SortRequest, sortBy proto_models.AdminListAttachmentsSortBy) ([]*common_persistence_models.Attachment, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("AdminListAttachments") {
		args := m.Called(ctx, pagination, userIDFilter, statusFilter, contentTypeFilter, minSizeBytes, maxSizeBytes, sort, sortBy)
		var attachments []*common_persistence_models.Attachment
		var paginationResp *proto_models.PaginationResponse
		if args.Get(0) != nil {
			attachments = args.Get(0).([]*common_persistence_models.Attachment)
		}
		if args.Get(1) != nil {
			paginationResp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return attachments, paginationResp, args.Error(2)
	}
	return nil, nil, nil
}

func (m *MockPersistenceHandler) AdminGetAttachmentDetails(ctx context.Context, attachmentID uint64) (*common_persistence_models.Attachment, error) {
	if m.hasExpectation("AdminGetAttachmentDetails") {
		args := m.Called(ctx, attachmentID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.Attachment), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AdminUpdateAttachmentStatus(ctx context.Context, attachmentID uint64, status, validationDetails string) error {
	if m.hasExpectation("AdminUpdateAttachmentStatus") {
		args := m.Called(ctx, attachmentID, status, validationDetails)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) AdminDeleteAttachment(ctx context.Context, attachmentID uint64, deleteFiles bool, reason string) error {
	if m.hasExpectation("AdminDeleteAttachment") {
		args := m.Called(ctx, attachmentID, deleteFiles, reason)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) AdminListChatChannels(ctx context.Context, req *proto_models.AdminListChatChannelsRequest) ([]*common_service_models.AdminChatChannelDetails, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("AdminListChatChannels") {
		args := m.Called(ctx, req)
		var channels []*common_service_models.AdminChatChannelDetails
		var paginationResp *proto_models.PaginationResponse
		if args.Get(0) != nil {
			channels = args.Get(0).([]*common_service_models.AdminChatChannelDetails)
		}
		if args.Get(1) != nil {
			paginationResp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return channels, paginationResp, args.Error(2)
	}
	return nil, nil, nil
}

func (m *MockPersistenceHandler) AdminGetChatChannel(ctx context.Context, channelID uint64, includeDeleted bool) (*common_service_models.AdminChatChannelDetails, error) {
	if m.hasExpectation("AdminGetChatChannel") {
		args := m.Called(ctx, channelID, includeDeleted)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_service_models.AdminChatChannelDetails), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AdminUpdateChatChannel(ctx context.Context, req *proto_models.AdminUpdateChatChannelRequest) (*common_service_models.AdminChatChannelDetails, error) {
	if m.hasExpectation("AdminUpdateChatChannel") {
		args := m.Called(ctx, req)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_service_models.AdminChatChannelDetails), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AdminDeleteChatChannel(ctx context.Context, channelID uint64, reason string) error {
	if m.hasExpectation("AdminDeleteChatChannel") {
		args := m.Called(ctx, channelID, reason)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) AdminListChatChannelParticipants(ctx context.Context, channelID uint64, pagination *proto_models.PaginationRequest, includeLeft bool) ([]*common_service_models.ChatChannelParticipant, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("AdminListChatChannelParticipants") {
		args := m.Called(ctx, channelID, pagination, includeLeft)
		var participants []*common_service_models.ChatChannelParticipant
		var paginationResp *proto_models.PaginationResponse
		if args.Get(0) != nil {
			participants = args.Get(0).([]*common_service_models.ChatChannelParticipant)
		}
		if args.Get(1) != nil {
			paginationResp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return participants, paginationResp, args.Error(2)
	}
	return nil, nil, nil
}

func (m *MockPersistenceHandler) AdminAddChatChannelParticipants(ctx context.Context, channelID uint64, userIDs []uint64) ([]*common_service_models.ChatChannelParticipant, error) {
	if m.hasExpectation("AdminAddChatChannelParticipants") {
		args := m.Called(ctx, channelID, userIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_service_models.ChatChannelParticipant), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AdminRemoveChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64) error {
	if m.hasExpectation("AdminRemoveChatChannelParticipant") {
		args := m.Called(ctx, channelID, userID)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) AdminUpdateChatChannelParticipantRole(ctx context.Context, channelID uint64, userID uint64, role proto_models.ChannelRole) (*common_service_models.ChatChannelParticipant, error) {
	if m.hasExpectation("AdminUpdateChatChannelParticipantRole") {
		args := m.Called(ctx, channelID, userID, role)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_service_models.ChatChannelParticipant), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AdminGetUserAnalytics(ctx context.Context, filter *proto_models.FilterRequest, groupBy string) (*proto_models.AdminUserAnalyticsResponse, error) {
	if m.hasExpectation("AdminGetUserAnalytics") {
		args := m.Called(ctx, filter, groupBy)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*proto_models.AdminUserAnalyticsResponse), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AdminGetStorageAnalytics(ctx context.Context, filter *proto_models.FilterRequest, groupBy string) (*proto_models.AdminStorageAnalyticsResponse, error) {
	if m.hasExpectation("AdminGetStorageAnalytics") {
		args := m.Called(ctx, filter, groupBy)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*proto_models.AdminStorageAnalyticsResponse), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AdminListTokens(ctx context.Context, req *proto_models.AdminListTokensRequest) ([]*common_service_models.AdminSessionToken, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("AdminListTokens") {
		args := m.Called(ctx, req)
		var tokens []*common_service_models.AdminSessionToken
		var paginationResp *proto_models.PaginationResponse
		if args.Get(0) != nil {
			tokens = args.Get(0).([]*common_service_models.AdminSessionToken)
		}
		if args.Get(1) != nil {
			paginationResp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return tokens, paginationResp, args.Error(2)
	}
	return nil, nil, nil
}

func (m *MockPersistenceHandler) AdminRevokeToken(ctx context.Context, tokenID, reason string) error {
	if m.hasExpectation("AdminRevokeToken") {
		args := m.Called(ctx, tokenID, reason)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CreateInferenceModel(ctx context.Context, model *common_persistence_models.InferenceModel) error {
	if m.hasExpectation("CreateInferenceModel") {
		args := m.Called(ctx, model)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateInferenceModel(ctx context.Context, model *common_persistence_models.InferenceModel) error {
	if m.hasExpectation("UpdateInferenceModel") {
		args := m.Called(ctx, model)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) DeleteInferenceModel(ctx context.Context, inferenceModelID uint64) error {
	if m.hasExpectation("DeleteInferenceModel") {
		args := m.Called(ctx, inferenceModelID)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ListInferenceModels(ctx context.Context) ([]*common_persistence_models.InferenceModel, error) {
	if m.hasExpectation("ListInferenceModels") {
		args := m.Called(ctx)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.InferenceModel), args.Error(1)
	}
	return []*common_persistence_models.InferenceModel{}, nil
}

func (m *MockPersistenceHandler) ListInferenceModelsPage(
	ctx context.Context,
	pagination *proto_models.PaginationRequest,
) ([]*common_persistence_models.InferenceModel, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListInferenceModelsPage") {
		args := m.Called(ctx, pagination)
		var models []*common_persistence_models.InferenceModel
		if args.Get(0) != nil {
			models = args.Get(0).([]*common_persistence_models.InferenceModel)
		}
		var response *proto_models.PaginationResponse
		if args.Get(1) != nil {
			response = args.Get(1).(*proto_models.PaginationResponse)
		}
		return models, response, args.Error(2)
	}
	return []*common_persistence_models.InferenceModel{}, common_util.CreatePaginationResponseWithoutTotalCount(
		pagination.GetLimit(),
		pagination.GetPage(),
		false,
	), nil
}

func (m *MockPersistenceHandler) GetInferenceModelByID(ctx context.Context, inferenceModelID uint64) (*common_persistence_models.InferenceModel, error) {
	if m.hasExpectation("GetInferenceModelByID") {
		args := m.Called(ctx, inferenceModelID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.InferenceModel), args.Error(1)
	}
	return nil, gorm.ErrRecordNotFound
}

func (m *MockPersistenceHandler) GetInferenceModelByProviderModelID(ctx context.Context, provider string, providerModelID string) (*common_persistence_models.InferenceModel, error) {
	if m.hasExpectation("GetInferenceModelByProviderModelID") {
		args := m.Called(ctx, provider, providerModelID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.InferenceModel), args.Error(1)
	}
	return nil, gorm.ErrRecordNotFound
}

// ResolveLedgerInferenceModelID mirrors the concrete handler's best-effort catalog-id resolver
// (common_persistence.LedgerInferenceModelResolver). With no expectation set it returns nil, so
// inference-ledger stamping is a no-op by default and only stamps when a test opts in.
func (m *MockPersistenceHandler) ResolveLedgerInferenceModelID(ctx context.Context, provider string, providerModelID string) *uint64 {
	if m.hasExpectation("ResolveLedgerInferenceModelID") {
		args := m.Called(ctx, provider, providerModelID)
		if args.Get(0) == nil {
			return nil
		}
		return args.Get(0).(*uint64)
	}
	return nil
}

func (m *MockPersistenceHandler) CountInferenceModels(ctx context.Context) (int64, error) {
	if m.hasExpectation("CountInferenceModels") {
		args := m.Called(ctx)
		return args.Get(0).(int64), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) SeedInferenceModelsIfEmpty(ctx context.Context, models []*common_persistence_models.InferenceModel) (bool, int64, error) {
	if m.hasExpectation("SeedInferenceModelsIfEmpty") {
		args := m.Called(ctx, models)
		return args.Bool(0), args.Get(1).(int64), args.Error(2)
	}
	return false, 0, nil
}

func (m *MockPersistenceHandler) AdminGetSystemStats(ctx context.Context) (*proto_models.AdminSystemStatsResponse, error) {
	if m.hasExpectation("AdminGetSystemStats") {
		args := m.Called(ctx)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*proto_models.AdminSystemStatsResponse), args.Error(1)
	}
	return nil, nil
}

// ============================================================================
// ORGANIZATION MANAGEMENT METHODS
// ============================================================================

func (m *MockPersistenceHandler) CreateOrganization(ctx context.Context, organization *common_persistence_models.Organization) error {
	if m.hasExpectation("CreateOrganization") {
		args := m.Called(ctx, organization)
		// Set a mock ID for created organization (common test pattern)
		if organization.ID == 0 {
			organization.ID = 1
		}
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CreatePersonalWorkspaceRoot(ctx context.Context, organization *common_persistence_models.Organization) (bool, error) {
	args := m.Called(ctx, organization)
	if args.Bool(0) && organization != nil && organization.ID == 0 {
		organization.ID = 1
	}
	return args.Bool(0), args.Error(1)
}

func (m *MockPersistenceHandler) LockPersonalWorkspaceOwner(ctx context.Context, ownerUserID uint64, options common_persistence.PersonalWorkspaceOwnerLockOptions) error {
	return m.Called(ctx, ownerUserID, options).Error(0)
}

func (m *MockPersistenceHandler) GetPersonalWorkspaceRootByOwner(ctx context.Context, ownerUserID uint64, options common_persistence.PersonalWorkspaceReadOptions) (*common_persistence_models.Organization, error) {
	args := m.Called(ctx, ownerUserID, options)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common_persistence_models.Organization), args.Error(1)
}

func (m *MockPersistenceHandler) HasPersonalWorkspaceOrganizationsByOwner(ctx context.Context, ownerUserID uint64) (bool, error) {
	args := m.Called(ctx, ownerUserID)
	return args.Bool(0), args.Error(1)
}

func (m *MockPersistenceHandler) GetPersonalWorkspaceTree(ctx context.Context, rootOrganizationID int64, ownerUserID uint64, options common_persistence.PersonalWorkspaceReadOptions) ([]*common_persistence_models.Organization, error) {
	args := m.Called(ctx, rootOrganizationID, ownerUserID, options)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common_persistence_models.Organization), args.Error(1)
}

func (m *MockPersistenceHandler) SetMissingPersonalWorkspaceTreeOwner(ctx context.Context, ownerUserID uint64, rootOrganizationID int64, organizationIDs []int64) error {
	return m.Called(ctx, ownerUserID, rootOrganizationID, organizationIDs).Error(0)
}

func (m *MockPersistenceHandler) EnsurePersonalWorkspaceOwnerMembership(ctx context.Context, ownerUserID uint64, organizationID int64, role proto_models.OrganizationRole) (*common_persistence.PersonalWorkspaceMembershipEnsureResult, error) {
	args := m.Called(ctx, ownerUserID, organizationID, role)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common_persistence.PersonalWorkspaceMembershipEnsureResult), args.Error(1)
}

func (m *MockPersistenceHandler) RepairPersonalWorkspaceOwnerMembershipRoles(ctx context.Context, ownerUserID uint64, organizationIDs []int64, role proto_models.OrganizationRole) error {
	return m.Called(ctx, ownerUserID, organizationIDs, role).Error(0)
}

func (m *MockPersistenceHandler) ListPersonalWorkspaceMemberships(ctx context.Context, organizationIDs []int64) ([]*common_persistence_models.UserOrganization, error) {
	args := m.Called(ctx, organizationIDs)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*common_persistence_models.UserOrganization), args.Error(1)
}

func (m *MockPersistenceHandler) SoftDeletePersonalWorkspaceOrganizations(ctx context.Context, organizationIDs []int64, ownerUserID uint64) error {
	return m.Called(ctx, organizationIDs, ownerUserID).Error(0)
}

func (m *MockPersistenceHandler) RestorePersonalWorkspaceOrganizations(ctx context.Context, organizationIDs []int64, ownerUserID uint64) error {
	return m.Called(ctx, organizationIDs, ownerUserID).Error(0)
}

func (m *MockPersistenceHandler) GetOrganizationByID(ctx context.Context, id int64) (*common_persistence_models.Organization, error) {
	if m.hasExpectation("GetOrganizationByID") {
		args := m.Called(ctx, id)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.Organization), args.Error(1)
	}
	return nil, common_persistence.ErrOrganizationNotFound
}

func (m *MockPersistenceHandler) GetOrganizationBySlug(ctx context.Context, slug string) (*common_persistence_models.Organization, error) {
	if m.hasExpectation("GetOrganizationBySlug") {
		args := m.Called(ctx, slug)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.Organization), args.Error(1)
	}
	return nil, common_persistence.ErrOrganizationNotFound
}

func (m *MockPersistenceHandler) UpdateOrganization(ctx context.Context, organization *common_persistence_models.Organization) error {
	// Check hook first (used by some tests for custom behavior)
	if m.UpdateOrganizationHook != nil {
		return m.UpdateOrganizationHook(ctx, organization)
	}
	if m.hasExpectation("UpdateOrganization") {
		args := m.Called(ctx, organization)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) DeleteOrganization(ctx context.Context, id int64) error {
	// Check hook first (used by some tests for custom behavior)
	if m.DeleteOrganizationHook != nil {
		return m.DeleteOrganizationHook(ctx, id)
	}
	if m.hasExpectation("DeleteOrganization") {
		args := m.Called(ctx, id)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ListOrganizations(ctx context.Context, offset, limit int, search string, organizationTypeFilter proto_models.OrganizationType, sortColumn string, sortDesc bool) ([]*common_persistence_models.Organization, int64, error) {
	if m.hasExpectation("ListOrganizations") {
		args := m.Called(ctx, offset, limit, search, organizationTypeFilter, sortColumn, sortDesc)
		var orgs []*common_persistence_models.Organization
		if args.Get(0) != nil {
			orgs = args.Get(0).([]*common_persistence_models.Organization)
		}
		return orgs, args.Get(1).(int64), args.Error(2)
	}
	return nil, 0, nil
}

func (m *MockPersistenceHandler) ListAdminOrganizations(ctx context.Context, query *common_persistence.AdminOrganizationsQuery) ([]*common_persistence_models.Organization, int64, error) {
	if m.hasExpectation("ListAdminOrganizations") {
		args := m.Called(ctx, query)
		var orgs []*common_persistence_models.Organization
		if args.Get(0) != nil {
			orgs = args.Get(0).([]*common_persistence_models.Organization)
		}
		return orgs, args.Get(1).(int64), args.Error(2)
	}
	return nil, 0, nil
}

func (m *MockPersistenceHandler) ListAdminOrganizationParentOptions(ctx context.Context, query *common_persistence.AdminOrganizationParentOptionsQuery) ([]*common_persistence.AdminOrganizationFilterParentOption, error) {
	if m.hasExpectation("ListAdminOrganizationParentOptions") {
		args := m.Called(ctx, query)
		var options []*common_persistence.AdminOrganizationFilterParentOption
		if args.Get(0) != nil {
			options = args.Get(0).([]*common_persistence.AdminOrganizationFilterParentOption)
		}
		return options, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetAdminOrganizationFilterMetadata(ctx context.Context, search string, organizationTypeFilter proto_models.OrganizationType) (*common_persistence.AdminOrganizationFilterMetadata, error) {
	if m.hasExpectation("GetAdminOrganizationFilterMetadata") {
		args := m.Called(ctx, search, organizationTypeFilter)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.AdminOrganizationFilterMetadata), args.Error(1)
	}
	return &common_persistence.AdminOrganizationFilterMetadata{}, nil
}

func (m *MockPersistenceHandler) UpdateOrganizationType(ctx context.Context, organizationID int64, organizationType proto_models.OrganizationType) error {
	if m.hasExpectation("UpdateOrganizationType") {
		args := m.Called(ctx, organizationID, organizationType)
		return args.Error(0)
	}
	return nil
}

// ============================================================================
// ORGANIZATION EMAIL DOMAIN MANAGEMENT
// ============================================================================

func (m *MockPersistenceHandler) CreateOrganizationEmailDomain(ctx context.Context, domain *common_persistence_models.OrganizationEmailDomain) error {
	if m.hasExpectation("CreateOrganizationEmailDomain") {
		args := m.Called(ctx, domain)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) DeleteOrganizationEmailDomain(ctx context.Context, id uint64) error {
	if m.hasExpectation("DeleteOrganizationEmailDomain") {
		args := m.Called(ctx, id)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetOrganizationEmailDomainByDomain(ctx context.Context, domain string) (*common_persistence_models.OrganizationEmailDomain, error) {
	if m.hasExpectation("GetOrganizationEmailDomainByDomain") {
		args := m.Called(ctx, domain)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.OrganizationEmailDomain), args.Error(1)
	}
	return nil, common_persistence.ErrOrganizationEmailDomainNotFound
}

func (m *MockPersistenceHandler) GetOrganizationEmailDomainByID(ctx context.Context, id uint64) (*common_persistence_models.OrganizationEmailDomain, error) {
	if m.hasExpectation("GetOrganizationEmailDomainByID") {
		args := m.Called(ctx, id)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.OrganizationEmailDomain), args.Error(1)
	}
	return nil, common_persistence.ErrOrganizationEmailDomainNotFound
}

func (m *MockPersistenceHandler) ListOrganizationEmailDomains(ctx context.Context, organizationID int64) ([]*common_persistence_models.OrganizationEmailDomain, error) {
	if m.hasExpectation("ListOrganizationEmailDomains") {
		args := m.Called(ctx, organizationID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.OrganizationEmailDomain), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ListOrganizationEmailDomainsPage(ctx context.Context, organizationID int64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.OrganizationEmailDomain, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListOrganizationEmailDomainsPage") {
		args := m.Called(ctx, organizationID, pagination)
		var domains []*common_persistence_models.OrganizationEmailDomain
		if args.Get(0) != nil {
			domains = args.Get(0).([]*common_persistence_models.OrganizationEmailDomain)
		}
		var paginationResponse *proto_models.PaginationResponse
		if args.Get(1) != nil {
			paginationResponse = args.Get(1).(*proto_models.PaginationResponse)
		}
		return domains, paginationResponse, args.Error(2)
	}
	return nil, common_util.CreatePaginationResponseWithoutTotalCount(pagination.GetLimit(), pagination.GetPage(), false), nil
}

// ============================================================================
// USER-ORGANIZATION RELATIONSHIP METHODS
// ============================================================================

func (m *MockPersistenceHandler) AddUserToOrganization(ctx context.Context, userID uint64, organizationID int64, role proto_models.OrganizationRole) error {
	if m.hasExpectation("AddUserToOrganization") {
		args := m.Called(ctx, userID, organizationID, role)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) RemoveUserFromOrganizationAndDescendants(ctx context.Context, userID uint64, organizationID int64) error {
	if m.hasExpectation("RemoveUserFromOrganizationAndDescendants") {
		args := m.Called(ctx, userID, organizationID)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) RemoveUserFromOrganizationAndDescendantsGuardingLastOwner(ctx context.Context, userID uint64, organizationID int64, ownerRole proto_models.OrganizationRole) error {
	if m.hasExpectation("RemoveUserFromOrganizationAndDescendantsGuardingLastOwner") {
		args := m.Called(ctx, userID, organizationID, ownerRole)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateUserOrganizationRole(ctx context.Context, userID uint64, organizationID int64, role proto_models.OrganizationRole) error {
	if m.hasExpectation("UpdateUserOrganizationRole") {
		args := m.Called(ctx, userID, organizationID, role)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateUserOrganizationRoleGuardingLastOwner(ctx context.Context, userID uint64, organizationID int64, newRole, ownerRole proto_models.OrganizationRole) error {
	if m.hasExpectation("UpdateUserOrganizationRoleGuardingLastOwner") {
		args := m.Called(ctx, userID, organizationID, newRole, ownerRole)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetUserExplicitOrganizationRole(ctx context.Context, userID uint64, organizationID int64) (proto_models.OrganizationRole, error) {
	if m.hasExpectation("GetUserExplicitOrganizationRole") {
		args := m.Called(ctx, userID, organizationID)
		return args.Get(0).(proto_models.OrganizationRole), args.Error(1)
	}
	return proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED, common_persistence.ErrUserOrganizationNotFound
}

func (m *MockPersistenceHandler) GetUserEffectiveOrganizationRoleByLineage(
	ctx context.Context,
	userID uint64,
	organizationID int64,
) (common_persistence.OrganizationEffectiveRoleResult, error) {
	if m.hasExpectation("GetUserEffectiveOrganizationRoleByLineage") {
		args := m.Called(ctx, userID, organizationID)
		if args.Get(0) == nil {
			return common_persistence.OrganizationEffectiveRoleResult{}, args.Error(1)
		}
		return args.Get(0).(common_persistence.OrganizationEffectiveRoleResult), args.Error(1)
	}
	return common_persistence.OrganizationEffectiveRoleResult{
		ExplicitRole:  proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED,
		EffectiveRole: proto_models.OrganizationRole_ORGANIZATION_ROLE_UNSPECIFIED,
	}, nil
}

func (m *MockPersistenceHandler) GetUsersEffectiveOrganizationRolesByLineage(
	ctx context.Context,
	userIDs []uint64,
	organizationID int64,
) (map[uint64]common_persistence.OrganizationEffectiveRoleResult, error) {
	if m.hasExpectation("GetUsersEffectiveOrganizationRolesByLineage") {
		args := m.Called(ctx, userIDs, organizationID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(map[uint64]common_persistence.OrganizationEffectiveRoleResult), args.Error(1)
	}
	return map[uint64]common_persistence.OrganizationEffectiveRoleResult{}, nil
}

func (m *MockPersistenceHandler) GetUserOrganization(ctx context.Context, userID uint64, organizationID int64) (*common_persistence_models.UserOrganization, error) {
	if m.hasExpectation("GetUserOrganization") {
		args := m.Called(ctx, userID, organizationID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.UserOrganization), args.Error(1)
	}
	return nil, common_persistence.ErrUserOrganizationNotFound
}

func (m *MockPersistenceHandler) GetUserOrganizations(ctx context.Context, userID uint64, query *common_persistence.UserOrganizationsQuery) ([]*common_persistence.UserOrganizationWithRoleInfo, int64, error) {
	if m.hasExpectation("GetUserOrganizations") {
		args := m.Called(ctx, userID, query)
		if args.Get(0) == nil {
			return nil, args.Get(1).(int64), args.Error(2)
		}
		return args.Get(0).([]*common_persistence.UserOrganizationWithRoleInfo), args.Get(1).(int64), args.Error(2)
	}
	return nil, 0, nil
}

func (m *MockPersistenceHandler) GetOrganizationUsers(ctx context.Context, organizationID int64, roleFilter proto_models.OrganizationRole, offset, limit int) ([]*common_persistence_models.UserOrganization, int64, error) {
	if m.hasExpectation("GetOrganizationUsers") {
		args := m.Called(ctx, organizationID, roleFilter, offset, limit)
		var users []*common_persistence_models.UserOrganization
		if args.Get(0) != nil {
			users = args.Get(0).([]*common_persistence_models.UserOrganization)
		}
		return users, args.Get(1).(int64), args.Error(2)
	}
	return nil, 0, nil
}

func (m *MockPersistenceHandler) GetOrganizationsByIDs(ctx context.Context, organizationIDs []int64) ([]*common_persistence_models.Organization, error) {
	if m.hasExpectation("GetOrganizationsByIDs") {
		args := m.Called(ctx, organizationIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.Organization), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetOrganizationsByParent(ctx context.Context, parentOrganizationID *int64, offset, limit int) ([]*common_persistence_models.Organization, int64, error) {
	if m.hasExpectation("GetOrganizationsByParent") {
		args := m.Called(ctx, parentOrganizationID, offset, limit)
		var organizations []*common_persistence_models.Organization
		if args.Get(0) != nil {
			organizations = args.Get(0).([]*common_persistence_models.Organization)
		}
		return organizations, args.Get(1).(int64), args.Error(2)
	}
	return []*common_persistence_models.Organization{}, 0, nil
}

func (m *MockPersistenceHandler) GetOrganizationsByParentForUser(ctx context.Context, userID uint64, parentOrganizationID *int64, offset, limit int) ([]*common_persistence_models.Organization, int64, error) {
	if m.hasExpectation("GetOrganizationsByParentForUser") {
		args := m.Called(ctx, userID, parentOrganizationID, offset, limit)
		var organizations []*common_persistence_models.Organization
		if args.Get(0) != nil {
			organizations = args.Get(0).([]*common_persistence_models.Organization)
		}
		return organizations, args.Get(1).(int64), args.Error(2)
	}
	return []*common_persistence_models.Organization{}, 0, nil
}

func (m *MockPersistenceHandler) GetOrganizationDescendantsPage(ctx context.Context, organizationID, afterOrganizationID int64, limit int) ([]*common_persistence_models.Organization, bool, error) {
	if m.hasExpectation("GetOrganizationDescendantsPage") {
		args := m.Called(ctx, organizationID, afterOrganizationID, limit)
		var organizations []*common_persistence_models.Organization
		if args.Get(0) != nil {
			organizations = args.Get(0).([]*common_persistence_models.Organization)
		}
		return organizations, args.Bool(1), args.Error(2)
	}
	return []*common_persistence_models.Organization{}, false, nil
}

func (m *MockPersistenceHandler) GetOrganizationsByLineageLimited(ctx context.Context, organizationID int64, maxNodes int) ([]*common_persistence_models.Organization, bool, error) {
	if m.hasExpectation("GetOrganizationsByLineageLimited") {
		args := m.Called(ctx, organizationID, maxNodes)
		var organizations []*common_persistence_models.Organization
		if args.Get(0) != nil {
			organizations = args.Get(0).([]*common_persistence_models.Organization)
		}
		return organizations, args.Bool(1), args.Error(2)
	}
	return []*common_persistence_models.Organization{}, false, nil
}

func (m *MockPersistenceHandler) GetOrganizationsByLineageForUserLimited(ctx context.Context, userID uint64, organizationID int64, maxNodes int) ([]*common_persistence_models.Organization, bool, error) {
	if m.hasExpectation("GetOrganizationsByLineageForUserLimited") {
		args := m.Called(ctx, userID, organizationID, maxNodes)
		var organizations []*common_persistence_models.Organization
		if args.Get(0) != nil {
			organizations = args.Get(0).([]*common_persistence_models.Organization)
		}
		return organizations, args.Bool(1), args.Error(2)
	}
	return []*common_persistence_models.Organization{}, false, nil
}

func (m *MockPersistenceHandler) GetDescendantOrganizationIDs(ctx context.Context, organizationID int64) ([]uint64, error) {
	if m.hasExpectation("GetDescendantOrganizationIDs") {
		args := m.Called(ctx, organizationID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]uint64), args.Error(1)
	}
	return []uint64{}, nil
}

func (m *MockPersistenceHandler) UpdateOrganizationAvatar(ctx context.Context, organizationID int64, attachmentID *uint64) error {
	if m.hasExpectation("UpdateOrganizationAvatar") {
		args := m.Called(ctx, organizationID, attachmentID)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetOrganizationMemberCountAtOrAboveRole(ctx context.Context, organizationID int64, minRole proto_models.OrganizationRole) (int64, error) {
	if m.hasExpectation("GetOrganizationMemberCountAtOrAboveRole") {
		args := m.Called(ctx, organizationID, minRole)
		return args.Get(0).(int64), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) GetOrganizationMemberCount(ctx context.Context, organizationID int64) (int64, error) {
	if m.hasExpectation("GetOrganizationMemberCount") {
		args := m.Called(ctx, organizationID)
		return args.Get(0).(int64), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) GetOrganizationMemberCounts(ctx context.Context, organizationIDs []int64) (map[int64]int64, error) {
	if m.hasExpectation("GetOrganizationMemberCounts") {
		args := m.Called(ctx, organizationIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(map[int64]int64), args.Error(1)
	}
	return map[int64]int64{}, nil
}

// ============================================================================
// ORGANIZATION INVITATION METHODS
// ============================================================================

func (m *MockPersistenceHandler) CreateOrganizationInvitation(ctx context.Context, invitation *common_persistence_models.OrganizationInvitation) error {
	if m.hasExpectation("CreateOrganizationInvitation") {
		args := m.Called(ctx, invitation)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) LoadOrganizationInvitationNotificationSources(ctx context.Context, invitationIDs, recipientIDs []uint64) (*common_persistence.OrganizationInvitationNotificationSources, error) {
	if m.hasExpectation("LoadOrganizationInvitationNotificationSources") {
		args := m.Called(ctx, invitationIDs, recipientIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.OrganizationInvitationNotificationSources), args.Error(1)
	}
	return &common_persistence.OrganizationInvitationNotificationSources{Invitations: map[uint64]*common_persistence_models.OrganizationInvitation{}, Users: map[uint64]*common_persistence_models.User{}}, nil
}

func (m *MockPersistenceHandler) GetOrganizationInvitationByID(ctx context.Context, id uint64) (*common_persistence_models.OrganizationInvitation, error) {
	if m.hasExpectation("GetOrganizationInvitationByID") {
		args := m.Called(ctx, id)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.OrganizationInvitation), args.Error(1)
	}
	return nil, common_persistence.ErrInvitationNotFound
}

func (m *MockPersistenceHandler) GetPendingInvitationByEmailAndOrg(ctx context.Context, email string, organizationID int64) (*common_persistence_models.OrganizationInvitation, error) {
	if m.hasExpectation("GetPendingInvitationByEmailAndOrg") {
		args := m.Called(ctx, email, organizationID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.OrganizationInvitation), args.Error(1)
	}
	return nil, common_persistence.ErrInvitationNotFound
}

func (m *MockPersistenceHandler) GetPendingInvitationsByEmail(ctx context.Context, email string) ([]*common_persistence_models.OrganizationInvitation, error) {
	if m.hasExpectation("GetPendingInvitationsByEmail") {
		args := m.Called(ctx, email)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.OrganizationInvitation), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetPendingInvitationsByEmailPage(ctx context.Context, email string, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.OrganizationInvitation, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("GetPendingInvitationsByEmailPage") {
		args := m.Called(ctx, email, pagination)
		var invitations []*common_persistence_models.OrganizationInvitation
		if args.Get(0) != nil {
			invitations = args.Get(0).([]*common_persistence_models.OrganizationInvitation)
		}
		var paginationResponse *proto_models.PaginationResponse
		if args.Get(1) != nil {
			paginationResponse = args.Get(1).(*proto_models.PaginationResponse)
		}
		return invitations, paginationResponse, args.Error(2)
	}
	return nil, common_util.CreatePaginationResponseWithoutTotalCount(pagination.GetLimit(), pagination.GetPage(), false), nil
}

func (m *MockPersistenceHandler) MarkOrganizationInvitationAccepted(ctx context.Context, id uint64) error {
	if m.hasExpectation("MarkOrganizationInvitationAccepted") {
		args := m.Called(ctx, id)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) MarkOrganizationInvitationDeclined(ctx context.Context, id uint64) error {
	if m.hasExpectation("MarkOrganizationInvitationDeclined") {
		args := m.Called(ctx, id)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) DeleteExpiredInvitations(ctx context.Context) error {
	if m.hasExpectation("DeleteExpiredInvitations") {
		args := m.Called(ctx)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) DeleteOrganizationInvitation(ctx context.Context, id uint64) error {
	if m.hasExpectation("DeleteOrganizationInvitation") {
		args := m.Called(ctx, id)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetPendingInvitationsByOrganization(ctx context.Context, organizationID int64, limit, offset int) ([]*common_persistence_models.OrganizationInvitation, int64, error) {
	if m.hasExpectation("GetPendingInvitationsByOrganization") {
		args := m.Called(ctx, organizationID, limit, offset)
		var invitations []*common_persistence_models.OrganizationInvitation
		if args.Get(0) != nil {
			invitations = args.Get(0).([]*common_persistence_models.OrganizationInvitation)
		}
		return invitations, args.Get(1).(int64), args.Error(2)
	}
	return nil, 0, nil
}

func (m *MockPersistenceHandler) GetPendingAutoAddEntriesByEmail(ctx context.Context, email string) ([]*common_persistence_models.OrganizationInvitation, error) {
	if m.hasExpectation("GetPendingAutoAddEntriesByEmail") {
		args := m.Called(ctx, email)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.OrganizationInvitation), args.Error(1)
	}
	return nil, nil
}

// ============================================================================
// CHAT METHODS
// ============================================================================

func (m *MockPersistenceHandler) CreateChatChannel(
	ctx context.Context,
	createdByUserID uint64,
	name string,
	description *string,
	contextID *uint64,
	contextDisplayName *string,
	contextEntityID *uint64,
	channelType proto_models.ChannelType,
	participantUserIDs []uint64,
	featureFlags common_persistence_models.ChatChannelFeatureFlags,
) (*common_persistence_models.ChatChannel, error) {
	if m.hasExpectation("CreateChatChannel") {
		args := m.Called(ctx, createdByUserID, name, description, contextID, contextDisplayName, contextEntityID, channelType, participantUserIDs, featureFlags)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatChannel), args.Error(1)
	}
	return nil, common_persistence.ErrChatChannelNotFound
}

func (m *MockPersistenceHandler) CopyChatChannelFork(ctx context.Context, input common_persistence.CopyChatChannelForkInput) (*common_persistence.CopyChatChannelForkResult, error) {
	if m.hasExpectation("CopyChatChannelFork") {
		args := m.Called(ctx, input)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.CopyChatChannelForkResult), args.Error(1)
	}
	return nil, common_persistence.ErrChatChannelNotFound
}

func (m *MockPersistenceHandler) GetChatChannelByForkRequestKey(ctx context.Context, createdByUserID uint64, requestKey string) (*common_persistence_models.ChatChannel, error) {
	if m.hasExpectation("GetChatChannelByForkRequestKey") {
		args := m.Called(ctx, createdByUserID, requestKey)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatChannel), args.Error(1)
	}
	return nil, common_persistence.ErrChatChannelNotFound
}

func (m *MockPersistenceHandler) GetChatChannelFeatures(ctx context.Context, channelID uint64) (common_persistence_models.ChatChannelFeatures, error) {
	if m.hasExpectation("GetChatChannelFeatures") {
		args := m.Called(ctx, channelID)
		return args.Get(0).(common_persistence_models.ChatChannelFeatures), args.Error(1)
	}
	return 0, common_persistence.ErrChatChannelNotFound
}

func (m *MockPersistenceHandler) GetChatChannelByID(ctx context.Context, channelID uint64) (*common_persistence_models.ChatChannel, error) {
	if m.hasExpectation("GetChatChannelByID") {
		args := m.Called(ctx, channelID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatChannel), args.Error(1)
	}
	return nil, common_persistence.ErrChatChannelNotFound
}

func (m *MockPersistenceHandler) GetChatChannelAccess(ctx context.Context, channelID uint64, userID uint64) (*common_persistence.ChatChannelAccess, error) {
	if m.hasExpectation("GetChatChannelAccess") {
		args := m.Called(ctx, channelID, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.ChatChannelAccess), args.Error(1)
	}
	// Fall back to deriving access data from GetChatChannelByID so tests that
	// stub a channel with participants drive both code paths.
	channel, err := m.GetChatChannelByID(ctx, channelID)
	if err != nil {
		return nil, err
	}
	access := &common_persistence.ChatChannelAccess{Channel: channel}
	for i := range channel.Participants {
		p := &channel.Participants[i]
		if p.LeftAt != nil {
			continue
		}
		if p.UserID == userID {
			access.Participant = p
		}
		if p.User.ID != 0 && p.User.IsBot {
			access.BotUserIDs = append(access.BotUserIDs, p.UserID)
		}
	}
	return access, nil
}

// SetChatChannelOrganizationAccess records the call when an expectation is registered and
// otherwise succeeds, so tests that do not exercise org-derived channel access need no setup.
func (m *MockPersistenceHandler) SetChatChannelOrganizationAccess(ctx context.Context, channelID uint64, organizationID *int64, minRoleRead proto_models.OrganizationRole) error {
	if m.hasExpectation("SetChatChannelOrganizationAccess") {
		args := m.Called(ctx, channelID, organizationID, minRoleRead)
		return args.Error(0)
	}
	return nil
}

// SetChatChannelPublicRead records the call when an expectation is registered and otherwise
// succeeds with a bare row, so tests that do not exercise public channel reads need no setup.
func (m *MockPersistenceHandler) SetChatChannelPublicRead(ctx context.Context, channelID uint64, level proto_models.ChatChannelPublicRead) (*common_persistence_models.ChatChannel, error) {
	if m.hasExpectation("SetChatChannelPublicRead") {
		args := m.Called(ctx, channelID, level)
		row, _ := args.Get(0).(*common_persistence_models.ChatChannel)
		return row, args.Error(1)
	}
	return &common_persistence_models.ChatChannel{ID: channelID, PublicRead: level}, nil
}

func (m *MockPersistenceHandler) AcquireChatChannelMemberTupleLock(ctx context.Context, key string) error {
	if m.hasExpectation("AcquireChatChannelMemberTupleLock") {
		args := m.Called(ctx, key)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) FindChatChannelByExactActiveMembers(ctx context.Context, memberUserIDs []uint64) (*common_persistence_models.ChatChannel, error) {
	if m.hasExpectation("FindChatChannelByExactActiveMembers") {
		args := m.Called(ctx, memberUserIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatChannel), args.Error(1)
	}
	return nil, common_persistence.ErrChatChannelNotFound
}

func (m *MockPersistenceHandler) ListChatChannelsForUser(ctx context.Context, userID uint64, pagination *proto_models.PaginationRequest, includeArchived bool) ([]*common_persistence_models.ChatChannel, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListChatChannelsForUser") {
		args := m.Called(ctx, userID, pagination, includeArchived)
		var channels []*common_persistence_models.ChatChannel
		if args.Get(0) != nil {
			channels = args.Get(0).([]*common_persistence_models.ChatChannel)
		}
		var resp *proto_models.PaginationResponse
		if args.Get(1) != nil {
			resp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return channels, resp, args.Error(2)
	}
	return []*common_persistence_models.ChatChannel{}, &proto_models.PaginationResponse{}, nil
}

func (m *MockPersistenceHandler) UpdateChatChannel(ctx context.Context, channelID uint64, actorUserID uint64, name *string, description *string, avatarAttachmentID *uint64, isArchived *bool, unreadNotificationsEnabled *bool) (*common_persistence_models.ChatChannel, error) {
	if m.hasExpectation("UpdateChatChannel") {
		args := m.Called(ctx, channelID, actorUserID, name, description, avatarAttachmentID, isArchived, unreadNotificationsEnabled)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatChannel), args.Error(1)
	}
	return nil, common_persistence.ErrChatChannelNotFound
}

func (m *MockPersistenceHandler) LeaveChatChannel(ctx context.Context, channelID uint64, userID uint64) error {
	if m.hasExpectation("LeaveChatChannel") {
		args := m.Called(ctx, channelID, userID)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) AddChatChannelParticipants(ctx context.Context, channelID uint64, userIDs []uint64) ([]*common_persistence_models.ChatChannelParticipant, error) {
	if m.hasExpectation("AddChatChannelParticipants") {
		args := m.Called(ctx, channelID, userIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.ChatChannelParticipant), args.Error(1)
	}
	return []*common_persistence_models.ChatChannelParticipant{}, nil
}

func (m *MockPersistenceHandler) RemoveChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64) error {
	if m.hasExpectation("RemoveChatChannelParticipant") {
		args := m.Called(ctx, channelID, userID)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateChatChannelParticipantRole(ctx context.Context, channelID uint64, userID uint64, role proto_models.ChannelRole) (*common_persistence_models.ChatChannelParticipant, error) {
	if m.hasExpectation("UpdateChatChannelParticipantRole") {
		args := m.Called(ctx, channelID, userID, role)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatChannelParticipant), args.Error(1)
	}
	return nil, common_persistence.ErrChatParticipantNotFound
}

func (m *MockPersistenceHandler) GetChatChannelParticipant(ctx context.Context, channelID uint64, userID uint64) (*common_persistence_models.ChatChannelParticipant, error) {
	if m.hasExpectation("GetChatChannelParticipant") {
		args := m.Called(ctx, channelID, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatChannelParticipant), args.Error(1)
	}
	return nil, common_persistence.ErrChatParticipantNotFound
}

func (m *MockPersistenceHandler) UpdateChatChannelParticipantPreferences(ctx context.Context, channelID uint64, userID uint64, notificationMode *proto_models.ChannelNotificationMode, isPinned *bool) (*common_persistence_models.ChatChannelParticipant, bool, error) {
	if m.hasExpectation("UpdateChatChannelParticipantPreferences") {
		args := m.Called(ctx, channelID, userID, notificationMode, isPinned)
		if args.Get(0) == nil {
			return nil, false, args.Error(2)
		}
		return args.Get(0).(*common_persistence_models.ChatChannelParticipant), args.Bool(1), args.Error(2)
	}
	return nil, false, common_persistence.ErrChatParticipantNotFound
}

func (m *MockPersistenceHandler) ListChatChannelParticipants(ctx context.Context, channelID uint64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.ChatChannelParticipant, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListChatChannelParticipants") {
		args := m.Called(ctx, channelID, pagination)
		var participants []*common_persistence_models.ChatChannelParticipant
		if args.Get(0) != nil {
			participants = args.Get(0).([]*common_persistence_models.ChatChannelParticipant)
		}
		var resp *proto_models.PaginationResponse
		if args.Get(1) != nil {
			resp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return participants, resp, args.Error(2)
	}
	return []*common_persistence_models.ChatChannelParticipant{}, &proto_models.PaginationResponse{}, nil
}

func (m *MockPersistenceHandler) ListChatChannelParticipantsBatch(ctx context.Context, channelIDs []uint64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.ChatChannelParticipantsBatchItem, error) {
	if m.hasExpectation("ListChatChannelParticipantsBatch") {
		args := m.Called(ctx, channelIDs, pagination)
		var items []*common_persistence_models.ChatChannelParticipantsBatchItem
		if args.Get(0) != nil {
			items = args.Get(0).([]*common_persistence_models.ChatChannelParticipantsBatchItem)
		}
		return items, args.Error(1)
	}
	return []*common_persistence_models.ChatChannelParticipantsBatchItem{}, nil
}

func (m *MockPersistenceHandler) GetActiveChannelParticipantCount(ctx context.Context, channelID uint64) (int64, error) {
	if m.hasExpectation("GetActiveChannelParticipantCount") {
		args := m.Called(ctx, channelID)
		return args.Get(0).(int64), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) GetActiveChannelParticipantCountByUserIDs(ctx context.Context, channelID uint64, userIDs []uint64) (int64, error) {
	if m.hasExpectation("GetActiveChannelParticipantCountByUserIDs") {
		args := m.Called(ctx, channelID, userIDs)
		return args.Get(0).(int64), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) GetActiveChannelParticipantUserIDs(ctx context.Context, channelID uint64) ([]uint64, error) {
	if m.hasExpectation("GetActiveChannelParticipantUserIDs") {
		args := m.Called(ctx, channelID)
		var userIDs []uint64
		if args.Get(0) != nil {
			userIDs = args.Get(0).([]uint64)
		}
		return userIDs, args.Error(1)
	}
	return []uint64{}, nil
}

func (m *MockPersistenceHandler) GetHiddenAuthorizedChannelParticipantUserIDs(ctx context.Context, channelID uint64) ([]uint64, error) {
	if m.hasExpectation("GetHiddenAuthorizedChannelParticipantUserIDs") {
		args := m.Called(ctx, channelID)
		var userIDs []uint64
		if args.Get(0) != nil {
			userIDs = args.Get(0).([]uint64)
		}
		return userIDs, args.Error(1)
	}
	return []uint64{}, nil
}

func (m *MockPersistenceHandler) SendChatMessage(ctx context.Context, in common_persistence.SendChatMessageInput) (*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("SendChatMessage") {
		args := m.Called(ctx, in)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatMessage), args.Error(1)
	}
	return nil, common_persistence.ErrChatMessageNotFound
}

func (m *MockPersistenceHandler) LockChatClientMessageID(ctx context.Context, senderID uint64, channelID uint64, clientMessageID string) (common_persistence.ChatClientMessageIDOwner, error) {
	if m.hasExpectation("LockChatClientMessageID") {
		args := m.Called(ctx, senderID, channelID, clientMessageID)
		return args.Get(0).(common_persistence.ChatClientMessageIDOwner), args.Error(1)
	}
	return common_persistence.ChatClientMessageIDOwner{}, nil
}

func (m *MockPersistenceHandler) CreateChatMessageEmbeds(ctx context.Context, messageID uint64, embeds []common_persistence.CreateChatMessageEmbed) error {
	if m.hasExpectation("CreateChatMessageEmbeds") {
		args := m.Called(ctx, messageID, embeds)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UpdateChatMessageAttachmentIDs(ctx context.Context, messageID uint64, attachmentIDs []uint64) (*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("UpdateChatMessageAttachmentIDs") {
		args := m.Called(ctx, messageID, attachmentIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatMessage), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetChatMessageByID(ctx context.Context, messageID uint64) (*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("GetChatMessageByID") {
		args := m.Called(ctx, messageID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatMessage), args.Error(1)
	}
	return nil, common_persistence.ErrChatMessageNotFound
}

func (m *MockPersistenceHandler) GetChatMessageChannelID(ctx context.Context, messageID uint64) (uint64, error) {
	if m.hasExpectation("GetChatMessageChannelID") {
		args := m.Called(ctx, messageID)
		return args.Get(0).(uint64), args.Error(1)
	}
	return 0, common_persistence.ErrChatMessageNotFound
}

func (m *MockPersistenceHandler) EditChatMessage(ctx context.Context, input common_persistence.EditChatMessageInput) (*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("EditChatMessage") {
		args := m.Called(ctx, input)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatMessage), args.Error(1)
	}
	return nil, common_persistence.ErrChatMessageNotFound
}

func (m *MockPersistenceHandler) DeleteChatMessage(ctx context.Context, channelID uint64, messageID uint64) error {
	if m.hasExpectation("DeleteChatMessage") {
		args := m.Called(ctx, channelID, messageID)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetChatMessageReplyCounts(ctx context.Context, channelID uint64, messageIDs []uint64) (map[uint64]uint32, error) {
	if m.hasExpectation("GetChatMessageReplyCounts") {
		args := m.Called(ctx, channelID, messageIDs)
		var counts map[uint64]uint32
		if args.Get(0) != nil {
			counts = args.Get(0).(map[uint64]uint32)
		}
		return counts, args.Error(1)
	}
	return map[uint64]uint32{}, nil
}

func (m *MockPersistenceHandler) AddChatMessageReaction(ctx context.Context, messageID uint64, userID uint64, emoji string) (*common_persistence_models.ChatMessageReaction, error) {
	if m.hasExpectation("AddChatMessageReaction") {
		args := m.Called(ctx, messageID, userID, emoji)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatMessageReaction), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) AddChatMessageReactionSingle(ctx context.Context, messageID uint64, userID uint64, emoji string) (*common_persistence_models.ChatMessageReaction, error) {
	if m.hasExpectation("AddChatMessageReactionSingle") {
		args := m.Called(ctx, messageID, userID, emoji)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatMessageReaction), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) RemoveChatMessageReaction(ctx context.Context, messageID uint64, userID uint64, emoji string) error {
	if m.hasExpectation("RemoveChatMessageReaction") {
		args := m.Called(ctx, messageID, userID, emoji)
		return args.Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ListChatMessageReactions(ctx context.Context, messageID uint64, pagination *proto_models.PaginationRequest, emojiFilter *string) ([]*common_persistence_models.ChatMessageReaction, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListChatMessageReactions") {
		args := m.Called(ctx, messageID, pagination, emojiFilter)
		var reactions []*common_persistence_models.ChatMessageReaction
		if args.Get(0) != nil {
			reactions = args.Get(0).([]*common_persistence_models.ChatMessageReaction)
		}
		var resp *proto_models.PaginationResponse
		if args.Get(1) != nil {
			resp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return reactions, resp, args.Error(2)
	}
	return []*common_persistence_models.ChatMessageReaction{}, &proto_models.PaginationResponse{}, nil
}

// --- Agentic transcript: visibility-aware reads ---

func (m *MockPersistenceHandler) GetChatMessageByIDWithOptions(ctx context.Context, messageID uint64, opts common_persistence.ChatMessageReadOptions) (*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("GetChatMessageByIDWithOptions") {
		args := m.Called(ctx, messageID, opts)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatMessage), args.Error(1)
	}
	return nil, common_persistence.ErrChatMessageNotFound
}

func (m *MockPersistenceHandler) BatchGetChatMessagesWithOptions(ctx context.Context, channelID uint64, messageIDs []uint64, opts common_persistence.ChatMessageReadOptions) ([]*common_persistence_models.ChatMessage, []uint64, error) {
	if m.hasExpectation("BatchGetChatMessagesWithOptions") {
		args := m.Called(ctx, channelID, messageIDs, opts)
		var messages []*common_persistence_models.ChatMessage
		if args.Get(0) != nil {
			messages = args.Get(0).([]*common_persistence_models.ChatMessage)
		}
		var missing []uint64
		if args.Get(1) != nil {
			missing = args.Get(1).([]uint64)
		}
		return messages, missing, args.Error(2)
	}
	return []*common_persistence_models.ChatMessage{}, []uint64{}, nil
}

func (m *MockPersistenceHandler) ListChatMessagesWithOptions(ctx context.Context, channelID uint64, pagination *proto_models.PaginationRequest, parentMessageID uint64, opts common_persistence.ChatMessageReadOptions, sort *proto_models.SortRequest, sortBy proto_models.ListMessagesSortBy) ([]*common_persistence_models.ChatMessage, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListChatMessagesWithOptions") {
		args := m.Called(ctx, channelID, pagination, parentMessageID, opts, sort, sortBy)
		var messages []*common_persistence_models.ChatMessage
		if args.Get(0) != nil {
			messages = args.Get(0).([]*common_persistence_models.ChatMessage)
		}
		var resp *proto_models.PaginationResponse
		if args.Get(1) != nil {
			resp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return messages, resp, args.Error(2)
	}
	return []*common_persistence_models.ChatMessage{}, &proto_models.PaginationResponse{}, nil
}

// --- Agentic transcript: agent sessions + config snapshots ---

func (m *MockPersistenceHandler) CreateChatAgentSession(ctx context.Context, session *common_persistence_models.ChatAgentSession) error {
	if m.hasExpectation("CreateChatAgentSession") {
		return m.Called(ctx, session).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetChatAgentSessionByID(ctx context.Context, sessionID uint64) (*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("GetChatAgentSessionByID") {
		args := m.Called(ctx, sessionID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentSession), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentSessionNotFound
}

func (m *MockPersistenceHandler) LockChatChannelForAgentSession(ctx context.Context, channelID uint64) (*common_persistence_models.ChatChannel, error) {
	if !m.hasExpectation("LockChatChannelForAgentSession") {
		return nil, common_persistence.ErrChatChannelNotFound
	}
	args := m.Called(ctx, channelID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*common_persistence_models.ChatChannel), args.Error(1)
}

func (m *MockPersistenceHandler) GetChatAgentSessionInLocation(ctx context.Context, channelID, parentMessageID uint64) (*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("GetChatAgentSessionInLocation") {
		args := m.Called(ctx, channelID, parentMessageID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentSession), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentSessionNotFound
}

func (m *MockPersistenceHandler) GetChatAgentSessionByScopedKey(ctx context.Context, organizationID int64, agentType string, userID uint64, contextID uint64, contextEntityID uint64) (*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("GetChatAgentSessionByScopedKey") {
		args := m.Called(ctx, organizationID, agentType, userID, contextID, contextEntityID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentSession), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentSessionNotFound
}

func (m *MockPersistenceHandler) GetChatAgentSessionByCreateIdempotencyKey(ctx context.Context, organizationID int64, createdByUserID uint64, userID uint64, agentType string, key string) (*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("GetChatAgentSessionByCreateIdempotencyKey") {
		args := m.Called(ctx, organizationID, createdByUserID, userID, agentType, key)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentSession), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentSessionNotFound
}

func (m *MockPersistenceHandler) ArchiveChatAgentSession(ctx context.Context, sessionID uint64) (common_persistence.ChatAgentSessionArchive, error) {
	if m.hasExpectation("ArchiveChatAgentSession") {
		args := m.Called(ctx, sessionID)
		return args.Get(0).(common_persistence.ChatAgentSessionArchive), args.Error(1)
	}
	return common_persistence.ChatAgentSessionArchive{}, nil
}

func (m *MockPersistenceHandler) ArchiveReplacedChatAgentSession(ctx context.Context, sessionID uint64) (common_persistence.ChatAgentQueuedInputDelivery, error) {
	if m.hasExpectation("ArchiveReplacedChatAgentSession") {
		args := m.Called(ctx, sessionID)
		return args.Get(0).(common_persistence.ChatAgentQueuedInputDelivery), args.Error(1)
	}
	return common_persistence.ChatAgentQueuedInputDelivery{}, nil
}

func (m *MockPersistenceHandler) ListRetiringDelegatedChatAgentSessions(ctx context.Context, afterSessionID uint64, limit int) ([]*common_persistence.RetiringDelegatedChatAgentSession, error) {
	if m.hasExpectation("ListRetiringDelegatedChatAgentSessions") {
		args := m.Called(ctx, afterSessionID, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence.RetiringDelegatedChatAgentSession), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) UpdateChatAgentSessionAutoRunPolicy(
	ctx context.Context,
	sessionID uint64,
	policy common_persistence_models.ChatAgentSessionAutoRunPolicy,
) (common_persistence.ChatAgentQueuedInputDelivery, error) {
	if m.hasExpectation("UpdateChatAgentSessionAutoRunPolicy") {
		args := m.Called(ctx, sessionID, policy)
		return args.Get(0).(common_persistence.ChatAgentQueuedInputDelivery), args.Error(1)
	}
	return common_persistence.ChatAgentQueuedInputDelivery{}, nil
}

func (m *MockPersistenceHandler) SetChatAgentSessionInitialArtifacts(ctx context.Context, sessionID uint64, initialMessageID uint64, initialRunID uint64) error {
	if m.hasExpectation("SetChatAgentSessionInitialArtifacts") {
		return m.Called(ctx, sessionID, initialMessageID, initialRunID).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) InsertChatAgentSessionConfigSnapshot(ctx context.Context, snapshot *common_persistence_models.ChatAgentSessionConfigSnapshot, toolConfigs []common_persistence_models.ChatAgentSessionToolConfig) error {
	if m.hasExpectation("InsertChatAgentSessionConfigSnapshot") {
		return m.Called(ctx, snapshot, toolConfigs).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) BumpChatAgentSessionActiveConfigVersion(ctx context.Context, sessionID uint64, expectedCurrentVersion uint64, newVersion uint64) error {
	if m.hasExpectation("BumpChatAgentSessionActiveConfigVersion") {
		return m.Called(ctx, sessionID, expectedCurrentVersion, newVersion).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetChatAgentSessionConfigSnapshot(ctx context.Context, sessionID uint64, version uint64) (*common_persistence_models.ChatAgentSessionConfigSnapshot, []common_persistence_models.ChatAgentSessionToolConfig, error) {
	if m.hasExpectation("GetChatAgentSessionConfigSnapshot") {
		args := m.Called(ctx, sessionID, version)
		var snapshot *common_persistence_models.ChatAgentSessionConfigSnapshot
		if args.Get(0) != nil {
			snapshot = args.Get(0).(*common_persistence_models.ChatAgentSessionConfigSnapshot)
		}
		var tools []common_persistence_models.ChatAgentSessionToolConfig
		if args.Get(1) != nil {
			tools = args.Get(1).([]common_persistence_models.ChatAgentSessionToolConfig)
		}
		return snapshot, tools, args.Error(2)
	}
	return nil, nil, common_persistence.ErrChatAgentSessionConfigNotFound
}

func (m *MockPersistenceHandler) GetChatAgentInteractionPolicy(ctx context.Context, sessionID uint64, version uint64) (common_persistence_models.ChatAgentInteractionPolicy, error) {
	if m.hasExpectation("GetChatAgentInteractionPolicy") {
		args := m.Called(ctx, sessionID, version)
		return args.Get(0).(common_persistence_models.ChatAgentInteractionPolicy), args.Error(1)
	}
	return common_persistence_models.ChatAgentInteractionPolicy{}, common_persistence.ErrChatAgentSessionConfigNotFound
}

// --- Agentic transcript: agent input admission ---

func (m *MockPersistenceHandler) LockChatChannelForSend(ctx context.Context, channelID uint64, agentRunID uint64) error {
	if m.hasExpectation("LockChatChannelForSend") {
		return m.Called(ctx, channelID, agentRunID).Error(0)
	}
	return nil
}

// LockChatAgentSessionsForSend defaults to idle sessions with nothing waiting, so a send without
// an expectation starts runs exactly as it did before input admission.
func (m *MockPersistenceHandler) LockChatAgentSessionsForSend(ctx context.Context, channelID uint64, locks []common_persistence.ChatAgentSessionSendLock) ([]common_persistence.ChatAgentSessionInputState, error) {
	if m.hasExpectation("LockChatAgentSessionsForSend") {
		args := m.Called(ctx, channelID, locks)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]common_persistence.ChatAgentSessionInputState), args.Error(1)
	}
	return make([]common_persistence.ChatAgentSessionInputState, len(locks)), nil
}

func (m *MockPersistenceHandler) EnqueueChatAgentInput(ctx context.Context, in common_persistence.NewChatAgentQueuedInput) (*common_persistence_models.ChatAgentQueuedInput, error) {
	if m.hasExpectation("EnqueueChatAgentInput") {
		args := m.Called(ctx, in)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentQueuedInput), args.Error(1)
	}
	if in.Session == nil {
		return nil, common_persistence.ErrChatAgentSessionNotFound
	}
	return &common_persistence_models.ChatAgentQueuedInput{AgentSessionID: in.Session.ID, SenderID: in.SenderID}, nil
}

func (m *MockPersistenceHandler) SetChatAgentQueuedInputAttachmentIDs(ctx context.Context, queuedInputID uint64, attachmentIDs []uint64) error {
	if m.hasExpectation("SetChatAgentQueuedInputAttachmentIDs") {
		return m.Called(ctx, queuedInputID, attachmentIDs).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetChatAgentQueuedInput(ctx context.Context, queuedInputID uint64) (*common_persistence_models.ChatAgentQueuedInput, error) {
	if m.hasExpectation("GetChatAgentQueuedInput") {
		args := m.Called(ctx, queuedInputID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentQueuedInput), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentQueuedInputNotQueued
}

func (m *MockPersistenceHandler) ListChatAgentQueuedInputs(ctx context.Context, sessionID uint64, pagination *proto_models.PaginationRequest, limit int) ([]*common_persistence_models.ChatAgentQueuedInput, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListChatAgentQueuedInputs") {
		args := m.Called(ctx, sessionID, pagination, limit)
		var rows []*common_persistence_models.ChatAgentQueuedInput
		if args.Get(0) != nil {
			rows = args.Get(0).([]*common_persistence_models.ChatAgentQueuedInput)
		}
		var page *proto_models.PaginationResponse
		if args.Get(1) != nil {
			page = args.Get(1).(*proto_models.PaginationResponse)
		}
		return rows, page, args.Error(2)
	}
	return nil, nil, nil
}

func (m *MockPersistenceHandler) LockChatAgentQueuedInput(ctx context.Context, queuedInputID uint64) (*common_persistence_models.ChatAgentQueuedInput, error) {
	if m.hasExpectation("LockChatAgentQueuedInput") {
		args := m.Called(ctx, queuedInputID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentQueuedInput), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentQueuedInputNotQueued
}

func (m *MockPersistenceHandler) EditChatAgentQueuedInput(ctx context.Context, queuedInputID uint64, content common_persistence.ChatAgentQueuedInputContent) (*common_persistence_models.ChatAgentQueuedInput, error) {
	if m.hasExpectation("EditChatAgentQueuedInput") {
		args := m.Called(ctx, queuedInputID, content)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentQueuedInput), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentQueuedInputNotQueued
}

func (m *MockPersistenceHandler) RemoveChatAgentQueuedInput(ctx context.Context, queuedInputID uint64) (*common_persistence_models.ChatAgentQueuedInput, error) {
	if m.hasExpectation("RemoveChatAgentQueuedInput") {
		args := m.Called(ctx, queuedInputID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentQueuedInput), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentQueuedInputNotQueued
}

func (m *MockPersistenceHandler) DrainChatAgentInput(ctx context.Context, sessionID uint64) (*common_persistence_models.ChatAgentRun, common_persistence.ChatAgentQueuedInputDelivery, error) {
	if m.hasExpectation("DrainChatAgentInput") {
		args := m.Called(ctx, sessionID)
		var run *common_persistence_models.ChatAgentRun
		if args.Get(0) != nil {
			run = args.Get(0).(*common_persistence_models.ChatAgentRun)
		}
		return run, args.Get(1).(common_persistence.ChatAgentQueuedInputDelivery), args.Error(2)
	}
	return nil, common_persistence.ChatAgentQueuedInputDelivery{}, nil
}

func (m *MockPersistenceHandler) GetChatAgentInputReceipts(ctx context.Context, messageID uint64) ([]common_persistence.ChatAgentInputReceipt, error) {
	if m.hasExpectation("GetChatAgentInputReceipts") {
		args := m.Called(ctx, messageID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]common_persistence.ChatAgentInputReceipt), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ListChatAgentRunAdmittedMessageIDs(ctx context.Context, runID, throughMessageID uint64) ([]uint64, error) {
	if m.hasExpectation("ListChatAgentRunAdmittedMessageIDs") {
		args := m.Called(ctx, runID, throughMessageID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]uint64), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) StopChatAgentRun(ctx context.Context, runID, stoppedByUserID uint64) (*common_persistence.ChatAgentRunStop, error) {
	if m.hasExpectation("StopChatAgentRun") {
		args := m.Called(ctx, runID, stoppedByUserID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.ChatAgentRunStop), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ResumeChatAgentSession(ctx context.Context, sessionID, suspendedRunID uint64) (*common_persistence.ChatAgentSessionResume, error) {
	if m.hasExpectation("ResumeChatAgentSession") {
		args := m.Called(ctx, sessionID, suspendedRunID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.ChatAgentSessionResume), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentSessionNotFound
}

func (m *MockPersistenceHandler) HasChatAgentRunAdmittedInputFrom(ctx context.Context, runID, userID uint64) (bool, error) {
	if m.hasExpectation("HasChatAgentRunAdmittedInputFrom") {
		args := m.Called(ctx, runID, userID)
		return args.Bool(0), args.Error(1)
	}
	return false, nil
}

func (m *MockPersistenceHandler) GetChatAgentRunControlEvidence(ctx context.Context, ids []uint64, userID uint64) (map[uint64]common_persistence.ChatAgentRunControlEvidence, error) {
	if m.hasExpectation("GetChatAgentRunControlEvidence") {
		args := m.Called(ctx, ids, userID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(map[uint64]common_persistence.ChatAgentRunControlEvidence), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ListSuspendedChatAgentRuns(ctx context.Context, afterRunID uint64, limit int) ([]uint64, error) {
	if m.hasExpectation("ListSuspendedChatAgentRuns") {
		args := m.Called(ctx, afterRunID, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]uint64), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetChatAgentSessionInteractionState(ctx context.Context, sessionID uint64) (*common_persistence.ChatAgentSessionInteractionState, error) {
	if m.hasExpectation("GetChatAgentSessionInteractionState") {
		args := m.Called(ctx, sessionID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.ChatAgentSessionInteractionState), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentSessionNotFound
}

func (m *MockPersistenceHandler) ListChatAgentSessionInteractionStates(ctx context.Context, channelID uint64, parentMessageIDs []uint64) ([]*common_persistence.ChatAgentSessionInteractionState, error) {
	if m.hasExpectation("ListChatAgentSessionInteractionStates") {
		args := m.Called(ctx, channelID, parentMessageIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence.ChatAgentSessionInteractionState), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ReserveChatAgentRunStepAndAdmit(ctx context.Context, runID uint64, maxGenerations uint64, maxToolCalls uint64, admit bool) (bool, common_persistence.ChatAgentQueuedInputDelivery, error) {
	if m.hasExpectation("ReserveChatAgentRunStepAndAdmit") {
		args := m.Called(ctx, runID, maxGenerations, maxToolCalls, admit)
		return args.Bool(0), args.Get(1).(common_persistence.ChatAgentQueuedInputDelivery), args.Error(2)
	}
	return true, common_persistence.ChatAgentQueuedInputDelivery{}, nil
}

// --- Agentic transcript: agent runs ---

func (m *MockPersistenceHandler) CreateChatAgentRun(ctx context.Context, run *common_persistence_models.ChatAgentRun) error {
	if m.hasExpectation("CreateChatAgentRun") {
		return m.Called(ctx, run).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CreateChatAgentRunCapturingConfig(ctx context.Context, run *common_persistence_models.ChatAgentRun) error {
	if m.hasExpectation("CreateChatAgentRunCapturingConfig") {
		return m.Called(ctx, run).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CreateChatAgentRunForSend(ctx context.Context, run *common_persistence_models.ChatAgentRun) error {
	if m.hasExpectation("CreateChatAgentRunForSend") {
		return m.Called(ctx, run).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetChatAgentRunByID(ctx context.Context, runID uint64) (*common_persistence_models.ChatAgentRun, error) {
	if m.hasExpectation("GetChatAgentRunByID") {
		args := m.Called(ctx, runID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentRun), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentRunNotFound
}

func (m *MockPersistenceHandler) CompareAndSetChatAgentRunStatus(ctx context.Context, runID uint64, fromStatuses []proto_models.ChatAgentRunStatus, toStatus proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason) (bool, error) {
	if m.hasExpectation("CompareAndSetChatAgentRunStatus") {
		args := m.Called(ctx, runID, fromStatuses, toStatus, reason)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) ClaimChatAgentRunDrive(ctx context.Context, runID uint64, leaseOwner string, leaseDuration time.Duration) (bool, error) {
	if m.hasExpectation("ClaimChatAgentRunDrive") {
		args := m.Called(ctx, runID, leaseOwner, leaseDuration)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) RenewChatAgentRunDrive(ctx context.Context, runID uint64, leaseOwner string, leaseDuration time.Duration) (bool, error) {
	if m.hasExpectation("RenewChatAgentRunDrive") {
		args := m.Called(ctx, runID, leaseOwner, leaseDuration)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) ReleaseChatAgentRunDrive(ctx context.Context, runID uint64, leaseOwner string, retryAfter time.Duration, retryReason proto_models.ChatAgentRunStatusReason) (bool, error) {
	if m.hasExpectation("ReleaseChatAgentRunDrive") {
		args := m.Called(ctx, runID, leaseOwner, retryAfter, retryReason)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) AbandonChatAgentRunDrive(ctx context.Context, runID uint64, abandonment common_persistence.ChatAgentRunDriveAbandonment) (common_persistence.ChatAgentRunDriveAbandonOutcome, uint64, error) {
	if m.hasExpectation("AbandonChatAgentRunDrive") {
		args := m.Called(ctx, runID, abandonment)
		outcome, _ := args.Get(0).(common_persistence.ChatAgentRunDriveAbandonOutcome)
		noticeID, _ := args.Get(1).(uint64)
		return outcome, noticeID, args.Error(2)
	}
	return common_persistence.ChatAgentRunDriveAbandonRetry, 0, nil
}

func (m *MockPersistenceHandler) ListRecoverableChatAgentRuns(ctx context.Context, limit int) ([]uint64, error) {
	if m.hasExpectation("ListRecoverableChatAgentRuns") {
		args := m.Called(ctx, limit)
		var ids []uint64
		if args.Get(0) != nil {
			ids = args.Get(0).([]uint64)
		}
		return ids, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ListRecoverableChatAgentRunsAfterID(ctx context.Context, afterID uint64, limit int) ([]uint64, error) {
	if m.hasExpectation("ListRecoverableChatAgentRunsAfterID") {
		args := m.Called(ctx, afterID, limit)
		var ids []uint64
		if args.Get(0) != nil {
			ids = args.Get(0).([]uint64)
		}
		return ids, args.Error(1)
	}
	return m.ListRecoverableChatAgentRuns(ctx, limit)
}

func (m *MockPersistenceHandler) AssertChatAgentRunDriveLease(ctx context.Context, runID uint64) error {
	if m.hasExpectation("AssertChatAgentRunDriveLease") {
		return m.Called(ctx, runID).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) SealChatAgentRunStep(ctx context.Context, runID uint64, stepIndex uint32, disposition common_persistence.ChatAgentRunStepDisposition, toolCallCount uint32, maxGenerations uint64, steer bool, consumedPending uint64) (common_persistence.ChatAgentRunStepDisposition, bool, error) {
	if m.hasExpectation("SealChatAgentRunStep") {
		args := m.Called(ctx, runID, stepIndex, disposition, toolCallCount, maxGenerations, steer, consumedPending)
		return args.Get(0).(common_persistence.ChatAgentRunStepDisposition), args.Bool(1), args.Error(2)
	}
	return disposition, true, nil
}

func (m *MockPersistenceHandler) ReconcileSealedChatAgentRunToolStep(ctx context.Context, runID uint64, stepIndex uint32, resumeOnDenial bool) (common_persistence.ChatAgentRunToolStepResolution, error) {
	if m.hasExpectation("ReconcileSealedChatAgentRunToolStep") {
		args := m.Called(ctx, runID, stepIndex, resumeOnDenial)
		return args.Get(0).(common_persistence.ChatAgentRunToolStepResolution), args.Error(1)
	}
	return common_persistence.ChatAgentRunToolStepResolutionContinue, nil
}

func (m *MockPersistenceHandler) FailChatAgentRunWithNotice(ctx context.Context, runID uint64, reason proto_models.ChatAgentRunStatusReason) (bool, uint64, error) {
	if m.hasExpectation("FailChatAgentRunWithNotice") {
		args := m.Called(ctx, runID, reason)
		noticeID, _ := args.Get(1).(uint64)
		return args.Bool(0), noticeID, args.Error(2)
	}
	return true, 1, nil
}

func (m *MockPersistenceHandler) ResetIncompleteWorkflowAgentRunDrive(ctx context.Context, runID uint64) (bool, error) {
	if m.hasExpectation("ResetIncompleteWorkflowAgentRunDrive") {
		args := m.Called(ctx, runID)
		return args.Bool(0), args.Error(1)
	}
	return false, nil
}

func (m *MockPersistenceHandler) FailIncompleteChatAgentRunDrive(ctx context.Context, runID uint64) (bool, error) {
	if m.hasExpectation("FailIncompleteChatAgentRunDrive") {
		args := m.Called(ctx, runID)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) GetChatAgentRunStepArtifactState(ctx context.Context, runID uint64) (*common_persistence.ChatAgentRunStepArtifactState, error) {
	if m.hasExpectation("GetChatAgentRunStepArtifactState") {
		args := m.Called(ctx, runID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.ChatAgentRunStepArtifactState), args.Error(1)
	}
	return &common_persistence.ChatAgentRunStepArtifactState{}, nil
}

func (m *MockPersistenceHandler) LockChatAgentRunForUpdate(ctx context.Context, runID uint64) error {
	if m.hasExpectation("LockChatAgentRunForUpdate") {
		args := m.Called(ctx, runID)
		return args.Error(0)
	}
	return nil
}

// --- Agentic transcript: inference ledger + totals ---

func (m *MockPersistenceHandler) RecordChatInferenceCall(ctx context.Context, call *common_persistence_models.ChatInferenceCall) (bool, error) {
	if m.hasExpectation("RecordChatInferenceCall") {
		args := m.Called(ctx, call)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) GetChatInferenceTotals(ctx context.Context, channelID uint64) (*common_persistence_models.ChatInferenceTotals, error) {
	if m.hasExpectation("GetChatInferenceTotals") {
		args := m.Called(ctx, channelID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatInferenceTotals), args.Error(1)
	}
	return &common_persistence_models.ChatInferenceTotals{ChannelID: channelID}, nil
}

func (m *MockPersistenceHandler) GetLatestChatInferenceInputTokens(ctx context.Context, runID uint64) (int64, error) {
	if m.hasExpectation("GetLatestChatInferenceInputTokens") {
		args := m.Called(ctx, runID)
		return args.Get(0).(int64), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) GetMessageInferenceAggregate(ctx context.Context, messageID uint64) (*common_persistence.MessageInferenceAggregate, error) {
	if m.hasExpectation("GetMessageInferenceAggregate") {
		args := m.Called(ctx, messageID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.MessageInferenceAggregate), args.Error(1)
	}
	return &common_persistence.MessageInferenceAggregate{MessageID: messageID}, nil
}

func (m *MockPersistenceHandler) BatchGetMessageInferenceAggregates(ctx context.Context, channelID uint64, messageIDs []uint64) (map[uint64]*common_persistence.MessageInferenceAggregate, error) {
	if m.hasExpectation("BatchGetMessageInferenceAggregates") {
		args := m.Called(ctx, channelID, messageIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(map[uint64]*common_persistence.MessageInferenceAggregate), args.Error(1)
	}
	return map[uint64]*common_persistence.MessageInferenceAggregate{}, nil
}

func (m *MockPersistenceHandler) ListChatInferenceCallsByRun(ctx context.Context, runID uint64, pagination *proto_models.PaginationRequest) ([]*common_persistence_models.ChatInferenceCall, *proto_models.PaginationResponse, error) {
	if m.hasExpectation("ListChatInferenceCallsByRun") {
		args := m.Called(ctx, runID, pagination)
		var calls []*common_persistence_models.ChatInferenceCall
		if args.Get(0) != nil {
			calls = args.Get(0).([]*common_persistence_models.ChatInferenceCall)
		}
		var resp *proto_models.PaginationResponse
		if args.Get(1) != nil {
			resp = args.Get(1).(*proto_models.PaginationResponse)
		}
		return calls, resp, args.Error(2)
	}
	return []*common_persistence_models.ChatInferenceCall{}, &proto_models.PaginationResponse{}, nil
}

func (m *MockPersistenceHandler) GetChatInferenceTotalsForSessions(ctx context.Context, agentSessionIDs []uint64) (*common_persistence_models.ChatInferenceTotals, error) {
	if m.hasExpectation("GetChatInferenceTotalsForSessions") {
		args := m.Called(ctx, agentSessionIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatInferenceTotals), args.Error(1)
	}
	return &common_persistence_models.ChatInferenceTotals{}, nil
}

func (m *MockPersistenceHandler) ListChatInferenceDimensionsForSessions(ctx context.Context, ids []uint64, dimensions map[common_persistence.InferenceBreakdownDimension]*proto_models.PaginationRequest) (map[common_persistence.InferenceBreakdownDimension]*common_persistence.InferenceUsageDimension, error) {
	if m.hasExpectation("ListChatInferenceDimensionsForSessions") {
		args := m.Called(ctx, ids, dimensions)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(map[common_persistence.InferenceBreakdownDimension]*common_persistence.InferenceUsageDimension), args.Error(1)
	}
	out := make(map[common_persistence.InferenceBreakdownDimension]*common_persistence.InferenceUsageDimension, len(dimensions))
	for dim := range dimensions {
		out[dim] = &common_persistence.InferenceUsageDimension{Rows: []*common_persistence.InferenceUsageBreakdown{}}
	}
	return out, nil
}

// --- Agentic transcript: compaction + context reads ---

func (m *MockPersistenceHandler) GetLatestCompaction(ctx context.Context, channelID uint64, parentMessageID uint64) (*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("GetLatestCompaction") {
		args := m.Called(ctx, channelID, parentMessageID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatMessage), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) StreamChatMessagesAfter(ctx context.Context, channelID uint64, parentMessageID uint64, afterMessageID uint64, opts common_persistence.ChatMessageReadOptions, batchSize int, fn func(batch []*common_persistence_models.ChatMessage) (stop bool, err error)) error {
	if m.hasExpectation("StreamChatMessagesAfter") {
		return m.Called(ctx, channelID, parentMessageID, afterMessageID, opts, batchSize, fn).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CountVisibleCountingMessagesAfter(ctx context.Context, channelID uint64, parentMessageID uint64, afterMessageID uint64, excludeSenderID uint64, excludeKinds []proto_models.MessageKind) (int64, error) {
	if m.hasExpectation("CountVisibleCountingMessagesAfter") {
		args := m.Called(ctx, channelID, parentMessageID, afterMessageID, excludeSenderID, excludeKinds)
		var count int64
		if args.Get(0) != nil {
			count = args.Get(0).(int64)
		}
		return count, args.Error(1)
	}
	return 0, nil
}

// --- Agentic transcript: tool-call approval / durable execution ---

func (m *MockPersistenceHandler) ResolveToolCallApproval(ctx context.Context, messageID uint64, fromStatus proto_models.ToolCallApprovalStatus, toStatus proto_models.ToolCallApprovalStatus, toExecState proto_models.ToolCallExecutionState, resolvedByUserID *uint64, reason *string, resolvedAt time.Time) (bool, error) {
	if m.hasExpectation("ResolveToolCallApproval") {
		args := m.Called(ctx, messageID, fromStatus, toStatus, toExecState, resolvedByUserID, reason, resolvedAt)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) ListPendingToolCallsToExpire(ctx context.Context, now time.Time, limit int) ([]*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("ListPendingToolCallsToExpire") {
		args := m.Called(ctx, now, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.ChatMessage), args.Error(1)
	}
	return []*common_persistence_models.ChatMessage{}, nil
}

func (m *MockPersistenceHandler) ListPendingToolCallExpiryBatch(ctx context.Context, now time.Time, after *common_persistence.ToolCallExpiryCursor, limit int) (*common_persistence.PendingToolCallExpiryBatch, error) {
	if m.hasExpectation("ListPendingToolCallExpiryBatch") {
		args := m.Called(ctx, now, after, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.PendingToolCallExpiryBatch), args.Error(1)
	}
	return &common_persistence.PendingToolCallExpiryBatch{}, nil
}

func (m *MockPersistenceHandler) ListClaimableApprovedToolCalls(ctx context.Context, channelID uint64, now time.Time, limit int) ([]*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("ListClaimableApprovedToolCalls") {
		args := m.Called(ctx, channelID, now, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.ChatMessage), args.Error(1)
	}
	return []*common_persistence_models.ChatMessage{}, nil
}

func (m *MockPersistenceHandler) ClaimApprovedToolCall(ctx context.Context, messageID uint64, leaseOwner string, leaseExpiresAt time.Time, now time.Time) (bool, error) {
	if m.hasExpectation("ClaimApprovedToolCall") {
		args := m.Called(ctx, messageID, leaseOwner, leaseExpiresAt, now)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) ListClaimableToolCalls(ctx context.Context, channelID uint64, now time.Time, limit int) ([]*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("ListClaimableToolCalls") {
		args := m.Called(ctx, channelID, now, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.ChatMessage), args.Error(1)
	}
	return []*common_persistence_models.ChatMessage{}, nil
}

func (m *MockPersistenceHandler) ClaimToolCall(ctx context.Context, messageID uint64, leaseOwner string, leaseExpiresAt time.Time, now time.Time) (bool, error) {
	if m.hasExpectation("ClaimToolCall") {
		args := m.Called(ctx, messageID, leaseOwner, leaseExpiresAt, now)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) ClaimFreshInlineToolCall(ctx context.Context, messageID uint64, leaseOwner string, leaseExpiresAt time.Time, now time.Time) (bool, error) {
	if m.hasExpectation("ClaimFreshInlineToolCall") {
		args := m.Called(ctx, messageID, leaseOwner, leaseExpiresAt, now)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) ReleaseToolCallLease(ctx context.Context, messageID uint64, leaseOwner string, retryable bool, lastErr *string) (bool, error) {
	if m.hasExpectation("ReleaseToolCallLease") {
		args := m.Called(ctx, messageID, leaseOwner, retryable, lastErr)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) MarkInlineToolCallExecutionFailure(ctx context.Context, messageID uint64, retryable bool, lastErr *string) (bool, error) {
	if m.hasExpectation("MarkInlineToolCallExecutionFailure") {
		args := m.Called(ctx, messageID, retryable, lastErr)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) MarkToolCallExecuted(ctx context.Context, messageID uint64, leaseOwner string, executedAt time.Time) (bool, error) {
	if m.hasExpectation("MarkToolCallExecuted") {
		args := m.Called(ctx, messageID, leaseOwner, executedAt)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) CreateTranscriptMessage(ctx context.Context, in common_persistence.CreateTranscriptMessageInput) (uint64, bool, error) {
	if m.hasExpectation("CreateTranscriptMessage") {
		args := m.Called(ctx, in)
		var messageID uint64
		if args.Get(0) != nil {
			messageID = args.Get(0).(uint64)
		}
		return messageID, args.Bool(1), args.Error(2)
	}
	return 0, true, nil
}

func (m *MockPersistenceHandler) CommitClaimedToolCallResult(ctx context.Context, toolCallMessageID uint64, leaseOwner string, executedAt time.Time, in common_persistence.CreateTranscriptMessageInput) (uint64, bool, bool, error) {
	if m.hasExpectation("CommitClaimedToolCallResult") {
		args := m.Called(ctx, toolCallMessageID, leaseOwner, executedAt, in)
		var messageID uint64
		if args.Get(0) != nil {
			messageID = args.Get(0).(uint64)
		}
		return messageID, args.Bool(1), args.Bool(2), args.Error(3)
	}
	return 0, false, false, nil
}

func (m *MockPersistenceHandler) CountUnresolvedToolCallsInStep(ctx context.Context, agentRunID uint64, agentStepIndex uint32) (int, error) {
	if m.hasExpectation("CountUnresolvedToolCallsInStep") {
		args := m.Called(ctx, agentRunID, agentStepIndex)
		return args.Int(0), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) HasDeniedOrExpiredToolCallsInStep(ctx context.Context, agentRunID uint64, agentStepIndex uint32) (bool, error) {
	if m.hasExpectation("HasDeniedOrExpiredToolCallsInStep") {
		args := m.Called(ctx, agentRunID, agentStepIndex)
		return args.Bool(0), args.Error(1)
	}
	return false, nil
}

func (m *MockPersistenceHandler) ReserveChatAgentRunBudget(
	ctx context.Context,
	runID uint64,
	generationDelta uint64,
	toolCallDelta uint64,
	maxGenerations uint64,
	maxToolCalls uint64,
) (bool, error) {
	if m.hasExpectation("ReserveChatAgentRunBudget") {
		args := m.Called(ctx, runID, generationDelta, toolCallDelta, maxGenerations, maxToolCalls)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) AddChatAgentRunActiveDuration(ctx context.Context, runID uint64, elapsed time.Duration) error {
	if m.hasExpectation("AddChatAgentRunActiveDuration") {
		return m.Called(ctx, runID, elapsed).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) GetUserForUpdate(ctx context.Context, id uint64) (*common_persistence_models.User, error) {
	if m.hasExpectation("GetUserForUpdate") {
		args := m.Called(ctx, id)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.User), args.Error(1)
	}
	return nil, common_persistence.ErrUserNotFound
}
func (m *MockPersistenceHandler) HasPendingAccountInvitation(ctx context.Context, id uint64, email string) (bool, error) {
	if m.hasExpectation("HasPendingAccountInvitation") {
		args := m.Called(ctx, id, email)
		return args.Bool(0), args.Error(1)
	}
	return false, nil
}
func (m *MockPersistenceHandler) CancelChatAgentRun(ctx context.Context, runID uint64, fromStatuses []proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason, reasonCode string) (*common_persistence.ChatAgentRunCancellation, error) {
	if m.hasExpectation("CancelChatAgentRun") {
		args := m.Called(ctx, runID, fromStatuses, reason, reasonCode)
		cancellation, _ := args.Get(0).(*common_persistence.ChatAgentRunCancellation)
		return cancellation, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) TerminalizeChatAgentRunWithNotice(ctx context.Context, runID uint64, fromStatuses []proto_models.ChatAgentRunStatus, toStatus proto_models.ChatAgentRunStatus, reason proto_models.ChatAgentRunStatusReason) (bool, uint64, error) {
	if m.hasExpectation("TerminalizeChatAgentRunWithNotice") {
		args := m.Called(ctx, runID, fromStatuses, toStatus, reason)
		return args.Bool(0), args.Get(1).(uint64), args.Error(2)
	}
	return false, 0, nil
}

// --- Agentic transcript: delegation ---

func (m *MockPersistenceHandler) SetChatAgentMessageRecorder(recorder common_persistence.ChatAgentMessageRecorder) {
	if m.hasExpectation("SetChatAgentMessageRecorder") {
		m.Called(recorder)
	}
}

func (m *MockPersistenceHandler) SetChatAgentRunBudgetCeilingsProvider(provider common_persistence.ChatAgentRunBudgetCeilingsProvider) {
	if m.hasExpectation("SetChatAgentRunBudgetCeilingsProvider") {
		m.Called(provider)
	}
}

func (m *MockPersistenceHandler) SetChatAgentWorkflowTypesProvider(provider common_persistence.ChatAgentWorkflowTypesProvider) {
	if m.hasExpectation("SetChatAgentWorkflowTypesProvider") {
		m.Called(provider)
	}
}

// ChatAgentGraphBudgetPersistence: without an expectation no graph has a pool.

func (m *MockPersistenceHandler) SetChatAgentGraphBudget(ctx context.Context, rootSessionID uint64, limits common_persistence_models.ChatAgentGraphBudget) (common_persistence.ChatAgentGraphBudgetDrain, error) {
	if m.hasExpectation("SetChatAgentGraphBudget") {
		args := m.Called(ctx, rootSessionID, limits)
		drained, _ := args.Get(0).(common_persistence.ChatAgentGraphBudgetDrain)
		return drained, args.Error(1)
	}
	return common_persistence.ChatAgentGraphBudgetDrain{}, nil
}

func (m *MockPersistenceHandler) GetChatAgentGraphBudget(ctx context.Context, rootSessionID uint64) (*common_persistence_models.ChatAgentGraphBudget, error) {
	if m.hasExpectation("GetChatAgentGraphBudget") {
		args := m.Called(ctx, rootSessionID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentGraphBudget), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetChatAgentGraphDriveState(ctx context.Context, rootSessionID uint64) (*common_persistence.ChatAgentGraphDriveState, error) {
	if m.hasExpectation("GetChatAgentGraphDriveState") {
		args := m.Called(ctx, rootSessionID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.ChatAgentGraphDriveState), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentSessionNotFound
}

func (m *MockPersistenceHandler) RevokeChatAgentGraphDriveLeases(ctx context.Context, rootSessionID uint64) error {
	if m.hasExpectation("RevokeChatAgentGraphDriveLeases") {
		return m.Called(ctx, rootSessionID).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) CheckChatAgentGraphBudget(ctx context.Context, rootSessionID uint64) error {
	if m.hasExpectation("CheckChatAgentGraphBudget") {
		return m.Called(ctx, rootSessionID).Error(0)
	}
	return nil
}

// ChatAgentMessagingPersistence: every argument is forwarded so expectations can match on it.

func (m *MockPersistenceHandler) SetChatAgentChildWakeLimitsProvider(provider common_persistence.ChatAgentChildWakeLimitsProvider) {
	if m.hasExpectation("SetChatAgentChildWakeLimitsProvider") {
		m.Called(provider)
	}
}

func (m *MockPersistenceHandler) SendDelegatedAgentMessage(ctx context.Context, in common_persistence.ChatAgentChildMessageInput, write func() (uint64, error)) (*common_persistence_models.ChatAgentMessageReceipt, bool, error) {
	if m.hasExpectation("SendDelegatedAgentMessage") {
		args := m.Called(ctx, in, write)
		if args.Get(0) == nil {
			return nil, args.Bool(1), args.Error(2)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentMessageReceipt), args.Bool(1), args.Error(2)
	}
	return nil, false, common_persistence.ErrChatAgentChildUnavailable
}

func (m *MockPersistenceHandler) ResumeChatAgentParent(ctx context.Context, in common_persistence.ChatAgentParentResumeInput) (*common_persistence_models.ChatAgentMessageReceipt, bool, error) {
	if m.hasExpectation("ResumeChatAgentParent") {
		args := m.Called(ctx, in)
		if args.Get(0) == nil {
			return nil, args.Bool(1), args.Error(2)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentMessageReceipt), args.Bool(1), args.Error(2)
	}
	return nil, false, common_persistence.ErrChatAgentParentUnavailable
}

func (m *MockPersistenceHandler) ReadDelegatedAgentMessages(ctx context.Context, in common_persistence.ChatAgentMessagePageInput) (*common_persistence.ChatAgentMessagePage, error) {
	if m.hasExpectation("ReadDelegatedAgentMessages") {
		args := m.Called(ctx, in)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.ChatAgentMessagePage), args.Error(1)
	}
	return &common_persistence.ChatAgentMessagePage{}, nil
}

func (m *MockPersistenceHandler) CountChatAgentSessionRuns(ctx context.Context, sessionIDs []uint64) (map[uint64]uint64, error) {
	if m.hasExpectation("CountChatAgentSessionRuns") {
		args := m.Called(ctx, sessionIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(map[uint64]uint64), args.Error(1)
	}
	return map[uint64]uint64{}, nil
}

func (m *MockPersistenceHandler) GetChildFollowUpRun(ctx context.Context, runID, sessionID uint64) (*common_persistence_models.ChatAgentRun, error) {
	if m.hasExpectation("GetChildFollowUpRun") {
		args := m.Called(ctx, runID, sessionID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentRun), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentRunNotFound
}

func (m *MockPersistenceHandler) LatestDiscardedChildMessages(ctx context.Context, childSessionIDs []uint64) (map[uint64]*common_persistence_models.ChatAgentMessageReceipt, error) {
	if m.hasExpectation("LatestDiscardedChildMessages") {
		args := m.Called(ctx, childSessionIDs)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(map[uint64]*common_persistence_models.ChatAgentMessageReceipt), args.Error(1)
	}
	return map[uint64]*common_persistence_models.ChatAgentMessageReceipt{}, nil
}

func (m *MockPersistenceHandler) ListDelegatedChatAgentSessionsNeedingRetirementByParent(ctx context.Context, parentSessionID, afterSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("ListDelegatedChatAgentSessionsNeedingRetirementByParent") {
		args := m.Called(ctx, parentSessionID, afterSessionID, limit)
		var children []*common_persistence_models.ChatAgentSession
		if args.Get(0) != nil {
			children = args.Get(0).([]*common_persistence_models.ChatAgentSession)
		}
		return children, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ListActiveChatAgentRunsBySessions(ctx context.Context, sessionIDs []uint64) ([]*common_persistence_models.ChatAgentRun, error) {
	if m.hasExpectation("ListActiveChatAgentRunsBySessions") {
		args := m.Called(ctx, sessionIDs)
		var runs []*common_persistence_models.ChatAgentRun
		if args.Get(0) != nil {
			runs = args.Get(0).([]*common_persistence_models.ChatAgentRun)
		}
		return runs, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ListChatAgentSessionsByParent(ctx context.Context, parentAgentSessionID uint64) ([]*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("ListChatAgentSessionsByParent") {
		args := m.Called(ctx, parentAgentSessionID)
		var sessions []*common_persistence_models.ChatAgentSession
		if args.Get(0) != nil {
			sessions = args.Get(0).([]*common_persistence_models.ChatAgentSession)
		}
		return sessions, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ListChatAgentSessionsByParentAfterID(ctx context.Context, parentAgentSessionID, afterSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("ListChatAgentSessionsByParentAfterID") {
		args := m.Called(ctx, parentAgentSessionID, afterSessionID, limit)
		var sessions []*common_persistence_models.ChatAgentSession
		if args.Get(0) != nil {
			sessions = args.Get(0).([]*common_persistence_models.ChatAgentSession)
		}
		return sessions, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ListActiveChatAgentSessionsByParentAfterID(ctx context.Context, parentSessionID, afterSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("ListActiveChatAgentSessionsByParentAfterID") {
		args := m.Called(ctx, parentSessionID, afterSessionID, limit)
		var children []*common_persistence_models.ChatAgentSession
		if args.Get(0) != nil {
			children = args.Get(0).([]*common_persistence_models.ChatAgentSession)
		}
		return children, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ListRecentChatAgentSessionsByParent(ctx context.Context, parentAgentSessionID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, uint32, error) {
	if m.hasExpectation("ListRecentChatAgentSessionsByParent") {
		args := m.Called(ctx, parentAgentSessionID, limit)
		var sessions []*common_persistence_models.ChatAgentSession
		if args.Get(0) != nil {
			sessions = args.Get(0).([]*common_persistence_models.ChatAgentSession)
		}
		return sessions, args.Get(1).(uint32), args.Error(2)
	}
	return nil, 0, nil
}

func (m *MockPersistenceHandler) ListDelegatedThreadRootsByChannel(ctx context.Context, channelID uint64, limit int) (common_persistence.DelegatedThreadRoots, error) {
	if m.hasExpectation("ListDelegatedThreadRootsByChannel") {
		args := m.Called(ctx, channelID, limit)
		var roots common_persistence.DelegatedThreadRoots
		if args.Get(0) != nil {
			roots = args.Get(0).(common_persistence.DelegatedThreadRoots)
		}
		return roots, args.Error(1)
	}
	return common_persistence.DelegatedThreadRoots{}, nil
}

func (m *MockPersistenceHandler) ListDelegatedThreadRootsAmong(ctx context.Context, channelID uint64, roots []uint64) ([]uint64, error) {
	if m.hasExpectation("ListDelegatedThreadRootsAmong") {
		args := m.Called(ctx, channelID, roots)
		var delegated []uint64
		if args.Get(0) != nil {
			delegated = args.Get(0).([]uint64)
		}
		return delegated, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetChatAgentSessionByIDForShare(ctx context.Context, sessionID uint64) (*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("GetChatAgentSessionByIDForShare") {
		args := m.Called(ctx, sessionID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentSession), args.Error(1)
	}
	return m.GetChatAgentSessionByID(ctx, sessionID)
}

func (m *MockPersistenceHandler) CountRunningDelegatedChildren(ctx context.Context, parentAgentSessionID uint64) (uint32, error) {
	if m.hasExpectation("CountRunningDelegatedChildren") {
		args := m.Called(ctx, parentAgentSessionID)
		return args.Get(0).(uint32), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) CountActiveDelegatedChildrenByDriver(ctx context.Context) (uint64, uint64, error) {
	if m.hasExpectation("CountActiveDelegatedChildrenByDriver") {
		args := m.Called(ctx)
		return args.Get(0).(uint64), args.Get(1).(uint64), args.Error(2)
	}
	return 0, 0, nil
}

func (m *MockPersistenceHandler) CountDelegatedChildren(ctx context.Context, parentAgentSessionID uint64) (uint32, error) {
	if m.hasExpectation("CountDelegatedChildren") {
		args := m.Called(ctx, parentAgentSessionID)
		return args.Get(0).(uint32), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) GetChatAgentSessionByParentCreateIdempotencyKey(ctx context.Context, parentAgentSessionID uint64, key string) (*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("GetChatAgentSessionByParentCreateIdempotencyKey") {
		args := m.Called(ctx, parentAgentSessionID, key)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentSession), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentSessionNotFound
}

func (m *MockPersistenceHandler) GetChatAgentSessionReplacing(ctx context.Context, childSessionID uint64) (*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("GetChatAgentSessionReplacing") {
		args := m.Called(ctx, childSessionID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentSession), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentSessionNotFound
}

func (m *MockPersistenceHandler) ListActiveChatAgentRunsBySession(ctx context.Context, agentSessionID uint64) ([]*common_persistence_models.ChatAgentRun, error) {
	if m.hasExpectation("ListActiveChatAgentRunsBySession") {
		args := m.Called(ctx, agentSessionID)
		var runs []*common_persistence_models.ChatAgentRun
		if args.Get(0) != nil {
			runs = args.Get(0).([]*common_persistence_models.ChatAgentRun)
		}
		return runs, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetLatestBroadcastReplyByRun(ctx context.Context, runID uint64, afterMessageID uint64) (*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("GetLatestBroadcastReplyByRun") {
		args := m.Called(ctx, runID, afterMessageID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatMessage), args.Error(1)
	}
	return nil, common_persistence.ErrChatMessageNotFound
}

func (m *MockPersistenceHandler) ListLatestDelegationReportsByThread(ctx context.Context, channelID uint64, rootMessageIDs []uint64) (map[uint64]*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("ListLatestDelegationReportsByThread") {
		args := m.Called(ctx, channelID, rootMessageIDs)
		var reports map[uint64]*common_persistence_models.ChatMessage
		if args.Get(0) != nil {
			reports = args.Get(0).(map[uint64]*common_persistence_models.ChatMessage)
		}
		return reports, args.Error(1)
	}
	return map[uint64]*common_persistence_models.ChatMessage{}, nil
}

func (m *MockPersistenceHandler) ListChatAgentRunsStartedBy(ctx context.Context, runID uint64) ([]*common_persistence_models.ChatAgentRun, error) {
	if m.hasExpectation("ListChatAgentRunsStartedBy") {
		args := m.Called(ctx, runID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.ChatAgentRun), args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) MarkChatMessageBroadcastToChannel(ctx context.Context, messageID uint64) error {
	if m.hasExpectation("MarkChatMessageBroadcastToChannel") {
		return m.Called(ctx, messageID).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ListChatAgentSessionsByChannel(ctx context.Context, channelID uint64, limit int) ([]*common_persistence_models.ChatAgentSession, error) {
	if m.hasExpectation("ListChatAgentSessionsByChannel") {
		args := m.Called(ctx, channelID, limit)
		var sessions []*common_persistence_models.ChatAgentSession
		if args.Get(0) != nil {
			sessions = args.Get(0).([]*common_persistence_models.ChatAgentSession)
		}
		return sessions, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) CreateOrCoalesceParentWakeRun(ctx context.Context, in common_persistence.ChatAgentParentWakeInput) (*common_persistence.ChatAgentSessionWakeResult, error) {
	if m.hasExpectation("CreateOrCoalesceParentWakeRun") {
		args := m.Called(ctx, in)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence.ChatAgentSessionWakeResult), args.Error(1)
	}
	return &common_persistence.ChatAgentSessionWakeResult{}, nil
}

func (m *MockPersistenceHandler) ListLatestChatAgentRunsByThread(ctx context.Context, channelID uint64, parentMessageIDs []uint64, perLocation int) ([]*common_persistence_models.ChatAgentRun, error) {
	if m.hasExpectation("ListLatestChatAgentRunsByThread") {
		args := m.Called(ctx, channelID, parentMessageIDs, perLocation)
		var runs []*common_persistence_models.ChatAgentRun
		if args.Get(0) != nil {
			runs = args.Get(0).([]*common_persistence_models.ChatAgentRun)
		}
		return runs, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetChatAgentRunProgress(ctx context.Context, runIDs []uint64) (map[uint64]*common_persistence.ChatAgentRunProgress, error) {
	if m.hasExpectation("GetChatAgentRunProgress") {
		args := m.Called(ctx, runIDs)
		var progress map[uint64]*common_persistence.ChatAgentRunProgress
		if args.Get(0) != nil {
			progress = args.Get(0).(map[uint64]*common_persistence.ChatAgentRunProgress)
		}
		return progress, args.Error(1)
	}
	return map[uint64]*common_persistence.ChatAgentRunProgress{}, nil
}

func (m *MockPersistenceHandler) ClaimWorkflowAgentRunDrive(ctx context.Context, runID uint64, owner string, duration time.Duration) (bool, error) {
	if m.hasExpectation("ClaimWorkflowAgentRunDrive") {
		args := m.Called(ctx, runID, owner, duration)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}
func (m *MockPersistenceHandler) ListRecoverableWorkflowToolCalls(ctx context.Context, runID uint64, now time.Time, limit int) ([]*common_persistence_models.ChatMessage, error) {
	if m.hasExpectation("ListRecoverableWorkflowToolCalls") {
		args := m.Called(ctx, runID, now, limit)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]*common_persistence_models.ChatMessage), args.Error(1)
	}
	return nil, nil
}
func (m *MockPersistenceHandler) ClaimWorkflowToolCall(ctx context.Context, runID, messageID uint64, owner string, expiresAt, now time.Time) (bool, error) {
	if m.hasExpectation("ClaimWorkflowToolCall") {
		args := m.Called(ctx, runID, messageID, owner, expiresAt, now)
		return args.Bool(0), args.Error(1)
	}
	return true, nil
}

func (m *MockPersistenceHandler) CountRejectedArgumentSteps(ctx context.Context, runID uint64) (uint32, error) {
	if m.hasExpectation("CountRejectedArgumentSteps") {
		args := m.Called(ctx, runID)
		return args.Get(0).(uint32), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) PrepareChatAgentStreamRecovery(ctx context.Context, runID uint64, step uint32, maxAttempts int) (*common_persistence.AgentStreamRecovery, error) {
	if !m.hasExpectation("PrepareChatAgentStreamRecovery") {
		return nil, common_persistence.ErrChatAgentRunStepSealInvalid
	}
	args := m.Called(ctx, runID, step, maxAttempts)
	result, _ := args.Get(0).(*common_persistence.AgentStreamRecovery)
	return result, args.Error(1)
}

func (m *MockPersistenceHandler) RecoverInterruptedChatAgentRunStep(ctx context.Context, runID uint64, maxAttempts int) (*common_persistence.AgentStreamRecovery, error) {
	if !m.hasExpectation("RecoverInterruptedChatAgentRunStep") {
		return nil, common_persistence.ErrChatAgentRunStepSealInvalid
	}
	args := m.Called(ctx, runID, maxAttempts)
	result, _ := args.Get(0).(*common_persistence.AgentStreamRecovery)
	return result, args.Error(1)
}

func (m *MockPersistenceHandler) FinalizeChatAgentSessionPrompt(ctx context.Context, sessionID uint64, prompt string, customization []byte) error {
	if m.hasExpectation("FinalizeChatAgentSessionPrompt") {
		return m.Called(ctx, sessionID, prompt, customization).Error(0)
	}
	return nil
}

// ============================================================================
// CHAT NOTIFICATION READERS (common_persistence.ChatNotificationPersistence)
// ============================================================================
//
// Defaults are safe when no expectation is set: empty snapshots, no members,
// no participant, unread notifications disabled, and no-op locks. They must
// never panic because most service tests never install notification expectations.

func emptyChatNotificationSources() *common_persistence.ChatNotificationSources {
	return &common_persistence.ChatNotificationSources{
		Messages:    map[uint64]*common_persistence_models.ChatMessage{},
		Channels:    map[uint64]*common_persistence_models.ChatChannel{},
		Users:       map[uint64]*common_persistence_models.User{},
		Memberships: map[[2]uint64]*common_persistence_models.ChatChannelParticipant{},
		Runs:        map[uint64]*common_persistence_models.ChatAgentRun{},
		Policies:    map[[2]uint64]*common_persistence_models.ChatAgentSessionConfigSnapshot{},
	}
}

func (m *MockPersistenceHandler) LoadChatNotificationSourcesWithOptions(ctx context.Context, candidates []common_persistence.ChatNotificationCandidate, options common_persistence.ChatNotificationSourceOptions) (*common_persistence.ChatNotificationSources, error) {
	if m.hasExpectation("LoadChatNotificationSourcesWithOptions") {
		args := m.Called(ctx, candidates, options)
		sources, _ := args.Get(0).(*common_persistence.ChatNotificationSources)
		return sources, args.Error(1)
	}
	return emptyChatNotificationSources(), nil
}

func (m *MockPersistenceHandler) ListChatNotificationRecipientIDs(ctx context.Context, channelID, after uint64, limit int) ([]uint64, error) {
	if m.hasExpectation("ListChatNotificationRecipientIDs") {
		args := m.Called(ctx, channelID, after, limit)
		ids, _ := args.Get(0).([]uint64)
		return ids, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) GetChatNotificationParticipant(ctx context.Context, channelID, userID uint64) (*common_persistence_models.ChatChannelParticipant, error) {
	if m.hasExpectation("GetChatNotificationParticipant") {
		args := m.Called(ctx, channelID, userID)
		participant, _ := args.Get(0).(*common_persistence_models.ChatChannelParticipant)
		return participant, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) LoadChatActivitySources(ctx context.Context, candidates []common_persistence.ChatActivityCandidate) (*common_persistence.ChatNotificationSources, error) {
	if m.hasExpectation("LoadChatActivitySources") {
		args := m.Called(ctx, candidates)
		sources, _ := args.Get(0).(*common_persistence.ChatNotificationSources)
		return sources, args.Error(1)
	}
	return emptyChatNotificationSources(), nil
}

func (m *MockPersistenceHandler) LoadChatNotificationDirectParticipants(ctx context.Context, channelIDs []uint64) (map[uint64][]uint64, error) {
	if m.hasExpectation("LoadChatNotificationDirectParticipants") {
		args := m.Called(ctx, channelIDs)
		participants, _ := args.Get(0).(map[uint64][]uint64)
		return participants, args.Error(1)
	}
	return map[uint64][]uint64{}, nil
}

func (m *MockPersistenceHandler) LoadChatNotificationDisplayUsers(ctx context.Context, userIDs []uint64) (map[uint64]*common_persistence_models.User, error) {
	if m.hasExpectation("LoadChatNotificationDisplayUsers") {
		args := m.Called(ctx, userIDs)
		users, _ := args.Get(0).(map[uint64]*common_persistence_models.User)
		return users, args.Error(1)
	}
	return map[uint64]*common_persistence_models.User{}, nil
}

func (m *MockPersistenceHandler) LockChatActivityEmission(ctx context.Context, channelID uint64, recipients []uint64) error {
	if m.hasExpectation("LockChatActivityEmission") {
		return m.Called(ctx, channelID, recipients).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ChatUnreadNotificationsEnabled(ctx context.Context, channelID uint64) (bool, error) {
	if m.hasExpectation("ChatUnreadNotificationsEnabled") {
		args := m.Called(ctx, channelID)
		return args.Bool(0), args.Error(1)
	}
	return false, nil
}

func (m *MockPersistenceHandler) ListChatNotificationRecipientCandidates(ctx context.Context, channelID, after uint64, limit int, source common_persistence_models.NotificationTransactionStamp, snapshot common_persistence_models.NotificationVisibilitySnapshot) ([]common_persistence.ChatNotificationRecipient, error) {
	if m.hasExpectation("ListChatNotificationRecipientCandidates") {
		args := m.Called(ctx, channelID, after, limit, source, snapshot)
		rows, _ := args.Get(0).([]common_persistence.ChatNotificationRecipient)
		return rows, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) FilterChatNotificationRecipientCandidates(ctx context.Context, channelID uint64, ids []uint64, source common_persistence_models.NotificationTransactionStamp, snapshot common_persistence_models.NotificationVisibilitySnapshot) ([]common_persistence.ChatNotificationRecipient, error) {
	if m.hasExpectation("FilterChatNotificationRecipientCandidates") {
		args := m.Called(ctx, channelID, ids, source, snapshot)
		rows, _ := args.Get(0).([]common_persistence.ChatNotificationRecipient)
		return rows, args.Error(1)
	}
	return nil, nil
}

func (m *MockPersistenceHandler) ValidateChatNotificationRecipient(ctx context.Context, channelID, messageID, recipientID uint64, observed common_persistence_models.NotificationTransactionStamp, source common_persistence.ChatNotificationSourceState) (bool, error) {
	if m.hasExpectation("ValidateChatNotificationRecipient") {
		args := m.Called(ctx, channelID, messageID, recipientID, observed, source)
		return args.Bool(0), args.Error(1)
	}
	return false, nil
}

func (m *MockPersistenceHandler) GetChatAgentRunWokenBy(ctx context.Context, runID, sessionID uint64) (*common_persistence_models.ChatAgentRun, error) {
	if m.hasExpectation("GetChatAgentRunWokenBy") {
		args := m.Called(ctx, runID, sessionID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).(*common_persistence_models.ChatAgentRun), args.Error(1)
	}
	return nil, common_persistence.ErrChatAgentRunNotFound
}

func (m *MockPersistenceHandler) GetChatAgentSessionPendingWakeMessageID(ctx context.Context, sessionID uint64) (uint64, error) {
	if m.hasExpectation("GetChatAgentSessionPendingWakeMessageID") {
		args := m.Called(ctx, sessionID)
		return args.Get(0).(uint64), args.Error(1)
	}
	return 0, nil
}

func (m *MockPersistenceHandler) ListUnconsumedChildMessageIDs(ctx context.Context, sessionID, throughID uint64) ([]uint64, error) {
	if m.hasExpectation("ListUnconsumedChildMessageIDs") {
		args := m.Called(ctx, sessionID, throughID)
		if args.Get(0) == nil {
			return nil, args.Error(1)
		}
		return args.Get(0).([]uint64), args.Error(1)
	}
	return nil, nil
}

// ============================================================================
// CHAT AGENT GRAPH PARK AND RESUME
// ============================================================================

func (m *MockPersistenceHandler) SetChatAgentGraphEventNotifier(notifier common_persistence.ChatAgentGraphEventNotifier) {
	if m.hasExpectation("SetChatAgentGraphEventNotifier") {
		m.Called(notifier)
	}
}

func (m *MockPersistenceHandler) AdvanceChatAgentGraphResumeRevision(ctx context.Context, sessionID uint64, kind proto_models.GraphEventKind) error {
	if m.hasExpectation("AdvanceChatAgentGraphResumeRevision") {
		return m.Called(ctx, sessionID, kind).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ParkChatAgentGraph(ctx context.Context, rootSessionID, revision uint64) error {
	if m.hasExpectation("ParkChatAgentGraph") {
		return m.Called(ctx, rootSessionID, revision).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) UnparkChatAgentGraph(ctx context.Context, rootSessionID uint64) error {
	if m.hasExpectation("UnparkChatAgentGraph") {
		return m.Called(ctx, rootSessionID).Error(0)
	}
	return nil
}

func (m *MockPersistenceHandler) ListUnparkableChatAgentGraphs(ctx context.Context, afterRootSessionID uint64, limit int) ([]uint64, error) {
	if m.hasExpectation("ListUnparkableChatAgentGraphs") {
		args := m.Called(ctx, afterRootSessionID, limit)
		roots, _ := args.Get(0).([]uint64)
		return roots, args.Error(1)
	}
	return []uint64{}, nil
}
