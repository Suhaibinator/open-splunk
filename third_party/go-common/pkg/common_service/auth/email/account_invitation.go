package email

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"html"
	"net/url"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	tokenService "github.com/Suhaibinator/go-common/pkg/common_service/token"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

const AccountInvitationPath = "/auth/email/accept-invitation"

// SetInitialPassword sets up trusted password authentication without sending credentials.
func (h *EmailAuthHandler[U]) SetInitialPassword(ctx context.Context, userID uint64, password string) error {
	if _, ok := common_persistence.DatabasePostCommitHookRegistrarFromContext(ctx); !ok {
		return apperrors.ErrTransactionNotFound
	}
	user, err := h.persistenceHandler.GetUserForUpdate(ctx, userID)
	if err != nil {
		return err
	}
	if user.IsBot || user.Banned {
		return apperrors.ErrInvalidRequest
	}
	existing, err := h.persistenceHandler.GetUserPassword(ctx, userID)
	if err != nil {
		return err
	}
	if existing != nil {
		return apperrors.ErrPasswordAuthAlreadyExists
	}
	salt, err := GenerateSalt()
	if err != nil {
		return err
	}
	if err := h.persistenceHandler.CreateUserPassword(ctx, userID, base64.StdEncoding.EncodeToString(HashPassword(password, salt)), base64.StdEncoding.EncodeToString(salt)); err != nil {
		return err
	}
	if _, err := h.persistenceHandler.MarkEmailVerified(ctx, userID); err != nil {
		return err
	}
	if h.userCallbacks != nil && h.userCallbacks.SyncCallbacks != nil && h.userCallbacks.SyncCallbacks.OnEmailVerified != nil {
		if err := h.userCallbacks.SyncCallbacks.OnEmailVerified(ctx, common_service_models.UserIdType(userID), true); err != nil {
			return err
		}
	}
	return nil
}

// CreateAccountInvitation stores a single-use invitation and schedules delivery only after commit.
func (h *EmailAuthHandler[U]) CreateAccountInvitation(ctx context.Context, userID uint64, frontendOrigin string, resend bool) error {
	registrar, ok := common_persistence.DatabasePostCommitHookRegistrarFromContext(ctx)
	if !ok {
		return apperrors.ErrTransactionNotFound
	}
	if h.emailClient == nil {
		return apperrors.ErrInternalError
	}
	user, err := h.persistenceHandler.GetUserForUpdate(ctx, userID)
	if err != nil {
		return err
	}
	// Acceptance compares the normalized account address, so issue and resend use the same form.
	boundEmail := common_persistence_models.NormalizeUserEmail(user.Email)
	if user.IsBot || user.Banned || boundEmail == "" {
		return apperrors.ErrInvalidRequest
	}
	password, err := h.persistenceHandler.GetUserPassword(ctx, userID)
	if err != nil {
		return err
	}
	if password != nil {
		return apperrors.ErrPasswordAuthAlreadyExists
	}
	if resend {
		pending, err := h.persistenceHandler.HasPendingAccountInvitation(ctx, userID, boundEmail)
		if err != nil {
			return err
		}
		if !pending {
			return apperrors.ErrInvalidRequest
		}
	}
	if err := h.persistenceHandler.RevokeAllUserTokens(ctx, userID, proto_models.TokenType_TOKEN_TYPE_ACCOUNT_INVITATION); err != nil {
		return err
	}
	view := h.runtimeConfig()
	token, err := h.tokenService.GenerateSecurityToken(ctx, tokenService.SecurityTokenParams{UserID: userID, TokenType: proto_models.TokenType_TOKEN_TYPE_ACCOUNT_INVITATION, AuthMethod: proto_models.AuthProvider_AUTH_PROVIDER_EMAIL, ExpiresIn: view.VerificationTokenValidity(), FrontendOrigin: frontendOrigin, BoundEmail: boundEmail})
	if err != nil {
		return err
	}
	link := strings.TrimRight(frontendBase(view, frontendOrigin), "/") + AccountInvitationPath + "?token=" + url.QueryEscape(token)
	body := fmt.Sprintf(`<h2>Your account invitation</h2><p>An administrator created an account for you. Choose a password to verify your email and finish setup:</p><p><a href="%s">Accept invitation</a></p><p>This link expires in %s.</p>`, html.EscapeString(link), formatDuration(view.VerificationTokenValidity()))
	registrar.AddPostCommitHook(func(hookCtx context.Context) error {
		return h.emailClient.SendHTMLEmail(hookCtx, user.Email, "You're invited to create your account", body)
	})
	return nil
}

// AcceptAccountInvitation serializes against resends and identity changes using the owning user row.
// The invitation is consumed under the account lock order (a token of another type reads as not
// found), and its binding is checked afterwards in the same transaction. That is safe because every
// rejection is returned: EmailAcceptInvitation propagates it, so the request transaction rolls
// back and the invitation stays unused.
func (h *EmailAuthHandler[U]) AcceptAccountInvitation(ctx context.Context, token, password string) (uint64, error) {
	if _, ok := common_persistence.DatabasePostCommitHookRegistrarFromContext(ctx); !ok {
		return 0, apperrors.ErrTransactionNotFound
	}
	invitation, err := h.persistenceHandler.ConsumeSecurityToken(ctx, token, proto_models.TokenType_TOKEN_TYPE_ACCOUNT_INVITATION)
	switch {
	case errors.Is(err, common_persistence.ErrTokenAlreadyUsed):
		return 0, apperrors.ErrTokenUsed
	case errors.Is(err, common_persistence.ErrTokenRevoked), errors.Is(err, common_persistence.ErrTokenExpired):
		return 0, apperrors.ErrTokenExpired
	case errors.Is(err, common_persistence.ErrSessionTokenNotFound), errors.Is(err, common_persistence.ErrUserNotFound):
		return 0, apperrors.ErrInvalidToken
	case err != nil:
		return 0, fmt.Errorf("consume account invitation: %w", err)
	}
	// Already locked by the consumption; this reads the locked row.
	user, err := h.persistenceHandler.GetUserForUpdate(ctx, invitation.UserID)
	if err != nil {
		if errors.Is(err, common_persistence.ErrUserNotFound) || errors.Is(err, apperrors.ErrUserNotFound) {
			return 0, apperrors.ErrInvalidToken
		}
		return 0, fmt.Errorf("reload account invitation user: %w", err)
	}
	if user.IsBot || user.Banned || invitation.BoundEmail == "" || common_persistence_models.NormalizeUserEmail(user.Email) != invitation.BoundEmail {
		return 0, apperrors.ErrInvalidToken
	}
	if err := h.SetInitialPassword(ctx, user.ID, password); err != nil {
		return 0, err
	}
	return user.ID, nil
}
