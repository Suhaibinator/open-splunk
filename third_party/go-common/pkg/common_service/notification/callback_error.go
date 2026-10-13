package notification

import (
	"errors"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// callbackFailure keeps an application's error available to errors.Is without
// exposing its text, fields, or error chain to the HTTP logging boundary.
// It deliberately has no Unwrap or As method.
type callbackFailure struct {
	stage string
	cause error
}

func (e *callbackFailure) Error() string        { return "notification callback failed at " + e.stage }
func (e *callbackFailure) Is(target error) bool { return errors.Is(e.cause, target) }

func safeCallbackError(stage string, cause error) error {
	if cause == nil {
		return nil
	}
	// Stages are fixed code-owned values; unknown input never enters a log message.
	switch stage {
	case "producer_validate", "producer_snapshot", "producer_prepare", "evaluate_sources", "authorize", "resolved", "validate", "group_key", "activity_scope", "policy_provider", "fanout_mode", "global_preferences", "suppress_delivery", "delivery_preference", "render_email", "prepare_email", "send_email", "publish":
	default:
		stage = "unknown"
	}
	return appLogger.WithErrorFields(&callbackFailure{stage: stage, cause: cause}, zap.String(logkeys.Stage, stage))
}

// SourceAuthorizationError hides private callback details while preserving
// errors.Is classification for native producers' pre-emission eligibility checks.
func SourceAuthorizationError(cause error) error {
	return safeCallbackError("evaluate_sources", cause)
}
