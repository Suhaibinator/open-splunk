package controlplane

import "github.com/Suhaibinator/go-common/pkg/genai/retryclassify"

// Classification aliases retain the control-plane API while sharing classification with agents.
type RetryReason = retryclassify.RetryReason
type RetryDecision = retryclassify.RetryDecision
type ShouldRetryFunc = retryclassify.ShouldRetryFunc
type ProviderCallGateError = retryclassify.ProviderCallGateError
type HTTPStatusError = retryclassify.HTTPStatusError

const (
	RetryReasonUnspecified             = retryclassify.RetryReasonUnspecified
	RetryReasonContextCanceled         = retryclassify.RetryReasonContextCanceled
	RetryReasonContextDeadlineExceeded = retryclassify.RetryReasonContextDeadlineExceeded
	RetryReasonHTTPStatus              = retryclassify.RetryReasonHTTPStatus
	RetryReasonHTTPStatusTerminal      = retryclassify.RetryReasonHTTPStatusTerminal
	RetryReasonNetworkTimeout          = retryclassify.RetryReasonNetworkTimeout
	RetryReasonNetworkTemporary        = retryclassify.RetryReasonNetworkTemporary
	RetryReasonMessageHTTPStatus       = retryclassify.RetryReasonMessageHTTPStatus
	RetryReasonMessageRateLimit        = retryclassify.RetryReasonMessageRateLimit
	RetryReasonMessageUnavailable      = retryclassify.RetryReasonMessageUnavailable
	RetryReasonMessageTimeout          = retryclassify.RetryReasonMessageTimeout
	RetryReasonSchemaGeneration        = retryclassify.RetryReasonSchemaGeneration
	RetryReasonNotRetryable            = retryclassify.RetryReasonNotRetryable
	RetryReasonStreamCommitted         = retryclassify.RetryReasonStreamCommitted
)

func ClassifyRetryableError(err error) RetryDecision {
	return retryclassify.ClassifyRetryableError(err)
}

func IsProviderCallGateError(err error) bool { return retryclassify.IsProviderCallGateError(err) }

func IsRetryableHTTPStatus(status int) bool { return retryclassify.IsRetryableHTTPStatus(status) }
