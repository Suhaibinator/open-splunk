package controlplane

import (
	"fmt"
	"math"
	"time"
)

// RetryPolicy defines attempt-count, total-timeout, and backoff behavior for one
// resolved route, independent of rate limiting. It is the moved equivalent of the
// GradeThis route-level retry policy (total timeout, maximum attempts, initial and
// maximum backoff).
type RetryPolicy struct {
	// TotalTimeout bounds logical execution alongside any earlier caller deadline.
	TotalTimeout time.Duration
	// MaxAttempts is the maximum number of provider attempts (one-based). A value of
	// zero or less is treated as a single attempt at execution time.
	MaxAttempts int
	// InitialBackoff is the first retry backoff before jitter.
	InitialBackoff time.Duration
	// MaxBackoff caps the jittered backoff between attempts.
	MaxBackoff time.Duration
}

// BucketRateLimitPolicy defines one token-bucket rate limit: a starting/maximum
// capacity, a refill interval, the units added each interval, and the maximum time
// a caller waits for capacity before failing.
type BucketRateLimitPolicy struct {
	Capacity       int
	RefillInterval time.Duration
	RefillUnits    int
	WaitCap        time.Duration
}

// ModelRateLimitPolicy defines the optional, all-or-none request and token buckets
// shared by every route that targets one inference model. Each bucket is present
// only when its Has* flag is set; a set flag requires a fully valid bucket.
type ModelRateLimitPolicy struct {
	RequestLimit    BucketRateLimitPolicy
	HasRequestLimit bool
	TokenLimit      BucketRateLimitPolicy
	HasTokenLimit   bool
}

// ValidateRetryPolicy reports whether a retry policy is internally consistent.
func ValidateRetryPolicy(policy RetryPolicy) error {
	if policy.TotalTimeout <= 0 {
		return fmt.Errorf("total timeout must be > 0")
	}
	if policy.MaxAttempts <= 0 {
		return fmt.Errorf("max attempts must be > 0")
	}
	if policy.InitialBackoff <= 0 {
		return fmt.Errorf("initial backoff must be > 0")
	}
	if policy.MaxBackoff <= 0 {
		return fmt.Errorf("max backoff must be > 0")
	}
	if policy.MaxBackoff < policy.InitialBackoff {
		return fmt.Errorf("max backoff must be >= initial backoff")
	}
	return nil
}

// ValidateTemperature performs only provider-neutral structural validation. Exact support and
// bounds are provider/model-specific and are enforced by Registry.ValidateModelRoute.
func ValidateTemperature(temperature *float64) error {
	if temperature == nil {
		return nil
	}
	if math.IsNaN(*temperature) || math.IsInf(*temperature, 0) {
		return fmt.Errorf("temperature must be finite")
	}
	return nil
}

// ValidateBucketRateLimitPolicy reports whether one token-bucket policy is valid.
func ValidateBucketRateLimitPolicy(policy BucketRateLimitPolicy) error {
	if policy.Capacity <= 0 {
		return fmt.Errorf("bucket capacity must be > 0")
	}
	if policy.RefillInterval <= 0 {
		return fmt.Errorf("bucket refill interval must be > 0")
	}
	if policy.RefillUnits <= 0 {
		return fmt.Errorf("bucket refill tokens must be > 0")
	}
	if policy.WaitCap <= 0 {
		return fmt.Errorf("bucket wait cap must be > 0")
	}
	return nil
}

// ValidateModelRateLimitPolicy validates each present bucket. A cleared bucket
// (Has* flag false) is always valid regardless of its zero fields.
func ValidateModelRateLimitPolicy(policy ModelRateLimitPolicy) error {
	if policy.HasRequestLimit {
		if err := ValidateBucketRateLimitPolicy(policy.RequestLimit); err != nil {
			return fmt.Errorf("request limit: %w", err)
		}
	}
	if policy.HasTokenLimit {
		if err := ValidateBucketRateLimitPolicy(policy.TokenLimit); err != nil {
			return fmt.Errorf("token limit: %w", err)
		}
	}
	return nil
}
