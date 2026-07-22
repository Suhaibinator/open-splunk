package sender

import "time"

const (
	defaultBackoffInitial    = 500 * time.Millisecond
	defaultBackoffMax        = 30 * time.Second
	defaultBackoffMultiplier = 2.0
)

// backoffDelay computes the reconnect delay for a given zero-based attempt using
// bounded exponential growth with subtractive jitter. frac is a random value in
// [0,1) (injected for deterministic tests). The result is always in the range
// (0, Max]: growth is capped at Max and jitter can only reduce a delay, so the
// backoff is provably bounded.
func backoffDelay(p BackoffPolicy, attempt int, frac float64) time.Duration {
	initial := p.Initial
	if initial <= 0 {
		initial = defaultBackoffInitial
	}
	max := p.Max
	if max <= 0 {
		max = defaultBackoffMax
	}
	if max < initial {
		max = initial
	}
	multiplier := p.Multiplier
	if multiplier < 1 {
		multiplier = defaultBackoffMultiplier
	}

	base := float64(initial)
	limit := float64(max)
	for i := 0; i < attempt; i++ {
		base *= multiplier
		if base >= limit {
			base = limit
			break
		}
	}
	if base > limit {
		base = limit
	}

	jitter := p.Jitter
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	if frac < 0 {
		frac = 0
	}
	if frac >= 1 {
		frac = 0.999999
	}
	// Subtract up to jitter fraction of base; keeps the delay in (0, base].
	delay := base * (1 - jitter*frac)
	if delay < 0 {
		delay = 0
	}
	return time.Duration(delay)
}
