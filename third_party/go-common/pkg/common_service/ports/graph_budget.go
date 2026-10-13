package ports

import "time"

// GraphBudget is the optional budget pool of one delegation graph (a root session and its
// delegated children). Every run in the graph draws on it in addition to its own per-run limits,
// and its wake and follow-up runs carry no library work ceilings. Every limit must be positive.
type GraphBudget struct {
	// MaxGenerations counts per-run generation units: one per model step and one per compaction.
	MaxGenerations uint32
	// MaxToolCalls counts the tool requests charged per step.
	MaxToolCalls uint32
	// MaxActiveDuration bounds the sum of measured drive time, best effort: a drive's deadline is
	// its share of what remains, and cleanup after the deadline is measured too.
	MaxActiveDuration time.Duration
}
