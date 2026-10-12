// Package notificationnative limits native notification integration to the
// common_service implementation. Importing applications use ordinary producers.
package notificationnative

import "context"

// Capability permits registration of a native materialization adapter. Its zero
// value grants no capability, and its constructor is internal to common_service.
type Capability struct{ native bool }

func Native() Capability { return Capability{native: true} }

func (c Capability) Valid() bool { return c.native }

// PreparedInput carries native database checks across the package boundary.
// Applications cannot import this package or register a native adapter.
type PreparedInput[T any] struct {
	Input               T
	FreshCheck          func(context.Context) (bool, error)
	SkipRetirementFence bool
}
