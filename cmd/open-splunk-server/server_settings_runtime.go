package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

// runtimeServerSettings serializes versioned writes and publishes a committed
// policy to every live search consumer before the successful update returns.
type runtimeServerSettings struct {
	mu      sync.RWMutex
	store   *control.ServerSearchSettingsStore
	source  *searchlimits.Source
	jobs    *searchjobs.Manager
	current control.ServerSearchSettings
}

func (settings *runtimeServerSettings) Get(ctx context.Context) (control.ServerSearchSettings, error) {
	if ctx == nil {
		return control.ServerSearchSettings{}, fmt.Errorf("%w: settings context is required", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return control.ServerSearchSettings{}, err
	}
	return settings.Current(), nil
}

func (settings *runtimeServerSettings) Current() control.ServerSearchSettings {
	settings.mu.RLock()
	defer settings.mu.RUnlock()
	return settings.current
}

func (settings *runtimeServerSettings) Update(
	ctx context.Context,
	expectedVersion uint64,
	limits searchlimits.Policy,
) (control.ServerSearchSettings, error) {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	if err := searchlimits.Validate(limits); err != nil {
		return control.ServerSearchSettings{}, err
	}
	updated, err := settings.store.Update(ctx, expectedVersion, limits)
	if err != nil {
		return control.ServerSearchSettings{}, err
	}
	// One atomic source is authoritative for executor defaults, admissions, and
	// scheduler concurrency; publish only after the durable transaction.
	if err := settings.source.Store(limits); err != nil {
		panic("validated search-limit policy could not be published")
	}
	settings.current = updated
	settings.jobs.LimitsChanged()
	return updated, nil
}
