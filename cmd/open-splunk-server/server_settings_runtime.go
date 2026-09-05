package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/uipalette"
)

// runtimeServerSettings serializes versioned writes and publishes a committed
// policy to every live search consumer before the successful update returns.
// The instance UI palette shares the object and its mutex: bootstrap reads
// both snapshots, and the two singletons keep independent version lines.
type runtimeServerSettings struct {
	mu              sync.RWMutex
	store           *control.ServerSearchSettingsStore
	source          *searchlimits.Source
	jobs            *searchjobs.Manager
	current         control.ServerSearchSettings
	appearanceStore *control.ServerAppearanceSettingsStore
	appearance      control.ServerAppearanceSettings
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

func (settings *runtimeServerSettings) GetAppearance(ctx context.Context) (control.ServerAppearanceSettings, error) {
	if ctx == nil {
		return control.ServerAppearanceSettings{}, fmt.Errorf("%w: appearance context is required", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return control.ServerAppearanceSettings{}, err
	}
	return settings.CurrentAppearance(), nil
}

func (settings *runtimeServerSettings) CurrentAppearance() control.ServerAppearanceSettings {
	settings.mu.RLock()
	defer settings.mu.RUnlock()
	return settings.appearance
}

// UpdateAppearance writes the palette durably and only then replaces the
// snapshot bootstrap serves, so a failed transaction leaves the live palette
// untouched. The search-limits source and job manager are not touched.
func (settings *runtimeServerSettings) UpdateAppearance(
	ctx context.Context,
	expectedVersion uint64,
	palette uipalette.Palette,
) (control.ServerAppearanceSettings, error) {
	settings.mu.Lock()
	defer settings.mu.Unlock()
	if err := uipalette.Validate(palette); err != nil {
		return control.ServerAppearanceSettings{}, fmt.Errorf("%w: %w", control.ErrInvalidArgument, err)
	}
	updated, err := settings.appearanceStore.Update(ctx, expectedVersion, palette)
	if err != nil {
		return control.ServerAppearanceSettings{}, err
	}
	settings.appearance = updated
	return updated, nil
}
