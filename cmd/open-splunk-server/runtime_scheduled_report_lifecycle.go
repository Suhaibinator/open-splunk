package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"github.com/Suhaibinator/open-splunk/internal/scheduler"
)

type runtimeScheduledReportLifecycle struct {
	journal      *runtimeScheduledReportJournal
	service      *scheduledreports.Service
	engine       *scheduler.Engine
	closeJournal func()
	closeOnce    sync.Once
}

func newRuntimeScheduledReportLifecycle() (*runtimeScheduledReportLifecycle, error) {
	journal, err := newRuntimeScheduledReportJournal(runtimeScheduledReportJournalOptions{})
	if err != nil {
		return nil, fmt.Errorf("create scheduled-report completion journal: %w", err)
	}
	return &runtimeScheduledReportLifecycle{journal: journal, closeJournal: journal.Close}, nil
}

func (lifecycle *runtimeScheduledReportLifecycle) Configure(
	ctx context.Context,
	controlDB *control.DB,
	admission *runtimeTrustedSearchAdmission,
	observer *runtimeFeatureOperations,
) error {
	repository, err := scheduledreports.NewRepository(controlDB.GORMDB(), scheduledreports.RepositoryOptions{})
	if err != nil {
		return fmt.Errorf("open scheduled reports: %w", err)
	}
	lifecycle.service, err = scheduledreports.NewService(scheduledreports.ServiceOptions{
		Store: repository, Admitter: admission, Observer: observer,
	})
	if err != nil {
		return fmt.Errorf("create scheduled report service: %w", err)
	}
	if _, err := lifecycle.service.Recover(ctx); err != nil {
		return fmt.Errorf("recover scheduled reports: %w", err)
	}
	if err := lifecycle.journal.Bind(lifecycle.service); err != nil {
		return fmt.Errorf("bind scheduled-report completion journal: %w", err)
	}
	lifecycle.engine, err = scheduler.NewEngine(scheduler.EngineOptions{Stepper: lifecycle.service})
	if err != nil {
		return fmt.Errorf("create scheduled report scheduler: %w", err)
	}
	return nil
}

func (lifecycle *runtimeScheduledReportLifecycle) Close() {
	if lifecycle == nil {
		return
	}
	lifecycle.closeOnce.Do(func() {
		if lifecycle.closeJournal != nil {
			lifecycle.closeJournal()
		}
	})
}
