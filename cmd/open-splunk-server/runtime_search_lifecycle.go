package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"

	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/knowledgepreview"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/searchws"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"go.uber.org/zap"
)

type runtimeSearchAuditAppender interface {
	knowledgeAuditAppender
	control.ServerSettingsMutationAuditAppender
}

type runtimeSearchLifecycleConfig struct {
	ctx                     context.Context
	controlDB               *control.DB
	connection              clickhousedriver.Conn
	clickHouseOptions       *clickhousedriver.Options
	indexReads              *indexReadLifecycle
	sequencer               visibility.Sequencer
	history                 *searchhistory.Store
	auditAppender           runtimeSearchAuditAppender
	featureOperations       *runtimeFeatureOperations
	exportSettings          exportRuntimeSettings
	masterKeyPath           string
	searchArtifactDirectory string
	exportArtifactDirectory string
	tenantID                string
	ownerID                 string
	logger                  *zap.Logger
	closeTimeout            time.Duration
}

type runtimeSearchLifecycle struct {
	indexStatistics       *internalclickhouse.IndexStatisticsReader
	serverSettings        *runtimeServerSettings
	artifacts             *searchartifacts.Store
	scheduledReports      *runtimeScheduledReportLifecycle
	knowledge             runtimeKnowledgeManagement
	jobs                  *searchjobs.Manager
	preview               *knowledgepreview.Service
	inspection            *runtimeSearchInspection
	exports               *exportjobs.Manager
	analysis              *runtimeSearchAnalysis
	webSocket             *searchws.Service
	logger                *zap.Logger
	closeTimeout          time.Duration
	closeWebSocket        func() error
	closeAnalysis         func() error
	closeExports          func() error
	closeInspection       func() error
	closeSearchJobs       func() error
	closeScheduledReports func()
	closeSearchArtifacts  func() error
	closeOnce             sync.Once
}

func newRuntimeSearchLifecycle(config runtimeSearchLifecycleConfig) (_ *runtimeSearchLifecycle, err error) {
	lifecycle := &runtimeSearchLifecycle{logger: config.logger, closeTimeout: config.closeTimeout}
	defer func() {
		if err != nil {
			lifecycle.Close()
		}
	}()
	lifecycle.indexStatistics, err = newRuntimeIndexStatisticsReader(
		config.connection,
		internalclickhouse.IndexStatisticsConfig{Database: "open_splunk", Table: "events"},
		config.indexReads.admission,
	)
	if err != nil {
		return nil, fmt.Errorf("create index statistics reader: %w", err)
	}
	settingsStore, err := control.NewServerSearchSettingsStore(config.controlDB, config.tenantID, config.auditAppender)
	if err != nil {
		return nil, fmt.Errorf("create server settings store: %w", err)
	}
	initialSettings, err := settingsStore.Get(config.ctx)
	if err != nil {
		return nil, fmt.Errorf("load server settings: %w", err)
	}
	appearanceStore, err := control.NewServerAppearanceSettingsStore(config.controlDB, config.tenantID, config.auditAppender)
	if err != nil {
		return nil, fmt.Errorf("create server appearance store: %w", err)
	}
	initialAppearance, err := appearanceStore.Get(config.ctx)
	if err != nil {
		return nil, fmt.Errorf("load server appearance settings: %w", err)
	}
	limitSource, err := searchlimits.NewSource(initialSettings.Limits)
	if err != nil {
		return nil, fmt.Errorf("load server settings policy: %w", err)
	}
	queryConfig, err := queryexec.ConfigFromPolicy(initialSettings.Limits)
	if err != nil {
		return nil, fmt.Errorf("derive server settings query policy: %w", err)
	}
	executor, err := newRuntimeQueryExecutor(config.connection, queryConfig, config.indexReads.admission)
	if err != nil {
		return nil, fmt.Errorf("create query executor: %w", err)
	}
	if err := executor.BindLimitSource(limitSource); err != nil {
		return nil, fmt.Errorf("bind query executor search limits: %w", err)
	}
	compiler := internalclickhouse.Compiler{Database: "open_splunk", Table: "events"}
	historyJournal, err := searchhistory.NewJobJournal(config.history)
	if err != nil {
		return nil, fmt.Errorf("create search-history job journal: %w", err)
	}
	lifecycle.artifacts, err = searchartifacts.New(config.ctx, searchartifacts.Config{
		DB: config.controlDB.SQLDB(), Directory: config.searchArtifactDirectory, Observer: config.featureOperations,
	})
	if err != nil {
		return nil, fmt.Errorf("open durable search artifacts: %w", err)
	}
	lifecycle.closeSearchArtifacts = lifecycle.artifacts.Close
	lifecycle.scheduledReports, err = newRuntimeScheduledReportLifecycle()
	if err != nil {
		return nil, err
	}
	lifecycle.closeScheduledReports = lifecycle.scheduledReports.Close
	jobJournal := searchjobs.NewCompositeJournal(lifecycle.artifacts, historyJournal, lifecycle.scheduledReports.journal)
	lifecycle.knowledge, err = newRuntimeKnowledgeManagement(config.ctx, config.controlDB, config.masterKeyPath, config.auditAppender)
	if err != nil {
		return nil, err
	}
	lifecycle.jobs, err = searchjobs.New(searchjobs.Config{
		Executor: executor, Snapshotter: visibilitySnapshotter{sequencer: config.sequencer}, Journal: jobJournal,
		KnowledgeResolver: lifecycle.knowledge.resolver, LookupResolver: lifecycle.knowledge.lookupResolver,
		OnJournalError: func(err error) { config.logger.Warn("persist search-job history", zap.Error(err)) },
		OnFailure:      func(notification searchjobs.FailureNotification) { logSearchFailure(config.logger, notification) },
		Compiler:       compiler, MaxConcurrent: int(searchlimits.SupportedRange().Maximum.MaxConcurrent),
		MaxRows: initialSettings.Limits.MaxResultRows, MaxBytes: initialSettings.Limits.MaxResultBytes,
		MaxTotalBytes: initialSettings.Limits.MaxTotalResultBytes, MaxRuntime: initialSettings.Limits.MaxRuntime,
		RetentionTTL: initialSettings.Limits.ResultRetention, LimitSource: limitSource,
	})
	if err != nil {
		return nil, fmt.Errorf("create search job manager: %w", err)
	}
	lifecycle.closeSearchJobs = lifecycle.jobs.Close
	lifecycle.serverSettings = &runtimeServerSettings{
		store: settingsStore, source: limitSource, jobs: lifecycle.jobs, current: initialSettings,
		appearanceStore: appearanceStore, appearance: initialAppearance,
	}
	lifecycle.preview, err = knowledgepreview.NewService(knowledgepreview.Config{
		Searches: lifecycle.jobs, Writer: lifecycle.knowledge.writer,
		Compiler: knowledgepreview.ProductionCompilerAdapter{Compiler: compiler}, Executor: executor,
	})
	if err != nil {
		return nil, fmt.Errorf("create knowledge preview service: %w", err)
	}
	lifecycle.inspection, err = newRuntimeSearchInspection(runtimeSearchInspectionConfig{
		Searches: lifecycle.jobs, Compiler: compiler, ClickHouseOptions: config.clickHouseOptions,
	})
	if err != nil {
		return nil, fmt.Errorf("create search inspection services: %w", err)
	}
	lifecycle.closeInspection = lifecycle.inspection.Close
	exportExecutor, err := newRuntimeQueryExecutor(config.connection, config.exportSettings.queryExecutorConfig(), config.indexReads.admission)
	if err != nil {
		return nil, fmt.Errorf("create export query executor: %w", err)
	}
	exportSource, err := exportjobs.NewReexecutionSource(exportjobs.ReexecutionSourceConfig{
		Searches: lifecycle.jobs, Executor: exportExecutor, Compiler: compiler,
		MaxRuntime: config.exportSettings.reexecutionMaxRuntime,
	})
	if err != nil {
		return nil, fmt.Errorf("create export re-execution source: %w", err)
	}
	lifecycle.exports, err = exportjobs.New(config.exportSettings.managerConfig(
		exportSource, config.exportArtifactDirectory, newExportCleanupErrorReporter(config.logger),
	))
	if err != nil {
		return nil, fmt.Errorf("create export manager: %w", err)
	}
	lifecycle.closeExports = lifecycle.exports.Close
	lifecycle.analysis, err = newRuntimeSearchAnalysis(runtimeSearchAnalysisConfig{
		Searches: lifecycle.jobs, Compiler: compiler, Executor: executor,
		FieldCursorScope: "open-splunk-server/search-fields",
	})
	if err != nil {
		return nil, fmt.Errorf("create search analysis services: %w", err)
	}
	lifecycle.closeAnalysis = lifecycle.analysis.Close
	lifecycle.webSocket, err = searchws.New(searchws.Config{
		Searches: lifecycle.jobs, Exports: lifecycle.exports,
		Access: searchjobs.AccessScope{TenantID: config.tenantID, OwnerID: config.ownerID},
	})
	if err != nil {
		return nil, fmt.Errorf("create search websocket service: %w", err)
	}
	lifecycle.closeWebSocket = func() error {
		ctx, cancel := context.WithTimeout(context.Background(), lifecycle.closeTimeout)
		defer cancel()
		return lifecycle.webSocket.Close(ctx)
	}
	return lifecycle, nil
}

func (lifecycle *runtimeSearchLifecycle) Close() {
	if lifecycle == nil {
		return
	}
	lifecycle.closeOnce.Do(func() {
		logger := lifecycle.logger
		if logger == nil {
			logger = zap.NewNop()
		}
		if lifecycle.closeWebSocket != nil {
			if err := lifecycle.closeWebSocket(); err != nil {
				logger.Warn("close search websocket service", zap.Error(err))
			}
		}
		if lifecycle.closeAnalysis != nil {
			if err := lifecycle.closeAnalysis(); err != nil {
				logger.Warn("close search analysis services", zap.Error(err))
			}
		}
		if lifecycle.closeExports != nil {
			if err := lifecycle.closeExports(); err != nil {
				logger.Warn("close exports", zap.Error(err))
			}
		}
		if lifecycle.closeInspection != nil {
			if err := lifecycle.closeInspection(); err != nil {
				logger.Warn("close search inspection services", zap.Error(err))
			}
		}
		if lifecycle.closeSearchJobs != nil {
			if err := lifecycle.closeSearchJobs(); err != nil {
				logger.Warn("close search jobs", zap.Error(err))
			}
		}
		if lifecycle.closeScheduledReports != nil {
			lifecycle.closeScheduledReports()
		}
		if lifecycle.closeSearchArtifacts != nil {
			if err := lifecycle.closeSearchArtifacts(); err != nil {
				logger.Warn("close durable search artifacts", zap.Error(err))
			}
		}
	})
}
