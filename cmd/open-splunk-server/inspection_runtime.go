package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/searchinspection"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type runtimeInspectionSearches interface {
	CompletedExecutionSnapshotFor(
		context.Context,
		searchjobs.AccessScope,
		string,
	) (searchjobs.ExecutionSnapshot, error)
}

type runtimeInspectionCompiler interface {
	Compile(*plan.Query) (clickhouse.CompiledQuery, error)
}

type runtimeInspectionExplainer interface {
	Explain(
		context.Context,
		clickhouse.CompiledQuery,
	) (queryexec.ExplainResult, error)
	Close() error
}

type runtimeInspectionExplainerFactory func(
	*clickhousedriver.Options,
	queryexec.Config,
) (runtimeInspectionExplainer, error)

type runtimeSearchInspectionConfig struct {
	Searches          runtimeInspectionSearches
	Compiler          runtimeInspectionCompiler
	ClickHouseOptions *clickhousedriver.Options
	NewExplainer      runtimeInspectionExplainerFactory
}

// runtimeSearchInspection owns the administrator-only inspection service and
// its two isolated ClickHouse EXPLAIN lanes. The search manager and compiler
// are borrowed. Close must therefore run after HTTP request drain and before
// either borrowed dependency is released.
type runtimeSearchInspection struct {
	service   *searchinspection.Service
	explainer runtimeInspectionExplainer

	closeOnce sync.Once
	closeErr  error
}

func newRuntimeSearchInspection(
	config runtimeSearchInspectionConfig,
) (*runtimeSearchInspection, error) {
	if nilRuntimeDependency(config.Searches) {
		return nil, errors.New(
			"compose search inspection runtime: completed search snapshots " +
				"are required",
		)
	}
	if nilRuntimeDependency(config.Compiler) {
		return nil, errors.New(
			"compose search inspection runtime: query compiler is required",
		)
	}
	if config.ClickHouseOptions == nil {
		return nil, errors.New(
			"compose search inspection runtime: ClickHouse options are required",
		)
	}
	newExplainer := config.NewExplainer
	if newExplainer == nil {
		newExplainer = func(
			options *clickhousedriver.Options,
			queryConfig queryexec.Config,
		) (runtimeInspectionExplainer, error) {
			return queryexec.NewExplainer(options, queryConfig)
		}
	}
	explainer, err := newExplainer(
		config.ClickHouseOptions,
		queryexec.Config{},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"compose search inspection runtime: %w",
			err,
		)
	}
	if nilRuntimeDependency(explainer) {
		return nil, errors.New(
			"compose search inspection runtime: query Explainer is required",
		)
	}
	service, err := searchinspection.New(searchinspection.Config{
		Searches:  config.Searches,
		Compiler:  config.Compiler,
		Explainer: explainer,
	})
	if err != nil {
		serviceErr := fmt.Errorf(
			"compose search inspection runtime: %w",
			err,
		)
		if closeErr := explainer.Close(); closeErr != nil {
			return nil, errors.Join(
				serviceErr,
				errors.New(
					"compose search inspection runtime: close query "+
						"Explainer after failed construction",
				),
			)
		}
		return nil, serviceErr
	}
	return &runtimeSearchInspection{
		service:   service,
		explainer: explainer,
	}, nil
}

// Close first stops inspection admission and waits for every request to release
// its borrowed search snapshot and EXPLAIN lane. Only then are the dedicated
// native connections closed.
func (inspection *runtimeSearchInspection) Close() error {
	if inspection == nil {
		return nil
	}
	inspection.closeOnce.Do(func() {
		serviceErr := inspection.service.Close(context.Background())
		var explainerErr error
		if nilRuntimeDependency(inspection.explainer) {
			explainerErr = errors.New(
				"close search inspection runtime: query Explainer is required",
			)
		} else {
			explainerErr = inspection.explainer.Close()
		}
		inspection.closeErr = errors.Join(serviceErr, explainerErr)
	})
	return inspection.closeErr
}
