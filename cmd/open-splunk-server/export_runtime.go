package main

import (
	"errors"
	"time"

	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
)

const (
	exportMaximumGroupedRows     = uint64(10_001)
	exportNativeResultByteMargin = uint64(16)
	exportMaximumExecutionTime   = time.Hour
)

// exportRuntimeSettings is the single source of truth for limits that span
// ClickHouse execution, serialization, bootstrap discovery, and HTTP delivery.
// Keeping these values together makes it difficult to accidentally advertise
// or admit an export that the dedicated query executor cannot produce.
type exportRuntimeSettings struct {
	defaultRowLimit         uint64
	maximumRowLimit         uint64
	defaultByteLimit        uint64
	maximumByteLimit        uint64
	maximumTotalBytes       uint64
	queryMaximumResultRows  uint64
	queryMaximumResultBytes uint64
	queryMaximumGroupedRows uint64
	expandTimechartGroups   bool
	reexecutionMaxRuntime   time.Duration
}

func defaultExportRuntimeSettings() exportRuntimeSettings {
	return exportRuntimeSettings{
		defaultRowLimit:         100_000,
		maximumRowLimit:         10_000_000,
		defaultByteLimit:        256 << 20,
		maximumByteLimit:        4 << 30,
		maximumTotalBytes:       4 << 30,
		queryMaximumResultRows:  10_000_001,
		queryMaximumResultBytes: 64 << 30,
		// GROUP BY cardinality is a memory bound, not an export-size bound.
		// Deliberately retain the interactive-search cap for export queries.
		queryMaximumGroupedRows: exportMaximumGroupedRows,
		expandTimechartGroups:   true,
		reexecutionMaxRuntime:   5 * time.Minute,
	}
}

func (settings exportRuntimeSettings) validate() error {
	if settings.defaultRowLimit == 0 || settings.maximumRowLimit == 0 ||
		settings.defaultRowLimit > settings.maximumRowLimit {
		return errors.New("invalid export row limits")
	}
	if settings.defaultByteLimit == 0 || settings.maximumByteLimit == 0 ||
		settings.defaultByteLimit > settings.maximumByteLimit ||
		settings.maximumTotalBytes < settings.maximumByteLimit {
		return errors.New("invalid export byte limits")
	}
	if settings.maximumRowLimit == ^uint64(0) ||
		settings.queryMaximumResultRows < settings.maximumRowLimit+1 {
		return errors.New("export query row limit cannot expose overflow")
	}
	// ClickHouse measures its native result stream rather than the final CSV
	// or JSONL artifact. Compact CSV cells can be much smaller than fixed-width
	// native values, so retain a conservative margin and let the streaming
	// serializer enforce the authoritative artifact boundary.
	if settings.maximumByteLimit > ^uint64(0)/exportNativeResultByteMargin ||
		settings.queryMaximumResultBytes < settings.maximumByteLimit*exportNativeResultByteMargin {
		return errors.New("export query byte limit lacks serialization margin")
	}
	if settings.queryMaximumGroupedRows != exportMaximumGroupedRows {
		return errors.New("export GROUP BY limit must retain the search cardinality bound")
	}
	if !settings.expandTimechartGroups {
		return errors.New("export timechart group expansion must match interactive searches")
	}
	if settings.reexecutionMaxRuntime <= 0 || settings.reexecutionMaxRuntime > exportMaximumExecutionTime {
		return errors.New("export re-execution runtime is outside its supported range")
	}
	return nil
}

func (settings exportRuntimeSettings) queryExecutorConfig() queryexec.Config {
	return queryexec.Config{
		MaxExecutionTime:          settings.reexecutionMaxRuntime,
		MaxResultRows:             settings.queryMaximumResultRows,
		MaxResultBytes:            settings.queryMaximumResultBytes,
		MaxRowsToGroupBy:          settings.queryMaximumGroupedRows,
		ExpandTimechartGroupLimit: settings.expandTimechartGroups,
	}
}

func (settings exportRuntimeSettings) managerConfig(source exportjobs.ResultSource, artifactDir string) exportjobs.Config {
	return exportjobs.Config{
		Source:           source,
		ArtifactDir:      artifactDir,
		DefaultRowLimit:  settings.defaultRowLimit,
		MaximumRowLimit:  settings.maximumRowLimit,
		DefaultByteLimit: settings.defaultByteLimit,
		MaximumByteLimit: settings.maximumByteLimit,
		MaxTotalBytes:    settings.maximumTotalBytes,
	}
}
