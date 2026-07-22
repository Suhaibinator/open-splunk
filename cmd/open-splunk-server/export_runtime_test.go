package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultExportRuntimeSettingsKeepAllBoundariesAligned(t *testing.T) {
	t.Parallel()
	settings := defaultExportRuntimeSettings()
	if err := settings.validate(); err != nil {
		t.Fatalf("validate() error = %v", err)
	}

	queryConfig := settings.queryExecutorConfig()
	managerConfig := settings.managerConfig(nil, "/private/export-base")
	if queryConfig.MaxResultRows < managerConfig.MaximumRowLimit+1 {
		t.Fatalf("query rows = %d, need at least %d", queryConfig.MaxResultRows, managerConfig.MaximumRowLimit+1)
	}
	if queryConfig.MaxResultBytes < managerConfig.MaximumByteLimit*exportNativeResultByteMargin {
		t.Fatalf("query bytes = %d, need at least %dx artifact maximum %d", queryConfig.MaxResultBytes, exportNativeResultByteMargin, managerConfig.MaximumByteLimit)
	}
	if queryConfig.MaxRowsToGroupBy != exportMaximumGroupedRows {
		t.Fatalf("GROUP BY rows = %d, want %d", queryConfig.MaxRowsToGroupBy, exportMaximumGroupedRows)
	}
	if !queryConfig.ExpandTimechartGroupLimit {
		t.Fatal("export executor does not preserve bounded timechart expansion")
	}
	if queryConfig.MaxExecutionTime != settings.reexecutionMaxRuntime {
		t.Fatalf("query runtime = %v, re-execution runtime = %v", queryConfig.MaxExecutionTime, settings.reexecutionMaxRuntime)
	}
	if queryConfig.MaxMemoryBytes != 0 || queryConfig.MaxRowsToRead != 0 ||
		queryConfig.MaxBytesToRead != 0 || queryConfig.MaxThreads != 0 {
		t.Fatalf("query config overrides shared conservative resource defaults: %+v", queryConfig)
	}
	if managerConfig.DefaultRowLimit != settings.defaultRowLimit ||
		managerConfig.MaximumRowLimit != settings.maximumRowLimit ||
		managerConfig.DefaultByteLimit != settings.defaultByteLimit ||
		managerConfig.MaximumByteLimit != settings.maximumByteLimit ||
		managerConfig.MaxTotalBytes != settings.maximumTotalBytes ||
		managerConfig.ArtifactDir != "/private/export-base" {
		t.Fatalf("manager config drifted from runtime settings: %+v", managerConfig)
	}
}

func TestExportRuntimeSettingsRejectUnsafeDrift(t *testing.T) {
	t.Parallel()
	valid := defaultExportRuntimeSettings()
	tests := map[string]func(*exportRuntimeSettings){
		"default rows exceed maximum": func(settings *exportRuntimeSettings) {
			settings.defaultRowLimit = settings.maximumRowLimit + 1
		},
		"total bytes below one maximum artifact": func(settings *exportRuntimeSettings) {
			settings.maximumTotalBytes = settings.maximumByteLimit - 1
		},
		"query cannot observe row overflow": func(settings *exportRuntimeSettings) {
			settings.queryMaximumResultRows = settings.maximumRowLimit
		},
		"query byte margin removed": func(settings *exportRuntimeSettings) {
			settings.queryMaximumResultBytes = settings.maximumByteLimit
		},
		"GROUP BY cardinality widened": func(settings *exportRuntimeSettings) {
			settings.queryMaximumGroupedRows++
		},
		"timechart expansion disabled": func(settings *exportRuntimeSettings) {
			settings.expandTimechartGroups = false
		},
		"non-positive execution runtime": func(settings *exportRuntimeSettings) {
			settings.reexecutionMaxRuntime = 0
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			settings := valid
			mutate(&settings)
			if err := settings.validate(); err == nil {
				t.Fatal("validate() unexpectedly succeeded")
			}
		})
	}
}

func TestNormalizeRuntimeOptionsDerivesAndCleansExportArtifactDirectory(t *testing.T) {
	t.Parallel()
	config := options{
		httpAddress:    "127.0.0.1:8080",
		controlDBPath:  filepath.Join("state", "control.db"),
		indexRetention: time.Hour,
		tenantID:       "tenant",
	}
	if err := normalizeRuntimeOptions(&config); err != nil {
		t.Fatalf("normalizeRuntimeOptions() error = %v", err)
	}
	if want := filepath.Join("state", "control.db.exports"); config.exportArtifactDir != want {
		t.Fatalf("derived artifact directory = %q, want %q", config.exportArtifactDir, want)
	}

	configured := options{
		httpAddress:       "127.0.0.1:8080",
		exportArtifactDir: " ./state/../private-exports ",
		indexRetention:    time.Hour,
		tenantID:          "tenant",
	}
	if err := normalizeRuntimeOptions(&configured); err != nil {
		t.Fatalf("normalize configured artifact directory: %v", err)
	}
	if configured.exportArtifactDir != "private-exports" {
		t.Fatalf("configured artifact directory = %q, want private-exports", configured.exportArtifactDir)
	}
}

func TestNormalizeRuntimeOptionsRejectsUnsafeExportArtifactDirectory(t *testing.T) {
	t.Parallel()
	for name, artifactDir := range map[string]string{
		"NUL":               "exports\x00other",
		"working directory": ".",
		"parent directory":  "../shared",
		"filesystem root":   string(filepath.Separator),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := options{
				httpAddress:       "127.0.0.1:8080",
				exportArtifactDir: artifactDir,
				indexRetention:    time.Hour,
				tenantID:          "tenant",
			}
			if err := normalizeRuntimeOptions(&config); err == nil {
				t.Fatal("normalizeRuntimeOptions() unexpectedly accepted an unsafe path")
			}
		})
	}
}
