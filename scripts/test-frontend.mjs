// Test files run serially to keep their repository and publication fixtures isolated.
import { spawn } from "node:child_process";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import process from "node:process";

const workspace = process.cwd();
const outputDirectory = await mkdtemp(path.join(tmpdir(), "open-splunk-frontend-tests-"));
const scriptTests = [
  "build-ui-output.test.mjs",
  "check-docs.test.mjs",
  "ci-workflow-policy.test.mjs",
  "compile-protos.test.mjs",
  "byte-formatting.test.mjs",
  "style-invariants.test.mjs",
  "style-guardrails.test.mjs",
  "safety-net.test.mjs",
  "materialize-git-snapshot.test.mjs",
  "pull-clickhouse-test-image.test.mjs",
  "verify-protobuf-generation.test.mjs",
  "run-go-race-shard.test.mjs",
  "run-development.test.mjs",
  "build-release.test.mjs",
  "build-oci.test.mjs",
];
const testFiles = [
  path.join("app", "_components", "app-icon.test.tsx"),
  path.join("app", "_components", "button.test.tsx"),
  path.join("app", "_components", "field-validation.test.tsx"),
  path.join("app", "_components", "modal.test.tsx"),
  path.join("app", "_components", "product-shell.test.tsx"),
  path.join("app", "_components", "select.test.tsx"),
  path.join("app", "_components", "status.test.tsx"),
  path.join("app", "home-dashboard-data.test.ts"),
  path.join("app", "analytics", "analytics-data.test.ts"),
  path.join("app", "analytics", "analytics-sample-status.test.ts"),
  path.join("app", "reports", "alerts-ui-state.test.ts"),
  path.join("app", "reports", "reports-view-state.test.ts"),
  path.join("app", "dashboards", "dashboard-manager-state.test.ts"),
  path.join("app", "dashboards", "dashboard-editor-state.test.ts"),
  path.join("app", "dashboards", "dashboard-panel-runner.test.ts"),
  path.join("app", "admin", "admin-resource-data.test.ts"),
  path.join("app", "admin", "backend-admin-console-hec.test.ts"),
  path.join("app", "admin", "token-creation-dialog.test.tsx"),
  path.join("app", "admin", "ingestion-policy-form.test.ts"),
  path.join("app", "admin", "search-limits-form.test.ts"),
  path.join("app", "admin", "appearance-form.test.ts"),
  path.join("app", "admin", "appearance-settings.test.tsx"),
  path.join("app", "admin", "appearance-settings.mount.test.tsx"),
  path.join("app", "admin", "backend-server-settings.mount.test.tsx"),
  path.join("app", "_components", "theme-sync.test.tsx"),
  path.join("app", "admin", "token-create-recovery-policy.test.ts"),
  path.join("app", "admin", "token-recovery-startup.test.ts"),
  path.join("app", "admin", "knowledge-manager-data.test.ts"),
  path.join("app", "admin", "lookup-manager-data.test.ts"),
  path.join("app", "admin", "lookup-manager-panel.test.ts"),
  path.join("app", "admin", "knowledge-manager-preview-data.test.ts"),
  path.join("app", "activity", "backend-audit-data.test.ts"),
  path.join("app", "activity", "backend-export-jobs.test.ts"),
  path.join("app", "search-workspace", "live-preview.test.ts"),
  path.join("app", "search-workspace", "backend-result-pages.test.ts"),
  path.join("app", "search-workspace", "progress-revision.test.ts"),
  path.join("app", "search-workspace", "running-search-controller.test.ts"),
  path.join("app", "search-workspace", "categorical-interaction.test.ts"),
  path.join("app", "search-workspace", "charts", "chart-stacking.test.ts"),
  path.join("app", "search-workspace", "charts", "time-series-line-chart.test.tsx"),
  path.join("app", "search-workspace", "clipboard-export.test.ts"),
  path.join("app", "search-workspace", "editor-history-recall.test.ts"),
  path.join("app", "search-workspace", "event-page-controls.test.ts"),
  path.join("app", "search-workspace", "components", "export-presentation.test.tsx"),
  path.join("app", "search-workspace", "panels", "events-table-columns.test.ts"),
  path.join("app", "search-workspace", "panels", "events-table.test.tsx"),
  path.join("app", "search-workspace", "components", "search-failure-panel.test.tsx"),
  path.join("app", "search-workspace", "panels", "events-pagination.test.tsx"),
  path.join("app", "search-workspace", "panels", "statistics-column-layout.test.ts"),
  path.join("app", "search-workspace", "panels", "statistics-panel.test.tsx"),
  path.join("app", "search-workspace", "panels", "visualization-panel.test.tsx"),
  path.join("app", "search-workspace", "components", "inactive-result-tab-panels.test.tsx"),
  path.join("app", "search-workspace", "components", "result-skeleton.test.tsx"),
  path.join("app", "search-workspace", "components", "search-editor.test.tsx"),
  path.join("app", "search-workspace", "components", "search-sharing-dialog.test.tsx"),
  path.join("app", "search-workspace", "statistics-sparkline.test.ts"),
  path.join("app", "search-workspace", "statistics-multivalue.test.ts"),
  path.join("app", "search-workspace", "time-range.test.ts"),
  path.join("app", "search-workspace", "timechart-series.test.ts"),
  path.join("app", "search-workspace", "use-search-sharing.test.ts"),
  path.join("app", "search-workspace", "workspace-utils.test.ts"),
  path.join("app", "search-workspace", "virtual-table.test.ts"),
  path.join("app", "datasets", "index-observability-data.test.ts"),
  path.join("lib", "byte-quantity.test.ts"),
  path.join("lib", "theme-preference.test.ts"),
  path.join("lib", "view-navigation.test.ts"),
  path.join("lib", "api", "pagination.test.ts"),
  path.join("lib", "api", "cursor-pages.test.ts"),
  path.join("lib", "api", "open-splunk-client-routes.test.ts"),
  path.join("lib", "api", "well-formed-text.test.ts"),
  path.join("lib", "api", "administrator-session.test.ts"),
  path.join("lib", "api", "protobuf-contracts.test.ts"),
  path.join("lib", "api", "result-column-presentation.test.ts"),
  path.join("lib", "api", "search-websocket.test.ts"),
  path.join("lib", "api", "system-bootstrap.test.ts"),
  path.join("lib", "api", "ui-palette.test.ts"),
  path.join("lib", "search", "backend-data.test.ts"),
  path.join("lib", "search", "app-navigation.test.ts"),
  path.join("lib", "search", "alert-form.test.ts"),
  path.join("lib", "search", "server-timeline.test.ts"),
  path.join("lib", "search", "server-exports.test.ts"),
  path.join("lib", "search", "server-alerts.test.ts"),
  path.join("lib", "search", "server-scheduled-reports.test.ts"),
  path.join("lib", "search", "example-drafts.test.ts"),
  path.join("lib", "search", "launch-url.test.ts"),
  path.join("lib", "search", "search-launch-state.test.ts"),
  path.join("lib", "search", "server-jobs.test.ts"),
  path.join("lib", "search", "server-job-settings.test.ts"),
  path.join("lib", "search", "query-pivots.test.ts"),
  path.join("lib", "search", "saved-search-names.test.ts"),
  path.join("lib", "search", "spl-editor.test.ts"),
  path.join("lib", "search", "spl-editor-interaction.test.ts"),
  path.join("lib", "search", "spl-syntax.test.ts"),
  path.join("lib", "search", "server-inspection.test.ts"),
  path.join("lib", "search", "streamstats-surface.test.ts"),
  path.join("integration", "browser_harness.test.ts"),
  path.join("app", "search-workspace", "completion-groups.test.ts"),
  path.join("app", "search-workspace", "completion-candidates.test.ts"),
  path.join("lib", "search", "spl-diagnostic-markers.test.ts"),
  path.join("app", "search-workspace", "spl-reference-data.test.ts"),
  path.join("app", "search-workspace", "components", "search-help-dialogs.test.tsx"),
  path.join("app", "search-workspace", "keyboard-shortcuts.test.ts"),
  path.join("app", "search-workspace", "search-failure-presentation.test.ts"),
];

function run(command, arguments_, environment = process.env) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, arguments_, {
      cwd: workspace,
      env: environment,
      stdio: "inherit",
    });
    child.on("error", reject);
    child.on("exit", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`${command} exited with ${code ?? signal ?? "an unknown status"}`));
    });
  });
}

try {
  await run(process.execPath, [
    "--test",
    ...scriptTests.map((file) => path.join(workspace, "scripts", file)),
  ]);
  const compilerConfig = path.join(outputDirectory, "tsconfig.json");
  await writeFile(compilerConfig, JSON.stringify({
    extends: path.join(workspace, "tsconfig.json"),
    compilerOptions: {
      incremental: false,
      module: "Node16",
      moduleResolution: "Node16",
      noEmit: false,
      outDir: outputDirectory,
      plugins: [],
      rootDir: workspace,
      target: "ES2023",
      typeRoots: [path.join(workspace, "node_modules", "@types")],
      types: ["node"],
    },
    files: testFiles.map((file) => path.join(workspace, file)),
    include: [],
  }));
  await run(path.join(workspace, "node_modules", ".bin", "tsc"), [
    "--project", compilerConfig,
    "--pretty", "false",
  ]);

  const aliasDirectory = path.join(outputDirectory, "node_modules", "@");
  const directoryLinkType = process.platform === "win32" ? "junction" : "dir";
  await mkdir(aliasDirectory, { recursive: true });
  await Promise.all([
    symlink(path.join(outputDirectory, "gen"), path.join(aliasDirectory, "gen"), directoryLinkType),
    symlink(path.join(outputDirectory, "lib"), path.join(aliasDirectory, "lib"), directoryLinkType),
  ]);
  await run(process.execPath, [
    "--test",
    // `.tsx` as well as `.ts`: tsc emits both as `.js`, and a pattern anchored
    // at `.ts$` silently handed node a path that does not exist, which it skips
    // without a word. Every component test in the list was passing by never
    // running.
    ...testFiles.map((file) => path.join(outputDirectory, file.replace(/\.tsx?$/u, ".js"))),
  ], {
    ...process.env,
    NODE_PATH: path.join(workspace, "node_modules"),
  });
} finally {
  await rm(outputDirectory, { recursive: true, force: true });
}
