/**
 * The one inventory for documentation validation and the bundled Help site.
 * `published: false` keeps contributor-only instructions checked without
 * exposing them in the product. Every published local link must resolve to
 * another published entry.
 */
export const DOCUMENTATION_REGISTRY = Object.freeze([
  { sourcePath: "README.md", slug: "project", title: "Open Splunk", section: "Project", published: true, kind: "markdown" },
  { sourcePath: "AGENTS.md", slug: "", title: "Agent guidelines", section: "Internal", published: false, kind: "markdown" },
  { sourcePath: "CLAUDE.md", slug: "", title: "Claude guidelines", section: "Internal", published: false, kind: "markdown" },
  { sourcePath: "docs/README.md", slug: "", title: "Documentation", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/architecture.md", slug: "architecture", title: "Architecture", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/api.md", slug: "api", title: "API", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/dashboards.md", slug: "dashboards", title: "Dashboard visualizations", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/spl.md", slug: "spl", title: "SPL", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/timechart.md", slug: "timechart", title: "Timechart", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/patterns.md", slug: "patterns", title: "Event patterns", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/knowledge.md", slug: "knowledge", title: "Knowledge and lookups", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/theming.md", slug: "theming", title: "Theming", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/ingestion.md", slug: "ingestion", title: "Ingestion", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/insert-coalescing.md", slug: "insert-coalescing", title: "Insert coalescing", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/collector-configuration.md", slug: "collector-configuration", title: "Collector configuration", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/hec.md", slug: "hec", title: "HTTP Event Collector", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/auditing.md", slug: "auditing", title: "Auditing", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/search-sharing-alerts.md", slug: "search-sharing-alerts", title: "Search sharing, schedules, and alerts", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/roadmap.md", slug: "roadmap", title: "Roadmap", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "docs/releasing.md", slug: "releasing", title: "Build and publication", section: "Guides", published: true, kind: "markdown" },
  { sourcePath: "deploy/README.md", slug: "examples/deployment", title: "Deployment and recovery", section: "Operations", published: true, kind: "markdown" },
  { sourcePath: "integration/README.md", slug: "examples/integration", title: "Integration testing", section: "Operations", published: true, kind: "markdown" },
  { sourcePath: "scripts/README.md", slug: "examples/automation", title: "Build and validation automation", section: "Operations", published: true, kind: "markdown" },
  { sourcePath: "migrations/README.md", slug: "examples/database", title: "Database mechanics", section: "Operations", published: true, kind: "markdown" },
  { sourcePath: "internal/hec/testdata/compatibility/README.md", slug: "examples/hec-compatibility", title: "HEC compatibility corpus", section: "Examples", published: true, kind: "markdown" },
  { sourcePath: "gen/go/README.md", slug: "examples/generated-go", title: "Generated Go bindings", section: "Examples", published: true, kind: "markdown" },
  { sourcePath: "gen/ts/README.md", slug: "examples/generated-typescript", title: "Generated TypeScript bindings", section: "Examples", published: true, kind: "markdown" },
  { sourcePath: "configs/examples/collector-container.yaml", slug: "examples/collector-container", title: "Container collector configuration", section: "Examples", published: true, kind: "source" },
  { sourcePath: "configs/examples/collector.yaml", slug: "examples/collector", title: "Collector configuration", section: "Examples", published: true, kind: "source" },
  { sourcePath: "deploy/docker-compose.yaml", slug: "examples/docker-compose", title: "Docker Compose deployment", section: "Examples", published: true, kind: "source" },
  { sourcePath: "deploy/docker-compose.recovery.yaml", slug: "examples/docker-compose-recovery", title: "Docker Compose recovery topology", section: "Examples", published: true, kind: "source" },
  { sourcePath: "deploy/docker-compose.recovery-restore.yaml", slug: "examples/docker-compose-recovery-restore", title: "Docker Compose recovery restore overlay", section: "Examples", published: true, kind: "source" },
  { sourcePath: "deploy/recovery/users.xml.template", slug: "examples/recovery-users", title: "Recovery users template", section: "Examples", published: true, kind: "source" },
]);

export const OWNED_MARKDOWN_PATHS = Object.freeze(
  DOCUMENTATION_REGISTRY
    .filter((entry) => entry.kind === "markdown")
    .map((entry) => entry.sourcePath),
);

export const CANONICAL_DOCUMENTATION_NAMES = Object.freeze(
  DOCUMENTATION_REGISTRY
    .filter((entry) => entry.sourcePath.startsWith("docs/"))
    .map((entry) => entry.sourcePath.slice("docs/".length)),
);
