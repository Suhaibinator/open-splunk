/**
 * Backend example links must not carry a fixture index into a real server.
 * Omitting the leading selector lets Search resolve the selected app's exact
 * default indexes (or the browser-authorized index scope) during submission.
 */
export function backendDraftWithoutIndexSelector(spl: string): string {
  const draft = spl.replace(/^\s*index=[^\s|]+\s*/i, "");
  return draft.length === 0 || draft.startsWith("|") ? `*${draft.length === 0 ? "" : ` ${draft}`}` : draft;
}

export interface ExampleDraft {
  readonly title: string;
  /** The search as the preview runs it, fixture index selector included. */
  readonly spl: string;
  readonly description: string;
  /**
   * Whether the search is written against one dataset's fields and index.
   * Against a real server the selector is dropped and the draft runs in the
   * selected app's indexes, so the gallery says which index it was meant for.
   */
  readonly needsIndex: boolean;
}

/**
 * The searches the Help menu's examples gallery offers and the home page's
 * preview "recent searches" table replays. One table, so a query the home page
 * links to is the same one the gallery loads into the editor.
 */
export const EXAMPLE_DRAFTS = [
  {
    title: "Production errors by service",
    spl: "index=gradethis level=ERROR | stats count by service",
    description: "Counts error-level events per service so the noisiest one is first to inspect.",
    needsIndex: true,
  },
  {
    title: "Slowest API routes",
    spl: "index=gradethis duration_ms=* | stats p95(duration_ms) AS p95_ms BY path | sort -p95_ms",
    description: "Ranks request paths by their 95th-percentile latency, slowest at the top.",
    needsIndex: true,
  },
  {
    title: "Notification worker retries",
    spl: "index=gradethis logger=notification-worker retry_count>0",
    description: "Lists the notification worker's events that retried at least once.",
    needsIndex: true,
  },
  {
    title: "Checkout trace investigation",
    spl: "index=payments trace_id=\"8e1c…\"",
    description: "Pulls every event of one trace; replace the id with the one you are chasing.",
    needsIndex: true,
  },
  {
    title: "Event volume over time",
    spl: "* | timechart span=5m count",
    description: "Buckets everything in the time range into five-minute counts for the timeline.",
    needsIndex: false,
  },
  {
    title: "Busiest sourcetypes",
    spl: "* | top limit=10 sourcetype",
    description: "Shows the ten most common sourcetypes with their share of events.",
    needsIndex: false,
  },
  {
    title: "Hosts that went quiet",
    spl: "* | stats latest(_time) AS last_seen, count BY host | sort last_seen",
    description: "Finds the host that has been silent longest by its most recent event.",
    needsIndex: false,
  },
] as const satisfies readonly ExampleDraft[];

export type ExampleDraftTitle = (typeof EXAMPLE_DRAFTS)[number]["title"];

/** Looks an example up by its title; the union type keeps every caller's title in the table. */
export function exampleDraft(title: ExampleDraftTitle): ExampleDraft {
  const example = EXAMPLE_DRAFTS.find((candidate) => candidate.title === title);
  if (example === undefined) throw new Error(`Unknown example draft “${title}”.`);
  return example;
}

/**
 * The text "Use" places in the editor. Connected to a server the fixture
 * index selector goes, exactly as the home page's links drop it, so the
 * search runs in the selected app's indexes rather than a preview index the
 * server does not have.
 */
export function exampleDraftSpl(example: ExampleDraft, connected: boolean): string {
  return connected ? backendDraftWithoutIndexSelector(example.spl) : example.spl;
}
