export interface SplPipelineCommandDefinition {
  name: string;
  insertion: string;
  detail: string;
}

export const SPL_PIPELINE_COMMANDS = [
  {
    name: "search",
    insertion: 'search level="ERROR"',
    detail: "Filter events or transformed rows.",
  },
  {
    name: "where",
    insertion: "where isnotnull(status) AND status >= 500",
    detail: "Filter with comparisons or direct isnull/isnotnull predicates.",
  },
  {
    name: "eval",
    insertion: 'eval availability=if(isnull(status), "missing", "present")',
    detail: "Create or replace fields; if(predicate, true_value, false_value) selects true_value only for true, while false or null selects false_value.",
  },
  {
    name: "rename",
    insertion: "rename logger AS component",
    detail: "Rename one or more exact fields.",
  },
  {
    name: "fields",
    insertion: "fields _time level logger message trace_id",
    detail: "Include or exclude exact fields.",
  },
  {
    name: "table",
    insertion: "table _time level logger message trace_id",
    detail: "Create an ordered statistics table.",
  },
  {
    name: "sort",
    insertion: "sort -_time",
    detail: "Sort rows by exact fields.",
  },
  {
    name: "dedup",
    insertion: "dedup trace_id",
    detail: "Keep the first row for each exact key.",
  },
  {
    name: "rex",
    insertion: 'rex field=_raw "(?<request_id>request_id=[^ ]+)"',
    detail: "Extract named fields with the supported RE2 subset.",
  },
  {
    name: "spath",
    insertion: "spath input=payload output=status path=response.status",
    detail: "Extract one typed scalar from an explicit JSON path.",
  },
  {
    name: "bin",
    insertion: "bin duration_ms span=100 AS duration_bucket",
    detail: "Bucket one numeric or canonical time field with an explicit span.",
  },
  {
    name: "bucket",
    insertion: "bucket _time span=5m",
    detail: "Alias of bin with an explicit span.",
  },
  {
    name: "head",
    insertion: "head 20",
    detail: "Keep the first rows.",
  },
  {
    name: "tail",
    insertion: "tail 20",
    detail: "Keep and reverse the final rows.",
  },
  {
    name: "stats",
    insertion: "stats count c(user) AS user_occurrences dc(user) AS user_count values(user) AS users min(duration_ms) AS min_ms max(duration_ms) AS max_ms earliest(status) AS first_status latest(status) AS last_status p50(duration_ms) AS median_ms p95(duration_ms) AS p95_ms sum(bytes) AS total_bytes avg(duration_ms) AS avg_ms BY level",
    detail: "Aggregate with row count, count/c field occurrences, dc/distinct_count, values, list, min, max, chronological earliest/latest, pN/percN percentiles (1-99), sum, and avg.",
  },
  {
    name: "top",
    insertion: "top limit=20 message",
    detail: "Find the most frequent values.",
  },
  {
    name: "rare",
    insertion: "rare limit=20 message",
    detail: "Find the least frequent values.",
  },
  {
    name: "timechart",
    insertion: "timechart span=5m count BY level",
    detail: "Chart count over fixed time buckets.",
  },
  {
    name: "chart",
    insertion: "chart count OVER status BY level",
    detail: "Build a bounded two-dimensional aggregate table.",
  },
] as const satisfies readonly SplPipelineCommandDefinition[];

export const UNSUPPORTED_SPL_PIPELINE_COMMANDS = [
  "eventstats",
  "streamstats",
  "transaction",
  "join",
  "map",
  "subsearch",
] as const;

const SUPPORTED_PIPELINE_COMMAND_SET = new Set<string>(
  SPL_PIPELINE_COMMANDS.map((command) => command.name),
);

export function isSupportedSplPipelineCommand(command: string): boolean {
  return SUPPORTED_PIPELINE_COMMAND_SET.has(command.toLowerCase());
}

export interface SplStructure {
  pipes: number[];
  unclosedQuote: { offset: number } | null;
}

/**
 * Locates pipeline boundaries and an unclosed double quote using the same
 * escape behavior as the backend lexer.
 */
export function scanSplStructure(spl: string): SplStructure {
  const pipes: number[] = [];
  let inDoubleQuotedString = false;
  let quoteOffset = -1;

  for (let offset = 0; offset < spl.length; offset += 1) {
    const character = spl[offset];
    if (inDoubleQuotedString) {
      if (character === "\\") {
        // A backslash consumes the next character only while scanning a
        // double-quoted value. Outside a quote it remains part of the token.
        offset += 1;
        continue;
      }
      if (character === '"') inDoubleQuotedString = false;
      continue;
    }
    if (character === '"') {
      inDoubleQuotedString = true;
      quoteOffset = offset;
      continue;
    }
    if (character === "|") pipes.push(offset);
  }

  return {
    pipes,
    unclosedQuote: inDoubleQuotedString ? { offset: quoteOffset } : null,
  };
}

export function splitSplPipeline(spl: string): string[] {
  const stages: string[] = [];
  let stageStart = 0;
  for (const pipe of scanSplStructure(spl).pipes) {
    stages.push(spl.slice(stageStart, pipe));
    stageStart = pipe + 1;
  }
  stages.push(spl.slice(stageStart));
  return stages;
}

export function firstSplPipelineBoundary(spl: string): number {
  return scanSplStructure(spl).pipes[0] ?? -1;
}

export function isSplOffsetInDoubleQuotedValue(spl: string, offset: number): boolean {
  const safeOffset = Math.max(0, Math.min(offset, spl.length));
  return scanSplStructure(spl.slice(0, safeOffset)).unclosedQuote !== null;
}

export type SplValue = string | number | boolean | null;

function escapeDoubleQuotedSplValue(value: string): string {
  return value
    .replaceAll("\\", "\\\\")
    .replaceAll('"', '\\"')
    .replaceAll("\n", "\\n")
    .replaceAll("\r", "\\r")
    .replaceAll("\t", "\\t");
}

export function formatSplValue(value: SplValue): string {
  if (value === null) return "null";
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return `"${escapeDoubleQuotedSplValue(value)}"`;
}
