import { patternWireSensitivity, type PatternContext, type PatternRow } from "./backend-patterns";

/** Capture at dialog admission so retries keep the originally selected relation. */
export function patternExportSource(context: PatternContext, pattern: PatternRow | null = null) {
  if (!context.searchJobId || !context.snapshotRef) {
    throw new Error("Patterns export requires a retained result snapshot.");
  }
  const common = {
    searchJobId: context.searchJobId,
    snapshotRef: context.snapshotRef,
    sensitivity: patternWireSensitivity(context.sensitivity),
  };
  if (pattern === null) {
    return Object.freeze({ $case: "patternSummary" as const, value: Object.freeze(common) });
  }
  if (!pattern.patternId) throw new Error("Pattern member export requires an exact retained group.");
  return Object.freeze({ $case: "patternMembers" as const, value: Object.freeze({ ...common, patternId: pattern.patternId }) });
}
