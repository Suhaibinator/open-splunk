import type { PatternRow } from "../backend-patterns";
import { NUMBER_FORMAT } from "../constants";

export function PatternFilterChip({ pattern, onClear }: { pattern: PatternRow; onClear: () => void }) {
  return <div className="event-toolbar" role="region" aria-label="Exact pattern event filter">
    <span className="badge">Pattern: <code>{pattern.signature || "(empty event)"}</code></span>
    <span>{NUMBER_FORMAT.format(pattern.count)} exact members in the retained snapshot</span>
    <button type="button" className="button button--link" onClick={onClear}>Clear pattern</button>
  </div>;
}
