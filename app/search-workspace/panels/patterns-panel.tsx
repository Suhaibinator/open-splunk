import { NUMBER_FORMAT } from "../constants";
import { AppIcon } from "../../_components/app-icon";
import type { MenuName, PatternSensitivity, ResultTab } from "../model";
import type { PatternCoverage, PatternRow } from "../backend-patterns";

interface PatternsPanelProps {
  menu: MenuName | null;
  patternRows: PatternRow[];
  patternSensitivity: PatternSensitivity;
  onMenuChange: (menu: MenuName | null) => void;
  onPatternSensitivityChange: (sensitivity: PatternSensitivity) => void;
  onShowToast: (message: string, tone?: "success" | "info" | "warning") => void;
  onViewEvents: (signature: string) => void;
  onTabChange: (tab: ResultTab) => void;
  coverage?: PatternCoverage | null;
  loading?: boolean;
  error?: string | null;
  pageNumber?: number;
  pageSize?: number;
  hasNextPage?: boolean;
  onPageChange?: (page: number) => void;
  onRetry?: () => void;
  onViewPattern?: (pattern: PatternRow) => void;
  onExport?: () => void;
}

export function PatternsPanel({
  menu,
  patternRows,
  patternSensitivity,
  onMenuChange,
  onPatternSensitivityChange,
  onShowToast,
  onViewEvents,
  onTabChange,
  coverage,
  loading = false,
  error = null,
  pageNumber = 1,
  pageSize = 20,
  hasNextPage = false,
  onPageChange,
  onRetry,
  onViewPattern,
  onExport,
}: PatternsPanelProps) {
  return (
    <section id="panel-patterns" role="tabpanel" aria-labelledby="tab-patterns" className="patterns-panel">
      <header className="result-view-header">
        <div><h2>Event patterns</h2><p>Similar raw events grouped into recurring signatures. Coverage is the share of eligible events.</p>
          {coverage ? <p>{NUMBER_FORMAT.format(coverage.eligibleRows)} eligible events of {NUMBER_FORMAT.format(coverage.retainedRows)} retained rows; {NUMBER_FORMAT.format(coverage.excludedRows)} excluded because the final _raw value is missing, null, or not a string.</p> : null}
          {coverage?.retainedTruncated ? <p>The retained snapshot is truncated. Patterns cover the retained rows; the full search can contain additional events.</p> : null}
          {coverage && !coverage.snapshotComplete ? <p>The retained snapshot is incomplete.</p> : null}
        </div>
        <div className="header-menu-wrap result-menu-wrap">
          <button className="button button--secondary button--compact" type="button" aria-haspopup="menu" aria-expanded={menu === "pattern-sensitivity"} onClick={() => onMenuChange(menu === "pattern-sensitivity" ? null : "pattern-sensitivity")}>Sensitivity: {patternSensitivity} <AppIcon name="chevron-down" size="xs" /></button>
          {menu === "pattern-sensitivity" ? (
            <div className="floating-menu result-control-menu" role="menu" aria-label="Pattern sensitivity">
              {(["Precise", "Balanced", "Broad"] as const).map((sensitivity) => (
                <button
                  role="menuitemradio"
                  aria-checked={patternSensitivity === sensitivity}
                  type="button"
                  key={sensitivity}
                  onClick={() => {
                    onPatternSensitivityChange(sensitivity);
                    onMenuChange(null);
                    onShowToast(`Pattern sensitivity set to ${sensitivity.toLowerCase()}.`, "success");
                  }}
                >
                  <span className="radio-mark">{patternSensitivity === sensitivity ? "●" : "○"}</span>
                  <span><strong>{sensitivity}</strong><small>{sensitivity === "Precise" ? "More, narrowly matched patterns" : sensitivity === "Balanced" ? "A practical grouping of recurring events" : "Fewer, more inclusive patterns"}</small></span>
                </button>
              ))}
            </div>
          ) : null}
        </div>
      </header>
      {error ? <p role="alert">{error} {onRetry ? <button type="button" className="button button--link" onClick={onRetry}>Retry patterns</button> : null}</p> : null}
      {loading ? <p role="status">Loading patterns…</p> : null}
      {!loading && !error && patternRows.length === 0 ? <p>No eligible event patterns in this retained result snapshot.</p> : null}
      {onExport ? <button type="button" className="button button--secondary button--compact" disabled={loading || !coverage || coverage.totalGroups === 0} onClick={onExport}>Export all patterns</button> : null}
      <div className="pattern-table" aria-busy={loading}>
        <div className="pattern-head"><span>Pattern</span><span className="pattern-events-head">Events</span><span className="pattern-coverage-head">Coverage</span><span className="pattern-action-head">Action</span></div>
        {patternRows.map((pattern, index) => {
          const roundedPercent = Math.round(pattern.percent * 10) / 10;
          return (
            <article key={pattern.patternId ?? pattern.signature}>
              <span className="pattern-rank">{(pageNumber - 1) * pageSize + index + 1}</span>
              <code title={pattern.signature}>{pattern.signature}</code>
              <strong className="pattern-event-count">{NUMBER_FORMAT.format(pattern.count)}<span> events</span></strong>
              <div className="pattern-coverage"><span style={{ width: `${Math.max(0, Math.min(100, roundedPercent))}%` }} /><b>{roundedPercent.toFixed(1)}%</b></div>
              <button className="button button--link pattern-action" type="button" disabled={loading || (pattern.patternId !== undefined && !onViewPattern)} onClick={() => { if (pattern.patternId !== undefined) onViewPattern?.(pattern); else { onTabChange("events"); onViewEvents(pattern.signature); } }}>View events <AppIcon name="chevron-right" size="xs" /></button>
            </article>
          );
        })}
      </div>
      {onPageChange ? <nav aria-label="Pattern pages">
        <button type="button" className="button button--secondary button--compact" disabled={loading || pageNumber <= 1} onClick={() => onPageChange(pageNumber - 1)}>Previous</button>
        <span>Page {NUMBER_FORMAT.format(pageNumber)}{coverage ? ` · ${NUMBER_FORMAT.format(coverage.totalGroups)} patterns` : ""}</span>
        <button type="button" className="button button--secondary button--compact" disabled={loading || !hasNextPage} onClick={() => onPageChange(pageNumber + 1)}>Next</button>
      </nav> : null}
    </section>
  );
}
