import { NUMBER_FORMAT } from "../constants";
import type { MenuName, PatternSensitivity, ResultTab } from "../model";

interface PatternRow {
  signature: string;
  count: number;
  percent: number;
}

interface PatternsPanelProps {
  menu: MenuName | null;
  patternRows: PatternRow[];
  patternSensitivity: PatternSensitivity;
  onMenuChange: (menu: MenuName | null) => void;
  onPatternSensitivityChange: (sensitivity: PatternSensitivity) => void;
  onShowToast: (message: string, tone?: "success" | "info" | "warning") => void;
  onViewEvents: (signature: string) => void;
  onTabChange: (tab: ResultTab) => void;
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
}: PatternsPanelProps) {
  return (
    <section id="panel-patterns" role="tabpanel" aria-labelledby="tab-patterns" className="patterns-panel">
      <header className="result-view-header">
        <div><h2>Event patterns</h2><p>Similar raw events grouped into recurring signatures.</p></div>
        <div className="header-menu-wrap result-menu-wrap">
          <button className="button secondary compact" type="button" aria-haspopup="menu" aria-expanded={menu === "pattern-sensitivity"} onClick={() => onMenuChange(menu === "pattern-sensitivity" ? null : "pattern-sensitivity")}>Sensitivity: {patternSensitivity} <span aria-hidden="true">▾</span></button>
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
      <div className="pattern-table">
        <div className="pattern-head"><span>Pattern</span><span className="pattern-events-head">Events</span><span className="pattern-coverage-head">Coverage</span><span className="pattern-action-head">Action</span></div>
        {patternRows.map((pattern, index) => {
          const roundedPercent = Math.round(pattern.percent * 10) / 10;
          return (
            <article key={pattern.signature}>
              <span className="pattern-rank">{index + 1}</span>
              <code title={pattern.signature}>{pattern.signature}</code>
              <strong className="pattern-event-count">{NUMBER_FORMAT.format(pattern.count)}<span> events</span></strong>
              <div className="pattern-coverage"><span style={{ width: `${Math.max(0, Math.min(100, roundedPercent))}%` }} /><b>{roundedPercent.toFixed(1)}%</b></div>
              <button className="pattern-action" type="button" onClick={() => { onTabChange("events"); onViewEvents(pattern.signature); }}>View events <span aria-hidden="true">›</span></button>
            </article>
          );
        })}
      </div>
    </section>
  );
}
