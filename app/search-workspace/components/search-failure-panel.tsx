import Link from "next/link";

import type { EditorProblem } from "@/lib/search/spl-diagnostic-markers";

import { AppIcon } from "../../_components/app-icon";
import type { SearchFailurePresentation } from "../search-failure-presentation";

interface SearchFailurePanelProps {
  activeTab: string;
  canNavigateSource: boolean;
  onFocusProblem: (problem: EditorProblem) => void;
  onRetry: () => void;
  problems: readonly EditorProblem[];
  serverSettingsHref: string;
  presentation: SearchFailurePresentation;
}

export function SearchFailurePanel({
  activeTab,
  canNavigateSource,
  onFocusProblem,
  onRetry,
  problems,
  serverSettingsHref,
  presentation,
}: SearchFailurePanelProps) {
  const titleId = "search-failure-title";
  return (
    <section
      aria-labelledby={`tab-${activeTab}`}
      className="search-failure-panel"
      data-testid="search-failure-panel"
      id={`panel-${activeTab}`}
      role="tabpanel"
    >
      <div aria-labelledby={titleId} className="search-failure" role="region">
        <span aria-hidden="true" className="search-failure__icon">
          <AppIcon name="warning" size="lg" />
        </span>
        <div className="search-failure__body">
          <h2 id={titleId}>{presentation.title}</h2>
          <p>{presentation.detail}</p>
          {problems.length === 0 ? null : (
            <ul className="search-failure__problems">
              {problems.map((problem) => {
                const range = problem.diagnostic.range;
                const rangeEnabled = canNavigateSource && range !== null;
                return (
                  <li key={`${problem.diagnostic.code}-${range?.start ?? "none"}-${range?.end ?? "none"}-${problem.diagnostic.message}`}>
                    <strong>{problem.diagnostic.code}</strong>
                    <span>{problem.diagnostic.message}</span>
                    {range === null ? null : (
                      <button
                        className="button button--ghost button--compact"
                        disabled={!rangeEnabled}
                        type="button"
                        onClick={() => onFocusProblem(problem)}
                      >
                        Line {range.line}, column {range.column}
                      </button>
                    )}
                  </li>
                );
              })}
            </ul>
          )}
          {presentation.guidance.length === 0 ? null : (
            <ul className="search-failure__guidance">
              {presentation.guidance.map((item) => <li key={item}>{item}</li>)}
            </ul>
          )}
          <div className="search-failure__actions">
            {presentation.actions.map((action) => action === "retry" ? (
              <button className="button button--secondary button--compact" key={action} type="button" onClick={onRetry}>
                <AppIcon name="refresh" size="xs" /> Retry
              </button>
            ) : (
              <Link className="button button--secondary button--compact" href={serverSettingsHref} key={action}>
                <AppIcon name="settings" size="xs" /> Server settings
              </Link>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
