import type { ResultTab } from "../model";

type SkeletonResultTab = Extract<ResultTab, "events" | "statistics" | "visualization">;

const LINE_WIDTHS = ["long", "medium", "short"] as const;

function SkeletonLines({ rows }: { rows: number }) {
  return Array.from({ length: rows }, (_, index) => (
    <span
      className={`skeleton skeleton--line skeleton--line-${LINE_WIDTHS[index % LINE_WIDTHS.length]}`}
      key={index}
    />
  ));
}

export function ResultSkeleton({ tab }: { tab: SkeletonResultTab }) {
  return (
    <section
      aria-busy="true"
      aria-labelledby={`tab-${tab}`}
      className={`result-skeleton result-skeleton--${tab}`}
      data-testid={`result-skeleton-${tab}`}
      id={`panel-${tab}`}
      role="tabpanel"
    >
      <div aria-hidden="true" className="result-skeleton__content">
        <header className="result-skeleton__header">
          <span className="skeleton skeleton--line skeleton--line-medium" />
          <span className="skeleton skeleton--line skeleton--line-short" />
        </header>
        {tab === "visualization" ? (
          <span className="skeleton skeleton--block result-skeleton__chart" />
        ) : (
          <div className="result-skeleton__rows">
            <SkeletonLines rows={tab === "events" ? 9 : 12} />
          </div>
        )}
      </div>
    </section>
  );
}
