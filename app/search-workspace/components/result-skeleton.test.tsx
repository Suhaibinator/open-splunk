import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { ResultSkeleton } from "./result-skeleton";

test("result skeletons expose busy tab panels and hide decorative geometry", () => {
  for (const tab of ["events", "statistics", "visualization"] as const) {
    const markup = renderToStaticMarkup(<ResultSkeleton tab={tab} />);
    assert.match(markup, /role="tabpanel"/u);
    assert.match(markup, /aria-busy="true"/u);
    assert.match(markup, /aria-hidden="true"/u);
    assert.match(markup, new RegExp(`id="panel-${tab}"`, "u"));
  }
});

test("visualization uses a chart block while tabular views use line rows", () => {
  assert.match(
    renderToStaticMarkup(<ResultSkeleton tab="visualization" />),
    /skeleton--block/u,
  );
  assert.doesNotMatch(
    renderToStaticMarkup(<ResultSkeleton tab="events" />),
    /skeleton--block/u,
  );
});
