import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { PatternFilterChip } from "./pattern-filter-chip";

test("exact pattern filter identifies the retained relation and has a Clear control", () => {
  const markup = renderToStaticMarkup(<PatternFilterChip pattern={{ patternId: "id", signature: "literal * <int>", count: 15, percent: 100 }} onClear={() => undefined} />);
  assert.match(markup, /aria-label="Exact pattern event filter"/u);
  assert.match(markup, /literal \* &lt;int&gt;/u);
  assert.match(markup, /15 exact members in the retained snapshot/u);
  assert.match(markup, /Clear pattern/u);
});
