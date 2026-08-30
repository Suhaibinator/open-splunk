import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { InactiveResultTabPanels } from "./inactive-result-tab-panels";

test("inactive result tabs retain hidden aria-controls targets", () => {
  const markup = renderToStaticMarkup(<InactiveResultTabPanels activeTab="statistics" />);

  for (const tab of ["events", "patterns", "visualization"]) {
    assert.match(markup, new RegExp(`id="panel-${tab}"`));
    assert.match(markup, new RegExp(`aria-labelledby="tab-${tab}"`));
  }
  assert.equal((markup.match(/role="tabpanel"/gu) ?? []).length, 3);
  assert.equal((markup.match(/hidden=""/gu) ?? []).length, 3);
  assert.doesNotMatch(markup, /id="panel-statistics"/u);
});
