import assert from "node:assert/strict";
import test from "node:test";

import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { AnalyticsSampleStatus } from "./analytics-sample-status";

test("sample status component labels complete live history", () => {
  const markup = renderToStaticMarkup(createElement(AnalyticsSampleStatus, {
    complete: true,
    loaded: 1,
    totalSize: 1n,
    totalSizeExact: true,
  }));
  assert.match(markup, /data-complete="true"/);
  assert.match(markup, /Live data · 1 search/);
});

test("sample status component discloses bounded partial history", () => {
  const markup = renderToStaticMarkup(createElement(AnalyticsSampleStatus, {
    complete: false,
    loaded: 800,
    totalSize: 1_240n,
    totalSizeExact: true,
  }));
  assert.match(markup, /data-complete="false"/);
  assert.match(markup, /Partial sample · 800 of 1,240/);
});
