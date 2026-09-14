import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { createNearbyDraft } from "@/lib/search/nearby-events";
import { NearbyContextEditor } from "./nearby-context-editor";

const draft = createNearbyDraft({
  anchorTime: "1900-01-01T00:00:00.000000001Z",
  earliest: "1900-01-01T00:00:00Z",
  latest: "1900-01-01T00:05:00.000000001Z",
  index: "main", host: "edge*", source: "file", clipped: true,
  fields: [{ field: "trace_id", scalar: { kind: "string", value: "a<b>" } }],
});
const callbacks = { onChange: () => undefined, onApply: () => undefined, onDetach: () => undefined };

test("nearby context shows clipped exact bounds, editable scope, inactive ID and one apply action", () => {
  const markup = renderToStaticMarkup(<NearbyContextEditor draft={draft} {...callbacks} />);
  assert.match(markup, /1900-01-01T00:00:00.000000001Z/u);
  assert.match(markup, /supported 1900–2262 timestamp range/u);
  assert.match(markup, /Earliest \(inclusive\)/u);
  assert.match(markup, /Latest \(exclusive\)/u);
  assert.match(markup, /data-enabled="false"[^>]*>\+ trace_id = a&lt;b&gt;/u);
  assert.equal((markup.match(/>Apply context</gu) ?? []).length, 1);
  assert.match(markup, /Use full SPL editor/u);
  assert.match(markup, /Add comparison/u);
  assert.doesNotMatch(markup, /<select/u);
});

test("nearby apply remains unavailable while pending or when exact scope is absent", () => {
  const busy = renderToStaticMarkup(<NearbyContextEditor draft={draft} busy {...callbacks} />);
  assert.match(busy, /disabled="">Apply context/u);
  const invalid = renderToStaticMarkup(<NearbyContextEditor draft={{ ...draft, comparisons: [] }} {...callbacks} />);
  assert.match(invalid, /Keep an exact index comparison enabled/u);
  assert.match(invalid, /disabled="">Apply context/u);
});
