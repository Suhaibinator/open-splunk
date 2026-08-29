import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { StatusDot, StatusLabel, statusClassName, type StatusTone } from "./status";

const TONES: StatusTone[] = ["success", "info", "warning", "error", "neutral", "running"];

test("a status class list names the block, one shape and one tone", () => {
  assert.equal(statusClassName("dot", "error"), "status status--dot status--error");
  assert.equal(statusClassName("icon", "success"), "status status--icon status--success");
  assert.equal(statusClassName("label", "neutral"), "status status--label status--neutral");
});

test("a feature class is appended without displacing the tone", () => {
  assert.equal(
    statusClassName("dot", "warning", "inspector-state"),
    "status status--dot status--warning inspector-state",
  );
});

test("every tone reaches the stylesheet through the one modifier family", () => {
  // `.status--*` in app/globals.css is the only place a tone is painted, so a
  // tone that stopped emitting its modifier would render a neutral swatch and
  // report the wrong outcome while looking perfectly healthy.
  for (const tone of TONES) {
    assert.match(renderToStaticMarkup(<StatusDot tone={tone} />), new RegExp(`status--${tone}"`));
  }
});

test("a bare dot is decoration and announces nothing", () => {
  // Every call site states the outcome in adjacent text; a dot that exposed
  // itself would make a screen reader read the state twice.
  const markup = renderToStaticMarkup(<StatusDot tone="success" />);
  assert.match(markup, /aria-hidden="true"/);
  assert.equal(markup, '<span aria-hidden="true" class="status status--dot status--success"></span>');
});

test("a label keeps its tone on the swatch rather than on the row", () => {
  // `.status--*` paints a background: on the row it would flood the table cell.
  const markup = renderToStaticMarkup(<StatusLabel tone="success">Completed</StatusLabel>);
  assert.match(markup, /^<span class="status status--label">/);
  assert.match(markup, /<i class="status status--dot status--success">/);
  assert.match(markup, /Completed/);
});

test("a label's feature class lands on the row, not on its swatch", () => {
  const markup = renderToStaticMarkup(<StatusLabel className="inspector-state" tone="running">Running</StatusLabel>);
  assert.match(markup, /^<span class="status status--label inspector-state">/);
  assert.match(markup, /<i class="status status--dot status--running">/);
});
