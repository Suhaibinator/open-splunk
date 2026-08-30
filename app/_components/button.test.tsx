import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { Button, buttonClassName } from "./button";

test("the default button names the primitive and nothing else", () => {
  // The point of the consolidation is that a plain button is one class. If a
  // default ever starts emitting `button button--default`, every call site that
  // spells the class by hand has silently stopped matching the component.
  assert.equal(buttonClassName(), "button");
  assert.equal(buttonClassName({ size: "default", variant: "default" }), "button");
});

test("each option contributes exactly one modifier, in a stable order", () => {
  assert.equal(buttonClassName({ variant: "primary" }), "button button--primary");
  assert.equal(buttonClassName({ variant: "danger", size: "compact" }), "button button--danger button--compact");
  assert.equal(buttonClassName({ icon: true, variant: "ghost" }), "button button--ghost button--icon");
  assert.equal(buttonClassName({ block: true }), "button button--block");
  assert.equal(
    buttonClassName({ block: true, className: "run-button", icon: true, size: "compact", variant: "secondary" }),
    "button button--secondary button--compact button--icon button--block run-button",
  );
});

test("a feature class rides alongside the primitive rather than replacing it", () => {
  // A feature class is only ever layout, so dropping the primitive would strip
  // the control's tone and height while still looking deliberate in review.
  assert.equal(buttonClassName({ className: "history-clear-button" }), "button history-clear-button");
});

test("the component renders a non-submitting button unless asked otherwise", () => {
  // A bare <button> inside a form submits it. Every migrated call site relied on
  // an explicit type="button", so the default has to keep that promise.
  const markup = renderToStaticMarkup(<Button>Cancel</Button>);
  assert.match(markup, /type="button"/);
  assert.match(markup, /class="button"/);

  const submit = renderToStaticMarkup(<Button type="submit" variant="primary">Apply</Button>);
  assert.match(submit, /type="submit"/);
  assert.match(submit, /class="button button--primary"/);
});

test("the component forwards the attributes a control needs to stay accessible", () => {
  const markup = renderToStaticMarkup(<Button aria-label="Delete index" disabled variant="danger">Delete</Button>);
  assert.match(markup, /aria-label="Delete index"/);
  assert.match(markup, /disabled=""/);
});
