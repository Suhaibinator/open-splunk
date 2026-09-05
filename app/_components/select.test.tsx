import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { Select, SelectOption } from "./select";

test("the select exposes a combobox, a manual popover listbox, and form value", () => {
  const markup = renderToStaticMarkup(
    <Select defaultValue="2" id="time-zone" name="time-zone" required>
      <SelectOption value="1">Pacific</SelectOption>
      <SelectOption value="2">UTC</SelectOption>
    </Select>,
  );
  assert.match(markup, /class="select"/u);
  assert.match(markup, /class="select__trigger"[^>]*role="combobox"/u);
  assert.match(markup, /id="time-zone-listbox"[^>]*popover="manual"[^>]*role="listbox"/u);
  assert.match(markup, /class="select__input"[^>]*required=""[^>]*name="time-zone"[^>]*value="2"/u);
  assert.match(markup, /role="option"/u);
  assert.match(markup, /aria-required="true"/u);
  assert.doesNotMatch(markup, /<select\b/u);
});

test("options take their label as a value when their value is omitted, including fragments", () => {
  const markup = renderToStaticMarkup(
    <Select defaultValue="UTC">
      <>
        <SelectOption>America/Los_Angeles</SelectOption>
        <SelectOption>UTC</SelectOption>
      </>
    </Select>,
  );
  assert.match(markup, /value="UTC"/u);
  assert.match(markup, /aria-selected="true"/u);
});

test("a disabled select keeps both its trigger and validation input disabled", () => {
  const markup = renderToStaticMarkup(
    <Select disabled value="UTC"><SelectOption>UTC</SelectOption></Select>,
  );
  assert.equal((markup.match(/disabled=""/gu) ?? []).length, 2);
});

test("an unmatched controlled value keeps its submitted value and shows the placeholder", () => {
  const markup = renderToStaticMarkup(
    <Select name="time-zone" placeholder="Choose a time zone" value="Mars/Olympus"><SelectOption>UTC</SelectOption></Select>,
  );
  assert.match(markup, /Choose a time zone/u);
  assert.match(markup, /name="time-zone"[^>]*value="Mars\/Olympus"/u);
  assert.doesNotMatch(markup, /aria-selected="true"/u);
});

test("an option cannot become a second tab stop", () => {
  const markup = renderToStaticMarkup(
    <Select><SelectOption>UTC</SelectOption></Select>,
  );
  assert.match(markup, /role="option"[^>]*tabindex="-1"/u);
});
