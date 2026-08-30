import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { FieldNote, fieldControlProps, fieldNoteId } from "./field-validation";

test("a valid control carries no aria-invalid at all", () => {
  // React drops an `undefined` attribute entirely, and the stylesheet keys on
  // the attribute's presence -- so a valid field is styled by the absence of a
  // rule rather than by a second rule undoing the first.
  const props = fieldControlProps("search-limit-memory", null);
  assert.equal(props["aria-invalid"], undefined);
  assert.equal(props["aria-describedby"], "search-limit-memory-note");
});

test("an invalid control is marked for the stylesheet and for a screen reader at once", () => {
  // These cannot come apart: `[aria-invalid="true"]` in the form primitive is
  // what paints the field, so a form that skipped the attribute would be styled
  // as valid, and one that skipped `aria-describedby` would show a colour a
  // keyboard user never hears.
  const props = fieldControlProps("search-limit-memory", "Enter 1 MiB–64 GiB.");
  assert.equal(props["aria-invalid"], true);
  assert.equal(props["aria-describedby"], fieldNoteId("search-limit-memory"));
});

test("the note the control points at is the note that is rendered", () => {
  const markup = renderToStaticMarkup(
    <FieldNote error={null} fieldId="search-limit-memory">1 MiB–64 GiB; default 1 GiB.</FieldNote>,
  );
  assert.equal(markup, '<small id="search-limit-memory-note">1 MiB–64 GiB; default 1 GiB.</small>');
});

test("an error replaces the hint rather than stacking under it", () => {
  // Every message this product writes restates the constraint the hint carried,
  // so showing both says the same thing twice and pushes the rest of the form
  // down a line while somebody is still typing.
  const markup = renderToStaticMarkup(
    <FieldNote error="Enter 1 MiB–64 GiB." fieldId="search-limit-memory">
      1 MiB–64 GiB; default 1 GiB.
    </FieldNote>,
  );
  assert.match(markup, /Enter 1 MiB–64 GiB\./u);
  assert.doesNotMatch(markup, /default 1 GiB/u);
});

test("an error is distinguished by a glyph as well as by colour", () => {
  // `.field-error` paints the note with --status-error, and colour alone does
  // not distinguish an error for a reader who cannot see the hue.
  const markup = renderToStaticMarkup(
    <FieldNote error="Enter a whole number greater than zero." fieldId="search-limit-threads" />,
  );
  assert.match(markup, /class="field-error"/u);
  assert.match(markup, /<svg[^>]*class="[^"]*\bapp-icon app-icon--xs\b/u);
  assert.match(markup, /aria-hidden="true"/u);
});

test("the note is not a live region, because these fields validate on every keystroke", () => {
  // `role="alert"` here would interrupt a screen reader with a half-typed
  // value's complaint on every character. The pairing announces on focus
  // instead, and a form that needs to say something on submit keeps its own
  // status region.
  const markup = renderToStaticMarkup(
    <FieldNote error="Enter 1 MiB–64 GiB." fieldId="search-limit-memory" />,
  );
  assert.doesNotMatch(markup, /role="alert"|aria-live/u);
});
