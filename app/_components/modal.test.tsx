import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { Modal } from "./modal";

test("dialogs connect unique titles and optional descriptions to their surfaces", () => {
  const markup = renderToStaticMarkup(
    <>
      <Modal title="First dialog" subtitle="First description" onClose={() => {}}>First body</Modal>
      <Modal title="Second dialog" subtitle="Second description" onClose={() => {}}>Second body</Modal>
    </>,
  );
  const relationships = [...markup.matchAll(/<dialog[^>]*aria-describedby="([^"]+)" aria-labelledby="([^"]+)"/gu)];
  assert.equal(relationships.length, 2);
  assert.notEqual(relationships[0]?.[1], relationships[1]?.[1]);
  assert.notEqual(relationships[0]?.[2], relationships[1]?.[2]);
  for (const relationship of relationships) {
    assert.match(markup, new RegExp(`id="${relationship[1]}"`, "u"));
    assert.match(markup, new RegExp(`id="${relationship[2]}"`, "u"));
  }
});

test("a dialog without a subtitle does not point at a missing description", () => {
  const markup = renderToStaticMarkup(<Modal title="Undescribed" onClose={() => {}}>Body</Modal>);
  assert.doesNotMatch(markup, /aria-describedby/u);
  assert.match(markup, /aria-labelledby=/u);
});
