import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { ProductShell, productMenuControlId } from "./product-shell";

test("menu triggers control only their mounted popover", () => {
  for (const menu of ["apps", "help", "user"] as const) {
    assert.equal(productMenuControlId(null, menu), undefined);
    assert.equal(productMenuControlId(menu, menu), `suite-${menu === "apps" ? "app" : menu}-popover`);

    for (const other of ["apps", "help", "user"] as const) {
      if (other !== menu) assert.equal(productMenuControlId(other, menu), undefined);
    }
  }
});

test("the closed product shell exposes collapsed menu semantics without dangling controls", () => {
  const markup = renderToStaticMarkup(
    <ProductShell activeSection="home" appName="Search & Reporting" dataMode="demo">
      <p>Page content</p>
    </ProductShell>,
  );

  assert.equal(markup.match(/aria-haspopup="menu"/gu)?.length, 3);
  assert.equal(markup.match(/aria-haspopup="menu" aria-expanded="false"/gu)?.length, 3);
  assert.doesNotMatch(markup, /aria-controls=/u);
  assert.doesNotMatch(markup, /id="suite-(?:app|help|user)-popover"/u);
});
