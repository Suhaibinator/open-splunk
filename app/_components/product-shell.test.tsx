import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { ProductShell, productMenuControlId, ThemeMenu } from "./product-shell";

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

test("the theme menu checks exactly one radio for every preference", () => {
  for (const preference of ["system", "light", "dark"] as const) {
    const markup = renderToStaticMarkup(<ThemeMenu preference={preference} onSelect={() => undefined} />);

    assert.equal(markup.match(/role="menuitemradio"/gu)?.length, 3);
    assert.equal(markup.match(/aria-checked="true"/gu)?.length, 1);
    assert.equal(markup.match(/aria-checked="false"/gu)?.length, 2);
    // The checked entry is the one carrying the requested label, and the
    // legend names the group for assistive technology.
    const label = preference === "system" ? "System" : preference === "light" ? "Light" : "Dark";
    assert.match(markup, new RegExp(`aria-checked="true"[^>]*>(?:<svg[^]*?</svg>)?${label}</button>`, "u"));
    assert.match(markup, /<fieldset class="suite-theme-menu"><legend class="suite-menu-label">Theme<\/legend>/u);
  }
});
