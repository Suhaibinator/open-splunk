import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import {
  StatsFlatMultivalueValue,
  statsFlatMultivalueDisplay,
  statsFlatMultivalueWhiteSpace,
} from "./statistics-multivalue";

test("flat stats multivalue display distinguishes absent and empty delimiters", () => {
  const members = ["alpha", "beta", "gamma"];
  assert.equal(statsFlatMultivalueDisplay(members, undefined), undefined);
  assert.equal(statsFlatMultivalueDisplay(members, " / "), "alpha / beta / gamma");
  assert.equal(statsFlatMultivalueDisplay(members, ""), "alphabetagamma");
  assert.equal(statsFlatMultivalueDisplay([], " / "), "");
});

test("flat stats multivalue display fails closed for unsupported cells", () => {
  assert.equal(statsFlatMultivalueDisplay("alpha", ","), undefined);
  assert.equal(statsFlatMultivalueDisplay(["alpha", {}], ","), undefined);
  assert.equal(statsFlatMultivalueDisplay(["alpha", []], ","), undefined);
  assert.equal(statsFlatMultivalueDisplay(["alpha", Number.NaN], ","), undefined);
  assert.equal(statsFlatMultivalueDisplay(["alpha", Number.POSITIVE_INFINITY], ","), undefined);
});

test("flat stats multivalue display canonically joins mixed native scalars", () => {
  assert.equal(
    statsFlatMultivalueDisplay(
      ["alpha", -7, 9, 1.25, -0, true, false, null, "9007199254740993"],
      "\n",
    ),
    "alpha\n-7\n9\n1.25\n-0\ntrue\nfalse\nnull\n9007199254740993",
  );
});

test("flat stats multivalue presentation preserves newline delimiters", () => {
  assert.equal(statsFlatMultivalueWhiteSpace(undefined), "nowrap");
  assert.equal(statsFlatMultivalueWhiteSpace(","), "nowrap");
  assert.equal(statsFlatMultivalueWhiteSpace("\n"), "pre-wrap");
  assert.equal(statsFlatMultivalueWhiteSpace(" / \r\n"), "pre-wrap");
});

test("multiline stats presentation stays inside the fixed virtual row", () => {
  const markup = renderToStaticMarkup(createElement(StatsFlatMultivalueValue, {
    delimiter: "\n",
    value: "alpha\nbeta\ngamma",
  }));
  assert.equal(
    markup,
    '<span class="statistics-multivalue-lines">alpha\nbeta\ngamma</span>',
  );
  assert.equal(renderToStaticMarkup(createElement(StatsFlatMultivalueValue, {
    delimiter: ",",
    value: "alpha,beta",
  })), "alpha,beta");

  const css = readFileSync(path.join(process.cwd(), "app", "globals.css"), "utf8");
  const rule = /\.statistics-multivalue-lines\s*\{([^}]*)\}/u.exec(css)?.[1];
  assert.ok(rule);
  assert.match(rule, /display:\s*block/u);
  assert.match(rule, /max-height:\s*calc\(var\(--statistics-row-height, 42px\) - 8px\)/u);
  assert.match(rule, /overflow:\s*hidden/u);
  assert.match(rule, /white-space:\s*pre-wrap/u);

  const panel = readFileSync(
    path.join(process.cwd(), "app", "search-workspace", "panels", "statistics-panel.tsx"),
    "utf8",
  );
  assert.match(panel, /<StatsFlatMultivalueValue/u);
  assert.match(panel, /tabIndex=\{virtualWindow\.virtualized \? 0 : undefined\}/u);
  assert.doesNotMatch(panel, /whiteSpace:\s*statsFlatMultivalueWhiteSpace/u);
});
