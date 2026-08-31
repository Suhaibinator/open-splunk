import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import {
  STATS_MULTIVALUE_TITLE_MEMBER_CAP,
  StatsFlatMultivalueValue,
  StatsMultivalueList,
  statsFlatMultivalueDisplay,
  statsFlatMultivalueWhiteSpace,
  statsMultivalueLineMembers,
  statsMultivalueTitle,
  statsMultivalueVisibleMemberCount,
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

  // The `.statistics-multivalue-lines` clamp is asserted against computed style
  // in integration/style-contracts/css-contracts.spec.ts.
  const panel = readFileSync(
    path.join(process.cwd(), "app", "search-workspace", "panels", "statistics-panel.tsx"),
    "utf8",
  );
  assert.match(panel, /<StatsFlatMultivalueValue/u);
  assert.match(panel, /tabIndex=\{virtualWindow\.virtualized \? 0 : undefined\}/u);
  assert.doesNotMatch(panel, /whiteSpace:\s*statsFlatMultivalueWhiteSpace/u);

  // The stacked presentation is wired through the panel, not only exported.
  assert.match(panel, /<StatsMultivalueList/u);
  assert.match(panel, /statsMultivalueLineMembers\(/u);
  assert.match(panel, /statsMultivalueTitle\(/u);
  assert.match(panel, /setMultivalueDialog/u);
  assert.match(panel, /<Modal/u);
});

test("stacked stats members require an invisible delimiter", () => {
  const members = ["alpha", "beta", "gamma"];
  assert.deepEqual(statsMultivalueLineMembers(members, " "), members);
  assert.deepEqual(statsMultivalueLineMembers(members, "\n"), members);
  assert.deepEqual(statsMultivalueLineMembers(members, ""), members);
  assert.deepEqual(statsMultivalueLineMembers(members, " \r\n "), members);
  assert.equal(statsMultivalueLineMembers(members, ","), undefined);
  assert.equal(statsMultivalueLineMembers(members, " / "), undefined);
  assert.equal(statsMultivalueLineMembers(members, undefined), undefined);
});

test("stacked stats members fail closed on unsupported cells", () => {
  assert.equal(statsMultivalueLineMembers("alpha", " "), undefined);
  assert.equal(statsMultivalueLineMembers(null, " "), undefined);
  assert.equal(statsMultivalueLineMembers(["alpha", {}], " "), undefined);
  assert.equal(statsMultivalueLineMembers(["alpha", ["beta"]], " "), undefined);
  assert.equal(statsMultivalueLineMembers(["alpha", Number.NaN], " "), undefined);
  assert.equal(
    statsMultivalueLineMembers(["alpha", Number.POSITIVE_INFINITY], " "),
    undefined,
  );
});

test("stacked stats members carry the canonical scalar text", () => {
  assert.deepEqual(
    statsMultivalueLineMembers(["alpha", -7, 1.25, -0, true, false, null], " "),
    ["alpha", "-7", "1.25", "-0", "true", "false", "null"],
  );
  assert.deepEqual(statsMultivalueLineMembers([], " "), []);
});

test("visible stats member counts keep every row the same height", () => {
  assert.equal(statsMultivalueVisibleMemberCount("compact", 2), 2);
  assert.equal(statsMultivalueVisibleMemberCount("standard", 2), 2);
  assert.equal(statsMultivalueVisibleMemberCount("compact", 5), 1);
  assert.equal(statsMultivalueVisibleMemberCount("standard", 5), 2);
});

test("stats multivalue titles cap their member list", () => {
  assert.equal(statsMultivalueTitle([]), "");
  assert.equal(statsMultivalueTitle(["alpha", "beta"]), "alpha\nbeta");

  const capped = Array.from(
    { length: STATS_MULTIVALUE_TITLE_MEMBER_CAP },
    (_unused, index) => `value-${index.toString()}`,
  );
  assert.equal(statsMultivalueTitle(capped), capped.join("\n"));

  const overflowing = Array.from(
    { length: STATS_MULTIVALUE_TITLE_MEMBER_CAP + 5 },
    (_unused, index) => `value-${index.toString()}`,
  );
  const lines = statsMultivalueTitle(overflowing).split("\n");
  assert.equal(lines.length, STATS_MULTIVALUE_TITLE_MEMBER_CAP + 1);
  assert.equal(lines[STATS_MULTIVALUE_TITLE_MEMBER_CAP - 1], "value-39");
  assert.equal(lines[STATS_MULTIVALUE_TITLE_MEMBER_CAP], "… +5 more");
});

test("stacked stats cells omit the overflow button when everything fits", () => {
  assert.equal(
    renderToStaticMarkup(createElement(StatsMultivalueList, {
      fieldName: "path",
      members: ["alpha", "beta"],
      visibleMemberCount: 2,
      onShowAll: () => undefined,
    })),
    '<span class="statistics-multivalue-list">'
    + '<span class="statistics-multivalue-item">alpha</span>'
    + '<span class="statistics-multivalue-item">beta</span>'
    + "</span>",
  );
});

test("stacked stats cells open the remaining members through a dialog button", () => {
  const markup = renderToStaticMarkup(createElement(StatsMultivalueList, {
    fieldName: "path",
    members: ["alpha", "beta", "gamma", "delta", "epsilon"],
    visibleMemberCount: 2,
    onShowAll: () => undefined,
  }));
  assert.equal(
    markup,
    '<span class="statistics-multivalue-list">'
    + '<span class="statistics-multivalue-item">alpha</span>'
    + '<span class="statistics-multivalue-item">beta</span>'
    + '<button aria-haspopup="dialog" aria-label="Show all 5 values for path"'
    + ' class="statistics-multivalue-more" type="button">+3 more</button>'
    + "</span>",
  );
});
