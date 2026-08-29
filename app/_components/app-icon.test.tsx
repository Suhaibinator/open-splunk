import assert from "node:assert/strict";
import test from "node:test";

import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { AppIcon, type AppIconName, type AppIconSize, StatusIcon } from "./app-icon";

const names: AppIconName[] = [
  "activity", "alert", "analytics", "bar-chart", "column-chart", "check", "chevron-down", "chevron-left",
  "chevron-right", "chevron-up", "circle-alert", "circle-x", "clock", "copy", "database",
  "download", "external-link", "file", "open", "history", "hourglass", "home",
  "info", "dashboard", "loading", "logout", "list", "menu", "minus", "more", "edit", "plus", "refresh",
  "save", "search", "settings", "share", "stop", "wrap", "trash", "warning", "users", "close", "mode",
];

test("every application icon renders a hidden, non-focusable Lucide SVG", () => {
  for (const name of names) {
    const markup = renderToStaticMarkup(createElement(AppIcon, { name }));
    assert.match(markup, /^<svg /);
    assert.match(markup, /aria-hidden="true"/);
    assert.match(markup, /focusable="false"/);
    assert.match(markup, /stroke-width="2"/);
  }
});

test("application icon size variants are stable", () => {
  for (const size of ["xs", "sm", "md", "lg"] satisfies AppIconSize[]) {
    const markup = renderToStaticMarkup(createElement(AppIcon, { name: "search", size }));
    assert.match(markup, new RegExp(`app-icon--${size}`));
  }
});

test("status icons expose tone styling without duplicating accessible text", () => {
  for (const tone of ["success", "info", "warning", "error", "neutral"] as const) {
    const markup = renderToStaticMarkup(createElement(StatusIcon, { icon: "check", tone }));
    assert.match(markup, /aria-hidden="true"/);
    assert.match(markup, new RegExp(`status-icon--${tone}`));
  }
});
