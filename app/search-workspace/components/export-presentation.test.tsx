import assert from "node:assert/strict";
import test from "node:test";

import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { ExportArtifactStatus, ExportDownloadContent } from "./export-presentation";

test("renders a valid export artifact with a text-backed success treatment", () => {
  const markup = renderToStaticMarkup(createElement(ExportArtifactStatus, {
    expired: false,
    expiry: "Aug 29, 2026, 12:03:22 AM PDT",
    metadataValid: true,
    titleId: "status-title",
  }));
  assert.match(markup, /status status--icon status--success/);
  assert.match(markup, /Export ready/);
  assert.match(markup, /finished materializing/);
});

test("renders an expired artifact with a warning and its expiry", () => {
  const markup = renderToStaticMarkup(createElement(ExportArtifactStatus, {
    expired: true,
    expiry: "Aug 29, 2026, 12:03:22 AM PDT",
    metadataValid: true,
    titleId: "status-title",
  }));
  assert.match(markup, /status status--icon status--warning/);
  assert.match(markup, /Export artifact expired/);
  assert.match(markup, /Aug 29, 2026, 12:03:22 AM PDT/);
});

test("renders invalid artifact metadata as an error", () => {
  const markup = renderToStaticMarkup(createElement(ExportArtifactStatus, {
    expired: false,
    expiry: null,
    metadataValid: false,
    titleId: "status-title",
  }));
  assert.match(markup, /status status--icon status--error/);
  assert.match(markup, /Export artifact unavailable/);
  assert.match(markup, /incomplete artifact metadata/);
});

test("renders a spinning loading icon only while a download is pending", () => {
  const pendingMarkup = renderToStaticMarkup(createElement(ExportDownloadContent, { format: "CSV", pending: true }));
  const readyMarkup = renderToStaticMarkup(createElement(ExportDownloadContent, { format: "JSON Lines", pending: false }));
  assert.match(pendingMarkup, /app-icon--spin/);
  assert.match(pendingMarkup, /Downloading…/);
  assert.doesNotMatch(readyMarkup, /app-icon--spin/);
  assert.match(readyMarkup, /Download JSON Lines/);
});
