import assert from "node:assert/strict";
import test from "node:test";
import { browser } from "@/lib/testing/fake-browser";
import { act, useState } from "react";
import { createRoot } from "react-dom/client";
import { fakeEvent } from "@/lib/testing/fake-dom";
import { createNearbyDraft, nearbySearch, type NearbyDraft } from "@/lib/search/nearby-events";
import { NearbyContextEditor } from "./nearby-context-editor";

const initial = createNearbyDraft({
  anchorTime: "2026-09-12T09:00:00.123456789Z",
  earliest: "2026-09-12T08:55:00.123456789Z",
  latest: "2026-09-12T09:05:00.123456789Z",
  index: "main", host: "edge", source: "file", clipped: false,
  fields: [{ field: "trace_id", scalar: { kind: "string", value: "trace*literal" } }],
});

test("context chip edits remain drafts until one Apply and full editor detaches", async () => {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  const changes: NearbyDraft[] = [];
  const applied: string[] = [];
  let detached = 0;
  function Harness() {
    const [draft, setDraft] = useState(initial);
    return <NearbyContextEditor draft={draft} onChange={(next) => { changes.push(next); setDraft(next); }} onApply={(next) => applied.push(nearbySearch(next).query)} onDetach={() => { detached += 1; }} />;
  }
  const click = async (text: string) => {
    const button = container.querySelectorAll("button").find((item) => item.textContent === text);
    assert.ok(button, `missing button ${text}`);
    await act(async () => { button.dispatchEvent(fakeEvent("click")); });
  };
  try {
    await act(async () => { root.render(<Harness />); });
    await click("+ trace_id = trace*literal");
    const enabled = container.querySelector('input[aria-label="Enable context comparison"]');
    assert.ok(enabled);
    await act(async () => { enabled.checked = true; enabled.dispatchEvent(fakeEvent("click")); });
    assert.equal(changes.at(-1)?.comparisons.find((item) => item.field === "trace_id")?.enabled, true);
    await click("✓ host = edge");
    const value = container.querySelector('textarea[aria-label="Context value"]');
    assert.ok(value);
    await act(async () => { value.value = "edge*second"; value.dispatchEvent(fakeEvent("input")); });
    assert.equal(changes.at(-1)?.comparisons.find((item) => item.field === "host")?.scalar.value, "edge*second");
    assert.deepEqual(applied, []);
    await click("Apply context");
    assert.equal(applied.length, 1);
    assert.match(applied[0]!, /'host' = "edge\*second"/u);
    assert.match(applied[0]!, /'trace_id' = "trace\*literal"/u);
    await click("Use full SPL editor");
    assert.equal(detached, 1);
    assert.equal(applied.length, 1);
  } finally {
    await act(async () => root.unmount());
    container.parentNode?.removeChild(container);
  }
});
