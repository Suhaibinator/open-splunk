import assert from "node:assert/strict";
import test from "node:test";

// Install the browser before react-dom/client chooses its DOM implementation.
import { browser } from "@/lib/testing/fake-browser";
import { act } from "react";
import { createRoot } from "react-dom/client";

import { GetHECOperationalSnapshotResponse } from "@/gen/ts/open_splunk/hec_admin_api";
import { GetSystemBootstrapResponse, ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api";
import * as clientModule from "@/lib/api/open-splunk-client";
import type { FakeElement } from "@/lib/testing/fake-dom";
import * as selectModule from "../_components/select";
import { BackendAdminConsole } from "./backend-admin-console";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function endpoint(enabled = true) {
  const response = deferred<GetHECOperationalSnapshotResponse>();
  const signals: AbortSignal[] = [];
  const client = {
    system: { bootstrap: () => Promise.resolve(GetSystemBootstrapResponse.fromPartial({
      features: enabled ? [ServerFeature.SERVER_FEATURE_HEC_INGESTION] : [],
      serverTime: new Date("2026-09-12T00:00:00Z"),
    })) },
    indexes: { list: () => Promise.resolve({ indexes: [] }) },
    ingestionTokens: { list: () => Promise.resolve({ ingestionTokens: [] }) },
    hec: {
      getOperationalSnapshot(_request: unknown, options: { signal: AbortSignal }) {
        signals.push(options.signal);
        // Deliberately ignore cancellation: completion guards must protect the UI too.
        return response.promise;
      },
    },
  } as unknown as OpenSplunkApiClient;
  return { client, response, signals };
}

function snapshot(requestTotal: bigint) {
  return GetHECOperationalSnapshotResponse.fromPartial({
    observedAt: new Date("2026-09-12T00:00:00Z"),
    request: { requests: requestTotal },
  });
}

async function settle() {
  await act(async () => new Promise((resolve) => setImmediate(resolve)));
}

function requests(container: FakeElement) {
  const label = container.querySelectorAll("dt").find((element) => element.textContent === "Requests");
  return label?.parentNode?.querySelector("dd")?.textContent;
}

test("HEC console clears old values, rejects stale responses and aborts on unmount", async () => {
  const realFactory = clientModule.createOpenSplunkApiClient;
  const factory = clientModule as unknown as { createOpenSplunkApiClient: typeof realFactory };
  const realSelect = selectModule.Select;
  const selects = selectModule as unknown as { Select: typeof realSelect };
  // The unrelated navigation select has separate browser focus contracts.
  selects.Select = () => <span />;
  const clients = new Map<string, ReturnType<typeof endpoint>>();
  factory.createOpenSplunkApiClient = (options) => {
    const target = clients.get(options?.baseUrl ?? "");
    assert.ok(target, `unknown endpoint ${options?.baseUrl}`);
    return target.client;
  };
  Object.assign(window.location, {
    href: "https://hec-lifecycle.test/admin/?section=server",
    origin: "https://hec-lifecycle.test",
    pathname: "/admin/",
    search: "?section=server",
  });
  browser.storage.clear();
  const errors: unknown[][] = [];
  const realError = console.error;
  console.error = (...arguments_: unknown[]) => { errors.push(arguments_); };
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  let mounted = true;
  async function connect(name: string, enabled = true) {
    const target = endpoint(enabled);
    const url = `https://${name}.test`;
    clients.set(url, target);
    await act(async () => root.render(<BackendAdminConsole apiBaseUrl={url} />));
    await settle();
    return target;
  }
  try {
    const first = await connect("first");
    assert.match(container.textContent, /Loading HEC operations/u);
    await act(async () => first.response.resolve(snapshot(18_446_744_073_709_551_615n)));
    assert.equal(requests(container), 18_446_744_073_709_551_615n.toLocaleString());

    const abandoned = await connect("abandoned");
    assert.equal(first.signals[0].aborted, true);
    assert.equal(requests(container), undefined, "old counters remained during replacement load");
    assert.match(container.textContent, /Loading HEC operations/u);
    const current = await connect("current");
    assert.equal(abandoned.signals[0].aborted, true);
    await act(async () => current.response.resolve(snapshot(0n)));
    assert.equal(requests(container), "0", "a process reset must replace the previous high counter");
    await act(async () => abandoned.response.resolve(snapshot(999n)));
    assert.equal(requests(container), "0", "an aborted request overwrote the current snapshot");

    const failure = await connect("failure");
    await act(async () => failure.response.reject(new Error("HEC snapshot failed")));
    assert.match(container.textContent, /HEC operations could not be loaded/u);
    assert.equal(requests(container), undefined);

    const pending = await connect("pending");
    const disabled = await connect("disabled", false);
    assert.equal(pending.signals[0].aborted, true);
    assert.equal(disabled.signals.length, 0, "a disabled backend was queried for HEC telemetry");
    await act(async () => pending.response.reject(new Error("stale failure")));
    assert.match(container.textContent, /HTTP Event Collector is disabled/u);
    assert.doesNotMatch(container.textContent, /stale failure/u);

    const unmounting = await connect("unmounting");
    await act(async () => root.unmount());
    mounted = false;
    assert.equal(unmounting.signals[0].aborted, true);
    await act(async () => unmounting.response.resolve(snapshot(321n)));
    assert.equal(container.textContent, "");
    assert.deepEqual(errors, []);
  } finally {
    if (mounted) await act(async () => root.unmount());
    container.parentNode?.removeChild(container);
    factory.createOpenSplunkApiClient = realFactory;
    selects.Select = realSelect;
    console.error = realError;
  }
});

test.after(() => browser.uninstall());
