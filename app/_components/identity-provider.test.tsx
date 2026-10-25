import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM.
import { browser } from "@/lib/testing/fake-browser";
import { act, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { IdentityError, type IdentityClient } from "@/lib/api/identity";
import { FAKE_IDENTITY_PASSWORD, createFakeIdentityClient } from "@/lib/api/identity-fake";

import {
  IdentityProvider,
  RequireCapability,
  createIdentitySessionStore,
  useIdentity,
  type IdentityContextValue,
} from "./identity-provider";

async function mount(node: ReactNode): Promise<{ container: ReturnType<typeof browser.document.createElement>; unmount: () => Promise<void> }> {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  await act(async () => root.render(node));
  return {
    container,
    unmount: async () => {
      await act(async () => root.unmount());
      container.parentNode?.removeChild(container);
    },
  };
}

/** Reports the latest context value and prints the status and capability gates. */
function Probe({ capture }: { capture: (value: IdentityContextValue) => void }) {
  const identity = useIdentity();
  capture(identity);
  return (
    <>
      <span data-probe="status">{identity.status}</span>
      <span data-probe="organization">{identity.activeOrganization?.slug ?? "none"}</span>
      <RequireCapability capability="search" fallback={<span data-gate="search">denied</span>} loading={<span data-gate="search">loading</span>}>
        <span data-gate="search">allowed</span>
      </RequireCapability>
      <RequireCapability capability="members.admin" fallback={<span data-gate="members">denied</span>}>
        <span data-gate="members">allowed</span>
      </RequireCapability>
      <RequireCapability capability={["server.admin", "search"]} match="any" fallback={<span data-gate="any">denied</span>}>
        <span data-gate="any">allowed</span>
      </RequireCapability>
      <RequireCapability capability={["server.admin", "search"]} fallback={<span data-gate="all">denied</span>}>
        <span data-gate="all">allowed</span>
      </RequireCapability>
      <RequireCapability capability={[]} fallback={<span data-gate="none">denied</span>}>
        <span data-gate="none">allowed</span>
      </RequireCapability>
    </>
  );
}

/** Lets every pending client promise, and the renders they cause, settle. */
async function settle(): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      globalThis.setTimeout(resolve, 10);
    });
  });
}

function read(container: { querySelector(selector: string): { textContent: string | null } | null }, selector: string): string | null {
  return container.querySelector(selector)?.textContent ?? null;
}

function deferredClient(base: IdentityClient): { client: IdentityClient; release: () => void } {
  const { promise: gate, resolve: release } = Promise.withResolvers<void>();
  const client = Object.create(base) as IdentityClient;
  client.getPrincipal = async () => {
    await gate;
    return base.getPrincipal();
  };
  return { client, release };
}

test("the provider reads the principal once and gates on its capabilities", async () => {
  const fake = createFakeIdentityClient({ initialSignedInEmail: "mark@open-splunk.test" });
  const { client, release } = deferredClient(fake);
  const sink: { current: IdentityContextValue | null } = { current: null };
  const view = await mount(<IdentityProvider client={client}><Probe capture={(value) => { sink.current = value; }} /></IdentityProvider>);
  try {
    assert.equal(read(view.container, '[data-probe="status"]'), "loading");
    assert.equal(read(view.container, '[data-gate="search"]'), "loading");
    assert.equal(read(view.container, '[data-gate="members"]'), null, "no loading node renders nothing");

    release();
    await settle();
    assert.equal(read(view.container, '[data-probe="status"]'), "signed_in");
    assert.equal(read(view.container, '[data-probe="organization"]'), "acme");
    assert.equal(read(view.container, '[data-gate="search"]'), "allowed");
    assert.equal(read(view.container, '[data-gate="members"]'), "denied");
    assert.equal(read(view.container, '[data-gate="any"]'), "allowed");
    assert.equal(read(view.container, '[data-gate="all"]'), "denied");
    assert.equal(read(view.container, '[data-gate="none"]'), "denied", "an empty requirement never grants");
    assert.equal(sink.current?.client, client);
    assert.ok(sink.current?.hasCapability("objects.own"));
    assert.ok(!sink.current?.hasCapability("org.admin"));
  } finally {
    await view.unmount();
  }
});

test("switching organization and signing out flow through the context", async () => {
  const client = createFakeIdentityClient({ initialSignedInEmail: "owen@open-splunk.test" });
  const initialPrincipal = await client.getPrincipal();
  const sink: { current: IdentityContextValue | null } = { current: null };
  const view = await mount(<IdentityProvider client={client} initialPrincipal={initialPrincipal}><Probe capture={(value) => { sink.current = value; }} /></IdentityProvider>);
  try {
    assert.equal(read(view.container, '[data-probe="status"]'), "signed_in", "a known principal skips the loading state");
    assert.equal(read(view.container, '[data-gate="members"]'), "denied");

    await act(async () => {
      await sink.current?.setActiveOrganization("2");
    });
    assert.equal(read(view.container, '[data-probe="organization"]'), "globex");
    assert.equal(sink.current?.principal?.role, "owner");
    assert.equal(read(view.container, '[data-gate="members"]'), "allowed");

    await act(async () => {
      await sink.current?.signOut();
    });
    assert.equal(read(view.container, '[data-probe="status"]'), "signed_out");
    assert.equal(read(view.container, '[data-probe="organization"]'), "none");
    assert.equal(read(view.container, '[data-gate="search"]'), "denied");
    assert.equal(await client.getPrincipal(), null, "signing out ends the client session");

    const result = await client.loginWithEmail({ email: "alice@open-splunk.test", password: FAKE_IDENTITY_PASSWORD });
    assert.equal(result.status, "signed_in");
    await act(async () => sink.current?.adoptPrincipal(result.principal));
    assert.equal(read(view.container, '[data-probe="status"]'), "signed_in");
    assert.equal(read(view.container, '[data-gate="members"]'), "allowed");
  } finally {
    await view.unmount();
  }
});

test("a backend page without a server binding renders as unavailable, not as an error boundary", async () => {
  const sink: { current: IdentityContextValue | null } = { current: null };
  const view = await mount(<IdentityProvider dataMode="backend"><Probe capture={(value) => { sink.current = value; }} /></IdentityProvider>);
  try {
    assert.equal(read(view.container, '[data-probe="status"]'), "unavailable");
    assert.equal(sink.current?.error?.kind, "not_configured");
    assert.equal(sink.current?.client, null);
    assert.equal(read(view.container, '[data-gate="search"]'), "denied");
    await assert.rejects(sink.current?.refresh() ?? Promise.resolve(), (error: unknown) => error instanceof IdentityError && error.kind === "not_configured");
  } finally {
    await view.unmount();
  }
});

test("the demo data mode signs in as the seeded site admin", async () => {
  const sink: { current: IdentityContextValue | null } = { current: null };
  const view = await mount(<IdentityProvider dataMode="demo"><Probe capture={(value) => { sink.current = value; }} /></IdentityProvider>);
  try {
    await settle();
    assert.equal(read(view.container, '[data-probe="status"]'), "signed_in");
    assert.equal(sink.current?.principal?.user.email, "ada@open-splunk.test");
    assert.ok(sink.current?.hasCapability("server.admin"));
    assert.equal(read(view.container, '[data-gate="all"]'), "allowed");
  } finally {
    await view.unmount();
  }
});

function Orphan() {
  useIdentity();
  return null;
}

test("useIdentity outside a provider fails loudly", async () => {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  try {
    await assert.rejects(async () => {
      await act(async () => root.render(<Orphan />));
    }, /needs an IdentityProvider/u);
  } finally {
    await act(async () => root.unmount());
    container.parentNode?.removeChild(container);
  }
});

test("the session store keeps a valid principal through a failed refresh and drops it when the session ends", async () => {
  const client = createFakeIdentityClient({ initialSignedInEmail: "mark@open-splunk.test" });
  const store = createIdentitySessionStore(client);
  const notifications: string[] = [];
  const unsubscribe = store.subscribe(() => notifications.push(store.getSnapshot().status));
  assert.equal(store.getSnapshot().status, "loading");

  const principal = await store.refresh();
  assert.equal(principal?.user.username, "mark");
  assert.equal(store.getSnapshot().status, "signed_in");

  client.failNext("getPrincipal");
  await assert.rejects(store.refresh(), (error: unknown) => error instanceof IdentityError && error.kind === "network");
  assert.equal(store.getSnapshot().status, "signed_in");
  assert.equal(store.getSnapshot().principal?.user.username, "mark");
  assert.equal(store.getSnapshot().error?.kind, "network");

  client.failNext("getPrincipal", new IdentityError("unauthenticated", "expired"));
  await assert.rejects(store.refresh());
  assert.equal(store.getSnapshot().status, "signed_out");
  assert.equal(store.getSnapshot().principal, null);

  unsubscribe();
  store.adopt(null);
  assert.deepEqual(notifications, ["signed_in", "signed_in", "signed_out"]);
});

test("a failed first read with no principal is unavailable, and a newer read wins a race", async () => {
  const client = createFakeIdentityClient({ initialSignedInEmail: "mark@open-splunk.test" });
  client.failNext("getPrincipal");
  const store = createIdentitySessionStore(client);
  await assert.rejects(store.refresh());
  assert.equal(store.getSnapshot().status, "unavailable");
  assert.equal(store.getSnapshot().error?.kind, "network");

  const slow = createFakeIdentityClient({ initialSignedInEmail: "mark@open-splunk.test", latencyMs: 5 });
  const racing = createIdentitySessionStore(slow);
  const stale = racing.refresh();
  racing.adopt(null);
  assert.equal((await stale)?.user.username, "mark", "the superseded read still answers its caller");
  assert.equal(racing.getSnapshot().status, "signed_out", "an adoption supersedes the read already in flight");

  const signingOut = createFakeIdentityClient({ initialSignedInEmail: "ada@open-splunk.test" });
  const signedIn = createIdentitySessionStore(signingOut, { principal: await signingOut.getPrincipal() });
  signingOut.failNext("logout");
  await assert.rejects(signedIn.signOut());
  assert.equal(signedIn.getSnapshot().status, "signed_in", "a failed sign-out does not pretend the session ended");
  await signedIn.signOut();
  assert.equal(signedIn.getSnapshot().status, "signed_out");
});
