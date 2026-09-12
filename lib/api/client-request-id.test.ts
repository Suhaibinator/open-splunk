import assert from "node:assert/strict";
import test from "node:test";

import { BrowserCreateAction, browserClientRequestId } from "./client-request-id";

test("a logical create retains its UUID across retries and rotates after completion or edits", () => {
  const action = new BrowserCreateAction();
  const intent = { name: "example", limit: 3n, columns: ["a", "b"] };
  const first = action.requestId(intent);
  assert.match(first, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.equal(action.requestId({ columns: ["a", "b"], limit: 3n, name: "example" }), first);
  assert.notEqual(action.requestId({ ...intent, name: "edited" }), first);
  const edited = action.requestId({ ...intent, name: "edited" });
  action.complete();
  assert.notEqual(action.requestId({ ...intent, name: "edited" }), edited);
});

test("independent actions and browser identities never share a key", () => {
  assert.notEqual(new BrowserCreateAction().requestId({}), new BrowserCreateAction().requestId({}));
  assert.notEqual(browserClientRequestId(), browserClientRequestId());
});

test("binary intent and typed primitives cannot alias one another", () => {
  const action = new BrowserCreateAction();
  const first = action.requestId({ csv: new Uint8Array([1, 2]), value: 1n });
  assert.equal(action.requestId({ csv: new Uint8Array([1, 2]), value: 1n }), first);
  assert.notEqual(action.requestId({ csv: new Uint8Array([1, 3]), value: 1n }), first);
  assert.notEqual(action.requestId({ csv: [1, 2], value: "1" }), first);
});

test("late acceptance of an older action preserves the newer ambiguous retry identity", () => {
  const action = new BrowserCreateAction();
  const older = action.requestId({ query: "index=old" });
  const newerIntent = { query: "index=new" };
  const newer = action.requestId(newerIntent);
  assert.notEqual(newer, older);
  action.complete(older);
  assert.equal(action.requestId(newerIntent), newer);
  action.complete(newer);
  const subsequent = action.requestId(newerIntent);
  assert.notEqual(subsequent, newer);
  // A duplicated completion is stale once a later logical action exists too.
  action.complete(newer);
  assert.equal(action.requestId(newerIntent), subsequent);
});
