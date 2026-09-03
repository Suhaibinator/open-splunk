import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { IngestionToken, IngestionTokenState } from "@/gen/ts/open_splunk/collector_admin";

import { IssuedTokenDialog, TokenCreationDialog, issuedTokenCompletionDisabled } from "./token-creation-dialog";

const scopeOptions = [{ id: "index-main", name: "main", displayName: "Main", ingestible: true }];

function renderCreationDialog(busy: boolean): string {
  return renderToStaticMarkup(
    <TokenCreationDialog
      blockReason={null}
      busy={busy}
      hecEnabled
      onClose={() => {}}
      onSubmit={() => {}}
      scopeOptions={scopeOptions}
      scopeSource="bootstrap"
    />,
  );
}

function issuedToken(state: IngestionTokenState): IngestionToken {
  return IngestionToken.fromPartial({
    ingestionTokenId: "token-1",
    name: "collector-token",
    tokenPrefix: "ost_123",
    state,
    version: 1n,
    purpose: 1,
    createdAt: new Date("2026-09-03T00:00:00.000Z"),
  });
}

function renderIssuedDialog(token: IngestionToken, secret: string | null): string {
  return renderToStaticMarkup(
    <IssuedTokenDialog
      busy={null}
      curlExample={secret === null ? null : "curl example"}
      dismissible={secret === null}
      issuedToken={token}
      onAcknowledge={() => {}}
      onClose={() => {}}
      onCopyResult={() => {}}
      onRevoke={() => {}}
      ownership="owned"
      secret={secret}
    />,
  );
}

test("token creation removes every dismissal affordance while generation is busy", () => {
  const idle = renderCreationDialog(false);
  assert.equal((idle.match(/aria-label="Close dialog"/gu) ?? []).length, 2);

  const busy = renderCreationDialog(true);
  assert.doesNotMatch(busy, /aria-label="Close dialog"/u);
  assert.match(busy, /<div class="modal-backdrop" aria-hidden="true"><\/div>/u);
  assert.match(busy, /disabled="">Generating…<\/button>/u);
});

test("issued-token completion follows acknowledgement, ownership, and revocation state", () => {
  assert.equal(issuedTokenCompletionDisabled({ busy: false, canRevoke: true, hasSecret: true, ownership: "owned", secretAcknowledged: false }), true);
  assert.equal(issuedTokenCompletionDisabled({ busy: false, canRevoke: true, hasSecret: true, ownership: "owned", secretAcknowledged: true }), false);
  assert.equal(issuedTokenCompletionDisabled({ busy: false, canRevoke: true, hasSecret: false, ownership: "owned", secretAcknowledged: false }), true);
  assert.equal(issuedTokenCompletionDisabled({ busy: false, canRevoke: false, hasSecret: false, ownership: "owned", secretAcknowledged: false }), false);
  assert.equal(issuedTokenCompletionDisabled({ busy: false, canRevoke: false, hasSecret: false, ownership: "contended", secretAcknowledged: true }), true);

  const revealed = renderIssuedDialog(
    issuedToken(IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE),
    "ost_secret",
  );
  assert.match(revealed, /Copy token/u);
  assert.match(revealed, /I stored this token securely/u);
  assert.match(revealed, /<button class="button button--primary" type="button" disabled="">Done<\/button>/u);

  const recovered = renderIssuedDialog(
    issuedToken(IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED),
    null,
  );
  assert.doesNotMatch(recovered, /I stored this token securely/u);
  assert.match(recovered, /<button class="button button--primary" type="button">Done<\/button>/u);
});
