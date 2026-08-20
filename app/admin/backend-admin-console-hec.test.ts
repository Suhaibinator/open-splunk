import assert from "node:assert/strict";
import test from "node:test";

import {
  IngestionTokenPurpose,
  IngestionTokenState,
} from "../../gen/ts/open_splunk/collector_admin";
import {
  hecCurlExample,
  hecProfileFromForm,
  parsePersistedTokenCreateGuard,
  serializeTokenCreateGuard,
  tokenCanSetEnabled,
  tokenPatternsFromForm,
  tokenPurposeLabel,
  validHECMetadataDefault,
} from "./backend-admin-console";

test("only active and disabled tokens expose reversible state controls", () => {
  for (const purpose of [
    IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR,
    IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC,
  ]) {
    assert.equal(tokenCanSetEnabled({
      ingestionTokenId: "token",
      version: 1n,
      name: "managed",
      tokenPrefix: "ost_safe",
      purpose,
      state: IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE,
      constraints: undefined,
      hecProfile: undefined,
      description: undefined,
      createdAt: undefined,
      updatedAt: undefined,
      lastUsedAt: undefined,
      expiresAt: undefined,
      revokedAt: undefined,
      ingestionRateLimits: undefined,
    }), true);
    assert.equal(tokenCanSetEnabled({
      ingestionTokenId: "token",
      version: 2n,
      name: "managed",
      tokenPrefix: "ost_safe",
      purpose,
      state: IngestionTokenState.INGESTION_TOKEN_STATE_DISABLED,
      constraints: undefined,
      hecProfile: undefined,
      description: undefined,
      createdAt: undefined,
      updatedAt: undefined,
      lastUsedAt: undefined,
      expiresAt: undefined,
      revokedAt: undefined,
      ingestionRateLimits: undefined,
    }), true);
  }
  for (const state of [
    IngestionTokenState.INGESTION_TOKEN_STATE_REVOKED,
    IngestionTokenState.INGESTION_TOKEN_STATE_EXPIRED,
  ]) {
    assert.equal(tokenCanSetEnabled({ state } as never), false);
  }
});

test("HEC profile form preserves authored defaults and acknowledgment mode", () => {
  assert.deepEqual(hecProfileFromForm({
    defaultIndexName: "main",
    defaultHost: "api.example.com",
    defaultSource: "http:orders",
    defaultSourcetype: "_json",
    indexerAcknowledgment: true,
  }), {
    defaultIndexName: "main",
    defaultHost: "api.example.com",
    defaultSource: "http:orders",
    defaultSourcetype: "_json",
    indexerAcknowledgment: true,
  });
  assert.deepEqual(hecProfileFromForm({
    defaultIndexName: "",
    defaultHost: "",
    defaultSource: "",
    defaultSourcetype: "",
    indexerAcknowledgment: false,
  }), {
    defaultIndexName: undefined,
    defaultHost: undefined,
    defaultSource: undefined,
    defaultSourcetype: undefined,
    indexerAcknowledgment: false,
  });
});

test("HEC metadata defaults enforce the documented byte and control bounds", () => {
  assert.equal(validHECMetadataDefault(""), true);
  assert.equal(validHECMetadataDefault("x".repeat(255)), true);
  assert.equal(validHECMetadataDefault("x".repeat(256)), false);
  assert.equal(validHECMetadataDefault("é".repeat(127)), true);
  assert.equal(validHECMetadataDefault("é".repeat(128)), false);
  assert.equal(validHECMetadataDefault(" leading"), false);
  assert.equal(validHECMetadataDefault("trailing\t"), false);
  assert.equal(validHECMetadataDefault("has\u0000nul"), false);
  assert.equal(validHECMetadataDefault("unpaired\ud800surrogate"), false);
  assert.equal(validHECMetadataDefault("\u00a0authored\u00a0"), true);
});

test("curl example exists only for an in-memory HEC plaintext result", () => {
  const token = "os_hec_secret";
  assert.equal(hecCurlExample(
    "https://splunk.example/api",
    IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR,
    token,
    "main",
  ), null);
  assert.equal(hecCurlExample(
    "https://splunk.example/api",
    IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC,
    null,
    "main",
  ), null);
  assert.equal(hecCurlExample(
    "https://splunk.example/api",
    IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC,
    token,
    null,
  ), null);

  const example = hecCurlExample(
    "https://splunk.example/api",
    IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC,
    token,
    "main",
  );
  assert.ok(example?.includes("https://splunk.example/api/services/collector/event"));
  assert.equal(example?.includes(token), false);
  assert.ok(example?.includes("read -r -s -p 'HEC token: '"));
  assert.ok(example?.includes("umask 077"));
  assert.ok(example?.includes("mktemp"));
  assert.ok(example?.includes('chmod 600 "$OPEN_SPLUNK_HEC_CONFIG"'));
  assert.ok(example?.includes("trap 'rm -f"));
  assert.ok(example?.includes('--config "$OPEN_SPLUNK_HEC_CONFIG"'));
  assert.ok(example?.includes("X-Splunk-Request-Channel: 00000000-0000-0000-0000-000000000001"));
  assert.ok(example?.includes("hello from Open Splunk"));
  assert.ok(example?.includes('"index":"main"'));
});

test("purpose labels distinguish native and HEC credentials", () => {
  assert.equal(
    tokenPurposeLabel(IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR),
    "Native collector",
  );
  assert.equal(tokenPurposeLabel(IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC), "HEC");
});

test("token policy patterns preserve RE2 source text and enforce transport bounds", () => {
  assert.deepEqual(tokenPatternsFromForm(
    "worker-[0-9]+\napi-[0-9]+\nworker-[0-9]+",
    "Allowed host",
  ), ["api-[0-9]+", "worker-[0-9]+"]);
  assert.deepEqual(tokenPatternsFromForm("", "Allowed source"), []);
  assert.throws(
    () => tokenPatternsFromForm(
      Array.from({ length: 17 }, (_, index) => `host-${index}`).join("\n"),
      "Allowed host",
    ),
    /at most 16 unique patterns/,
  );
  assert.throws(
    () => tokenPatternsFromForm("x".repeat(513), "Allowed source"),
    /512 UTF-8 bytes/,
  );
  assert.throws(
    () => tokenPatternsFromForm("bad\0pattern", "Allowed source"),
    /NUL/,
  );
});

test("ambiguous-create guard round-trips HEC identity without a secret", () => {
  const apiBaseUrl = "https://splunk.example";
  const serialized = serializeTokenCreateGuard(apiBaseUrl, {
    attemptId: "attempt-1",
    ownerId: "owner-1",
    definition: {
      name: "orders-hec",
      description: "Orders API",
      boundCollectorId: "",
      allowedIndexNames: ["main", "orders"],
      allowedHostRegexes: ["api-[0-9]+"],
      allowedSourceRegexes: ["http:orders"],
      maxEventsPerSecond: 500n,
      maxUncompressedBytesPerSecond: 1_048_576n,
      purpose: IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC,
      hecProfile: {
        defaultIndexName: "orders",
        defaultHost: "api.example.com",
        defaultSource: "http:orders",
        defaultSourcetype: "_json",
        indexerAcknowledgment: true,
      },
      expiresAt: undefined,
      armedServerTimeMs: 1_000,
      dispatchedServerTimeMs: 1_010,
      outcomeObservedServerTimeMs: null,
      requestRoundTripMs: null,
      requestTimeoutMs: 30_000,
      clockUncertaintyMs: 10,
      outcomeKind: "pending",
    },
    preexistingTokenIds: new Set(["existing-token"]),
    confirmedRevokedTokenIds: new Set(),
    failureMessage: "pending",
    candidates: [],
    reconciliationError: null,
  }, null);

  const raw = JSON.stringify(serialized);
  assert.equal(raw.includes("plaintext"), false);
  assert.equal(raw.includes("os_hec_secret"), false);
  const restored = parsePersistedTokenCreateGuard(raw, apiBaseUrl);
  assert.equal(
    restored?.recovery.definition.purpose,
    IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC,
  );
  assert.deepEqual(restored?.recovery.definition.hecProfile, {
    defaultIndexName: "orders",
    defaultHost: "api.example.com",
    defaultSource: "http:orders",
    defaultSourcetype: "_json",
    indexerAcknowledgment: true,
  });
  assert.deepEqual(
    restored?.recovery.definition.allowedHostRegexes,
    ["api-[0-9]+"],
  );
  assert.deepEqual(restored?.recovery.definition.allowedSourceRegexes, ["http:orders"]);
  assert.equal(restored?.recovery.definition.maxEventsPerSecond, 500n);
  assert.equal(restored?.recovery.definition.maxUncompressedBytesPerSecond, 1_048_576n);
});

test("unsupported create-guard schemas fail closed", () => {
  assert.equal(parsePersistedTokenCreateGuard(
    JSON.stringify({ schemaVersion: 2 }),
    "https://splunk.example",
  ), null);
});

test("incomplete create guards fail closed within the current schema", () => {
  const apiBaseUrl = "https://splunk.example";
  const incompleteGuard = {
    schemaVersion: 1,
    apiBaseUrl,
    attemptId: "attempt-incomplete",
    ownerId: "owner-incomplete",
    mode: "ambiguous",
    definition: {
      name: "native-token",
      description: "",
      boundCollectorId: "collector-1",
      allowedIndexNames: ["main"],
      expiresAt: null,
      armedServerTimeMs: 1_000,
      dispatchedServerTimeMs: 1_010,
      outcomeObservedServerTimeMs: null,
      requestRoundTripMs: null,
      requestTimeoutMs: 30_000,
      clockUncertaintyMs: 10,
      outcomeKind: "pending",
    },
    preexistingTokenIds: [],
    confirmedRevokedTokenIds: [],
    failureMessage: "pending",
    knownIssuedTokenId: null,
  };
  const restored = parsePersistedTokenCreateGuard(JSON.stringify(incompleteGuard), apiBaseUrl);
  assert.equal(restored, null);
});

test("malformed persisted HEC scope fails closed without throwing", () => {
  const apiBaseUrl = "https://splunk.example";
  const malformed = JSON.stringify({
    schemaVersion: 1,
    apiBaseUrl,
    attemptId: "attempt-malformed",
    ownerId: "owner-malformed",
    mode: "ambiguous",
    definition: {
      name: "hec-token",
      description: "",
      boundCollectorId: "",
      allowedIndexNames: 42,
      purpose: IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_HEC,
      hecProfile: {
        defaultIndexName: "main",
        defaultHost: null,
        defaultSource: null,
        defaultSourcetype: null,
        indexerAcknowledgment: false,
      },
    },
  });
  assert.doesNotThrow(() => parsePersistedTokenCreateGuard(malformed, apiBaseUrl));
  assert.equal(parsePersistedTokenCreateGuard(malformed, apiBaseUrl), null);
});
