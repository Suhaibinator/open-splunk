import type { ComponentType } from "react";

export type BackendAdminSection =
  | "overview"
  | "apps"
  | "indexes"
  | "collector-fleet"
  | "collectors"
  | "knowledge"
  | "lookups"
  | "access"
  | "server";

export interface BackendAdminNavigationItem {
  key: BackendAdminSection;
  label: string;
  detail: string;
  icon: string;
}

const BASE_NAVIGATION: readonly BackendAdminNavigationItem[] = [
  { key: "overview", label: "System overview", detail: "Capabilities and limits", icon: "▥" },
  { key: "apps", label: "Apps", detail: "Workspaces and defaults", icon: "◇" },
  { key: "indexes", label: "Indexes", detail: "State and retention", icon: "▦" },
  { key: "collector-fleet", label: "Collector fleet", detail: "Health, queues, and inputs", icon: "⌁" },
  { key: "collectors", label: "Ingestion tokens", detail: "Credentials and scopes", icon: "⇣" },
  { key: "access", label: "Users & access", detail: "Not exposed by this server", icon: "♙" },
  { key: "server", label: "Server settings", detail: "Read-only limits", icon: "⚙" },
];

const KNOWLEDGE_NAVIGATION: BackendAdminNavigationItem = {
  key: "knowledge",
  label: "Knowledge Manager",
  detail: "Tier-1 definitions",
  icon: "⌘",
};

const LOOKUP_NAVIGATION: BackendAdminNavigationItem = {
  key: "lookups",
  label: "Lookup tables",
  detail: "Exact CSV enrichment",
  icon: "⊞",
};

/** Returns the existing array untouched unless trusted bootstrap advertises knowledge. */
export function backendAdminNavigation(
  knowledgeAdvertised: boolean,
): readonly BackendAdminNavigationItem[] {
  if (!knowledgeAdvertised) return BASE_NAVIGATION;
  return [
    ...BASE_NAVIGATION.slice(0, 5),
    KNOWLEDGE_NAVIGATION,
    LOOKUP_NAVIGATION,
    ...BASE_NAVIGATION.slice(5),
  ];
}

export interface KnowledgeManagerAppOption {
  appId: string;
  label: string;
}

export const KNOWLEDGE_MANAGER_MAXIMUM_BOOTSTRAP_APPS = 256;

const MAXIMUM_APP_ID_BYTES = 128;
const MAXIMUM_APP_LABEL_CODE_POINTS = 255;
const MAXIMUM_APP_LABEL_BYTES = 1_020;

interface BootstrapAppOptionSource {
  readonly appId: string;
  readonly displayName: string;
  readonly slug: string;
}

function boundedText(
  value: string,
  maximumCodePoints: number,
  maximumBytes: number,
): boolean {
  if (!value.isWellFormed()) return false;
  if (value.length > maximumCodePoints * 2) return false;
  let codePoints = 0;
  let bytes = 0;
  for (let index = 0; index < value.length; index += 1) {
    const high = value.charCodeAt(index);
    let codePoint = high;
    if (high >= 0xd800 && high <= 0xdbff) {
      codePoint = ((high - 0xd800) << 10) + value.charCodeAt(index + 1) - 0xdc00 + 0x1_0000;
      index += 1;
    }
    codePoints += 1;
    bytes += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
    if (
      codePoints > maximumCodePoints
      || bytes > maximumBytes
      || codePoint <= 0x1f
      || (codePoint >= 0x7f && codePoint <= 0x9f)
    ) return false;
  }
  return true;
}

function validAppId(value: unknown): value is string {
  return typeof value === "string"
    && value.length > 0
    && boundedText(value, MAXIMUM_APP_ID_BYTES, MAXIMUM_APP_ID_BYTES)
    && value.trim() === value;
}

function safeAppLabel(value: unknown, fallback: string): string {
  if (
    typeof value !== "string"
    || !boundedText(value, MAXIMUM_APP_LABEL_CODE_POINTS, MAXIMUM_APP_LABEL_BYTES)
  ) return fallback;
  return value.trim() || fallback;
}

function appendSafeAppOption(
  output: KnowledgeManagerAppOption[],
  seen: Set<string>,
  appId: unknown,
  label: unknown,
): void {
  if (!validAppId(appId) || seen.has(appId)) return;
  seen.add(appId);
  output.push({ appId, label: safeAppLabel(label, appId) });
}

/** Fails closed before reading any entries from an oversized bootstrap array. */
export function knowledgeManagerAppOptionsFromBootstrap(
  apps: readonly BootstrapAppOptionSource[],
): KnowledgeManagerAppOption[] | null {
  if (apps.length > KNOWLEDGE_MANAGER_MAXIMUM_BOOTSTRAP_APPS) return null;
  const output: KnowledgeManagerAppOption[] = [];
  const seen = new Set<string>();
  for (let index = 0; index < apps.length; index += 1) {
    const app = apps[index];
    if (app === undefined) continue;
    appendSafeAppOption(
      output,
      seen,
      app.appId,
      app.displayName || app.slug || app.appId,
    );
  }
  return output;
}

/** Defense in depth for fixtures or callers that bypass the bootstrap adapter. */
export function safeKnowledgeManagerAppOptions(
  apps: readonly KnowledgeManagerAppOption[],
): KnowledgeManagerAppOption[] | null {
  if (apps.length > KNOWLEDGE_MANAGER_MAXIMUM_BOOTSTRAP_APPS) return null;
  const output: KnowledgeManagerAppOption[] = [];
  const seen = new Set<string>();
  for (let index = 0; index < apps.length; index += 1) {
    const app = apps[index];
    if (app === undefined) continue;
    appendSafeAppOption(output, seen, app.appId, app.label);
  }
  return output;
}

export interface KnowledgeManagerPanelProps {
  apiBaseUrl: string;
  apps: readonly KnowledgeManagerAppOption[];
  initialAppId: string | null;
  maximumPageSize: number;
}

export interface KnowledgeManagerPanelModule {
  KnowledgeManagerPanel: ComponentType<KnowledgeManagerPanelProps>;
}
