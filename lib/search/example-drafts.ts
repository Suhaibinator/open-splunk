/**
 * Backend example links must not carry a fixture index into a real server.
 * Omitting the leading selector lets Search resolve the selected app's exact
 * default indexes (or the browser-authorized index scope) during submission.
 */
export function backendDraftWithoutIndexSelector(spl: string): string {
  const draft = spl.replace(/^\s*index=[^\s|]+\s*/i, "");
  return draft.length === 0 || draft.startsWith("|") ? `*${draft.length === 0 ? "" : ` ${draft}`}` : draft;
}
