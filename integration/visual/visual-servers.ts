/**
 * Addresses of the two static exports the visual suite renders.
 *
 * `playwright.visual.config.ts` starts one server process that owns both, and
 * the specs pick the export they need with `test.use({ baseURL })`.
 */

function visualPort(): number {
  const port = Number(process.env.OPEN_SPLUNK_VISUAL_PORT ?? 43180);
  if (!Number.isInteger(port) || port < 1 || port > 65534) {
    throw new Error("OPEN_SPLUNK_VISUAL_PORT must be an integer between 1 and 65534.");
  }
  return port;
}

/** Demo data mode: fixtures compiled into the bundle, no backend of any kind. */
export const DEMO_EXPORT_PORT = visualPort();

/** Backend data mode: the spec supplies every protobuf response it needs. */
export const BACKEND_EXPORT_PORT = visualPort() + 1;

export const DEMO_EXPORT_URL = `http://127.0.0.1:${DEMO_EXPORT_PORT}/`;
export const BACKEND_EXPORT_URL = `http://127.0.0.1:${BACKEND_EXPORT_PORT}/`;

/** Directory holding the backend-mode export, produced by `scripts/build-visual-exports.mjs`. */
export const BACKEND_EXPORT_ROOT = ".cache/visual/backend-export";
