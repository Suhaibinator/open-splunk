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

/**
 * Directories holding the two exports, produced by
 * `scripts/build-visual-exports.mjs`.
 *
 * Both live under `.cache/visual` rather than in `out/`: that directory is the
 * release payload `webui.go` embeds, and a test target must not leave a
 * demo-mode, manifest-less build sitting in it.
 */
export const DEMO_EXPORT_ROOT = ".cache/visual/demo-export";

export const BACKEND_EXPORT_ROOT = ".cache/visual/backend-export";
