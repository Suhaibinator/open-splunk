/**
 * The fake browser installed as a module side effect.
 *
 * `react-dom/client` decides whether it has a DOM the moment it is loaded, so
 * the globals must exist before that module does. Module evaluation follows
 * import order: a test imports this file *first*, then `react-dom/client`,
 * and the comment on that import says why the order is load-bearing.
 *
 * Test-only: no page or library module imports this file.
 */

import { installFakeBrowser } from "./fake-dom";

export const browser = installFakeBrowser();
