import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";

import { ExportByteLimitValue } from "./workspace-dialogs";

test("export byte limits distinguish the server default from an accepted job limit", () => {
  assert.equal(renderToStaticMarkup(<ExportByteLimitValue byteLimit="server-default" />), "Server default");
  assert.equal(renderToStaticMarkup(<ExportByteLimitValue byteLimit={256_000_000n} />), "256 MB");
  assert.equal(renderToStaticMarkup(<ExportByteLimitValue byteLimit={null} />), "Not advertised");
});
