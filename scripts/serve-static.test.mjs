import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

import { createStaticServer, parseServerArguments, resolveRequestPath } from "./serve-static.mjs";

async function exportFixture(t) {
  const parent = await mkdtemp(path.join(tmpdir(), "open-splunk-static-"));
  t.after(() => rm(parent, { recursive: true, force: true }));
  await writeFile(path.join(parent, "outside.txt"), "outside the export");
  const root = path.join(parent, "out");
  await mkdir(path.join(root, "admin"), { recursive: true });
  await writeFile(path.join(root, "index.html"), "<!doctype html>home");
  await writeFile(path.join(root, "admin", "index.html"), "<!doctype html>admin");
  await writeFile(path.join(root, "404.html"), "<!doctype html>missing");
  await writeFile(path.join(root, "secret.txt"), "unreachable");
  return root;
}

async function withServer(t, root, visit) {
  const server = createStaticServer(root);
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  const { port } = server.address();
  await visit(`http://127.0.0.1:${port}`);
}

test("parseServerArguments collects one site per port", () => {
  const options = parseServerArguments([
    "--root", "out",
    "--port", "4300",
    "--root", ".cache/visual/backend-export",
    "--port", "4301",
  ]);
  assert.equal(options.host, "127.0.0.1");
  assert.equal(options.sites.length, 2);
  assert.equal(options.sites[0].port, 4300);
  assert.equal(options.sites[1].port, 4301);
  assert.equal(path.basename(options.sites[1].root), "backend-export");
});

test("parseServerArguments rejects a port outside the valid range", () => {
  assert.throws(() => parseServerArguments(["--port", "70000"]), /port must be an integer/);
  assert.throws(() => parseServerArguments(["--unknown", "value"]), /usage/);
});

test("resolveRequestPath keeps every traversal inside the export root", () => {
  const root = path.resolve("/exports/out");
  assert.equal(resolveRequestPath(root, "/admin/index.html"), path.join(root, "admin", "index.html"));
  // Leading `..` segments collapse against the root rather than reaching above it.
  assert.equal(resolveRequestPath(root, "/../etc/passwd"), path.join(root, "etc", "passwd"));
  assert.equal(resolveRequestPath(root, "/admin/../../etc/passwd"), path.join(root, "etc", "passwd"));
  assert.equal(resolveRequestPath(root, "/%2e%2e/%2e%2e/etc/passwd"), path.join(root, "etc", "passwd"));
  assert.equal(resolveRequestPath(root, "/%ZZ"), null);
  assert.equal(resolveRequestPath(root, "/admin/%00.html"), null);
});

test("the static server mirrors the exported trailing-slash routing", async (t) => {
  const root = await exportFixture(t);
  await withServer(t, root, async (origin) => {
    for (const [request, expected] of [
      ["/", "home"],
      ["/admin/", "admin"],
      ["/admin", "admin"],
      ["/index.html", "home"],
    ]) {
      const response = await fetch(`${origin}${request}`);
      assert.equal(response.status, 200, request);
      assert.match(await response.text(), new RegExp(`${expected}$`, "u"), request);
    }
  });
});

test("the static server answers an unknown route with the exported 404 page", async (t) => {
  const root = await exportFixture(t);
  await withServer(t, root, async (origin) => {
    const response = await fetch(`${origin}/nowhere/`);
    assert.equal(response.status, 404);
    assert.match(await response.text(), /missing$/u);
  });
});

test("the static server never serves a file above the export root", async (t) => {
  const root = await exportFixture(t);
  await withServer(t, root, async (origin) => {
    for (const request of ["/../outside.txt", "/admin/../../outside.txt", "/%2e%2e/outside.txt"]) {
      const response = await fetch(`${origin}${request}`);
      assert.equal(response.status, 404, request);
      assert.doesNotMatch(await response.text(), /outside the export/u, request);
    }
  });
});

test("the static server rejects a write method", async (t) => {
  const root = await exportFixture(t);
  await withServer(t, root, async (origin) => {
    const response = await fetch(`${origin}/`, { method: "POST" });
    assert.equal(response.status, 405);
    assert.equal(response.headers.get("allow"), "GET, HEAD");
  });
});
