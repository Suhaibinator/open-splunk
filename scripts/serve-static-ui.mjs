// Serve the static export in `out/` the way the embedded Go file server does,
// so the workspace behaviour harness exercises the pages users actually get:
// `trailingSlash: true` maps `/search/` onto `out/search/index.html`, and any
// path the export does not contain falls back to `out/404.html`.
import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const output = path.join(repository, "out");
const host = "127.0.0.1";
const port = Number.parseInt(process.env.PORT ?? "4173", 10);
const contentTypes = new Map([
  [".css", "text/css; charset=utf-8"],
  [".html", "text/html; charset=utf-8"],
  [".ico", "image/x-icon"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".png", "image/png"],
  [".svg", "image/svg+xml"],
  [".txt", "text/plain; charset=utf-8"],
  [".woff2", "font/woff2"],
]);

async function isFile(candidate) {
  try {
    return (await stat(candidate)).isFile();
  } catch {
    return false;
  }
}

async function resolveExport(pathname) {
  const decoded = decodeURIComponent(pathname);
  const target = path.resolve(output, `.${decoded}`);
  if (target !== output && !target.startsWith(`${output}${path.sep}`)) return null;
  const candidates = [target, path.join(target, "index.html"), `${target}.html`];
  const present = await Promise.all(candidates.map(isFile));
  return candidates.find((_, index) => present[index]) ?? null;
}

const server = createServer(async (request, response) => {
  const { pathname } = new URL(request.url ?? "/", `http://${host}`);
  const found = await resolveExport(pathname);
  const fallback = path.join(output, "404.html");
  const file = found ?? ((await isFile(fallback)) ? fallback : null);
  if (file === null) {
    response.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    response.end("not found");
    return;
  }
  response.writeHead(found === null ? 404 : 200, {
    "cache-control": "no-store",
    "content-type": contentTypes.get(path.extname(file)) ?? "application/octet-stream",
  });
  response.end(await readFile(file));
});

server.listen(port, host, () => {
  console.log(`serving ${output} at http://${host}:${port}/`);
});
