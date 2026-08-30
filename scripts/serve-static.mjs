#!/usr/bin/env node
// Dependency-free static server for the Next.js export in `out/`.
//
// The visual-regression suite must render the shipped UI without the Go server
// or ClickHouse, so this serves exactly the exported bytes and mirrors the
// `trailingSlash` routing the production server performs.
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { createServer } from "node:http";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const repository = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const CONTENT_TYPES = Object.freeze({
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".ico": "image/x-icon",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".map": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".txt": "text/plain; charset=utf-8",
  ".webmanifest": "application/manifest+json",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
});

function usage() {
  return "usage: node scripts/serve-static.mjs [--host <host>] [--root <directory> --port <port>]...";
}

/**
 * Parses one or more `--root … --port …` pairs.
 *
 * A single process owns every root so the visual harness has one server
 * lifecycle to start and stop, even when it compares two exports built in
 * different data modes.
 */
export function parseServerArguments(arguments_) {
  const sites = [];
  let host = "127.0.0.1";
  let root = path.join(repository, "out");
  for (let index = 0; index < arguments_.length; index += 2) {
    const flag = arguments_[index];
    const value = arguments_[index + 1];
    if (value === undefined) throw new Error(usage());
    if (flag === "--root") root = path.resolve(value);
    else if (flag === "--host") host = value;
    else if (flag === "--port") {
      const port = Number(value);
      if (!Number.isInteger(port) || port < 0 || port > 65535) {
        throw new Error(`port must be an integer between 0 and 65535: ${value}`);
      }
      sites.push({ port, root });
    } else throw new Error(usage());
  }
  if (sites.length === 0) sites.push({ port: 0, root });
  return { host, sites };
}

/** Resolves a request path to a file inside `root`, or null when it escapes or is missing. */
export function resolveRequestPath(root, requestPath) {
  let decoded;
  try {
    decoded = decodeURIComponent(requestPath);
  } catch {
    return null;
  }
  if (decoded.includes("\0")) return null;
  const normalized = path.normalize(decoded);
  const candidate = path.resolve(root, `.${normalized.startsWith("/") ? normalized : `/${normalized}`}`);
  const relative = path.relative(root, candidate);
  if (relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
    return null;
  }
  return candidate;
}

async function readableFile(candidate) {
  try {
    const metadata = await stat(candidate);
    return metadata.isFile() ? candidate : null;
  } catch (error) {
    if (error?.code === "ENOENT" || error?.code === "ENOTDIR") return null;
    throw error;
  }
}

/** Mirrors the exported `trailingSlash` layout: `/admin/` and `/admin` both resolve to `admin/index.html`. */
async function locateExport(root, requestPath) {
  const base = resolveRequestPath(root, requestPath);
  if (base === null) return null;
  const candidates = requestPath.endsWith("/")
    ? [path.join(base, "index.html")]
    : [base, `${base}.html`, path.join(base, "index.html")];
  const resolved = await Promise.all(candidates.map(readableFile));
  return resolved.find((candidate) => candidate !== null) ?? null;
}

function contentType(filename) {
  return CONTENT_TYPES[path.extname(filename).toLocaleLowerCase("en-US")] ?? "application/octet-stream";
}

export function createStaticServer(root) {
  return createServer((request, response) => {
    void (async () => {
      if (request.method !== "GET" && request.method !== "HEAD") {
        response.writeHead(405, { allow: "GET, HEAD", "content-type": CONTENT_TYPES[".txt"] });
        response.end("method not allowed\n");
        return;
      }
      const requestPath = new URL(request.url ?? "/", "http://localhost").pathname;
      let filename = await locateExport(root, requestPath);
      let status = 200;
      if (filename === null) {
        filename = await readableFile(path.join(root, "404.html"));
        status = 404;
      }
      if (filename === null) {
        response.writeHead(404, { "content-type": CONTENT_TYPES[".txt"] });
        response.end("not found\n");
        return;
      }
      response.writeHead(status, {
        "cache-control": "no-store",
        "content-type": contentType(filename),
      });
      if (request.method === "HEAD") {
        response.end();
        return;
      }
      createReadStream(filename).pipe(response);
    })().catch((error) => {
      response.writeHead(500, { "content-type": CONTENT_TYPES[".txt"] });
      response.end(`${error instanceof Error ? error.message : String(error)}\n`);
    });
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  let options;
  try {
    options = parseServerArguments(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`${error.message}\n`);
    process.exit(2);
  }
  const roots = await Promise.all(options.sites.map(async (site) => {
    const metadata = await stat(site.root).catch(() => null);
    return metadata !== null && metadata.isDirectory();
  }));
  const missing = options.sites.filter((_, index) => !roots[index]);
  if (missing.length > 0) {
    process.stderr.write(
      `static root is missing; run 'npm run build' first: ${missing.map((site) => site.root).join(", ")}\n`,
    );
    process.exit(1);
  }
  const servers = [];
  for (const site of options.sites) {
    const server = createStaticServer(site.root);
    servers.push(server);
    server.listen(site.port, options.host, () => {
      const address = server.address();
      const port = typeof address === "object" && address !== null ? address.port : site.port;
      process.stdout.write(`serving ${site.root} at http://${options.host}:${port}/\n`);
    });
  }
  let remaining = servers.length;
  const stop = () => {
    for (const server of servers) {
      server.close(() => {
        remaining -= 1;
        if (remaining === 0) process.exit(0);
      });
    }
  };
  process.on("SIGINT", stop);
  process.on("SIGTERM", stop);
}
