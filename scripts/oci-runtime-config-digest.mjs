#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

// A containerd image store can report an attested index as the image ID.
// Hash the exported platform config itself, which also commits to RootFS diff IDs.
const [platform, image, ...extra] = process.argv.slice(2);
if (!/^linux\/(amd64|arm64)$/u.test(platform ?? "") || !image || image.startsWith("-") || extra.length) {
  throw new Error("usage: oci-runtime-config-digest.mjs <linux/amd64|linux/arm64> <image>");
}
const temporary = mkdtempSync(path.join(tmpdir(), "open-splunk-oci-config-"));
try {
  const archive = path.join(temporary, "image.tar");
  execFileSync("docker", ["image", "save", "--platform", platform, "--output", archive, image], { stdio: ["ignore", "ignore", "inherit"] });
  const manifest = JSON.parse(execFileSync("tar", ["-xOf", archive, "manifest.json"], { encoding: "utf8" }));
  if (!Array.isArray(manifest) || manifest.length !== 1 ||
      !/^(?:[a-f0-9]{64}\.json|blobs\/sha256\/[a-f0-9]{64})$/u.test(manifest[0]?.Config ?? "")) {
    throw new Error("image export must contain exactly one platform config");
  }
  const config = execFileSync("tar", ["-xOf", archive, manifest[0].Config]);
  const metadata = JSON.parse(config.toString("utf8"));
  if (`${metadata.os}/${metadata.architecture}` !== platform ||
      metadata.rootfs?.type !== "layers" || !Array.isArray(metadata.rootfs.diff_ids)) {
    throw new Error("image export has an invalid platform or root filesystem config");
  }
  process.stdout.write(`sha256:${createHash("sha256").update(config).digest("hex")}\n`);
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
