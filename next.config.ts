import { createHash } from "node:crypto";
import type { NextConfig } from "next";

const developmentRevision = "development";
const sourceRevision = (process.env.OPEN_SPLUNK_SOURCE_REVISION ?? developmentRevision).trim();
if (
  sourceRevision !== developmentRevision
  && !/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(sourceRevision)
) {
  throw new Error(
    "OPEN_SPLUNK_SOURCE_REVISION must be development or a full lowercase Git hash.",
  );
}

const revisionBuildIDCharacters: Readonly<Record<string, string>> = {
  a: "g",
  b: "h",
  c: "j",
  d: "k",
  e: "m",
  f: "n",
};
const identityDigest = createHash("sha256")
  .update("open-splunk-ui-build\0")
  .update(sourceRevision)
  .digest("hex");
const uiBuildID = `r${identityDigest.replace(
  /[a-f]/g,
  (character) => revisionBuildIDCharacters[character],
)}`;
const distDir = process.env.OPEN_SPLUNK_NEXT_DIST_DIR?.trim() || ".next";

const nextConfig: NextConfig = {
  allowedDevOrigins: ["127.0.0.1"],
  experimental: {
    // TypeScript 7 intentionally does not expose the legacy compiler API.
    // Next.js 16.3 must invoke the project-local TypeScript CLI instead.
    useTypeScriptCli: true,
  },
  output: "export",
  distDir,
  reactStrictMode: true,
  trailingSlash: true,
  generateBuildId: async () => uiBuildID,
  env: {
    NEXT_PUBLIC_OPEN_SPLUNK_SOURCE_REVISION: sourceRevision,
  },
  images: {
    unoptimized: true,
  },
};

export default nextConfig;
