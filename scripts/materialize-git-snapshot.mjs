#!/usr/bin/env node

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import {
  chmod,
  lstat,
  mkdir,
  open,
  readdir,
  realpath,
  rm,
  stat,
} from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { spawn, spawnSync } from "node:child_process";
import { TextDecoder } from "node:util";

const maximumGitOutputBytes = 1 << 20;
const maximumGitErrorBytes = 1 << 20;
const maximumGitHeaderBytes = 256;
const maximumSnapshotFiles = 100_000;
const maximumSnapshotFileBytes = 128 << 20;
const maximumSnapshotBytes = 512 << 20;
const maximumSnapshotPathBytes = 768;
const maximumSnapshotPathComponents = 64;
const maximumSnapshotComponentBytes = 255;
const maximumSnapshotPathBytesTotal = 64 << 20;
const maximumSnapshotDirectories = 10_000;
const maximumSnapshotDirectoryPathBytes = 32 << 20;
const maximumPortableAbsolutePathBytes = 1023;
const maximumGitTreeRecordOverheadBytes = 80;
const maximumGitTreeOutputBytes =
  maximumSnapshotPathBytesTotal +
  maximumSnapshotFiles * maximumGitTreeRecordOverheadBytes +
  1;
const decoder = new TextDecoder("utf-8", { fatal: true });

function fail(message) {
  throw new Error(message);
}

async function forEachSequential(values, visit) {
  const iterator = values[Symbol.iterator]();
  async function visitNext() {
    const next = iterator.next();
    if (next.done) return;
    await visit(next.value);
    await visitNext();
  }
  await visitNext();
}

function sanitizedGitEnvironment() {
  const environment = { ...process.env };
  for (const name of [
    "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_COMMON_DIR",
    "GIT_DIR",
    "GIT_INDEX_FILE",
    "GIT_NAMESPACE",
    "GIT_OBJECT_DIRECTORY",
    "GIT_PREFIX",
    "GIT_WORK_TREE",
  ]) {
    delete environment[name];
  }
  return {
    ...environment,
    GIT_CONFIG_GLOBAL: "/dev/null",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_NO_REPLACE_OBJECTS: "1",
    GIT_OPTIONAL_LOCKS: "0",
  };
}

function gitArguments(repositoryRoot, arguments_) {
  return [
    "-c",
    "core.fsmonitor=false",
    "-c",
    "core.hooksPath=/dev/null",
    "-C",
    repositoryRoot,
    ...arguments_,
  ];
}

function createGitRunner(repositoryRoot) {
  return function runGit(arguments_, options = {}) {
    const result = spawnSync(
      "git",
      gitArguments(repositoryRoot, arguments_),
      {
        encoding: null,
        env: sanitizedGitEnvironment(),
        maxBuffer: maximumGitOutputBytes,
        ...options,
      },
    );
    if (result.error) {
      throw new Error(`run git ${arguments_[0]}: ${result.error.message}`);
    }
    if (result.status !== 0) {
      const detail = result.stderr?.toString("utf8").trim();
      throw new Error(
        `git ${arguments_[0]} exited ${result.status}${detail ? `: ${detail}` : ""}`,
      );
    }
    return result.stdout;
  };
}

function createGitBlobHash(objectID, size) {
  const algorithm = objectID.length === 40 ? "sha1" : "sha256";
  const hash = createHash(algorithm);
  hash.update(`blob ${size}\0`, "ascii");
  return hash;
}

function decodeUTF8(buffer, description) {
  try {
    return decoder.decode(buffer);
  } catch (error) {
    throw new Error(`${description} is not valid UTF-8: ${error.message}`, {
      cause: error,
    });
  }
}

function* splitNUL(buffer) {
  if (buffer.length === 0) {
    return;
  }
  if (buffer.at(-1) !== 0) {
    fail("Git tree listing is not NUL terminated");
  }
  let start = 0;
  for (let index = 0; index < buffer.length; index += 1) {
    if (buffer[index] !== 0) {
      continue;
    }
    yield buffer.subarray(start, index);
    start = index + 1;
  }
}

function crossPlatformPathKey(value) {
  // APFS/HFS+ use Unicode-aware case folding and normalization. Compatibility
  // normalization plus the upper/lower round trip covers characters such as
  // final sigma, sharp s, long s, and presentation-form ligatures.
  return value.normalize("NFKC").toUpperCase().toLowerCase();
}

function validateTreePath(filePath, seenPaths, directoryBudget) {
  if (
    filePath === "" ||
    path.posix.isAbsolute(filePath) ||
    path.posix.normalize(filePath) !== filePath
  ) {
    fail(`invalid path in Git tree: ${JSON.stringify(filePath)}`);
  }
  const components = filePath.split("/");
  if (components.length > maximumSnapshotPathComponents) {
    fail(
      `Git tree path has more than ${maximumSnapshotPathComponents} components: ${JSON.stringify(filePath)}`,
    );
  }
  let current = "";
  for (let index = 0; index < components.length; index += 1) {
    const component = components[index];
    const lowerComponent = component.toLowerCase();
    const componentBytes = Buffer.byteLength(component, "utf8");
    if (
      component === "" ||
      component === "." ||
      component === ".." ||
      [".bzr", ".git", ".hg", ".svn"].includes(lowerComponent)
    ) {
      fail(
        `unsafe path component ${JSON.stringify(component)} in Git tree path ${JSON.stringify(filePath)}`,
      );
    }
    if (componentBytes > maximumSnapshotComponentBytes) {
      fail(
        `Git tree path component exceeds ${maximumSnapshotComponentBytes} bytes: ${JSON.stringify(component)}`,
      );
    }
    current = current === "" ? component : `${current}/${component}`;
    const kind = index === components.length - 1 ? "file" : "directory";
    const key = crossPlatformPathKey(current);
    const previous = seenPaths.get(key);
    if (previous && (previous.path !== current || previous.kind !== kind)) {
      fail(
        `cross-platform path collision between ${JSON.stringify(previous.path)} and ${JSON.stringify(current)}`,
      );
    }
    if (previous && kind === "file") {
      fail(`duplicate file path in Git tree: ${JSON.stringify(current)}`);
    }
    if (!previous && kind === "directory") {
      const directoryBytes = Buffer.byteLength(current, "utf8");
      if (directoryBudget.count >= maximumSnapshotDirectories) {
        fail(
          `Git tree contains more than ${maximumSnapshotDirectories} directories`,
        );
      }
      if (
        directoryBudget.bytes >
        maximumSnapshotDirectoryPathBytes - directoryBytes
      ) {
        fail(
          `Git tree directory paths exceed ${maximumSnapshotDirectoryPathBytes} bytes`,
        );
      }
      directoryBudget.count += 1;
      directoryBudget.bytes += directoryBytes;
    }
    seenPaths.set(key, { path: current, kind });
  }
}

function parseTree(buffer, destination) {
  const entries = [];
  const seenPaths = new Map();
  const directoryBudget = { bytes: 0, count: 0 };
  let pathBytesTotal = 0;
  for (const record of splitNUL(buffer)) {
    if (entries.length >= maximumSnapshotFiles) {
      fail(`Git tree contains more than ${maximumSnapshotFiles} files`);
    }
    const separator = record.indexOf(0x09);
    if (separator < 0) {
      fail("malformed Git tree entry");
    }
    const header = record.subarray(0, separator).toString("ascii");
    const encodedPath = record.subarray(separator + 1);
    if (encodedPath.length > maximumSnapshotPathBytes) {
      fail(`Git tree path exceeds ${maximumSnapshotPathBytes} bytes`);
    }
    if (pathBytesTotal > maximumSnapshotPathBytesTotal - encodedPath.length) {
      fail(`Git tree paths exceed ${maximumSnapshotPathBytesTotal} bytes`);
    }
    pathBytesTotal += encodedPath.length;
    const filePath = decodeUTF8(encodedPath, "Git tree path");
    const match = /^([0-7]{6}) ([a-z]+) ([0-9a-f]{40}|[0-9a-f]{64})$/.exec(header);
    if (!match) {
      fail(`malformed Git tree entry for ${JSON.stringify(filePath)}: ${header}`);
    }
    const [, mode, type, object] = match;
    if (type !== "blob" || (mode !== "100644" && mode !== "100755")) {
      fail(
        `unsupported Git mode/type ${mode} ${type} for ${JSON.stringify(filePath)}; release snapshots cannot contain links, submodules, or special files`,
      );
    }
    validateTreePath(filePath, seenPaths, directoryBudget);
    const destinationPath = path.join(
      destination,
      ...filePath.split("/"),
    );
    if (
      Buffer.byteLength(destinationPath, "utf8") >
      maximumPortableAbsolutePathBytes
    ) {
      fail(
        `snapshot destination path exceeds ${maximumPortableAbsolutePathBytes} bytes: ${JSON.stringify(filePath)}`,
      );
    }
    entries.push({
      mode: mode === "100755" ? 0o755 : 0o644,
      object,
      path: filePath,
    });
  }
  return entries;
}

async function pathExists(target) {
  try {
    await lstat(target);
    return true;
  } catch (error) {
    if (error.code === "ENOENT") {
      return false;
    }
    throw error;
  }
}

async function physicalFiles(root) {
  const files = [];
  async function walk(directory, relativeDirectory) {
    const children = await readdir(directory, { withFileTypes: true });
    await forEachSequential(children, async (child) => {
      const relativePath =
        relativeDirectory === ""
          ? child.name
          : `${relativeDirectory}/${child.name}`;
      const absolutePath = path.join(directory, child.name);
      const information = await lstat(absolutePath);
      if (information.isSymbolicLink()) {
        fail(`snapshot contains a symbolic link: ${JSON.stringify(relativePath)}`);
      }
      if (information.isDirectory()) {
        await walk(absolutePath, relativePath);
      } else if (information.isFile()) {
        files.push(relativePath);
      } else {
        fail(`snapshot contains an irregular file: ${JSON.stringify(relativePath)}`);
      }
    });
  }
  await walk(root, "");
  return files;
}

class BufferedStreamReader {
  constructor(stream) {
    this.iterator = stream[Symbol.asyncIterator]();
    this.chunk = Buffer.alloc(0);
    this.offset = 0;
    this.ended = false;
  }

  async ensureChunk() {
    if (this.offset < this.chunk.length) return true;
    if (this.ended) return false;
    const next = await this.iterator.next();
    if (next.done) {
      this.ended = true;
      this.chunk = Buffer.alloc(0);
      this.offset = 0;
      return false;
    }
    this.chunk = Buffer.from(next.value);
    this.offset = 0;
    return this.ensureChunk();
  }

  async readLine(maximumBytes) {
    const parts = [];
    let length = 0;
    const readNextPart = async () => {
      if (!(await this.ensureChunk())) {
        fail("Git batch output ended before its header");
      }
      const newline = this.chunk.indexOf(0x0a, this.offset);
      const end = newline < 0 ? this.chunk.length : newline;
      const part = this.chunk.subarray(this.offset, end);
      length += part.length;
      if (length > maximumBytes) {
        fail(`Git batch header exceeds ${maximumBytes} bytes`);
      }
      parts.push(part);
      this.offset = newline < 0 ? end : end + 1;
      if (newline >= 0) {
        return Buffer.concat(parts, length);
      }
      return readNextPart();
    };
    return readNextPart();
  }

  async readExactly(length, consume) {
    let remaining = length;
    const readNextPart = async () => {
      if (remaining <= 0) return;
      if (!(await this.ensureChunk())) {
        fail(`Git batch output ended with ${remaining} bytes still expected`);
      }
      const count = Math.min(remaining, this.chunk.length - this.offset);
      const part = this.chunk.subarray(this.offset, this.offset + count);
      await consume(part);
      this.offset += count;
      remaining -= count;
      await readNextPart();
    };
    await readNextPart();
  }

  async expectEOF() {
    if (await this.ensureChunk()) {
      fail("Git batch output contains trailing bytes");
    }
  }
}

async function writeAll(file, contents) {
  let offset = 0;
  const writeRemaining = async () => {
    if (offset >= contents.length) return;
    const { bytesWritten } = await file.write(
      contents,
      offset,
      contents.length - offset,
      null,
    );
    if (bytesWritten <= 0) {
      fail("snapshot file write made no progress");
    }
    offset += bytesWritten;
    await writeRemaining();
  };
  await writeRemaining();
}

function gitBatchFailure(code, signal, stderr, truncated) {
  const status =
    signal === null
      ? `exited ${code}`
      : `was terminated by signal ${signal}`;
  const detail = stderr.toString("utf8").trim();
  return new Error(
    `git cat-file --batch ${status}` +
      `${detail ? `: ${detail}` : ""}` +
      `${truncated ? " [stderr truncated]" : ""}`,
  );
}

async function extractGitBlobs(repositoryRoot, entries, destination) {
  const child = spawn(
    "git",
    gitArguments(repositoryRoot, ["cat-file", "--batch"]),
    {
      env: sanitizedGitEnvironment(),
      stdio: ["pipe", "pipe", "pipe"],
    },
  );
  let stderr = Buffer.alloc(0);
  let stderrTruncated = false;
  let inputError;
  child.stderr.on("data", (chunk) => {
    const remaining = maximumGitErrorBytes - stderr.length;
    if (remaining > 0) {
      stderr = Buffer.concat([stderr, Buffer.from(chunk).subarray(0, remaining)]);
    }
    if (chunk.length > remaining) stderrTruncated = true;
  });
  child.stdin.on("error", (error) => {
    inputError ??= error;
  });
  const completion = new Promise((resolve) => {
    child.once("error", (error) => resolve(error));
    child.once("close", (code, signal) => {
      if (inputError !== undefined) {
        resolve(new Error(`write Git batch input: ${inputError.message}`));
      } else if (code === 0 && signal === null) {
        resolve(null);
      } else {
        resolve(gitBatchFailure(code, signal, stderr, stderrTruncated));
      }
    });
  });
  const reader = new BufferedStreamReader(child.stdout);
  child.stdin.end(entries.map((entry) => `${entry.object}\n`).join(""), "ascii");

  let totalBytes = 0;
  try {
    await forEachSequential(entries, async (entry) => {
      const header = (await reader.readLine(maximumGitHeaderBytes)).toString(
        "ascii",
      );
      const match =
        /^([0-9a-f]{40}|[0-9a-f]{64}) blob ([0-9]+)$/.exec(header);
      if (!match || match[1] !== entry.object) {
        fail(
          `unexpected Git batch header for ${JSON.stringify(entry.path)}: ${header}`,
        );
      }
      const sizeValue = BigInt(match[2]);
      if (sizeValue > BigInt(maximumSnapshotFileBytes)) {
        fail(
          `Git blob for ${JSON.stringify(entry.path)} exceeds ${maximumSnapshotFileBytes} bytes`,
        );
      }
      const size = Number(sizeValue);
      if (totalBytes > maximumSnapshotBytes - size) {
        fail(`Git snapshot exceeds ${maximumSnapshotBytes} bytes`);
      }
      totalBytes += size;

      const destinationPath = path.join(
        destination,
        ...entry.path.split("/"),
      );
      await mkdir(path.dirname(destinationPath), {
        mode: 0o755,
        recursive: true,
      });
      if (await pathExists(destinationPath)) {
        fail(
          `snapshot filesystem path collision while creating ${JSON.stringify(entry.path)}`,
        );
      }
      const file = await open(destinationPath, "wx", entry.mode);
      const hash = createGitBlobHash(entry.object, size);
      try {
        await reader.readExactly(size, async (chunk) => {
          await writeAll(file, chunk);
          hash.update(chunk);
        });
        let terminator;
        await reader.readExactly(1, async (chunk) => {
          terminator = chunk[0];
        });
        if (terminator !== 0x0a) {
          fail(`Git blob for ${JSON.stringify(entry.path)} is not newline terminated`);
        }
        await file.chmod(entry.mode);
      } finally {
        await file.close();
      }
      const actualObject = hash.digest("hex");
      if (actualObject !== entry.object) {
        fail(
          `Git returned bytes that do not match object ${entry.object}: ${JSON.stringify(entry.path)}`,
        );
      }
      entry.size = size;
    });
    await reader.expectEOF();
    const processError = await completion;
    if (processError !== null) throw processError;
  } catch (error) {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
    }
    const processError = await completion;
    if (processError !== null && processError !== error) {
      // AggregateError retains both the stream failure and process failure.
      throw new AggregateError(
        [error, processError],
        `${error.message}; ${processError.message}`,
        { cause: error },
      );
    }
    throw error;
  }
}

async function hashSnapshotFile(filePath, entry) {
  const hash = createGitBlobHash(entry.object, entry.size);
  let size = 0;
  for await (const chunk of createReadStream(filePath)) {
    size += chunk.length;
    if (size > entry.size) {
      fail(
        `snapshot bytes exceed Git object ${entry.object}: ${JSON.stringify(entry.path)}`,
      );
    }
    hash.update(chunk);
  }
  if (size !== entry.size) {
    fail(
      `snapshot size for ${JSON.stringify(entry.path)} is ${size}, want ${entry.size}`,
    );
  }
  return hash.digest("hex");
}

async function main() {
  if (process.argv.length !== 5) {
    console.error(
      "usage: materialize-git-snapshot.mjs <repository-root> <commit> <new-destination>",
    );
    process.exitCode = 2;
    return;
  }
  process.umask(0o022);
  const repositoryRoot = await realpath(
    path.resolve(process.cwd(), process.argv[2]),
  );
  const runGit = createGitRunner(repositoryRoot);
  const revision = process.argv[3];
  const requestedDestination = path.resolve(process.cwd(), process.argv[4]);
  const destinationParent = await realpath(path.dirname(requestedDestination));
  const destination = path.join(
    destinationParent,
    path.basename(requestedDestination),
  );
  if (
    Buffer.byteLength(destination, "utf8") >
    maximumPortableAbsolutePathBytes
  ) {
    fail(
      `snapshot destination exceeds ${maximumPortableAbsolutePathBytes} bytes`,
    );
  }
  const resolvedRoot = await realpath(
    runGit(["rev-parse", "--show-toplevel"]).toString("utf8").trim(),
  );
  if (resolvedRoot !== repositoryRoot) {
    fail(`Git repository root ${resolvedRoot} does not match ${repositoryRoot}`);
  }
  const resolvedRevision = runGit([
    "rev-parse",
    "--verify",
    "--end-of-options",
    `${revision}^{commit}`,
  ])
    .toString("ascii")
    .trim();
  if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(resolvedRevision)) {
    fail(`Git resolved an invalid commit object ID: ${resolvedRevision}`);
  }
  const entries = parseTree(
    runGit(
      ["ls-tree", "-r", "-z", "--full-tree", resolvedRevision],
      { maxBuffer: maximumGitTreeOutputBytes },
    ),
    destination,
  );

  if (await pathExists(destination)) {
    fail(`snapshot destination already exists: ${destination}`);
  }

  let createdDestination = false;
  try {
    await mkdir(destination, { mode: 0o755 });
    await chmod(destination, 0o755);
    createdDestination = true;
    // Extraction stays ordered so a case-folding filesystem cannot race two
    // colliding writes. One batch process streams every bounded blob.
    await extractGitBlobs(repositoryRoot, entries, destination);

    const expectedPaths = entries.map((entry) => entry.path).toSorted();
    const actualPaths = (await physicalFiles(destination)).toSorted();
    assert.deepEqual(
      actualPaths,
      expectedPaths,
      "snapshot physical path set does not exactly match the Git tree",
    );
    await forEachSequential(entries, async (entry) => {
      const destinationPath = path.join(destination, ...entry.path.split("/"));
      const information = await stat(destinationPath);
      if ((information.mode & 0o777) !== entry.mode) {
        fail(
          `snapshot mode for ${JSON.stringify(entry.path)} is ${(information.mode & 0o777).toString(8)}, want ${entry.mode.toString(8)}`,
        );
      }
      const actualHash = await hashSnapshotFile(destinationPath, entry);
      if (actualHash !== entry.object) {
        fail(
          `snapshot bytes do not match Git object ${entry.object}: ${JSON.stringify(entry.path)}`,
        );
      }
    });
  } catch (error) {
    if (createdDestination) {
      await rm(destination, { force: true, recursive: true });
    }
    throw error;
  }
}

try {
  await main();
} catch (error) {
  console.error(`error: ${error.message}`);
  process.exitCode = 1;
}
