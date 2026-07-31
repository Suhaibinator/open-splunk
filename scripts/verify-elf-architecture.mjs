#!/usr/bin/env node
import { open } from "node:fs/promises";
import process from "node:process";

const architectures = new Map([
  ["amd64", { class: 2, data: 1, machine: 62, name: "x86-64" }],
  ["arm64", { class: 2, data: 1, machine: 183, name: "AArch64" }],
]);

const [architecture, ...binaryPaths] = process.argv.slice(2);
const expected = architectures.get(architecture);
if (!expected || binaryPaths.length === 0) {
  process.stderr.write(
    "usage: verify-elf-architecture.mjs <amd64|arm64> <binary> [...]\n",
  );
  process.exit(2);
}

const verifications = await Promise.all(binaryPaths.map(async (binaryPath) => {
  let handle;
  let verificationError = null;
  try {
    handle = await open(binaryPath, "r");
    const metadata = await handle.stat();
    if (!metadata.isFile() || metadata.size < 24) {
      throw new Error("not a regular ELF binary");
    }
    const header = Buffer.allocUnsafe(24);
    const { bytesRead } = await handle.read(header, 0, header.length, 0);
    if (bytesRead !== header.length ||
        header[0] !== 0x7f ||
        header[1] !== 0x45 ||
        header[2] !== 0x4c ||
        header[3] !== 0x46) {
      throw new Error("not an ELF binary");
    }
    if (header[4] !== expected.class) {
      throw new Error(`ELF class ${header[4]} is not 64-bit`);
    }
    if (header[5] !== expected.data) {
      throw new Error(`ELF data encoding ${header[5]} is not little-endian`);
    }
    if (header[6] !== 1) {
      throw new Error(`unsupported ELF version ${header[6]}`);
    }
    const type = header.readUInt16LE(16);
    if (type !== 2 && type !== 3) {
      throw new Error(`ELF type ${type} is not executable or position-independent`);
    }
    const machine = header.readUInt16LE(18);
    if (machine !== expected.machine) {
      throw new Error(
        `ELF machine ${machine} does not match ${expected.name} (${expected.machine})`,
      );
    }
    if (header.readUInt32LE(20) !== 1) {
      throw new Error("invalid ELF header version");
    }
  } catch (error) {
    verificationError = error;
  }
  try {
    await handle?.close();
  } catch (error) {
    verificationError ??= error;
  }
  return { binaryPath, error: verificationError };
}));

let failed = false;
for (const verification of verifications) {
  if (verification.error) {
    failed = true;
    process.stderr.write(
      `${verification.binaryPath}: ${verification.error.message}\n`,
    );
  } else {
    process.stdout.write(`${verification.binaryPath}: ELF64 ${expected.name}\n`);
  }
}

if (failed) process.exit(1);
