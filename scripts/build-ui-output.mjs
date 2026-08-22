import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import path from "node:path";

export async function cleanUIOutput(output) {
  const parent = path.dirname(output);
  const tombstoneRoot = await mkdtemp(
    path.join(parent, `.${path.basename(output)}-clean-`),
  );
  await chmod(tombstoneRoot, 0o700);
  const tombstone = path.join(tombstoneRoot, "previous");
  let preserveTombstone = false;
  try {
    let moved = false;
    try {
      await rename(output, tombstone);
      moved = true;
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
    if (moved) {
      const information = await lstat(tombstone);
      let rejection;
      if (information.isSymbolicLink()) {
        rejection = new Error(
          `refusing to clean symbolic-link UI output directory: ${output}`,
        );
      } else if (!information.isDirectory()) {
        rejection = new Error(
          `refusing to clean non-directory UI output path: ${output}`,
        );
      }
      if (rejection !== undefined) {
        try {
          await rename(tombstone, output);
        } catch (restoreError) {
          preserveTombstone = true;
          // AggregateError retains both the rejection and the failed recovery.
          throw new AggregateError(
            [rejection, restoreError],
            `${rejection.message}; recovery data preserved at ${tombstoneRoot}`,
            { cause: restoreError },
          );
        }
        throw rejection;
      }
      await rm(tombstone, { recursive: true });
    }

    await mkdir(output, { mode: 0o755 });
    await chmod(output, 0o755);
    await writeFile(path.join(output, ".gitkeep"), "\n", {
      encoding: "utf8",
      flag: "wx",
      mode: 0o644,
    });
  } finally {
    if (!preserveTombstone) {
      await rm(tombstoneRoot, { recursive: true, force: true });
    }
  }
}
