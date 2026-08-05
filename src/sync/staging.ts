import { createHash } from 'node:crypto';
import { mkdir, open, readdir, rename, rm } from 'node:fs/promises';
import { basename, dirname, join, resolve } from 'node:path';

/** Longest sanitised prefix kept before the digest, so names stay under NAME_MAX. */
const MAX_NAME_PREFIX = 120;

/**
 * Message ids contain ':' and may contain '/', neither safe in a filename.
 *
 * Sanitising alone is lossy: `a:1` and `a/1` both collapse to `a_1`, so two
 * distinct outbox rows would point at one file and the second stage would
 * overwrite the first. The id comes from `m.key.id`, supplied by the sending
 * peer and never validated, so that collision is reachable on purpose. Append
 * a digest of the raw id to keep the mapping injective.
 */
function safeName(messageId: string): string {
  const digest = createHash('sha1').update(messageId).digest('hex').slice(0, 8);
  const prefix = messageId.replace(/[^A-Za-z0-9@._-]/g, '_').slice(0, MAX_NAME_PREFIX);
  return `${prefix}-${digest}`;
}

export function stagedPathFor(dir: string, messageId: string): string {
  return join(dir, safeName(messageId));
}

/**
 * Write media to the staging directory durably.
 *
 * Order matters: bytes land in `tmp/`, are fsynced, and only then renamed into
 * place. Rename is atomic, and the parent directory is fsynced afterwards so
 * the rename itself survives power loss — otherwise the outbox row (committed
 * through a `synchronous = FULL` WAL) could outlive the file it points at.
 * The caller inserts the outbox row only after this resolves.
 */
export async function stageFile(dir: string, messageId: string, bytes: Buffer): Promise<string> {
  const tmpDir = join(dir, 'tmp');
  await mkdir(tmpDir, { recursive: true });

  const name = safeName(messageId);
  const tmpPath = join(tmpDir, name);
  const finalPath = join(dir, name);

  const handle = await open(tmpPath, 'w');
  try {
    await handle.writeFile(bytes);
    await handle.sync();
  } finally {
    await handle.close();
  }

  await rename(tmpPath, finalPath);
  await syncDir(dirname(finalPath));
  return finalPath;
}

/** fsync a directory so a rename into it is durable, not just journalled. */
async function syncDir(dir: string): Promise<void> {
  const handle = await open(dir, 'r');
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

/**
 * Delete staged files that no queue row references, plus any leftover temp
 * files. Run at startup: a crash between the rename and the row insert leaves
 * exactly this kind of orphan.
 */
export async function sweepOrphans(dir: string, keep: string[]): Promise<number> {
  let entries;
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch (err) {
    // Not created yet is normal on first boot. Anything else (EACCES, ENOTDIR)
    // is a real fault and must not masquerade as "nothing to sweep" — that
    // would let the staging directory grow unbounded while reporting success.
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') return 0;
    throw err;
  }

  await rm(join(dir, 'tmp'), { recursive: true, force: true });

  // Compare within this directory only: a stale row pointing at a previous
  // staging location must not shield a same-named orphan here.
  const here = resolve(dir);
  const keepSet = new Set(keep.filter((p) => resolve(dirname(p)) === here).map((p) => basename(p)));

  let removed = 0;
  for (const entry of entries) {
    if (entry.name === 'tmp' || keepSet.has(entry.name)) continue;
    // Staging is flat by construction. Never recurse: a stray directory
    // (@eaDir, .stfolder) is not ours to delete, and rm would throw on it.
    if (!entry.isFile()) continue;
    await rm(join(dir, entry.name), { force: true });
    removed += 1;
  }
  return removed;
}
