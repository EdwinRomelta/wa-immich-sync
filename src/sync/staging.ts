import { mkdir, open, readdir, rename, rm } from 'node:fs/promises';
import { basename, join } from 'node:path';

/** Message ids contain ':' and may contain '/', neither safe in a filename. */
function safeName(messageId: string): string {
  return messageId.replace(/[^A-Za-z0-9@._-]/g, '_');
}

export function stagedPathFor(dir: string, messageId: string): string {
  return join(dir, safeName(messageId));
}

/**
 * Write media to the staging directory durably.
 *
 * Order matters: bytes land in `tmp/`, are fsynced, and only then renamed into
 * place. Rename is atomic, so a crash can leave an orphaned file but never a
 * queue row pointing at a truncated or missing one. The caller inserts the
 * outbox row only after this resolves.
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
  return finalPath;
}

/**
 * Delete staged files that no queue row references, plus any leftover temp
 * files. Run at startup: a crash between the rename and the row insert leaves
 * exactly this kind of orphan.
 */
export async function sweepOrphans(dir: string, keep: string[]): Promise<number> {
  let entries: string[];
  try {
    entries = await readdir(dir);
  } catch {
    return 0;
  }

  await rm(join(dir, 'tmp'), { recursive: true, force: true });

  const keepSet = new Set(keep.map((p) => basename(p)));
  let removed = 0;
  for (const entry of entries) {
    if (entry === 'tmp' || keepSet.has(entry)) continue;
    await rm(join(dir, entry), { force: true });
    removed += 1;
  }
  return removed;
}
