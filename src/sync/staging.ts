import { createHash } from 'node:crypto';
import { mkdir, open, readdir, rename, rm, writeFile } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve } from 'node:path';

/**
 * Marker written into the staging directory the first time it is prepared.
 * Its presence is what tells `sweepOrphans` "this directory is genuinely an
 * outbox staging area, safe to delete unreferenced files from" — see the
 * doc comment on `sweepOrphans` for why that distinction exists.
 */
export const OUTBOX_MARKER_FILE = '.wa-outbox';

/** A path `ensureOutboxDirWritable` must refuse to let the staging dir overlap. */
export interface OverlapGuard {
  /** Human-readable name used in the thrown error, e.g. "DEDUP_DB". */
  label: string;
  /**
   * The thing to guard — pass the actual path that must never be reachable
   * by a directory sweep. For a single file (e.g. the dedup db), pass the
   * file's own path, not its parent directory: the shipped defaults put
   * OUTBOX_DIR, DEDUP_DB, and WA_AUTH_DIR as siblings under the same `./data`
   * parent, so guarding on that shared parent directory would flag the
   * default configuration itself as an overlap. For a directory (e.g. the WA
   * auth dir), pass the directory itself.
   */
  path: string;
}

/** True when `other` is `ancestor` itself, or lives anywhere underneath it. */
function isSameOrWithin(ancestor: string, other: string): boolean {
  if (ancestor === other) return true;
  const rel = relative(ancestor, other);
  return rel !== '' && !rel.startsWith('..') && !isAbsolute(rel);
}

/**
 * Refuse to proceed if the staging directory is the same as, contains, or is
 * contained by, any guarded path. `sweepOrphans` deletes every regular file
 * it doesn't recognise from the staging directory; if that directory turns
 * out to BE (or to sit inside, or to sit around) a guarded path, the next
 * sweep can delete `synced.db` or `creds.json` instead of orphaned staged
 * media. Both containment directions matter: `OUTBOX_DIR=./data` puts the
 * dedup db file *inside* the staging dir, while `OUTBOX_DIR=./data/auth`
 * puts the staging dir itself *inside* the WA auth dir — either way the
 * sweep can reach files it must never touch.
 */
export function assertNoOverlap(stagingDir: string, guards: OverlapGuard[]): void {
  const staging = resolve(stagingDir);
  for (const guard of guards) {
    const other = resolve(guard.path);
    if (isSameOrWithin(staging, other) || isSameOrWithin(other, staging)) {
      throw new Error(
        `OUTBOX_DIR (${stagingDir}) overlaps with ${guard.label} (${guard.path}); refusing to ` +
          'start, since the startup orphan sweep would be able to delete files there. Point ' +
          'OUTBOX_DIR at a directory used for nothing else.',
      );
    }
  }
}

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
 * Create the outbox staging directory, confirm it is actually writable, and
 * mark it so `sweepOrphans` can recognise it as its own later.
 *
 * Without the write probe, a misconfigured OUTBOX_DIR (bad permissions, a
 * bind mount not ready yet, a typo'd path) makes ingest fail silently and
 * repeatedly: every single WhatsApp message returns 'error' forever, one log
 * line at a time, while traffic streams past unrecorded. This is the one
 * startup check that SHOULD block — unlike the Immich readiness gate, there
 * is no safe way to proceed without a writable place to put bytes.
 *
 * `guards` should name every path that must never be reachable by the
 * startup sweep (the dedup db file, the WhatsApp auth dir) — see
 * `assertNoOverlap`. Callers pass these in rather than this module reading
 * them from config, so staging.ts stays free of env access and unit
 * testable.
 */
export async function ensureOutboxDirWritable(dir: string, guards: OverlapGuard[] = []): Promise<void> {
  assertNoOverlap(dir, guards);
  await mkdir(dir, { recursive: true });
  const probePath = join(dir, '.write-probe');
  await writeFile(probePath, '');
  await rm(probePath, { force: true });
  // Written last, once the directory is confirmed writable: its presence is
  // what tells sweepOrphans (below) that this directory is a real outbox
  // staging area and not some arbitrary non-empty directory OUTBOX_DIR was
  // accidentally repointed at.
  await writeFile(join(dir, OUTBOX_MARKER_FILE), '');
}

/**
 * Delete staged files that no queue row references, plus any leftover temp
 * files. Run at startup: a crash between the rename and the row insert leaves
 * exactly this kind of orphan.
 *
 * A non-empty directory with no `OUTBOX_MARKER_FILE` is refused outright,
 * rather than swept. The overlap check in `ensureOutboxDirWritable` guards
 * the specific paths known at startup (the dedup db file, the WA auth dir),
 * but it can't guard against every possible future repoint of OUTBOX_DIR onto
 * arbitrary user data. The marker is the second, independent line of defense: it only
 * appears in a directory this code itself prepared, so a directory that's
 * merely non-empty (an unrelated folder, a mistake, a home directory) is
 * left alone instead of having its files deleted. An empty directory sweeps
 * fine without a marker — there's nothing there to protect yet.
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

  const hasMarker = entries.some((e) => e.name === OUTBOX_MARKER_FILE && e.isFile());
  if (entries.length > 0 && !hasMarker) {
    throw new Error(
      `refusing to sweep ${dir}: it is non-empty and has no ${OUTBOX_MARKER_FILE} marker, so it ` +
        "does not look like an outbox staging directory. Run ensureOutboxDirWritable() first, or " +
        'point OUTBOX_DIR somewhere else.',
    );
  }

  await rm(join(dir, 'tmp'), { recursive: true, force: true });

  // Compare within this directory only: a stale row pointing at a previous
  // staging location must not shield a same-named orphan here.
  const here = resolve(dir);
  const keepSet = new Set(keep.filter((p) => resolve(dirname(p)) === here).map((p) => basename(p)));
  keepSet.add(OUTBOX_MARKER_FILE);

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
