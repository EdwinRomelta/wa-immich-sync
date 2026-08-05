import { openAsBlob } from 'node:fs';
import { rm, stat } from 'node:fs/promises';
import { dirname } from 'node:path';
import type { ImmichClient, UploadMeta } from '../immich/client.ts';
import type { OutboxRow, OutboxStore } from './outboxStore.ts';
import { backoffDelayMs } from '../util/backoff.ts';

type DrainLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
  error: (...a: unknown[]) => void;
};

export interface DrainDeps {
  immich: Pick<ImmichClient, 'uploadBlob' | 'ensureAlbum' | 'addToAlbum'>;
  outbox: Pick<OutboxStore, 'due' | 'markSyncedAndRemove' | 'defer' | 'remove'>;
  logger: DrainLogger;
  batchSize: number;
  baseBackoffMs: number;
  maxBackoffMs: number;
  /** Loop period. Only used when autoStart is not false. */
  intervalMs?: number;
  /** Injectable clock (tests). */
  now?: () => number;
  /** Start the timer loop. Tests drive tick() by hand instead. */
  autoStart?: boolean;
  /**
   * A row whose staged file is confirmed terminal (missing with its parent
   * directory present, not a regular file, or empty) is only dropped once it
   * has been retried this many times. Below the threshold it defers instead,
   * so a one-off race (a concurrent writer still finalising the file) gets a
   * second look before the message is given up on. Defaults to 3.
   */
  dropAfterAttempts?: number;
  /**
   * Upper bound on how many rows a single tick may drop. Without this, a
   * whole-directory outage (STAGING_DIR misconfigured, bind mount not ready,
   * NAS unreachable) makes every row in the batch look terminal at once and
   * one tick can empty the entire outbox. Defaults to 5.
   */
  maxDropsPerTick?: number;
}

export interface DrainTally {
  uploaded: number;
  deferred: number;
  /** Terminal failures dropped instead of retried — see `dropRow`. */
  dropped: number;
}

function isEnoent(err: unknown): boolean {
  return typeof err === 'object' && err !== null && (err as NodeJS.ErrnoException).code === 'ENOENT';
}

/** Never throws: a directory that can't be stat'd is treated as absent. */
async function dirExists(path: string): Promise<boolean> {
  try {
    const s = await stat(path);
    return s.isDirectory();
  } catch {
    return false;
  }
}

/** Safe on any thrown value, unlike `(err as Error).message` which crashes on non-Error throws. */
function errMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

type StageStatus =
  | { kind: 'ok' }
  /** Parent directory itself is gone — looks like an outage, not a dead row. */
  | { kind: 'outage' }
  /** The file will never become uploadable: missing (parent present), not a regular file, or empty. */
  | { kind: 'terminal'; reason: string }
  /** stat() failed for a reason other than ENOENT — transient, defer as usual. */
  | { kind: 'error'; err: unknown };

/**
 * Classify the staged file before it is opened. `fs.openAsBlob()` does not
 * propagate errno codes on failure (it throws a generic ERR_INVALID_ARG_VALUE
 * regardless of cause), so a missing file can't be distinguished from any
 * other open failure by catching it directly — `stat` is used instead, since
 * `fs/promises.stat` still reports ENOENT faithfully.
 *
 * A bare ENOENT is not enough to call a row terminal: if the file's parent
 * directory is also gone, every row in the batch will look the same way, and
 * that pattern means a directory outage (changed STAGING_DIR, a bind mount
 * not ready yet, a NAS that dropped off), not dead rows. Only a missing file
 * with a present parent directory — or a present-but-unusable file — is
 * terminal.
 */
async function stageFileStatus(filePath: string): Promise<StageStatus> {
  let stats;
  try {
    stats = await stat(filePath);
  } catch (err) {
    if (!isEnoent(err)) return { kind: 'error', err };
    const parentPresent = await dirExists(dirname(filePath));
    return parentPresent ? { kind: 'terminal', reason: 'staged file missing' } : { kind: 'outage' };
  }
  if (!stats.isFile()) return { kind: 'terminal', reason: 'staged path is not a regular file' };
  if (stats.size === 0) return { kind: 'terminal', reason: 'staged file is empty' };
  return { kind: 'ok' };
}

/**
 * Move queued media into Immich.
 *
 * Never contacts WhatsApp: every field needed was captured at ingest, so a
 * retry hours later behaves exactly like the first attempt. A failure defers
 * the row with exponential backoff and leaves both the row and its staged file
 * untouched, so nothing is ever lost to an Immich outage.
 *
 * One failure mode is terminal rather than transient: the staged file itself
 * is unusable (partial cleanup, manual deletion, a crash between unlink and
 * row delete, or a zero-byte/non-file artifact). Deferring that row would
 * retry a dead file forever, and it would also permanently block re-ingest —
 * `ingest.known()` treats any existing outbox row as already handled, so the
 * message could never be captured again even if it resurfaces via history
 * sync or a zip backfill. That row is dropped instead: loudly logged, removed
 * from the outbox, and left out of `synced` so the message stays eligible for
 * re-ingest. Two guards keep a directory-level outage from being mistaken for
 * a batch of dead rows: a row is only dropped once it has earned it
 * (`dropAfterAttempts`), and a single tick can only drop so many
 * (`maxDropsPerTick`) before it stops and leaves the rest for next time.
 */
export function startDrain(deps: DrainDeps) {
  const now = deps.now ?? Date.now;
  const intervalMs = deps.intervalMs ?? 30_000;
  const dropAfterAttempts = deps.dropAfterAttempts ?? 3;
  const maxDropsPerTick = deps.maxDropsPerTick ?? 5;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;
  // Re-entrancy guard (M3): a tick kicked off by ingest could otherwise
  // overlap the timer's own tick, and both would upload the same rows —
  // fs.openAsBlob() does not hold the fd, so one tick can hold a blob open
  // for a file the other has already unlinked.
  let inFlight: Promise<DrainTally> | null = null;

  function deferRow(row: OutboxRow, at: number, err: unknown, tally: DrainTally): void {
    const message = errMessage(err);
    const delay = backoffDelayMs(row.attempts, {
      baseMs: deps.baseBackoffMs,
      maxMs: deps.maxBackoffMs,
      jitterRatio: 0.2,
    });
    deps.outbox.defer(row.messageId, message, at + delay);
    tally.deferred += 1;
    deps.logger.warn(
      { messageId: row.messageId, attempts: row.attempts + 1, retryInMs: delay, err: message },
      'drain deferred',
    );
  }

  function dropRow(row: OutboxRow, tally: DrainTally, reason: string): void {
    deps.outbox.remove(row.messageId);
    tally.dropped += 1;
    deps.logger.error(
      { messageId: row.messageId, filePath: row.filePath, attempts: row.attempts, reason },
      'drain dropped row: staged file unusable, cannot retry',
    );
  }

  async function tickAt(at: number): Promise<DrainTally> {
    const tally: DrainTally = { uploaded: 0, deferred: 0, dropped: 0 };

    for (const row of deps.outbox.due(at, deps.batchSize)) {
      const status = await stageFileStatus(row.filePath);

      if (status.kind === 'error') {
        deferRow(row, at, status.err, tally);
        continue;
      }

      if (status.kind === 'outage') {
        deferRow(row, at, new Error('staged directory missing'), tally);
        continue;
      }

      if (status.kind === 'terminal') {
        if (row.attempts < dropAfterAttempts) {
          deferRow(row, at, new Error(status.reason), tally);
          continue;
        }
        // A directory-wide outage can make every row in the batch look
        // terminal at once; cap the damage a single tick can do and leave
        // the rest for the next tick rather than emptying the queue.
        if (tally.dropped >= maxDropsPerTick) break;
        dropRow(row, tally, status.reason);
        continue;
      }

      let blob: Blob;
      try {
        blob = await openAsBlob(row.filePath, { type: row.mimeType });
      } catch (err) {
        deferRow(row, at, err, tally);
        continue;
      }

      try {
        const meta: UploadMeta = {
          messageId: row.messageId,
          fileName: row.fileName,
          mimeType: row.mimeType,
          timestamp: new Date(row.capturedAt),
        };
        const uploaded = await deps.immich.uploadBlob(blob, meta);

        if (row.albumName) {
          const albumId = await deps.immich.ensureAlbum(row.albumName);
          await deps.immich.addToAlbum(albumId, uploaded.assetId);
        }

        // Record and dequeue atomically, then drop the staged bytes. A crash
        // before the unlink leaves an orphan, which the startup sweep clears.
        deps.outbox.markSyncedAndRemove(row, uploaded.assetId, uploaded.status);

        // The row is already synced at this point — a failed unlink (EACCES,
        // EPERM, EBUSY, EROFS, EIO; `force: true` only swallows ENOENT) must
        // not turn a real success into a phantom deferral. `defer` would run
        // an UPDATE against a row that's already gone, silently doing
        // nothing while the tally lies about what happened. Warn and move on
        // instead — the startup sweep reclaims orphaned staged files.
        try {
          await rm(row.filePath, { force: true });
        } catch (rmErr) {
          deps.logger.warn(
            { messageId: row.messageId, filePath: row.filePath, err: errMessage(rmErr) },
            'staged file left behind after sync; the startup sweep will reclaim it',
          );
        }

        tally.uploaded += 1;
        deps.logger.info(
          { messageId: row.messageId, assetId: uploaded.assetId, status: uploaded.status },
          'synced',
        );
      } catch (err) {
        deferRow(row, at, err, tally);
      }
    }

    return tally;
  }

  function tick(): Promise<DrainTally> {
    if (inFlight) return inFlight;
    const run = tickAt(now()).finally(() => {
      inFlight = null;
    });
    inFlight = run;
    return run;
  }

  function loop(): void {
    if (stopped) return;
    void tick()
      .catch((err) => {
        deps.logger.error({ err: errMessage(err) }, 'drain tick failed');
      })
      .finally(() => {
        if (!stopped) timer = setTimeout(loop, intervalMs);
      });
  }

  function stop(): Promise<void> {
    stopped = true;
    if (timer) clearTimeout(timer);
    timer = null;
    if (!inFlight) return Promise.resolve();
    // Swallow rejection here too — a failing in-flight tick must not turn a
    // clean shutdown into an unhandled rejection or a non-zero exit.
    return inFlight.then(
      () => undefined,
      () => undefined,
    );
  }

  if (deps.autoStart !== false) timer = setTimeout(loop, intervalMs);

  return { stop, tick, tickAt };
}
