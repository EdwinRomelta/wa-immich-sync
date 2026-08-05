import { openAsBlob } from 'node:fs';
import { rm, stat } from 'node:fs/promises';
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

/**
 * Move queued media into Immich.
 *
 * Never contacts WhatsApp: every field needed was captured at ingest, so a
 * retry hours later behaves exactly like the first attempt. A failure defers
 * the row with exponential backoff and leaves both the row and its staged file
 * untouched, so nothing is ever lost to an Immich outage.
 *
 * One failure mode is terminal rather than transient: the staged file itself
 * is gone (partial cleanup, manual deletion, a crash between unlink and row
 * delete). Deferring that row would retry an absent file forever, and it
 * would also permanently block re-ingest — `ingest.known()` treats any
 * existing outbox row as already handled, so the message could never be
 * captured again even if it resurfaces via history sync or a zip backfill.
 * That row is dropped instead: loudly logged, removed from the outbox, and
 * left out of `synced` so the message stays eligible for re-ingest.
 */
export function startDrain(deps: DrainDeps) {
  const now = deps.now ?? Date.now;
  const intervalMs = deps.intervalMs ?? 30_000;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;

  function deferRow(row: OutboxRow, at: number, err: unknown, tally: DrainTally): void {
    const message = (err as Error).message;
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

  function dropRow(row: OutboxRow, tally: DrainTally): void {
    deps.outbox.remove(row.messageId);
    tally.dropped += 1;
    deps.logger.error(
      { messageId: row.messageId, filePath: row.filePath },
      'drain dropped row: staged file missing, cannot retry',
    );
  }

  async function tickAt(at: number): Promise<DrainTally> {
    const tally: DrainTally = { uploaded: 0, deferred: 0, dropped: 0 };

    for (const row of deps.outbox.due(at, deps.batchSize)) {
      // fs.openAsBlob() does not propagate errno codes on failure (it throws
      // a generic ERR_INVALID_ARG_VALUE regardless of cause), so a missing
      // file can't be distinguished from any other open failure by catching
      // it directly. Stat first: fs/promises.stat still reports ENOENT
      // faithfully, which is the one failure this loop must treat as
      // terminal rather than transient.
      try {
        await stat(row.filePath);
      } catch (err) {
        if (isEnoent(err)) {
          dropRow(row, tally);
        } else {
          deferRow(row, at, err, tally);
        }
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
        await rm(row.filePath, { force: true });

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

  const tick = (): Promise<DrainTally> => tickAt(now());

  function loop(): void {
    if (stopped) return;
    void tick().finally(() => {
      if (!stopped) timer = setTimeout(loop, intervalMs);
    });
  }

  function stop(): void {
    stopped = true;
    if (timer) clearTimeout(timer);
    timer = null;
  }

  if (deps.autoStart !== false) timer = setTimeout(loop, intervalMs);

  return { stop, tick, tickAt };
}
