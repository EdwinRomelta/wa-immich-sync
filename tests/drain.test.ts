import { chmodSync, existsSync, mkdirSync, mkdtempSync, openAsBlob, rmSync, statSync, writeFileSync } from 'node:fs';
import { rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import type { OutboxRow } from '../src/sync/outboxStore.ts';
import { startDrain } from '../src/sync/drain.ts';
import type { DrainDeps } from '../src/sync/drain.ts';
import type { UploadMeta } from '../src/immich/client.ts';

// Only the fake-timer "does not overlap" test needs these mocked: every other
// test lets them fall through to the real implementation (the default
// `vi.fn(actual.x)` behaviour below), since combining fake timers with real
// filesystem I/O races the virtual clock against real, non-timer-based async
// completion and makes the timer-loop assertions flaky.
vi.mock('node:fs', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs')>();
  return { ...actual, openAsBlob: vi.fn(actual.openAsBlob) };
});
vi.mock('node:fs/promises', async (importOriginal) => {
  const actual = await importOriginal<typeof import('node:fs/promises')>();
  return { ...actual, stat: vi.fn(actual.stat), rm: vi.fn(actual.rm) };
});

/** Polls a real clock even when fake timers are active elsewhere in the file. */
async function waitFor(condition: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) throw new Error('waitFor: condition not met in time');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function setup(albumName = 'Daycare') {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const dir = mkdtempSync(join(tmpdir(), 'drain-test-'));
  const filePath = join(dir, 'g_g.us_A1');
  writeFileSync(filePath, 'photo-bytes');

  outbox.enqueue({
    messageId: 'g@g.us:A1',
    groupJid: 'g@g.us',
    albumName,
    filePath,
    fileName: 'IMG-A1.jpg',
    mimeType: 'image/jpeg',
    capturedAt: 1_700_000_000_000,
    createdAt: 1_700_000_000_000,
  });

  const immich = {
    uploadBlob: vi.fn(async () => ({ assetId: 'asset-1', status: 'created' as const })),
    ensureAlbum: vi.fn(async () => 'album-1'),
    addToAlbum: vi.fn(async () => undefined),
  };
  const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const drain = startDrain({
    immich,
    outbox,
    logger,
    batchSize: 10,
    baseBackoffMs: 1000,
    maxBackoffMs: 60_000,
    now: () => 5_000,
    autoStart: false,
  });
  return { drain, outbox, dedup, immich, filePath, logger };
}

/** Bumps `attempts` without touching `next_try_at`, so the row stays due. */
function bumpAttempts(outbox: OutboxStore, messageId: string, times: number): void {
  for (let i = 0; i < times; i += 1) outbox.defer(messageId, 'seed', 0);
}

/** Fully mocked deps for tests about the timer loop's own behaviour, not row processing. */
function mockDeps(overrides: Partial<DrainDeps> = {}) {
  const outbox = {
    due: vi.fn((): OutboxRow[] => []),
    markSyncedAndRemove: vi.fn(),
    defer: vi.fn(),
    remove: vi.fn(),
  };
  const immich = {
    uploadBlob: vi.fn(async () => ({ assetId: 'asset-1', status: 'created' as const })),
    ensureAlbum: vi.fn(async () => 'album-1'),
    addToAlbum: vi.fn(async () => undefined),
  };
  const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const deps: DrainDeps = {
    immich,
    outbox,
    logger,
    batchSize: 10,
    baseBackoffMs: 1000,
    maxBackoffMs: 60_000,
    intervalMs: 10_000,
    ...overrides,
  };
  return { deps, outbox, immich, logger };
}

function makeRow(overrides: Partial<OutboxRow> = {}): OutboxRow {
  const dir = mkdtempSync(join(tmpdir(), 'drain-row-'));
  const filePath = join(dir, 'staged.jpg');
  writeFileSync(filePath, 'photo-bytes');
  return {
    messageId: 'g@g.us:A1',
    groupJid: 'g@g.us',
    albumName: '',
    filePath,
    fileName: 'IMG-A1.jpg',
    mimeType: 'image/jpeg',
    capturedAt: 1_700_000_000_000,
    attempts: 0,
    lastError: null,
    createdAt: 1_700_000_000_000,
    nextTryAt: 0,
    ...overrides,
  };
}

describe('drain', () => {
  it('uploads, files into the album, records synced and deletes the staged file', async () => {
    const { drain, outbox, dedup, immich, filePath } = setup();

    const tally = await drain.tick();

    expect(tally).toEqual({ uploaded: 1, deferred: 0, dropped: 0 });
    expect(immich.uploadBlob).toHaveBeenCalledTimes(1);
    expect(immich.ensureAlbum).toHaveBeenCalledWith('Daycare');
    expect(immich.addToAlbum).toHaveBeenCalledWith('album-1', 'asset-1');
    expect(dedup.has('g@g.us:A1')).toBe(true);
    expect(outbox.depth()).toBe(0);
    expect(existsSync(filePath)).toBe(false);
  });

  it('passes the captured timestamp through as the Immich creation date', async () => {
    const { drain, immich } = setup();
    await drain.tick();
    const [, meta] = immich.uploadBlob.mock.calls[0] as unknown as [Blob, UploadMeta];
    expect(meta.timestamp.getTime()).toBe(1_700_000_000_000);
    expect(meta.messageId).toBe('g@g.us:A1');
    expect(meta.fileName).toBe('IMG-A1.jpg');
  });

  it('skips album calls when the row has no album name', async () => {
    const { drain, immich } = setup('');
    await drain.tick();
    expect(immich.ensureAlbum).not.toHaveBeenCalled();
    expect(immich.addToAlbum).not.toHaveBeenCalled();
  });

  it('keeps the row and the file when the upload fails', async () => {
    const { drain, outbox, dedup, immich, filePath } = setup();
    immich.uploadBlob.mockRejectedValueOnce(new Error('ECONNREFUSED'));

    const tally = await drain.tick();

    expect(tally).toEqual({ uploaded: 0, deferred: 1, dropped: 0 });
    expect(outbox.depth()).toBe(1);
    expect(dedup.has('g@g.us:A1')).toBe(false);
    expect(existsSync(filePath)).toBe(true);
  });

  it('backs the row off so it is not retried immediately', async () => {
    const { drain, outbox, immich } = setup();
    immich.uploadBlob.mockRejectedValueOnce(new Error('ECONNREFUSED'));
    await drain.tick();

    expect(outbox.due(5_000, 10)).toHaveLength(0);
    const row = outbox.due(1_000_000, 10)[0]!;
    expect(row.attempts).toBe(1);
    expect(row.lastError).toContain('ECONNREFUSED');
  });

  it('succeeds on a later tick once Immich recovers', async () => {
    const { drain, outbox, dedup, immich } = setup();
    immich.uploadBlob.mockRejectedValueOnce(new Error('ECONNREFUSED'));
    await drain.tick();

    const later = await drain.tickAt(1_000_000);

    expect(later).toEqual({ uploaded: 1, deferred: 0, dropped: 0 });
    expect(dedup.has('g@g.us:A1')).toBe(true);
    expect(outbox.depth()).toBe(0);
  });

  it('still defers a normal upload failure rather than dropping it', async () => {
    const { drain, outbox, immich, logger } = setup();
    immich.uploadBlob.mockRejectedValueOnce(new Error('ECONNREFUSED'));

    const tally = await drain.tick();

    expect(tally.dropped).toBe(0);
    expect(tally.deferred).toBe(1);
    expect(outbox.depth()).toBe(1);
    expect(logger.error).not.toHaveBeenCalled();
  });

  it('stores a string last_error even when the thrown value is not an Error instance (m4)', async () => {
    const { drain, outbox, immich } = setup();
    // A non-Error rejection must never leave last_error NULL — verifies
    // errMessage()'s String(err) fallback rather than `(err as Error).message`.
    immich.uploadBlob.mockRejectedValueOnce('boom');

    await drain.tick();

    const row = outbox.due(1_000_000, 10)[0]!;
    expect(row.lastError).toBe('boom');
  });

  describe('missing staged file (C2)', () => {
    it('drops the row and logs an error once attempts reach the threshold, instead of deferring it forever', async () => {
      const { drain, outbox, dedup, immich, filePath, logger } = setup();
      // Simulate a missing staged file: partial cleanup, manual deletion, or a
      // crash between unlink and row delete. Retrying this row can never
      // succeed, and deferring it would deadlock re-ingest permanently (a
      // message with an existing outbox row never reaches enqueue again).
      // C2 requires the row to have earned the drop first.
      bumpAttempts(outbox, 'g@g.us:A1', 3);
      rmSync(filePath);

      const tally = await drain.tick();

      expect(tally).toEqual({ uploaded: 0, deferred: 0, dropped: 1 });
      expect(outbox.has('g@g.us:A1')).toBe(false);
      expect(outbox.depth()).toBe(0);
      // Dropping must not fabricate a fake success: the message never reached
      // Immich, so it must not be recorded as synced either.
      expect(dedup.has('g@g.us:A1')).toBe(false);
      expect(immich.uploadBlob).not.toHaveBeenCalled();
      expect(logger.error).toHaveBeenCalledTimes(1);
      const [payload] = logger.error.mock.calls[0]!;
      expect(payload).toMatchObject({ messageId: 'g@g.us:A1', filePath });
    });

    it('defers rather than drops when attempts are below the threshold (C2.2)', async () => {
      const { drain, outbox, dedup, immich, filePath, logger } = setup();
      bumpAttempts(outbox, 'g@g.us:A1', 2); // one short of the default threshold of 3
      rmSync(filePath);

      const tally = await drain.tick();

      expect(tally).toEqual({ uploaded: 0, deferred: 1, dropped: 0 });
      expect(outbox.has('g@g.us:A1')).toBe(true);
      expect(dedup.has('g@g.us:A1')).toBe(false);
      expect(immich.uploadBlob).not.toHaveBeenCalled();
      expect(logger.error).not.toHaveBeenCalled();
    });

    it('defers rather than drops when the staged file is missing because its parent directory is gone (C2.1)', async () => {
      const db = openDb(':memory:');
      const dedup = new DedupStore(db);
      const outbox = new OutboxStore(db);
      const dir = mkdtempSync(join(tmpdir(), 'drain-outage-'));
      const filePath = join(dir, 'ghost.jpg');
      writeFileSync(filePath, 'bytes');
      outbox.enqueue({
        messageId: 'g@g.us:A1',
        groupJid: 'g@g.us',
        albumName: '',
        filePath,
        fileName: 'ghost.jpg',
        mimeType: 'image/jpeg',
        capturedAt: 1_700_000_000_000,
        createdAt: 1_700_000_000_000,
      });
      // Even attempts at the drop threshold must not be dropped when the
      // directory itself is gone — that pattern means an outage, not a dead row.
      bumpAttempts(outbox, 'g@g.us:A1', 5);
      rmSync(dir, { recursive: true, force: true });

      const immich = {
        uploadBlob: vi.fn(async () => ({ assetId: 'asset-1', status: 'created' as const })),
        ensureAlbum: vi.fn(async () => 'album-1'),
        addToAlbum: vi.fn(async () => undefined),
      };
      const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
      const drain = startDrain({
        immich,
        outbox,
        logger,
        batchSize: 10,
        baseBackoffMs: 1000,
        maxBackoffMs: 60_000,
        now: () => 5_000,
        autoStart: false,
      });

      const tally = await drain.tick();

      expect(tally).toEqual({ uploaded: 0, deferred: 1, dropped: 0 });
      expect(outbox.has('g@g.us:A1')).toBe(true);
      expect(dedup.has('g@g.us:A1')).toBe(false);
      expect(immich.uploadBlob).not.toHaveBeenCalled();
      expect(logger.error).not.toHaveBeenCalled();
    });

    it('caps drops at maxDropsPerTick so a directory-wide outage cannot empty the queue in one tick', async () => {
      const db = openDb(':memory:');
      const outbox = new OutboxStore(db);
      const dir = mkdtempSync(join(tmpdir(), 'drain-cap-'));
      const rowCount = 4;
      for (let i = 0; i < rowCount; i += 1) {
        const filePath = join(dir, `row-${i}.jpg`);
        writeFileSync(filePath, 'bytes');
        outbox.enqueue({
          messageId: `g@g.us:A${i}`,
          groupJid: 'g@g.us',
          albumName: '',
          filePath,
          fileName: `row-${i}.jpg`,
          mimeType: 'image/jpeg',
          capturedAt: 1_700_000_000_000 + i,
          createdAt: 1_700_000_000_000 + i,
        });
        bumpAttempts(outbox, `g@g.us:A${i}`, 3);
        rmSync(filePath);
      }

      const immich = {
        uploadBlob: vi.fn(async () => ({ assetId: 'asset-1', status: 'created' as const })),
        ensureAlbum: vi.fn(async () => 'album-1'),
        addToAlbum: vi.fn(async () => undefined),
      };
      const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
      const drain = startDrain({
        immich,
        outbox,
        logger,
        batchSize: 10,
        baseBackoffMs: 1000,
        maxBackoffMs: 60_000,
        now: () => 5_000,
        autoStart: false,
        maxDropsPerTick: 2,
      });

      const tally = await drain.tick();

      expect(tally.dropped).toBe(2);
      expect(outbox.depth()).toBe(rowCount - 2);
    });
  });

  it('treats a directory staged where a file is expected as terminal (m5)', async () => {
    const db = openDb(':memory:');
    const outbox = new OutboxStore(db);
    const dir = mkdtempSync(join(tmpdir(), 'drain-dirbug-'));
    const subDir = join(dir, 'sub');
    mkdirSync(subDir);
    outbox.enqueue({
      messageId: 'g@g.us:A1',
      groupJid: 'g@g.us',
      albumName: '',
      filePath: subDir,
      fileName: 'sub',
      mimeType: 'image/jpeg',
      capturedAt: 1_700_000_000_000,
      createdAt: 1_700_000_000_000,
    });
    bumpAttempts(outbox, 'g@g.us:A1', 3);

    const immich = {
      uploadBlob: vi.fn(async () => ({ assetId: 'asset-1', status: 'created' as const })),
      ensureAlbum: vi.fn(async () => 'album-1'),
      addToAlbum: vi.fn(async () => undefined),
    };
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const drain = startDrain({
      immich,
      outbox,
      logger,
      batchSize: 10,
      baseBackoffMs: 1000,
      maxBackoffMs: 60_000,
      now: () => 5_000,
      autoStart: false,
    });

    const tally = await drain.tick();

    expect(tally).toEqual({ uploaded: 0, deferred: 0, dropped: 1 });
    expect(immich.uploadBlob).not.toHaveBeenCalled();
  });

  it('treats an empty staged file as terminal (m5)', async () => {
    const db = openDb(':memory:');
    const outbox = new OutboxStore(db);
    const dir = mkdtempSync(join(tmpdir(), 'drain-empty-'));
    const filePath = join(dir, 'empty.jpg');
    writeFileSync(filePath, '');
    outbox.enqueue({
      messageId: 'g@g.us:A1',
      groupJid: 'g@g.us',
      albumName: '',
      filePath,
      fileName: 'empty.jpg',
      mimeType: 'image/jpeg',
      capturedAt: 1_700_000_000_000,
      createdAt: 1_700_000_000_000,
    });
    bumpAttempts(outbox, 'g@g.us:A1', 3);

    const immich = {
      uploadBlob: vi.fn(async () => ({ assetId: 'asset-1', status: 'created' as const })),
      ensureAlbum: vi.fn(async () => 'album-1'),
      addToAlbum: vi.fn(async () => undefined),
    };
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const drain = startDrain({
      immich,
      outbox,
      logger,
      batchSize: 10,
      baseBackoffMs: 1000,
      maxBackoffMs: 60_000,
      now: () => 5_000,
      autoStart: false,
    });

    const tally = await drain.tick();

    expect(tally).toEqual({ uploaded: 0, deferred: 0, dropped: 1 });
    expect(immich.uploadBlob).not.toHaveBeenCalled();
  });

  it('continues past a failing row so a later row in the same batch still succeeds', async () => {
    const db = openDb(':memory:');
    const dedup = new DedupStore(db);
    const outbox = new OutboxStore(db);
    const dir = mkdtempSync(join(tmpdir(), 'drain-batch-'));
    const file1 = join(dir, 'a.jpg');
    const file2 = join(dir, 'b.jpg');
    writeFileSync(file1, 'x');
    writeFileSync(file2, 'y');
    outbox.enqueue({
      messageId: 'g@g.us:A1',
      groupJid: 'g@g.us',
      albumName: '',
      filePath: file1,
      fileName: 'a.jpg',
      mimeType: 'image/jpeg',
      capturedAt: 1_700_000_000_000,
      createdAt: 1_700_000_000_000,
    });
    outbox.enqueue({
      messageId: 'g@g.us:A2',
      groupJid: 'g@g.us',
      albumName: '',
      filePath: file2,
      fileName: 'b.jpg',
      mimeType: 'image/jpeg',
      capturedAt: 1_700_000_000_001,
      createdAt: 1_700_000_000_001,
    });

    const immich = {
      uploadBlob: vi
        .fn()
        .mockRejectedValueOnce(new Error('ECONNREFUSED'))
        .mockResolvedValueOnce({ assetId: 'asset-2', status: 'created' as const }),
      ensureAlbum: vi.fn(async () => 'album-1'),
      addToAlbum: vi.fn(async () => undefined),
    };
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
    const drain = startDrain({
      immich,
      outbox,
      logger,
      batchSize: 10,
      baseBackoffMs: 1000,
      maxBackoffMs: 60_000,
      now: () => 5_000,
      autoStart: false,
    });

    const tally = await drain.tick();

    expect(tally).toEqual({ uploaded: 1, deferred: 1, dropped: 0 });
    expect(immich.uploadBlob).toHaveBeenCalledTimes(2);
    expect(outbox.has('g@g.us:A1')).toBe(true);
    expect(dedup.has('g@g.us:A2')).toBe(true);
    expect(outbox.has('g@g.us:A2')).toBe(false);
  });

  describe('rm failure after a successful upload (M1)', () => {
    it('still counts the upload as synced and does not defer, even though the staged file could not be unlinked', async () => {
      const { drain, outbox, dedup, immich, filePath, logger } = setup();
      const dir = dirname(filePath);
      const originalMode = statSync(dir).mode;
      // Removing write permission on the parent directory makes unlink()
      // fail with EACCES — a real, non-ENOENT rm failure `force: true` does
      // not swallow.
      chmodSync(dir, 0o500);

      try {
        const tally = await drain.tick();

        expect(tally).toEqual({ uploaded: 1, deferred: 0, dropped: 0 });
        expect(dedup.has('g@g.us:A1')).toBe(true);
        expect(outbox.depth()).toBe(0);
        expect(logger.warn).toHaveBeenCalledTimes(1);
        const [payload] = logger.warn.mock.calls[0]!;
        expect(payload).toMatchObject({ messageId: 'g@g.us:A1', filePath });
        expect(logger.error).not.toHaveBeenCalled();
      } finally {
        chmodSync(dir, originalMode);
      }
    });
  });

  describe('timer loop', () => {
    afterEach(() => {
      vi.useRealTimers();
    });

    it('fires the first tick only after intervalMs elapses', async () => {
      vi.useFakeTimers();
      const { deps, outbox } = mockDeps({ intervalMs: 30_000 });

      startDrain(deps);
      expect(outbox.due).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(29_999);
      expect(outbox.due).not.toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(1);
      expect(outbox.due).toHaveBeenCalledTimes(1);
    });

    it('does not start a second tick while one is still running, even past intervalMs', async () => {
      vi.useFakeTimers();
      const { deps, outbox, immich } = mockDeps({ intervalMs: 10_000 });
      let resolveUpload: (() => void) | null = null;
      const row: OutboxRow = {
        messageId: 'g@g.us:A1',
        groupJid: 'g@g.us',
        albumName: '',
        filePath: '/mock/staged.jpg',
        fileName: 'staged.jpg',
        mimeType: 'image/jpeg',
        capturedAt: 1_700_000_000_000,
        attempts: 0,
        lastError: null,
        createdAt: 1_700_000_000_000,
        nextTryAt: 0,
      };
      outbox.due.mockReturnValueOnce([row]).mockReturnValue([]);
      // stat/openAsBlob/rm are synthetic here (no real filesystem I/O) so
      // resolving them is purely microtask-driven and plays nicely with
      // vi.advanceTimersByTimeAsync — see the vi.mock() calls above.
      vi.mocked(stat).mockResolvedValueOnce({ isFile: () => true, isDirectory: () => false, size: 100 } as never);
      vi.mocked(openAsBlob).mockResolvedValueOnce(new Blob(['bytes']) as never);
      vi.mocked(rm).mockResolvedValueOnce(undefined as never);
      immich.uploadBlob.mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveUpload = () => resolve({ assetId: 'asset-1', status: 'created' as const });
          }),
      );

      startDrain(deps);
      await vi.advanceTimersByTimeAsync(10_000);
      expect(outbox.due).toHaveBeenCalledTimes(1);
      expect(resolveUpload).not.toBeNull();

      // Far past the interval, but the first tick is still awaiting the upload.
      await vi.advanceTimersByTimeAsync(100_000);
      expect(outbox.due).toHaveBeenCalledTimes(1);

      resolveUpload!();
      await vi.advanceTimersByTimeAsync(0); // let the in-flight tick settle and reschedule

      await vi.advanceTimersByTimeAsync(9_999);
      expect(outbox.due).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(outbox.due).toHaveBeenCalledTimes(2);
    });

    it('stop() prevents further ticks', async () => {
      vi.useFakeTimers();
      const { deps, outbox } = mockDeps({ intervalMs: 10_000 });

      const drain = startDrain(deps);
      await vi.advanceTimersByTimeAsync(10_000);
      expect(outbox.due).toHaveBeenCalledTimes(1);

      await drain.stop();
      await vi.advanceTimersByTimeAsync(1_000_000);
      expect(outbox.due).toHaveBeenCalledTimes(1);
    });

    it('keeps looping and never produces an unhandled rejection when outbox.due throws (C1)', async () => {
      vi.useFakeTimers();
      const { deps, outbox, logger } = mockDeps({ intervalMs: 10_000 });
      outbox.due
        .mockImplementationOnce(() => {
          throw new Error('disk full');
        })
        .mockReturnValue([]);

      const unhandled = vi.fn();
      process.prependListener('unhandledRejection', unhandled);

      try {
        startDrain(deps);
        await vi.advanceTimersByTimeAsync(10_000);
        expect(outbox.due).toHaveBeenCalledTimes(1);
        expect(logger.error).toHaveBeenCalledTimes(1);

        // The loop must still be alive for the next tick despite the throw.
        await vi.advanceTimersByTimeAsync(10_000);
        expect(outbox.due).toHaveBeenCalledTimes(2);

        // Flush any pending unhandledRejection delivery before asserting.
        await vi.advanceTimersByTimeAsync(0);
        await Promise.resolve();
        await Promise.resolve();
        expect(unhandled).not.toHaveBeenCalled();
      } finally {
        process.off('unhandledRejection', unhandled);
      }
    });

    it('tick() rejects when called directly and outbox.due throws, so callers MUST .catch() it (M5)', async () => {
      // The internal `loop()` always wraps tick() in .catch() (see the C1
      // test above), which is why that path never produces an unhandled
      // rejection. index.ts now also calls drain.tick() directly once at
      // boot, to drain a backlog immediately instead of waiting out the
      // first DRAIN_INTERVAL_MS — bypassing loop()'s own .catch(). This
      // proves the promise tick() returns really can reject (better-sqlite3
      // calls inside tickAt are synchronous and sit outside any try/catch of
      // tickAt's own), so that direct call is only safe because it is itself
      // chained with .catch() in index.ts.
      const { deps, outbox } = mockDeps({ autoStart: false });
      outbox.due.mockImplementation(() => {
        throw new Error('disk full');
      });

      const drain = startDrain(deps);
      await expect(drain.tick()).rejects.toThrow('disk full');
    });

    it('tick() called while a tick is in flight returns the same run instead of starting a second (M3)', async () => {
      const { deps, outbox, immich } = mockDeps({ autoStart: false });
      let resolveUpload: (() => void) | null = null;
      outbox.due.mockReturnValueOnce([makeRow()]).mockReturnValue([]);
      immich.uploadBlob.mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveUpload = () => resolve({ assetId: 'asset-1', status: 'created' as const });
          }),
      );

      const drain = startDrain(deps);
      const first = drain.tick();
      const second = drain.tick();

      expect(second).toBe(first);
      expect(outbox.due).toHaveBeenCalledTimes(1);

      // The row's stat()/openAsBlob() go through the real filesystem here
      // (autoStart is off and no fake timers are involved), so give them a
      // real turn of the event loop before resolving the upload.
      await waitFor(() => resolveUpload !== null);
      resolveUpload!();
      const [firstTally, secondTally] = await Promise.all([first, second]);
      expect(firstTally).toBe(secondTally);
      expect(outbox.due).toHaveBeenCalledTimes(1);
    });
  });
});
