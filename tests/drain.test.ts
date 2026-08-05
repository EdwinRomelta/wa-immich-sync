import { existsSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { startDrain } from '../src/sync/drain.ts';
import type { UploadMeta } from '../src/immich/client.ts';

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

  it('drops the row and logs an error when the staged file is missing, instead of deferring it forever', async () => {
    const { drain, outbox, dedup, immich, filePath, logger } = setup();
    // Simulate a missing staged file: partial cleanup, manual deletion, or a
    // crash between unlink and row delete. Retrying this row can never
    // succeed, and deferring it would deadlock re-ingest permanently (a
    // message with an existing outbox row never reaches enqueue again).
    const { rmSync } = await import('node:fs');
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

  it('still defers a normal upload failure rather than dropping it', async () => {
    const { drain, outbox, immich, logger } = setup();
    immich.uploadBlob.mockRejectedValueOnce(new Error('ECONNREFUSED'));

    const tally = await drain.tick();

    expect(tally.dropped).toBe(0);
    expect(tally.deferred).toBe(1);
    expect(outbox.depth()).toBe(1);
    expect(logger.error).not.toHaveBeenCalled();
  });
});
