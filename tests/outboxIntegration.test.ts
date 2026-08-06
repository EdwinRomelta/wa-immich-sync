import { existsSync, mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import type { WAMessage } from '@whiskeysockets/baileys';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { createIngest } from '../src/sync/ingest.ts';
import { startDrain } from '../src/sync/drain.ts';
import { ensureOutboxDirWritable, sweepOrphans } from '../src/sync/staging.ts';
import type { AppConfig, MediaItem } from '../src/types.ts';

const config: AppConfig = {
  whitelist: ['Daycare'],
  mediaTypes: ['image', 'video'],
  backfill: false,
  albumMode: 'per-group',
  backfillGroupName: 'wa-immich-backfill',
};

const msg = () =>
  ({ key: { remoteJid: 'g@g.us', id: 'A1' }, message: { imageMessage: {} } }) as WAMessage;

const item = (): MediaItem => ({
  messageId: 'g@g.us:A1',
  rawMessageId: 'A1',
  groupJid: 'g@g.us',
  groupName: 'Daycare',
  kind: 'image',
  mimeType: 'image/jpeg',
  fileName: 'IMG-A1.jpg',
  timestamp: new Date('2026-07-28T08:28:06.000Z'),
  buffer: Buffer.from('irreplaceable-photo'),
});

/**
 * All six settings startDrain's tests elsewhere exercise individually
 * (intervalMs, batchSize, baseBackoffMs, maxBackoffMs, dropAfterAttempts,
 * maxDropsPerTick) are passed explicitly here so this regression suite does
 * not silently drift onto defaults if they ever change.
 */
const drainSettings = {
  intervalMs: 30_000,
  batchSize: 10,
  baseBackoffMs: 1_000,
  maxBackoffMs: 60_000,
  dropAfterAttempts: 3,
  maxDropsPerTick: 5,
};

describe('regression: media captured while Immich is unreachable must survive and land later', () => {
  it('keeps the bytes and the queue row durable through an outage, then uploads once Immich recovers', async () => {
    const db = openDb(':memory:');
    // DedupStore must be constructed before OutboxStore on a shared
    // connection: OutboxStore.markSyncedAndRemove writes to the `synced`
    // table that DedupStore's constructor creates.
    const dedup = new DedupStore(db);
    const outbox = new OutboxStore(db);
    const outboxDir = mkdtempSync(join(tmpdir(), 'integration-'));
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const sock = { sendMessage: vi.fn(), updateMediaMessage: vi.fn() };

    const ing = createIngest({
      config,
      dedup,
      outbox,
      outboxDir,
      logger,
      extract: (async () => item()) as never,
    });
    ing.setGroups([{ jid: 'g@g.us', name: 'Daycare' }]);

    // The photo arrives while Immich is down. Ingest never talks to Immich,
    // so this must succeed regardless.
    expect(await ing.ingest(sock as never, msg())).toBe('queued');

    // Real bytes durable on real disk, not a mock assertion.
    const row = outbox.due(Date.now(), 10)[0]!;
    expect(existsSync(row.filePath)).toBe(true);
    expect(readFileSync(row.filePath).toString()).toBe('irreplaceable-photo');
    expect(outbox.depth()).toBe(1);
    expect(dedup.has('g@g.us:A1')).toBe(false);

    let immichUp = false;
    const immich = {
      uploadBlob: vi.fn(async () => {
        if (!immichUp) throw new Error('ECONNREFUSED');
        return { assetId: 'asset-1', status: 'created' as const };
      }),
      ensureAlbum: vi.fn(async () => 'album-1'),
      addToAlbum: vi.fn(async () => undefined),
    };
    const drain = startDrain({ immich, outbox, logger, ...drainSettings, autoStart: false });

    // Repeated failed attempts across a whole outage window — nothing is
    // dropped, the row and its bytes stay exactly where they were.
    for (const at of [1_000, 100_000, 200_000]) {
      expect(await drain.tickAt(at)).toEqual({ uploaded: 0, deferred: 1, dropped: 0 });
    }
    expect(outbox.depth()).toBe(1);
    expect(dedup.has('g@g.us:A1')).toBe(false);
    expect(existsSync(row.filePath)).toBe(true);
    expect(readFileSync(row.filePath).toString()).toBe('irreplaceable-photo');

    // Immich comes back.
    immichUp = true;
    expect(await drain.tickAt(10_000_000)).toEqual({ uploaded: 1, deferred: 0, dropped: 0 });

    expect(dedup.has('g@g.us:A1')).toBe(true);
    expect(outbox.depth()).toBe(0);
    expect(existsSync(row.filePath)).toBe(false);

    await drain.stop();
  });
});

describe('regression: a daemon restart must not lose queued work', () => {
  it('survives closing and reopening the sqlite connection with the queue row and staged bytes intact, then drains', async () => {
    const root = mktemp();
    const dbPath = join(root, 'sync.db');
    const stagingDir = join(root, 'outbox');
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const sock = { sendMessage: vi.fn(), updateMediaMessage: vi.fn() };

    // --- Before the outage: the daemon is running, captures a message. ---
    await ensureOutboxDirWritable(stagingDir, []);
    // A real on-disk sqlite file, not ':memory:' — :memory: is destroyed the
    // instant the connection closes and cannot demonstrate restart survival.
    const db1 = openDb(dbPath);
    const dedup1 = new DedupStore(db1);
    const outbox1 = new OutboxStore(db1);
    const ingest1 = createIngest({
      config,
      dedup: dedup1,
      outbox: outbox1,
      outboxDir: stagingDir,
      logger,
      extract: (async () => item()) as never,
    });
    ingest1.setGroups([{ jid: 'g@g.us', name: 'Daycare' }]);

    expect(await ingest1.ingest(sock as never, msg())).toBe('queued');
    const stagedPath = outbox1.due(Date.now(), 10)[0]!.filePath;
    expect(existsSync(stagedPath)).toBe(true);
    expect(outbox1.depth()).toBe(1);

    // --- The process dies. Six days pass. dedup1.close() closes the shared
    // connection (DedupStore.close() delegates to the underlying db). ---
    dedup1.close();

    // --- Restart: a fresh process reopens the same file and staging dir,
    // rebuilding every store from scratch, exactly as src/index.ts does. ---
    const db2 = openDb(dbPath);
    const dedup2 = new DedupStore(db2);
    const outbox2 = new OutboxStore(db2);

    // The startup orphan sweep runs on every boot; it must recognise the
    // still-queued row's file and leave it alone.
    const swept = await sweepOrphans(stagingDir, outbox2.allFilePaths());
    expect(swept).toBe(0);

    expect(outbox2.depth()).toBe(1);
    expect(existsSync(stagedPath)).toBe(true);
    expect(readFileSync(stagedPath).toString()).toBe('irreplaceable-photo');
    expect(dedup2.has('g@g.us:A1')).toBe(false);

    // --- Drain resumes against the rebuilt stores and finishes the job. ---
    const immich = {
      uploadBlob: vi.fn(async () => ({ assetId: 'asset-1', status: 'created' as const })),
      ensureAlbum: vi.fn(async () => 'album-1'),
      addToAlbum: vi.fn(async () => undefined),
    };
    const drain = startDrain({ immich, outbox: outbox2, logger, ...drainSettings, autoStart: false });

    const tally = await drain.tickAt(Date.now());

    expect(tally).toEqual({ uploaded: 1, deferred: 0, dropped: 0 });
    expect(dedup2.has('g@g.us:A1')).toBe(true);
    expect(outbox2.depth()).toBe(0);
    expect(existsSync(stagedPath)).toBe(false);

    await drain.stop();
    dedup2.close();
  });
});

function mktemp(): string {
  return mkdtempSync(join(tmpdir(), 'restart-'));
}
