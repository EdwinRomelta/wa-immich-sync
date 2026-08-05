import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import type { WAMessage } from '@whiskeysockets/baileys';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { createIngest } from '../src/sync/ingest.ts';
import type { AppConfig, MediaItem } from '../src/types.ts';

const GROUP = { jid: 'g@g.us', name: 'Daycare' };

const config: AppConfig = {
  whitelist: ['Daycare'],
  mediaTypes: ['image', 'video'],
  backfill: false,
  albumMode: 'per-group',
  backfillGroupName: 'wa-immich-backfill',
};

const msg = (id = 'A1'): WAMessage =>
  ({ key: { remoteJid: 'g@g.us', id }, message: { imageMessage: {} } }) as WAMessage;

const item = (id = 'A1'): MediaItem => ({
  messageId: `g@g.us:${id}`,
  rawMessageId: id,
  groupJid: 'g@g.us',
  groupName: 'Daycare',
  kind: 'image',
  mimeType: 'image/jpeg',
  fileName: `IMG-${id}.jpg`,
  timestamp: new Date('2026-07-28T08:28:06.000Z'),
  buffer: Buffer.from('photo-bytes'),
});

function setup(overrides: Partial<AppConfig> = {}) {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const dir = mkdtempSync(join(tmpdir(), 'ingest-test-'));
  const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
  const sock = { sendMessage: vi.fn(async () => undefined), updateMediaMessage: vi.fn() };
  const extract = vi.fn(async () => item());

  const ing = createIngest({
    config: { ...config, ...overrides },
    dedup,
    outbox,
    outboxDir: dir,
    logger,
    extract: extract as never,
  });
  ing.setGroups([GROUP]);
  return { ing, outbox, dedup, dir, logger, sock, extract };
}

describe('ingest', () => {
  it('skips messages from non-whitelisted groups without extracting', async () => {
    const { ing, extract, sock } = setup();
    const foreign = { key: { remoteJid: 'other@g.us', id: 'X' } } as WAMessage;
    expect(await ing.ingest(sock as never, foreign)).toBe('skipped-not-whitelisted');
    expect(extract).not.toHaveBeenCalled();
  });

  it('skips an already-synced message before downloading', async () => {
    const { ing, dedup, extract, sock } = setup();
    dedup.markDone('g@g.us:A1', 'g@g.us', 'asset-1', 'created');
    expect(await ing.ingest(sock as never, msg())).toBe('skipped-dedup');
    expect(extract).not.toHaveBeenCalled();
  });

  it('skips a message already sitting in the outbox', async () => {
    const { ing, outbox, extract, sock } = setup();
    outbox.enqueue({
      messageId: 'g@g.us:A1',
      groupJid: 'g@g.us',
      albumName: 'Daycare',
      filePath: '/tmp/x',
      fileName: 'IMG-A1.jpg',
      mimeType: 'image/jpeg',
      capturedAt: 1,
      createdAt: 1,
    });
    expect(await ing.ingest(sock as never, msg())).toBe('skipped-dedup');
    expect(extract).not.toHaveBeenCalled();
  });

  it('skips when the message carries no media', async () => {
    const { ing, sock, extract } = setup();
    extract.mockResolvedValueOnce(null as never);
    expect(await ing.ingest(sock as never, msg())).toBe('skipped-no-media');
  });

  it('writes the bytes to disk and queues a row', async () => {
    const { ing, outbox, sock } = setup();
    expect(await ing.ingest(sock as never, msg())).toBe('queued');

    const rows = outbox.due(Date.now(), 10);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.messageId).toBe('g@g.us:A1');
    expect(rows[0]!.albumName).toBe('Daycare');
    expect(rows[0]!.capturedAt).toBe(new Date('2026-07-28T08:28:06.000Z').getTime());
    expect(readFileSync(rows[0]!.filePath).toString()).toBe('photo-bytes');
  });

  it('records an empty album name when albumMode is "none"', async () => {
    const { ing, outbox, sock } = setup({ albumMode: 'none' });
    await ing.ingest(sock as never, msg());
    expect(outbox.due(Date.now(), 10)[0]!.albumName).toBe('');
  });

  it('uses the single album name when albumMode is "single"', async () => {
    const { ing, outbox, sock } = setup({ albumMode: 'single', singleAlbumName: 'WhatsApp' });
    await ing.ingest(sock as never, msg());
    expect(outbox.due(Date.now(), 10)[0]!.albumName).toBe('WhatsApp');
  });

  it('reacts with the configured emoji once the media is safely captured', async () => {
    const { ing, sock } = setup({ reactionEmoji: 'X' });
    await ing.ingest(sock as never, msg());
    expect(sock.sendMessage).toHaveBeenCalledWith('g@g.us', {
      react: { text: 'X', key: { remoteJid: 'g@g.us', id: 'A1' } },
    });
  });

  it('does not react when no reactionEmoji is configured', async () => {
    const { ing, sock } = setup();
    await ing.ingest(sock as never, msg());
    expect(sock.sendMessage).not.toHaveBeenCalled();
  });

  it('still reports queued when the reaction fails', async () => {
    const { ing, sock } = setup({ reactionEmoji: 'X' });
    sock.sendMessage.mockRejectedValueOnce(new Error('rate limited'));
    expect(await ing.ingest(sock as never, msg())).toBe('queued');
  });

  it('reports error and queues nothing when staging fails', async () => {
    const db = openDb(':memory:');
    const dedup = new DedupStore(db);
    const outbox = new OutboxStore(db);
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const sock = { sendMessage: vi.fn(), updateMediaMessage: vi.fn() };
    const ing = createIngest({
      config,
      dedup,
      outbox,
      // A path under a regular file can never be created as a directory.
      // NOTE: __filename is not defined in TypeScript ESM; import.meta.filename
      // (Node 22+) is the ESM equivalent and points at this test file itself,
      // which is guaranteed to be a regular file.
      outboxDir: join(import.meta.filename, 'not-a-dir'),
      logger,
      extract: (async () => item()) as never,
    });
    ing.setGroups([GROUP]);

    expect(await ing.ingest(sock as never, msg())).toBe('error');
    expect(outbox.depth()).toBe(0);
  });
});
