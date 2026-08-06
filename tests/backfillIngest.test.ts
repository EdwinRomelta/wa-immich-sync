import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import AdmZip from 'adm-zip';
import type { WAMessage } from '@whiskeysockets/baileys';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { handleBackfillMessage } from '../src/sync/backfillIngest.ts';

const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };

function zipWith(files: Record<string, Buffer>): Buffer {
  const zip = new AdmZip();
  for (const [name, buf] of Object.entries(files)) zip.addFile(name, buf);
  return zip.toBuffer();
}

let dirs: string[] = [];
afterEach(() => {
  for (const d of dirs) rmSync(d, { recursive: true, force: true });
  dirs = [];
});

function makeDeps(overrides: Partial<Record<string, unknown>> = {}) {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const outboxDir = mkdtempSync(join(tmpdir(), 'backfill-outbox-'));
  dirs.push(outboxDir);
  return { outbox, dedup, outboxDir, logger, defaultAlbum: 'Default', ...overrides };
}

const zipMsg = (caption?: string): WAMessage =>
  ({
    key: { remoteJid: 'backfill@g.us', fromMe: false, id: 'm1' },
    message: { documentMessage: { fileName: 'export.zip', mimetype: 'application/zip', caption } },
  }) as unknown as WAMessage;

describe('handleBackfillMessage', () => {
  it('ignores non-zip messages', async () => {
    const deps = makeDeps();
    const m = { key: { remoteJid: 'backfill@g.us' }, message: { conversation: 'hi' } } as WAMessage;
    const sock = { updateMediaMessage: vi.fn(), sendMessage: vi.fn() } as never;
    expect(await handleBackfillMessage(sock, m, deps as never)).toBe(false);
    expect(deps.outbox.due(Date.now(), 10)).toHaveLength(0);
  });

  it('unzips a zip and queues its media via the outbox, using the caption as album name', async () => {
    const buffer = zipWith({ 'IMG-20240101-WA0001.jpg': Buffer.from([1, 2, 3]) });
    const deps = makeDeps({ download: vi.fn(async () => buffer) });
    const sendMessage = vi.fn(async () => {});
    const sock = { updateMediaMessage: vi.fn(), sendMessage } as never;

    const handled = await handleBackfillMessage(sock, zipMsg('My Album'), deps as never);

    expect(handled).toBe(true);
    const rows = deps.outbox.due(Date.now(), 10);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.albumName).toBe('My Album');
    expect(sendMessage).toHaveBeenCalledTimes(1);
    const calls = sendMessage.mock.calls as unknown as Array<[string, { text: string }]>;
    const reply = calls[0][1].text;
    expect(reply).toContain('My Album');
    expect(reply).toContain('queued: 1');
    // Must not claim the media is already in Immich — it is only queued.
    expect(reply).not.toContain('uploaded:');
  });

  it('falls back to the default album when there is no caption', async () => {
    const buffer = zipWith({ 'IMG-20240101-WA0002.jpg': Buffer.from([4, 5, 6]) });
    const deps = makeDeps({ download: vi.fn(async () => buffer) });
    const sock = { updateMediaMessage: vi.fn(), sendMessage: vi.fn(async () => {}) } as never;

    await handleBackfillMessage(sock, zipMsg(), deps as never);
    const rows = deps.outbox.due(Date.now(), 10);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.albumName).toBe('Default');
  });

  it('does not re-queue a file whose content is already in the outbox or synced', async () => {
    const buffer = zipWith({ 'IMG-20240101-WA0003.jpg': Buffer.from([7, 8, 9]) });
    const deps = makeDeps({ download: vi.fn(async () => buffer) });
    const sock = { updateMediaMessage: vi.fn(), sendMessage: vi.fn(async () => {}) } as never;

    await handleBackfillMessage(sock, zipMsg('First'), deps as never);
    expect(deps.outbox.due(Date.now(), 10)).toHaveLength(1);

    const sendMessage2 = vi.fn(async () => {});
    const sock2 = { updateMediaMessage: vi.fn(), sendMessage: sendMessage2 } as never;
    await handleBackfillMessage(sock2, zipMsg('Second'), deps as never);

    // Still only the one row from the first import — the second run's file
    // has identical content and must be skipped as a dedup, not re-queued.
    expect(deps.outbox.due(Date.now(), 10)).toHaveLength(1);
    const calls = sendMessage2.mock.calls as unknown as Array<[string, { text: string }]>;
    // The row from the first import is only in the outbox — `synced` is
    // provably empty here, nothing has been uploaded to Immich yet — so the
    // reply must report it as "already queued", never "already in Immich".
    expect(calls[0][1].text).toContain('already queued: 1');
    expect(calls[0][1].text).toContain('already in Immich: 0');
  });
});
