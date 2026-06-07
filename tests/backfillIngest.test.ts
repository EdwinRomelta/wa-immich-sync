import { describe, expect, it, vi } from 'vitest';
import AdmZip from 'adm-zip';
import type { WAMessage } from '@whiskeysockets/baileys';
import { handleBackfillMessage } from '../src/sync/backfillIngest.ts';

const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };

function zipWith(files: Record<string, Buffer>): Buffer {
  const zip = new AdmZip();
  for (const [name, buf] of Object.entries(files)) zip.addFile(name, buf);
  return zip.toBuffer();
}

function makeDeps(overrides: Partial<Record<string, unknown>> = {}) {
  const immich = {
    uploadAsset: vi.fn(async () => ({ assetId: 'a1', status: 'created' as const })),
    ensureAlbum: vi.fn(async () => 'album1'),
    addToAlbum: vi.fn(async () => {}),
  };
  const dedup = { has: vi.fn(() => false), markDone: vi.fn() };
  return { immich, dedup, logger, defaultAlbum: 'Default', ...overrides };
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
    expect(deps.immich.uploadAsset).not.toHaveBeenCalled();
  });

  it('unzips a zip and imports its media, using the caption as album name', async () => {
    const buffer = zipWith({ 'IMG-20240101-WA0001.jpg': Buffer.from([1, 2, 3]) });
    const deps = makeDeps({ download: vi.fn(async () => buffer) });
    const sendMessage = vi.fn(async () => {});
    const sock = { updateMediaMessage: vi.fn(), sendMessage } as never;

    const handled = await handleBackfillMessage(sock, zipMsg('My Album'), deps as never);

    expect(handled).toBe(true);
    expect(deps.immich.uploadAsset).toHaveBeenCalledTimes(1);
    expect(deps.immich.ensureAlbum).toHaveBeenCalledWith('My Album');
    expect(sendMessage).toHaveBeenCalledTimes(1);
    const calls = sendMessage.mock.calls as unknown as Array<[string, { text: string }]>;
    const reply = calls[0][1].text;
    expect(reply).toContain('My Album');
    expect(reply).toContain('uploaded: 1');
  });

  it('falls back to the default album when there is no caption', async () => {
    const buffer = zipWith({ 'IMG-20240101-WA0002.jpg': Buffer.from([4, 5, 6]) });
    const deps = makeDeps({ download: vi.fn(async () => buffer) });
    const sock = { updateMediaMessage: vi.fn(), sendMessage: vi.fn(async () => {}) } as never;

    await handleBackfillMessage(sock, zipMsg(), deps as never);
    expect(deps.immich.ensureAlbum).toHaveBeenCalledWith('Default');
  });
});
