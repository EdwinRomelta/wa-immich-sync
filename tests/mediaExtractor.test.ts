import { describe, expect, it, vi } from 'vitest';
import { extractMedia } from '../src/wa/mediaExtractor.ts';
import type { AppConfig } from '../src/types.ts';

const config: AppConfig = {
  whitelist: [],
  mediaTypes: ['image', 'video'],
  backfill: true,
  albumMode: 'per-group',
  backfillGroupName: 'wa-immich-backfill',
};

const sock = { updateMediaMessage: vi.fn() } as never;
const fakeDownload = vi.fn(async () => Buffer.from('img-bytes')) as never;

function imageMsg() {
  return {
    key: { remoteJid: 'g@g.us', id: 'MID1' },
    messageTimestamp: 1700000000,
    message: { imageMessage: { mimetype: 'image/jpeg' } },
  } as never;
}

describe('extractMedia', () => {
  it('extracts an image into a MediaItem', async () => {
    const item = await extractMedia(sock, imageMsg(), config, 'Fam', { download: fakeDownload });
    expect(item).not.toBeNull();
    expect(item!.kind).toBe('image');
    expect(item!.messageId).toBe('g@g.us:MID1');
    expect(item!.groupName).toBe('Fam');
    expect(item!.fileName).toBe('MID1.jpg');
    expect(item!.buffer.toString()).toBe('img-bytes');
    expect(item!.timestamp.getTime()).toBe(1700000000 * 1000);
  });

  it('returns null for a text message', async () => {
    const m = { key: { remoteJid: 'g@g.us', id: 'X' }, message: { conversation: 'hi' } } as never;
    expect(await extractMedia(sock, m, config, 'Fam', { download: fakeDownload })).toBeNull();
  });

  it('respects the mediaTypes filter (video excluded)', async () => {
    const cfg: AppConfig = { ...config, mediaTypes: ['image'] };
    const vid = {
      key: { remoteJid: 'g@g.us', id: 'V' },
      message: { videoMessage: { mimetype: 'video/mp4' } },
    } as never;
    expect(await extractMedia(sock, vid, cfg, 'Fam', { download: fakeDownload })).toBeNull();
  });

  it('unwraps an ephemeral wrapper', async () => {
    const m = {
      key: { remoteJid: 'g@g.us', id: 'E' },
      message: { ephemeralMessage: { message: { imageMessage: { mimetype: 'image/png' } } } },
    } as never;
    const item = await extractMedia(sock, m, config, 'Fam', { download: fakeDownload });
    expect(item?.kind).toBe('image');
    expect(item?.mimeType).toBe('image/png');
  });

  it('returns null when message id is missing', async () => {
    const m = { key: { remoteJid: 'g@g.us' }, message: { imageMessage: {} } } as never;
    expect(await extractMedia(sock, m, config, 'Fam', { download: fakeDownload })).toBeNull();
  });
});
