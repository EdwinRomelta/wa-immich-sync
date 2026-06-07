import { describe, expect, it, vi } from 'vitest';
import { ImmichClient } from '../src/immich/client.ts';
import type { MediaItem } from '../src/types.ts';

function makeItem(): MediaItem {
  return {
    messageId: 'g@g.us:1',
    rawMessageId: '1',
    groupJid: 'g@g.us',
    groupName: 'Fam',
    kind: 'image',
    mimeType: 'image/jpeg',
    fileName: '1.jpg',
    timestamp: new Date('2024-01-01T00:00:00.000Z'),
    buffer: Buffer.from('x'),
  };
}

function jsonRes(body: unknown, ok = true, status = 200) {
  return {
    ok,
    status,
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as Response;
}

describe('ImmichClient', () => {
  it('uploadAsset posts multipart and returns id + status', async () => {
    const fetchImpl = vi.fn(async () => jsonRes({ id: 'asset-1', status: 'created' }));
    const c = new ImmichClient({ baseUrl: 'http://immich/', apiKey: 'k', fetchImpl: fetchImpl as never });
    const r = await c.uploadAsset(makeItem());

    expect(r).toEqual({ assetId: 'asset-1', status: 'created' });
    const [url, opts] = fetchImpl.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe('http://immich/api/assets');
    expect(opts.method).toBe('POST');
    expect((opts.headers as Record<string, string>)['x-api-key']).toBe('k');
    expect(opts.body).toBeInstanceOf(FormData);
    expect((opts.body as FormData).get('deviceAssetId')).toBe('g@g.us:1');
    expect((opts.body as FormData).get('deviceId')).toBe('wa-immich-sync');
  });

  it('uploadAsset throws on a non-ok response', async () => {
    const fetchImpl = vi.fn(async () => jsonRes({ error: 'bad' }, false, 500));
    const c = new ImmichClient({ baseUrl: 'http://immich', apiKey: 'k', fetchImpl: fetchImpl as never });
    await expect(c.uploadAsset(makeItem())).rejects.toThrow(/500/);
  });

  it('ensureAlbum reuses an existing album and caches the id', async () => {
    const fetchImpl = vi.fn(async () => jsonRes([{ id: 'al-1', albumName: 'Fam' }]));
    const c = new ImmichClient({ baseUrl: 'http://immich', apiKey: 'k', fetchImpl: fetchImpl as never });
    expect(await c.ensureAlbum('Fam')).toBe('al-1');
    expect(await c.ensureAlbum('Fam')).toBe('al-1'); // cached
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('ensureAlbum creates an album when none matches', async () => {
    const fetchImpl = vi.fn(async (url: string | URL | Request, opts?: RequestInit) => {
      const u = String(url);
      if (u.endsWith('/api/albums') && opts?.method !== 'POST') return jsonRes([]);
      if (u.endsWith('/api/albums') && opts?.method === 'POST') return jsonRes({ id: 'al-new' });
      return jsonRes({});
    });
    const c = new ImmichClient({ baseUrl: 'http://immich', apiKey: 'k', fetchImpl: fetchImpl as never });
    expect(await c.ensureAlbum('New')).toBe('al-new');
  });

  it('addToAlbum PUTs the asset ids', async () => {
    const fetchImpl = vi.fn(async () => jsonRes([{ id: 'asset-1', success: true }]));
    const c = new ImmichClient({ baseUrl: 'http://immich', apiKey: 'k', fetchImpl: fetchImpl as never });
    await c.addToAlbum('al-1', 'asset-1');

    const [url, opts] = fetchImpl.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe('http://immich/api/albums/al-1/assets');
    expect(opts.method).toBe('PUT');
    expect(JSON.parse(opts.body as string)).toEqual({ ids: ['asset-1'] });
  });
});
