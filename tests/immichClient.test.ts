import { describe, expect, it, vi } from 'vitest';
import { ImmichClient } from '../src/immich/client.ts';
import type { UploadMeta } from '../src/immich/client.ts';

function makeMeta(overrides: Partial<UploadMeta> = {}): UploadMeta {
  return {
    messageId: 'g@g.us:1',
    fileName: '1.jpg',
    mimeType: 'image/jpeg',
    timestamp: new Date('2024-01-01T00:00:00.000Z'),
    ...overrides,
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
  it('uploadBlob posts a multipart form with every expected field and returns id + status', async () => {
    let captured: FormData | null = null;
    const fetchImpl = vi.fn(async (_url: string, init: RequestInit) => {
      captured = init.body as FormData;
      return jsonRes({ id: 'asset-1', status: 'created' });
    });
    const c = new ImmichClient({ baseUrl: 'http://immich/', apiKey: 'k', fetchImpl: fetchImpl as never });
    const meta = makeMeta({ messageId: 'g@g.us:1', fileName: '1.jpg', mimeType: 'image/jpeg' });

    const r = await c.uploadBlob(new Blob([new Uint8Array([1, 2, 3])], { type: meta.mimeType }), meta);

    expect(r).toEqual({ assetId: 'asset-1', status: 'created' });
    const [url, opts] = fetchImpl.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe('http://immich/api/assets');
    expect(opts.method).toBe('POST');
    expect((opts.headers as Record<string, string>)['x-api-key']).toBe('k');
    expect(opts.body).toBeInstanceOf(FormData);

    // Assert against a literal expected form rather than diffing two calls
    // against each other — this is what the server actually receives.
    const form = captured!;
    expect(form.get('deviceAssetId')).toBe('g@g.us:1');
    expect(form.get('deviceId')).toBe('wa-immich-sync');
    expect(form.get('filename')).toBe('1.jpg');
    expect(form.get('fileCreatedAt')).toBe('2024-01-01T00:00:00.000Z');
    expect(form.get('fileModifiedAt')).toBe('2024-01-01T00:00:00.000Z');
    const assetData = form.get('assetData') as Blob;
    expect(assetData).toBeInstanceOf(Blob);
    expect(assetData.type).toBe('image/jpeg');
    expect(assetData.size).toBe(3);
  });

  it('re-types an untyped blob (as fs.openAsBlob() produces) to the asset mime type without reading it into memory', async () => {
    let captured: FormData | null = null;
    const fetchImpl = (async (_url: string, init: RequestInit) => {
      captured = init.body as FormData;
      return new Response(JSON.stringify({ id: 'asset-7', status: 'created' }), { status: 201 });
    }) as unknown as typeof fetch;

    const client = new ImmichClient({ baseUrl: 'http://immich', apiKey: 'k', fetchImpl });
    const meta = makeMeta({ messageId: 'g@g.us:A1', fileName: 'IMG-1.jpg', mimeType: 'image/jpeg' });

    // Untyped — fs.openAsBlob() never infers a type from the file extension.
    const result = await client.uploadBlob(new Blob([new Uint8Array([1, 2, 3])]), meta);

    expect(result).toEqual({ assetId: 'asset-7', status: 'created' });
    expect(captured!.get('deviceAssetId')).toBe('g@g.us:A1');
    expect(captured!.get('filename')).toBe('IMG-1.jpg');
    expect(captured!.get('fileCreatedAt')).toBe('2024-01-01T00:00:00.000Z');
    const assetData = captured!.get('assetData') as Blob;
    expect(assetData.type).toBe('image/jpeg');
  });

  it('uploadBlob throws on a non-ok response', async () => {
    const fetchImpl = vi.fn(async () => jsonRes({ error: 'bad' }, false, 500));
    const c = new ImmichClient({ baseUrl: 'http://immich', apiKey: 'k', fetchImpl: fetchImpl as never });
    await expect(c.uploadBlob(new Blob([new Uint8Array([1])]), makeMeta())).rejects.toThrow(/500/);
  });

  it('rejects a 200 that carries no asset id instead of returning undefined', async () => {
    const fetchImpl = vi.fn(async () => jsonRes({ status: 'created' }));
    const c = new ImmichClient({
      baseUrl: 'http://immich',
      apiKey: 'k',
      fetchImpl: fetchImpl as never,
    });
    // Drain deletes the staged file once this resolves, so an id-less success
    // must throw rather than record an undefined asset id.
    await expect(
      c.uploadBlob(new Blob([new Uint8Array([1])]), makeMeta()),
    ).rejects.toThrow(/no asset id/);
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

  it('ping GETs /api/server/ping and resolves on ok', async () => {
    const fetchImpl = vi.fn(async () => jsonRes({ res: 'pong' }));
    const c = new ImmichClient({ baseUrl: 'http://immich', apiKey: 'k', fetchImpl: fetchImpl as never });
    await expect(c.ping()).resolves.toBeUndefined();
    const [url] = fetchImpl.mock.calls[0] as unknown as [string];
    expect(url).toBe('http://immich/api/server/ping');
  });

  it('ping throws when the server is unreachable', async () => {
    const fetchImpl = vi.fn(async () => {
      throw new TypeError('fetch failed: connect ECONNREFUSED');
    });
    const c = new ImmichClient({ baseUrl: 'http://immich', apiKey: 'k', fetchImpl: fetchImpl as never });
    await expect(c.ping()).rejects.toThrow(/ECONNREFUSED/);
  });

  it('ping throws on a non-ok response', async () => {
    const fetchImpl = vi.fn(async () => jsonRes({ error: 'boot' }, false, 503));
    const c = new ImmichClient({ baseUrl: 'http://immich', apiKey: 'k', fetchImpl: fetchImpl as never });
    await expect(c.ping()).rejects.toThrow(/503/);
  });
});
