import type { MediaItem, UploadResult } from '../types.ts';

export interface ImmichClientOptions {
  baseUrl: string;
  apiKey: string;
  deviceId?: string;
  /** Injectable fetch for tests. */
  fetchImpl?: typeof fetch;
}

interface AlbumSummary {
  id: string;
  albumName: string;
}

/** Thin REST client for an Immich server (API-key auth via `x-api-key`). */
export class ImmichClient {
  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly deviceId: string;
  private readonly fetch: typeof fetch;
  private readonly albumCache = new Map<string, string>(); // name -> id

  constructor(opts: ImmichClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, '');
    this.apiKey = opts.apiKey;
    this.deviceId = opts.deviceId ?? 'wa-immich-sync';
    this.fetch = opts.fetchImpl ?? fetch;
  }

  private headers(extra: Record<string, string> = {}): Record<string, string> {
    return { 'x-api-key': this.apiKey, accept: 'application/json', ...extra };
  }

  /**
   * Liveness probe. Throws while Immich is unreachable or still booting, so
   * callers can hold work (e.g. the WhatsApp connect) until uploads can land.
   */
  async ping(): Promise<void> {
    const res = await this.fetch(`${this.baseUrl}/api/server/ping`, { headers: this.headers() });
    if (!res.ok) {
      throw new Error(`Immich ping failed (${res.status}): ${await safeText(res)}`);
    }
  }

  /** Upload one asset. Immich dedupes by checksum and may return status 'duplicate'. */
  async uploadAsset(item: MediaItem): Promise<UploadResult> {
    const form = new FormData();
    form.append('assetData', new Blob([new Uint8Array(item.buffer)], { type: item.mimeType }), item.fileName);
    form.append('deviceAssetId', item.messageId);
    form.append('deviceId', this.deviceId);
    form.append('fileCreatedAt', item.timestamp.toISOString());
    form.append('fileModifiedAt', item.timestamp.toISOString());
    form.append('filename', item.fileName);

    const res = await this.fetch(`${this.baseUrl}/api/assets`, {
      method: 'POST',
      headers: this.headers(),
      body: form,
    });
    if (!res.ok) {
      throw new Error(`Immich upload failed (${res.status}): ${await safeText(res)}`);
    }
    const data = (await res.json()) as { id: string; status?: string };
    return { assetId: data.id, status: (data.status as UploadResult['status']) ?? 'created' };
  }

  /** Return the id of the album named `name`, creating it if needed. Cached. */
  async ensureAlbum(name: string): Promise<string> {
    const cached = this.albumCache.get(name);
    if (cached) return cached;

    const listRes = await this.fetch(`${this.baseUrl}/api/albums`, { headers: this.headers() });
    if (listRes.ok) {
      const albums = (await listRes.json()) as AlbumSummary[];
      const found = albums.find((a) => a.albumName === name);
      if (found) {
        this.albumCache.set(name, found.id);
        return found.id;
      }
    }

    const createRes = await this.fetch(`${this.baseUrl}/api/albums`, {
      method: 'POST',
      headers: this.headers({ 'content-type': 'application/json' }),
      body: JSON.stringify({ albumName: name }),
    });
    if (!createRes.ok) {
      throw new Error(`Immich create-album failed (${createRes.status}): ${await safeText(createRes)}`);
    }
    const album = (await createRes.json()) as { id: string };
    this.albumCache.set(name, album.id);
    return album.id;
  }

  /** Add an asset to an album (idempotent on Immich's side). */
  async addToAlbum(albumId: string, assetId: string): Promise<void> {
    const res = await this.fetch(`${this.baseUrl}/api/albums/${albumId}/assets`, {
      method: 'PUT',
      headers: this.headers({ 'content-type': 'application/json' }),
      body: JSON.stringify({ ids: [assetId] }),
    });
    if (!res.ok) {
      throw new Error(`Immich add-to-album failed (${res.status}): ${await safeText(res)}`);
    }
  }
}

async function safeText(res: Response): Promise<string> {
  try {
    return await res.text();
  } catch {
    return '';
  }
}
