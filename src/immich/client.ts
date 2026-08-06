import type { UploadResult } from '../types.ts';

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

/** Everything Immich needs about an asset besides its bytes. */
export interface UploadMeta {
  /** Becomes deviceAssetId — the stable per-message key. */
  messageId: string;
  fileName: string;
  mimeType: string;
  timestamp: Date;
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
   * Liveness probe. Throws while Immich is unreachable or still booting.
   *
   * Nothing in the daemon's startup path blocks on this any more — ingest
   * stages media to disk regardless of whether Immich is reachable, and
   * `drain` retries uploads with backoff until it answers, so there is no
   * gate left for this to hold open. Kept as a small, direct yes/no
   * readiness check for callers outside that flow — CLI tooling, a
   * monitoring probe, a manual sanity check against IMMICH_URL — rather than
   * as a precondition anything here waits on.
   */
  async ping(): Promise<void> {
    const res = await this.fetch(`${this.baseUrl}/api/server/ping`, { headers: this.headers() });
    if (!res.ok) {
      throw new Error(`Immich ping failed (${res.status}): ${await safeText(res)}`);
    }
  }

  /**
   * Upload from a Blob. Drain passes a file-backed Blob (`fs.openAsBlob`) so a
   * large video streams instead of being read entirely into memory.
   */
  async uploadBlob(blob: Blob, meta: UploadMeta): Promise<UploadResult> {
    // fs.openAsBlob() does not infer a type from the extension, so a file-backed
    // blob arrives untyped and would be sent as application/octet-stream. Re-type
    // it here rather than trusting every caller to remember; slice() keeps the
    // blob file-backed, so this does not read the media into memory.
    const body = blob.type === meta.mimeType ? blob : blob.slice(0, blob.size, meta.mimeType);

    const form = new FormData();
    form.append('assetData', body, meta.fileName);
    form.append('deviceAssetId', meta.messageId);
    form.append('deviceId', this.deviceId);
    form.append('fileCreatedAt', meta.timestamp.toISOString());
    form.append('fileModifiedAt', meta.timestamp.toISOString());
    form.append('filename', meta.fileName);

    const res = await this.fetch(`${this.baseUrl}/api/assets`, {
      method: 'POST',
      headers: this.headers(),
      body: form,
    });
    if (!res.ok) {
      throw new Error(`Immich upload failed (${res.status}): ${await safeText(res)}`);
    }
    const data = (await res.json()) as { id?: string; status?: string };
    // Validate before the caller acts on it. Drain deletes the staged file once
    // this resolves, so an id-less 200 would lose the media irrecoverably.
    if (typeof data?.id !== 'string' || data.id === '') {
      throw new Error(`Immich upload returned no asset id: ${JSON.stringify(data).slice(0, 200)}`);
    }
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
