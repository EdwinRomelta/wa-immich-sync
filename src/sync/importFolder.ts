import { createHash } from 'node:crypto';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { extname, join, relative } from 'node:path';
import type { ImmichClient } from '../immich/client.ts';
import type { DedupStore } from './dedupStore.ts';
import type { MediaItem, MediaKind } from '../types.ts';

export const MEDIA_MIME: Record<string, { kind: MediaKind; mime: string }> = {
  '.jpg': { kind: 'image', mime: 'image/jpeg' },
  '.jpeg': { kind: 'image', mime: 'image/jpeg' },
  '.png': { kind: 'image', mime: 'image/png' },
  '.webp': { kind: 'image', mime: 'image/webp' },
  '.gif': { kind: 'image', mime: 'image/gif' },
  '.heic': { kind: 'image', mime: 'image/heic' },
  '.mp4': { kind: 'video', mime: 'video/mp4' },
  '.3gp': { kind: 'video', mime: 'video/3gpp' },
  '.mov': { kind: 'video', mime: 'video/quicktime' },
  '.mkv': { kind: 'video', mime: 'video/x-matroska' },
  '.webm': { kind: 'video', mime: 'video/webm' },
  '.avi': { kind: 'video', mime: 'video/x-msvideo' },
};

export interface ImportStats {
  scanned: number;
  uploaded: number;
  duplicate: number;
  skippedDedup: number;
  skippedType: number;
  errors: number;
}

export interface ImportDeps {
  immich: Pick<ImmichClient, 'uploadAsset' | 'ensureAlbum' | 'addToAlbum'>;
  dedup: Pick<DedupStore, 'has' | 'markDone'>;
  albumName: string;
  logger: { info: (...a: unknown[]) => void; warn: (...a: unknown[]) => void };
}

function* walk(dir: string): Generator<string> {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) yield* walk(full);
    else if (entry.isFile()) yield full;
  }
}

/** Parse a creation date from common WhatsApp export filenames, else use mtime. */
export function dateForFile(path: string): Date {
  const name = path.split('/').pop() ?? '';
  // IMG-20240617-WA0001.jpg / VID-20240617-WA0001.mp4
  let m = name.match(/(?:IMG|VID)-(\d{4})(\d{2})(\d{2})-/);
  if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
  // WhatsApp Image 2024-06-17 at 10.30.00.jpeg
  m = name.match(/(\d{4})-(\d{2})-(\d{2}) at (\d{2})\.(\d{2})\.(\d{2})/);
  if (m) {
    return new Date(
      Number(m[1]), Number(m[2]) - 1, Number(m[3]),
      Number(m[4]), Number(m[5]), Number(m[6]),
    );
  }
  return statSync(path).mtime;
}

/**
 * Walk a folder and upload every supported image/video to Immich, adding each
 * to `albumName` (unless empty) and recording dedup keys so re-runs skip work.
 */
export async function importFolder(folder: string, deps: ImportDeps): Promise<ImportStats> {
  const stats: ImportStats = {
    scanned: 0,
    uploaded: 0,
    duplicate: 0,
    skippedDedup: 0,
    skippedType: 0,
    errors: 0,
  };
  let albumId: string | null = null;

  for (const path of walk(folder)) {
    stats.scanned += 1;
    const info = MEDIA_MIME[extname(path).toLowerCase()];
    if (!info) {
      stats.skippedType += 1;
      continue;
    }

    const rel = relative(folder, path);

    // Dedup by file CONTENT (sha1), matching Immich's checksum. This skips the
    // same photo even when a different person re-exported it under a different
    // filename. (A re-compressed/altered copy is genuinely different bytes and
    // cannot be deduped by either this store or Immich.)
    let buffer: Buffer;
    try {
      buffer = readFileSync(path);
    } catch (err) {
      stats.errors += 1;
      deps.logger.warn({ path: rel, err: (err as Error).message }, 'import: read failed');
      continue;
    }
    const messageId = `sha1:${createHash('sha1').update(buffer).digest('hex')}`;
    if (deps.dedup.has(messageId)) {
      stats.skippedDedup += 1;
      continue;
    }

    try {
      const item: MediaItem = {
        messageId,
        rawMessageId: rel,
        groupJid: 'import',
        groupName: deps.albumName,
        kind: info.kind,
        mimeType: info.mime,
        fileName: path.split('/').pop() ?? rel,
        timestamp: dateForFile(path),
        buffer,
      };

      const uploaded = await deps.immich.uploadAsset(item);
      if (deps.albumName) {
        if (!albumId) albumId = await deps.immich.ensureAlbum(deps.albumName);
        await deps.immich.addToAlbum(albumId, uploaded.assetId);
      }
      deps.dedup.markDone(messageId, 'import', uploaded.assetId, uploaded.status);

      if (uploaded.status === 'duplicate') stats.duplicate += 1;
      else stats.uploaded += 1;
      if ((stats.uploaded + stats.duplicate) % 25 === 0) deps.logger.info(stats, 'import progress');
    } catch (err) {
      stats.errors += 1;
      deps.logger.warn({ path: rel, err: (err as Error).message }, 'import failed for file');
    }
  }

  return stats;
}
