import { createHash } from 'node:crypto';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { rm } from 'node:fs/promises';
import { extname, join, relative } from 'node:path';
import type { DedupStore } from './dedupStore.ts';
import type { OutboxStore } from './outboxStore.ts';
import { stageFile } from './staging.ts';
import type { MediaKind } from '../types.ts';

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
  queued: number;
  skippedDedup: number;
  skippedType: number;
  errors: number;
}

export interface ImportDeps {
  outbox: Pick<OutboxStore, 'has' | 'enqueue'>;
  dedup: Pick<DedupStore, 'has'>;
  /** Directory staged media is written to. */
  outboxDir: string;
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
 * Guard against `dateForFile` (or a future regression in it) returning an
 * Invalid Date. `OutboxStore.enqueue` writes `captured_at` as `NOT NULL`;
 * `Invalid Date.getTime()` is `NaN`, better-sqlite3 binds `NaN` as `NULL`,
 * and the insert throws `NOT NULL constraint failed: outbox.captured_at`.
 * The live ingest path takes its timestamp from WhatsApp (already guarded);
 * this path parses it out of export filenames — a much weaker source — so
 * fall back to a known-valid timestamp rather than letting the insert fail.
 */
export function safeCapturedAtMs(date: Date, fallbackMs: number): number {
  return Number.isNaN(date.getTime()) ? fallbackMs : date.getTime();
}

/**
 * Walk a folder and queue every supported image/video for upload via the
 * outbox, adding each to `albumName` (unless empty) and recording dedup keys
 * so re-runs skip work. Deliberately knows nothing about Immich — matches
 * the live ingest path in ./ingest.ts, which the outbox exists to replace
 * the direct-upload behaviour of.
 */
export async function importFolder(folder: string, deps: ImportDeps): Promise<ImportStats> {
  const stats: ImportStats = {
    scanned: 0,
    queued: 0,
    skippedDedup: 0,
    skippedType: 0,
    errors: 0,
  };

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
    if (deps.outbox.has(messageId)) {
      stats.skippedDedup += 1;
      continue;
    }

    try {
      const capturedAt = safeCapturedAtMs(dateForFile(path), statSync(path).mtimeMs);

      // Bytes to disk FIRST, row second — same ordering as the live ingest
      // path in ./ingest.ts. A crash between the two leaves an orphan file
      // (swept at startup), never a row without its media.
      const filePath = await stageFile(deps.outboxDir, messageId, buffer);
      try {
        deps.outbox.enqueue({
          messageId,
          groupJid: 'import',
          albumName: deps.albumName,
          filePath,
          fileName: path.split('/').pop() ?? rel,
          mimeType: info.mime,
          capturedAt,
          createdAt: Date.now(),
        });
      } catch (err) {
        // The row never landed, so nothing will ever reference these bytes.
        // Drop them now rather than leaving an orphan for the startup sweep.
        await rm(filePath, { force: true }).catch(() => {});
        throw err;
      }

      stats.queued += 1;
      if (stats.queued % 25 === 0) deps.logger.info(stats, 'import progress');
    } catch (err) {
      stats.errors += 1;
      deps.logger.warn({ path: rel, err: (err as Error).message }, 'import failed for file');
    }
  }

  return stats;
}
