import { createHash } from 'node:crypto';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { rm } from 'node:fs/promises';
import { extname, join, relative } from 'node:path';
import type { DedupStore } from './dedupStore.ts';
import type { OutboxStore } from './outboxStore.ts';
import { stageFile } from './staging.ts';

// `kind` was dropped from here: it only ever fed MediaItem.kind, and nothing
// in the outbox-based import path (this file) constructs a MediaItem —
// importFolder only ever reads `.mime` below.
export const MEDIA_MIME: Record<string, { mime: string }> = {
  '.jpg': { mime: 'image/jpeg' },
  '.jpeg': { mime: 'image/jpeg' },
  '.png': { mime: 'image/png' },
  '.webp': { mime: 'image/webp' },
  '.gif': { mime: 'image/gif' },
  '.heic': { mime: 'image/heic' },
  '.mp4': { mime: 'video/mp4' },
  '.3gp': { mime: 'video/3gpp' },
  '.mov': { mime: 'video/quicktime' },
  '.mkv': { mime: 'video/x-matroska' },
  '.webm': { mime: 'video/webm' },
  '.avi': { mime: 'video/x-msvideo' },
};

export interface ImportStats {
  scanned: number;
  queued: number;
  /** Already in Immich (dedup.has) — genuinely synced, safe to call "done". */
  skippedSynced: number;
  /** Already staged in the outbox (outbox.has) — queued but not yet uploaded. */
  skippedQueued: number;
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
 *
 * `dateForFile`'s two regexes only ever feed digit-bounded values into
 * `new Date(...)` (which does not throw or return Invalid Date for
 * out-of-range components — it wraps), and its own fallback is
 * `statSync(path).mtime`, which is valid for any file that exists. So today
 * this guard is unreachable — it is a regression backstop for future edits
 * to `dateForFile`, not a live defence, and must not be described as one.
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
    skippedSynced: 0,
    skippedQueued: 0,
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
    // Report these separately: dedup.has means the asset is genuinely already
    // in Immich, but outbox.has only means it is staged and awaiting upload —
    // conflating the two under one counter misreports hundreds of not-yet-
    // uploaded photos as "already-synced" whenever the drain is behind.
    if (deps.dedup.has(messageId)) {
      stats.skippedSynced += 1;
      continue;
    }
    if (deps.outbox.has(messageId)) {
      stats.skippedQueued += 1;
      continue;
    }

    try {
      // Date.now(), not another statSync(path): the fallback is unreachable
      // today (see safeCapturedAtMs's doc comment) and mtimeMs was also
      // circular whenever it WAS reachable — Stats.mtime is derived from
      // mtimeMs, so an mtimeMs that made dateForFile's own NaN fallback fire
      // would be NaN here too. statSync(path) also ran on every file
      // regardless of whether dateForFile succeeded, which is a needless
      // syscall per file on a large export.
      const capturedAt = safeCapturedAtMs(dateForFile(path), Date.now());

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
