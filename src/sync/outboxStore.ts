import type Database from 'better-sqlite3';

/** A unit of work: media already on disk, not yet accepted by Immich. */
export interface OutboxRow {
  messageId: string;
  groupJid: string;
  /** Immich album to file it under; empty string means "no album". */
  albumName: string;
  filePath: string;
  fileName: string;
  mimeType: string;
  /** WhatsApp send time, epoch ms. */
  capturedAt: number;
  attempts: number;
  lastError: string | null;
  createdAt: number;
  nextTryAt: number;
}

export type NewOutboxItem = Omit<OutboxRow, 'attempts' | 'lastError' | 'nextTryAt'>;

interface RawRow {
  message_id: string;
  group_jid: string;
  album_name: string;
  file_path: string;
  file_name: string;
  mime_type: string;
  captured_at: number;
  attempts: number;
  last_error: string | null;
  created_at: number;
  next_try_at: number;
}

function toRow(r: RawRow): OutboxRow {
  return {
    messageId: r.message_id,
    groupJid: r.group_jid,
    albumName: r.album_name,
    filePath: r.file_path,
    fileName: r.file_name,
    mimeType: r.mime_type,
    capturedAt: r.captured_at,
    attempts: r.attempts,
    lastError: r.last_error,
    createdAt: r.created_at,
    nextTryAt: r.next_try_at,
  };
}

/**
 * Durable queue of media captured from WhatsApp but not yet stored in Immich.
 * A message is recorded here the moment its bytes are safe on disk, and only
 * moves to `synced` once Immich has accepted it. Nothing is ever dropped
 * because an upload failed.
 */
export class OutboxStore {
  constructor(private readonly db: Database.Database) {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS outbox (
        message_id  TEXT PRIMARY KEY,
        group_jid   TEXT NOT NULL,
        album_name  TEXT NOT NULL,
        file_path   TEXT NOT NULL,
        file_name   TEXT NOT NULL,
        mime_type   TEXT NOT NULL,
        captured_at INTEGER NOT NULL,
        attempts    INTEGER NOT NULL DEFAULT 0,
        last_error  TEXT,
        created_at  INTEGER NOT NULL,
        next_try_at INTEGER NOT NULL DEFAULT 0
      );
      CREATE INDEX IF NOT EXISTS idx_outbox_ready ON outbox(next_try_at, created_at);
    `);
  }

  enqueue(item: NewOutboxItem): void {
    this.db
      .prepare(
        // ON CONFLICT rather than OR IGNORE: re-enqueuing a known message is a
        // no-op, but a NOT NULL violation must still throw. OR IGNORE would
        // swallow it and silently drop media already staged on disk.
        `INSERT INTO outbox
           (message_id, group_jid, album_name, file_path, file_name, mime_type,
            captured_at, attempts, last_error, created_at, next_try_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, 0)
         ON CONFLICT(message_id) DO NOTHING`,
      )
      .run(
        item.messageId,
        item.groupJid,
        item.albumName,
        item.filePath,
        item.fileName,
        item.mimeType,
        item.capturedAt,
        item.createdAt,
      );
  }

  has(messageId: string): boolean {
    return this.db.prepare('SELECT 1 FROM outbox WHERE message_id = ?').get(messageId) !== undefined;
  }

  due(now: number, limit: number): OutboxRow[] {
    // SQLite reads a negative LIMIT as unbounded; clamp so a misconfigured
    // batch size cannot pull the entire queue into memory.
    const batch = Math.max(0, Math.trunc(limit));
    const rows = this.db
      .prepare('SELECT * FROM outbox WHERE next_try_at <= ? ORDER BY created_at ASC LIMIT ?')
      .all(now, batch) as RawRow[];
    return rows.map(toRow);
  }

  /** Record the Immich result and drop the queue entry in one transaction. */
  markSyncedAndRemove(row: OutboxRow, assetId: string, status: string): void {
    const tx = this.db.transaction(() => {
      this.db
        .prepare(
          `INSERT OR REPLACE INTO synced
             (message_id, group_jid, immich_asset_id, status, created_at, captured_at)
           VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .run(row.messageId, row.groupJid, assetId, status, Date.now(), row.capturedAt);
      this.db.prepare('DELETE FROM outbox WHERE message_id = ?').run(row.messageId);
    });
    tx();
  }

  defer(messageId: string, error: string, nextTryAt: number): void {
    this.db
      .prepare(
        'UPDATE outbox SET attempts = attempts + 1, last_error = ?, next_try_at = ? WHERE message_id = ?',
      )
      .run(error, nextTryAt, messageId);
  }

  /**
   * Drop a row without recording it as synced. For terminal failures where
   * retrying can never succeed (e.g. the staged file is gone) — unlike
   * `defer`, this makes the message eligible for re-ingest again, since
   * `ingest.known()` treats any existing outbox row as already handled.
   */
  remove(messageId: string): void {
    this.db.prepare('DELETE FROM outbox WHERE message_id = ?').run(messageId);
  }

  depth(): number {
    return (this.db.prepare('SELECT COUNT(*) AS c FROM outbox').get() as { c: number }).c;
  }

  allFilePaths(): string[] {
    const rows = this.db.prepare('SELECT file_path FROM outbox').all() as { file_path: string }[];
    return rows.map((r) => r.file_path);
  }

  /**
   * Diagnostic snapshot for `npm run status`. The `synced` table only shows
   * what has already reached Immich; with a drain stuck (bad API key, a
   * poison row, Immich down) that count freezes and gives no signal whether
   * 0 or 4,000 photos are sitting queued. This surfaces the outbox side of
   * that picture from columns the table already has.
   */
  snapshot(now: number = Date.now()): OutboxSnapshot {
    const depth = this.depth();
    if (depth === 0) {
      return { depth: 0, oldestPendingAgeMs: null, maxAttempts: 0, lastError: null };
    }

    const oldest = this.db
      .prepare('SELECT created_at FROM outbox ORDER BY created_at ASC LIMIT 1')
      .get() as { created_at: number };

    const { m: maxAttempts } = this.db.prepare('SELECT MAX(attempts) AS m FROM outbox').get() as {
      m: number | null;
    };

    // There is no separate "last attempted at" column, so next_try_at is
    // used as the recency proxy: defer() always sets it to `now + backoff`
    // at the moment of the most recent failure, so the row with the highest
    // next_try_at is the one that failed most recently.
    const lastErrorRow = this.db
      .prepare(
        `SELECT last_error FROM outbox WHERE last_error IS NOT NULL
         ORDER BY next_try_at DESC LIMIT 1`,
      )
      .get() as { last_error: string } | undefined;

    return {
      depth,
      oldestPendingAgeMs: Math.max(0, now - oldest.created_at),
      maxAttempts: maxAttempts ?? 0,
      lastError: lastErrorRow?.last_error ?? null,
    };
  }
}

/** Diagnostic summary of pending outbox work, see `OutboxStore.snapshot`. */
export interface OutboxSnapshot {
  depth: number;
  /** Age in ms of the oldest still-pending row, or null when the outbox is empty. */
  oldestPendingAgeMs: number | null;
  /** Highest retry count across all pending rows (0 if none have ever failed). */
  maxAttempts: number;
  /** The last_error of the most recently deferred row, or null if none has failed yet. */
  lastError: string | null;
}
