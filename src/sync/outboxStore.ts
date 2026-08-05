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
        `INSERT OR IGNORE INTO outbox
           (message_id, group_jid, album_name, file_path, file_name, mime_type,
            captured_at, attempts, last_error, created_at, next_try_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, 0)`,
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
    const rows = this.db
      .prepare('SELECT * FROM outbox WHERE next_try_at <= ? ORDER BY created_at ASC LIMIT ?')
      .all(now, limit) as RawRow[];
    return rows.map(toRow);
  }

  /** Record the Immich result and drop the queue entry in one transaction. */
  markSyncedAndRemove(row: OutboxRow, assetId: string, status: string): void {
    const tx = this.db.transaction(() => {
      this.db
        .prepare(
          `INSERT OR REPLACE INTO synced (message_id, group_jid, immich_asset_id, status, created_at)
           VALUES (?, ?, ?, ?, ?)`,
        )
        .run(row.messageId, row.groupJid, assetId, status, Date.now());
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

  depth(): number {
    return (this.db.prepare('SELECT COUNT(*) AS c FROM outbox').get() as { c: number }).c;
  }

  allFilePaths(): string[] {
    const rows = this.db.prepare('SELECT file_path FROM outbox').all() as { file_path: string }[];
    return rows.map((r) => r.file_path);
  }
}
