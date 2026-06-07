import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import Database from 'better-sqlite3';

export interface GroupCount {
  group_jid: string;
  c: number;
}

/**
 * Persistent record of already-synced WhatsApp messages, keyed by a stable
 * `${groupJid}:${messageId}`. Makes both backfill and live sync idempotent
 * across restarts.
 */
export class DedupStore {
  private readonly db: Database.Database;

  constructor(path: string) {
    if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
    this.db = new Database(path);
    this.db.pragma('journal_mode = WAL');
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS synced (
        message_id      TEXT PRIMARY KEY,
        group_jid       TEXT NOT NULL,
        immich_asset_id TEXT,
        status          TEXT NOT NULL,
        created_at      INTEGER NOT NULL
      )
    `);
  }

  has(messageId: string): boolean {
    return this.db.prepare('SELECT 1 FROM synced WHERE message_id = ?').get(messageId) !== undefined;
  }

  markDone(messageId: string, groupJid: string, immichAssetId: string, status: string): void {
    this.db
      .prepare(
        `INSERT OR REPLACE INTO synced (message_id, group_jid, immich_asset_id, status, created_at)
         VALUES (?, ?, ?, ?, ?)`,
      )
      .run(messageId, groupJid, immichAssetId, status, Date.now());
  }

  count(): number {
    return (this.db.prepare('SELECT COUNT(*) AS c FROM synced').get() as { c: number }).c;
  }

  countByGroup(): GroupCount[] {
    return this.db
      .prepare('SELECT group_jid, COUNT(*) AS c FROM synced GROUP BY group_jid ORDER BY c DESC')
      .all() as GroupCount[];
  }

  lastSyncedAt(): number | null {
    const row = this.db.prepare('SELECT MAX(created_at) AS m FROM synced').get() as { m: number | null };
    return row.m ?? null;
  }

  close(): void {
    this.db.close();
  }
}
