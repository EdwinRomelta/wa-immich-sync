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

  /**
   * Newest known message per group (by created_at), used to seed the backfill
   * cursor when WhatsApp's on-connect history sync is unavailable. Returns the
   * raw WhatsApp message id (the part after the `jid:` prefix).
   */
  newestByGroup(): { group_jid: string; raw_id: string; created_at: number }[] {
    const rows = this.db
      .prepare(
        `SELECT group_jid, message_id, created_at FROM synced s
         WHERE group_jid LIKE '%@g.us'
           AND created_at = (SELECT MAX(created_at) FROM synced WHERE group_jid = s.group_jid)
         GROUP BY group_jid`,
      )
      .all() as { group_jid: string; message_id: string; created_at: number }[];
    return rows.map((r) => ({
      group_jid: r.group_jid,
      raw_id: r.message_id.slice(r.group_jid.length + 1),
      created_at: r.created_at,
    }));
  }

  lastSyncedAt(): number | null {
    const row = this.db.prepare('SELECT MAX(created_at) AS m FROM synced').get() as { m: number | null };
    return row.m ?? null;
  }

  close(): void {
    this.db.close();
  }
}
