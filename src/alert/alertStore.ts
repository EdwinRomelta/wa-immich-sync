import type Database from 'better-sqlite3';

/** One condition's most recent alert, as stored. */
export interface AlertRecord {
  condition: string;
  lastSentAt: number;
}

/**
 * When each alert condition last fired, persisted so the cooldown outlives a
 * restart. Without persistence, a crash-looping daemon would re-alert on every
 * boot — turning the one condition most likely to be crash-looping into the
 * loudest possible notification storm.
 *
 * Shares the daemon's sqlite connection, like DedupStore and OutboxStore.
 */
export class AlertStore {
  constructor(private readonly db: Database.Database) {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS alert_state (
        condition    TEXT PRIMARY KEY,
        last_sent_at INTEGER NOT NULL
      );
    `);
  }

  lastSentAt(condition: string): number | null {
    const row = this.db
      .prepare('SELECT last_sent_at FROM alert_state WHERE condition = ?')
      .get(condition) as { last_sent_at: number } | undefined;
    return row?.last_sent_at ?? null;
  }

  recordSent(condition: string, at: number): void {
    this.db
      .prepare(
        `INSERT INTO alert_state (condition, last_sent_at) VALUES (?, ?)
         ON CONFLICT(condition) DO UPDATE SET last_sent_at = excluded.last_sent_at`,
      )
      .run(condition, at);
  }

  /** Every recorded condition, for `npm run status`. */
  all(): AlertRecord[] {
    const rows = this.db
      .prepare('SELECT condition, last_sent_at FROM alert_state ORDER BY condition ASC')
      .all() as { condition: string; last_sent_at: number }[];
    return rows.map((r) => ({ condition: r.condition, lastSentAt: r.last_sent_at }));
  }
}
