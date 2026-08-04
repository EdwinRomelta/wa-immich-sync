import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import Database from 'better-sqlite3';

/**
 * Open the sqlite database used by both the dedup store and the outbox.
 * They must share one connection: moving a row from `outbox` to `synced` is a
 * single transaction, and sqlite transactions do not span connections.
 */
export function openDb(path: string): Database.Database {
  if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
  const db = new Database(path);
  db.pragma('journal_mode = WAL');
  return db;
}
