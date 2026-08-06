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
  // WAL's default synchronous level is NORMAL, which fsyncs the WAL only at
  // checkpoints — a commit can return success and still be lost on power
  // loss. staging.ts's fsync-the-parent-directory argument (and the wider
  // stage-bytes-then-insert-row ordering the outbox depends on) is written
  // assuming a committed outbox row is durable; that assumption is false
  // under NORMAL. FULL fsyncs the WAL on every commit, closing that gap.
  db.pragma('synchronous = FULL');
  return db;
}
