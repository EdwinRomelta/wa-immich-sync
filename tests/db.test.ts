import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { openDb } from '../src/sync/db.ts';

// SQLite silently refuses WAL mode for `:memory:` databases (journal_mode
// reads back as 'memory' regardless), so this needs a real file on disk to
// actually exercise what production gets.
let dir: string | undefined;
afterEach(() => {
  if (dir) rmSync(dir, { recursive: true, force: true });
  dir = undefined;
});

describe('openDb', () => {
  it('sets both journal_mode = WAL and synchronous = FULL', () => {
    // The outbox's crash-safety ordering (stage bytes, fsync, atomic rename,
    // fsync the parent dir, THEN insert the outbox row — see
    // src/sync/staging.ts) is only sound if a committed row is actually
    // durable. WAL's default synchronous level is NORMAL, which fsyncs the
    // WAL only at checkpoints, so a "committed" row can still be lost on
    // power loss unless synchronous is explicitly raised to FULL.
    dir = mkdtempSync(join(tmpdir(), 'db-pragma-'));
    const db = openDb(join(dir, 'test.db'));
    try {
      // journal_mode is a string pragma, so it reads back as text.
      expect(db.pragma('journal_mode', { simple: true })).toBe('wal');
      // synchronous: OFF=0, NORMAL=1, FULL=2, EXTRA=3.
      expect(db.pragma('synchronous', { simple: true })).toBe(2);
    } finally {
      db.close();
    }
  });
});
