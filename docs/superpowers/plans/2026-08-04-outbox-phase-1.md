# Outbox (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a received photo impossible to lose to an Immich outage or a process crash, by staging bytes to disk and a queue row before any upload is attempted.

**Architecture:** Split the current single `pipeline.process` into two decoupled halves. `ingest` takes a WhatsApp message through whitelist, dedup, and extraction, then writes the bytes to a staging file and inserts an `outbox` row — and stops. A separate `drain` worker loop reads due outbox rows, uploads to Immich, assigns albums, moves the record into `synced`, and deletes the file. Drain never touches WhatsApp, so a retry hours later is identical to the first attempt.

**Tech Stack:** Node 22, TypeScript ESM (`.ts` import specifiers), tsx, better-sqlite3, pino, zod, vitest.

## Global Constraints

- Node >= 22. TypeScript ESM: **all relative imports end in `.ts`** (e.g. `./outboxStore.ts`).
- Files stay under 500 lines. Prefer extracting a module over growing one.
- TDD is mandatory: write the failing test, run it, watch it fail, then implement.
- No mutation of inputs; return new objects.
- No secrets in code or commits. Never commit `.env` or `data/`.
- Commit messages: `<type>: <description>`, types `feat|fix|refactor|docs|test|chore|perf|ci`.
- **Do not add a `Co-Authored-By` trailer.** This repo's `CLAUDE.md` forbids it and no `attribution.commit` key is set.
- Timestamps are integer epoch **milliseconds** everywhere, matching `synced.created_at`.
- Phases 2 (detection/alerting) and 3 (catch-up) are **out of scope**. Do not add gap detection, WhatsApp alerting, healthchecks, or `captured_at` on `synced`.

### Test-count constraint, stated honestly

The suite currently has **69 passing tests**. This phase deletes `src/sync/pipeline.ts` and `tests/pipeline.test.ts`, because the combined whitelist-through-upload flow it tests stops existing once ingest and drain are separate units.

**No assertion may be silently dropped.** Every case in `tests/pipeline.test.ts` maps to a case in the new suites:

| `tests/pipeline.test.ts` case | Migrates to |
|---|---|
| skips messages from non-whitelisted groups (without extracting) | `tests/ingest.test.ts` |
| skips when the message carries no media | `tests/ingest.test.ts` |
| skips when already deduped — before downloading (no extract) | `tests/ingest.test.ts` |
| uploads, adds to the per-group album, and marks done | split: `ingest.test.ts` (queues row) + `drain.test.ts` (uploads, album, marks done) |
| does NOT mark done on upload error (so it retries later) | `tests/drain.test.ts` (row stays in outbox, `attempts` incremented) |
| retries a transient upload failure then marks done | `tests/drain.test.ts` (deferred, then succeeds on a later tick) |
| albumMode "none" skips album calls | `tests/drain.test.ts` (empty `album_name` implies no album calls) |
| reacts with the configured emoji after a successful sync | `tests/ingest.test.ts` — **semantics change, see below** |
| does NOT react when no reactionEmoji is configured | `tests/ingest.test.ts` |
| still reports "uploaded" when the reaction fails | `tests/ingest.test.ts` (now: still reports `queued`) |

At the end of Task 10 the suite must be **green with more tests than it started with**.

### Deliberate behaviour change: when the reaction fires

`src/sync/pipeline.ts:121-130` currently reacts on WhatsApp *after* Immich confirms. Drain runs asynchronously and holds no socket, so the reaction moves to **ingest** and now means "captured safely" rather than "stored in Immich".

This is acceptable precisely because the outbox guarantees the upload eventually happens; capture is the point after which loss is no longer possible. Do not try to preserve the old timing by handing a socket to drain — that would recouple the halves this plan exists to separate.

---

## File Structure

**Create:**
- `src/sync/db.ts` — shared better-sqlite3 connection opener (one connection so outbox to synced is a single transaction)
- `src/sync/outboxStore.ts` — outbox table, queue queries, transactional completion
- `src/sync/staging.ts` — atomic staged-file write, orphan sweep
- `src/sync/ingest.ts` — WhatsApp message to staged file + outbox row
- `src/sync/drain.ts` — outbox row to Immich to synced
- `tests/outboxStore.test.ts`, `tests/staging.test.ts`, `tests/ingest.test.ts`, `tests/drain.test.ts`, `tests/outboxIntegration.test.ts`

**Modify:**
- `src/immich/client.ts` — extract `uploadBlob`, keep `uploadAsset` delegating to it
- `src/sync/dedupStore.ts` — accept a shared `Database` as well as a path
- `src/config.ts` — outbox/drain settings
- `src/sync/importFolder.ts` — enqueue instead of uploading
- `src/sync/backfillIngest.ts` — pass outbox deps through
- `src/index.ts` — wire ingest + drain, delete the Immich startup gate
- `tests/importFolder.test.ts`, `tests/backfillIngest.test.ts` — follow the reroute

**Delete:**
- `src/sync/pipeline.ts`, `tests/pipeline.test.ts` (behaviour migrated per the table above)

---

### Task 1: Shared database connection

`OutboxStore.markSyncedAndRemove` must insert into `synced` and delete from `outbox` atomically. SQLite transactions do not span connections, so both stores must share one.

**Files:**
- Create: `src/sync/db.ts`
- Modify: `src/sync/dedupStore.ts:18-31`
- Test: `tests/dedupStore.test.ts`

**Interfaces:**
- Consumes: nothing
- Produces: `openDb(path: string): Database.Database`; `new DedupStore(path: string | Database.Database)`

- [ ] **Step 1: Write the failing test**

Append to `tests/dedupStore.test.ts`:

```ts
import { openDb } from '../src/sync/db.ts';

it('accepts a shared Database instance so other stores can share the connection', () => {
  const db = openDb(':memory:');
  const store = new DedupStore(db);
  store.markDone('g@g.us:A1', 'g@g.us', 'asset-1', 'created');
  expect(store.count()).toBe(1);
  // The same connection sees the table.
  const row = db.prepare('SELECT COUNT(*) AS c FROM synced').get() as { c: number };
  expect(row.c).toBe(1);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/dedupStore.test.ts`
Expected: FAIL — `Failed to load url ../src/sync/db.ts`

- [ ] **Step 3: Write minimal implementation**

Create `src/sync/db.ts`:

```ts
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
```

In `src/sync/dedupStore.ts`, add `import { openDb } from './db.ts';`, drop the now-unused `mkdirSync`/`dirname` imports, and replace the constructor (lines 18-31):

```ts
  constructor(pathOrDb: string | Database.Database) {
    this.db = typeof pathOrDb === 'string' ? openDb(pathOrDb) : pathOrDb;
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npx vitest run tests/dedupStore.test.ts`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/sync/db.ts src/sync/dedupStore.ts tests/dedupStore.test.ts
git commit -m "refactor: allow DedupStore to share a database connection"
```

---

### Task 2: Outbox store

**Files:**
- Create: `src/sync/outboxStore.ts`
- Test: `tests/outboxStore.test.ts`

**Interfaces:**
- Consumes: `openDb` (Task 1)
- Produces:
  - `interface OutboxRow { messageId, groupJid, albumName, filePath, fileName, mimeType, capturedAt, attempts, lastError, createdAt, nextTryAt }`
  - `type NewOutboxItem = Omit<OutboxRow, 'attempts' | 'lastError' | 'nextTryAt'>`
  - `class OutboxStore` with `enqueue(item: NewOutboxItem): void`, `has(messageId: string): boolean`, `due(now: number, limit: number): OutboxRow[]`, `markSyncedAndRemove(row: OutboxRow, assetId: string, status: string): void`, `defer(messageId: string, error: string, nextTryAt: number): void`, `depth(): number`, `allFilePaths(): string[]`

- [ ] **Step 1: Write the failing test**

Create `tests/outboxStore.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore, type NewOutboxItem } from '../src/sync/outboxStore.ts';

function make(overrides: Partial<NewOutboxItem> = {}): NewOutboxItem {
  return {
    messageId: 'g@g.us:A1',
    groupJid: 'g@g.us',
    albumName: 'Daycare',
    filePath: '/tmp/outbox/g_g.us_A1',
    fileName: 'IMG-1.jpg',
    mimeType: 'image/jpeg',
    capturedAt: 1_700_000_000_000,
    createdAt: 1_700_000_000_000,
    ...overrides,
  };
}

function setup() {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  return { db, dedup, outbox };
}

describe('OutboxStore', () => {
  it('enqueues a row and reports it as present', () => {
    const { outbox } = setup();
    outbox.enqueue(make());
    expect(outbox.has('g@g.us:A1')).toBe(true);
    expect(outbox.depth()).toBe(1);
  });

  it('returns only rows whose next_try_at has arrived', () => {
    const { outbox } = setup();
    outbox.enqueue(make({ messageId: 'g@g.us:ready' }));
    outbox.enqueue(make({ messageId: 'g@g.us:later' }));
    outbox.defer('g@g.us:later', 'immich down', 5_000);

    const due = outbox.due(1_000, 10);
    expect(due.map((r) => r.messageId)).toEqual(['g@g.us:ready']);
  });

  it('respects the batch limit and returns oldest first', () => {
    const { outbox } = setup();
    outbox.enqueue(make({ messageId: 'g@g.us:B', createdAt: 200 }));
    outbox.enqueue(make({ messageId: 'g@g.us:A', createdAt: 100 }));
    const due = outbox.due(1_000, 1);
    expect(due.map((r) => r.messageId)).toEqual(['g@g.us:A']);
  });

  it('moves a row into synced and removes it from the outbox atomically', () => {
    const { outbox, dedup } = setup();
    outbox.enqueue(make());
    const row = outbox.due(1_000, 10)[0]!;

    outbox.markSyncedAndRemove(row, 'asset-99', 'created');

    expect(outbox.has('g@g.us:A1')).toBe(false);
    expect(outbox.depth()).toBe(0);
    expect(dedup.has('g@g.us:A1')).toBe(true);
  });

  it('increments attempts and records the error when deferring', () => {
    const { outbox } = setup();
    outbox.enqueue(make());
    outbox.defer('g@g.us:A1', 'ECONNREFUSED', 9_999);

    const row = outbox.due(10_000, 10)[0]!;
    expect(row.attempts).toBe(1);
    expect(row.lastError).toBe('ECONNREFUSED');
    expect(row.nextTryAt).toBe(9_999);
  });

  it('lists staged file paths so orphans can be swept', () => {
    const { outbox } = setup();
    outbox.enqueue(make({ messageId: 'g@g.us:A1', filePath: '/tmp/outbox/one' }));
    outbox.enqueue(make({ messageId: 'g@g.us:A2', filePath: '/tmp/outbox/two' }));
    expect(outbox.allFilePaths().sort()).toEqual(['/tmp/outbox/one', '/tmp/outbox/two']);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/outboxStore.test.ts`
Expected: FAIL — `Failed to load url ../src/sync/outboxStore.ts`

- [ ] **Step 3: Write minimal implementation**

Create `src/sync/outboxStore.ts`:

```ts
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
```

> Note: `markSyncedAndRemove` writes to `synced`, so the `DedupStore` sharing this connection must have created that table first. `index.ts` constructs `DedupStore` before `OutboxStore`; the tests above do the same.

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/outboxStore.test.ts`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/sync/outboxStore.ts tests/outboxStore.test.ts
git commit -m "feat: add durable outbox store for pending media"
```

---

### Task 3: Atomic staging and orphan sweep

**Files:**
- Create: `src/sync/staging.ts`
- Test: `tests/staging.test.ts`

**Interfaces:**
- Consumes: nothing
- Produces: `stagedPathFor(dir: string, messageId: string): string`, `stageFile(dir: string, messageId: string, bytes: Buffer): Promise<string>`, `sweepOrphans(dir: string, keep: string[]): Promise<number>`

Write to `<dir>/tmp/<safe>`, fsync, then rename into `<dir>/<safe>`. Rename is atomic, so a crash can only leave an orphan file with no row — never a row pointing at a truncated file.

- [ ] **Step 1: Write the failing test**

Create `tests/staging.test.ts`:

```ts
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import { stageFile, stagedPathFor, sweepOrphans } from '../src/sync/staging.ts';

const newDir = () => mkdtempSync(join(tmpdir(), 'outbox-test-'));

describe('stageFile', () => {
  it('writes the bytes and returns the final path', async () => {
    const dir = newDir();
    const path = await stageFile(dir, 'g@g.us:A1', Buffer.from('hello'));
    expect(path).toBe(stagedPathFor(dir, 'g@g.us:A1'));
    expect(readFileSync(path).toString()).toBe('hello');
  });

  it('sanitises message ids so they are safe as filenames', () => {
    const dir = newDir();
    const path = stagedPathFor(dir, 'g@g.us:A/1');
    expect(path.endsWith('g@g.us_A_1')).toBe(true);
  });

  it('overwrites cleanly when the same message is staged twice', async () => {
    const dir = newDir();
    await stageFile(dir, 'g@g.us:A1', Buffer.from('first'));
    const path = await stageFile(dir, 'g@g.us:A1', Buffer.from('second'));
    expect(readFileSync(path).toString()).toBe('second');
  });
});

describe('sweepOrphans', () => {
  it('deletes staged files with no matching queue row', async () => {
    const dir = newDir();
    const keep = await stageFile(dir, 'g@g.us:KEEP', Buffer.from('x'));
    const orphan = await stageFile(dir, 'g@g.us:GONE', Buffer.from('y'));

    const removed = await sweepOrphans(dir, [keep]);

    expect(removed).toBe(1);
    expect(existsSync(keep)).toBe(true);
    expect(existsSync(orphan)).toBe(false);
  });

  it('clears leftover temp files from a crash mid-write', async () => {
    const dir = newDir();
    mkdirSync(join(dir, 'tmp'), { recursive: true });
    const partial = join(dir, 'tmp', 'half-written');
    writeFileSync(partial, 'partial');

    await sweepOrphans(dir, []);

    expect(existsSync(partial)).toBe(false);
  });

  it('returns 0 on a directory that does not exist yet', async () => {
    expect(await sweepOrphans(join(newDir(), 'missing'), [])).toBe(0);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/staging.test.ts`
Expected: FAIL — `Failed to load url ../src/sync/staging.ts`

- [ ] **Step 3: Write minimal implementation**

Create `src/sync/staging.ts`:

```ts
import { mkdir, open, readdir, rename, rm } from 'node:fs/promises';
import { basename, join } from 'node:path';

/** Message ids contain ':' and may contain '/', neither safe in a filename. */
function safeName(messageId: string): string {
  return messageId.replace(/[^A-Za-z0-9@._-]/g, '_');
}

export function stagedPathFor(dir: string, messageId: string): string {
  return join(dir, safeName(messageId));
}

/**
 * Write media to the staging directory durably.
 *
 * Order matters: bytes land in `tmp/`, are fsynced, and only then renamed into
 * place. Rename is atomic, so a crash can leave an orphaned file but never a
 * queue row pointing at a truncated or missing one. The caller inserts the
 * outbox row only after this resolves.
 */
export async function stageFile(dir: string, messageId: string, bytes: Buffer): Promise<string> {
  const tmpDir = join(dir, 'tmp');
  await mkdir(tmpDir, { recursive: true });

  const name = safeName(messageId);
  const tmpPath = join(tmpDir, name);
  const finalPath = join(dir, name);

  const handle = await open(tmpPath, 'w');
  try {
    await handle.writeFile(bytes);
    await handle.sync();
  } finally {
    await handle.close();
  }

  await rename(tmpPath, finalPath);
  return finalPath;
}

/**
 * Delete staged files that no queue row references, plus any leftover temp
 * files. Run at startup: a crash between the rename and the row insert leaves
 * exactly this kind of orphan.
 */
export async function sweepOrphans(dir: string, keep: string[]): Promise<number> {
  let entries: string[];
  try {
    entries = await readdir(dir);
  } catch {
    return 0;
  }

  await rm(join(dir, 'tmp'), { recursive: true, force: true });

  const keepSet = new Set(keep.map((p) => basename(p)));
  let removed = 0;
  for (const entry of entries) {
    if (entry === 'tmp' || keepSet.has(entry)) continue;
    await rm(join(dir, entry), { force: true });
    removed += 1;
  }
  return removed;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/staging.test.ts`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/sync/staging.ts tests/staging.test.ts
git commit -m "feat: add atomic media staging with orphan sweep"
```

---

### Task 4: Immich uploadBlob

Drain uploads from a file, not a buffer. `fs.openAsBlob` streams it so a large video is not read whole into memory.

**Files:**
- Modify: `src/immich/client.ts:47-66`
- Test: `tests/immichClient.test.ts`

**Interfaces:**
- Consumes: nothing
- Produces: `interface UploadMeta { messageId: string; fileName: string; mimeType: string; timestamp: Date }`; `ImmichClient.uploadBlob(blob: Blob, meta: UploadMeta): Promise<UploadResult>`. `uploadAsset(item: MediaItem)` keeps its signature and delegates.

- [ ] **Step 1: Write the failing test**

Append to `tests/immichClient.test.ts`:

```ts
import type { UploadMeta } from '../src/immich/client.ts';

it('uploads a Blob directly with the same fields as uploadAsset', async () => {
  let captured: FormData | null = null;
  const fetchImpl = (async (_url: string, init: RequestInit) => {
    captured = init.body as FormData;
    return new Response(JSON.stringify({ id: 'asset-7', status: 'created' }), { status: 201 });
  }) as unknown as typeof fetch;

  const client = new ImmichClient({ baseUrl: 'http://immich', apiKey: 'k', fetchImpl });
  const meta: UploadMeta = {
    messageId: 'g@g.us:A1',
    fileName: 'IMG-1.jpg',
    mimeType: 'image/jpeg',
    timestamp: new Date('2026-07-28T08:28:06.000Z'),
  };

  const result = await client.uploadBlob(new Blob([new Uint8Array([1, 2, 3])]), meta);

  expect(result).toEqual({ assetId: 'asset-7', status: 'created' });
  expect(captured!.get('deviceAssetId')).toBe('g@g.us:A1');
  expect(captured!.get('filename')).toBe('IMG-1.jpg');
  expect(captured!.get('fileCreatedAt')).toBe('2026-07-28T08:28:06.000Z');
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/immichClient.test.ts`
Expected: FAIL — `client.uploadBlob is not a function`

- [ ] **Step 3: Write minimal implementation**

In `src/immich/client.ts`, add the exported type near the other interfaces:

```ts
/** Everything Immich needs about an asset besides its bytes. */
export interface UploadMeta {
  /** Becomes deviceAssetId — the stable per-message key. */
  messageId: string;
  fileName: string;
  mimeType: string;
  timestamp: Date;
}
```

Replace `uploadAsset` (lines 47-66) with:

```ts
  /** Upload one asset. Immich dedupes by checksum and may return status 'duplicate'. */
  async uploadAsset(item: MediaItem): Promise<UploadResult> {
    const blob = new Blob([new Uint8Array(item.buffer)], { type: item.mimeType });
    return this.uploadBlob(blob, item);
  }

  /**
   * Upload from a Blob. Drain passes a file-backed Blob (`fs.openAsBlob`) so a
   * large video streams instead of being read entirely into memory.
   */
  async uploadBlob(blob: Blob, meta: UploadMeta): Promise<UploadResult> {
    const form = new FormData();
    form.append('assetData', blob, meta.fileName);
    form.append('deviceAssetId', meta.messageId);
    form.append('deviceId', this.deviceId);
    form.append('fileCreatedAt', meta.timestamp.toISOString());
    form.append('fileModifiedAt', meta.timestamp.toISOString());
    form.append('filename', meta.fileName);

    const res = await this.fetch(`${this.baseUrl}/api/assets`, {
      method: 'POST',
      headers: this.headers(),
      body: form,
    });
    if (!res.ok) {
      throw new Error(`Immich upload failed (${res.status}): ${await safeText(res)}`);
    }
    const data = (await res.json()) as { id: string; status?: string };
    return { assetId: data.id, status: (data.status as UploadResult['status']) ?? 'created' };
  }
```

`MediaItem` structurally satisfies `UploadMeta`, so the delegation needs no adapter.

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/immichClient.test.ts`
Expected: PASS (9 tests — the 8 existing plus the new one)

- [ ] **Step 5: Commit**

```bash
git add src/immich/client.ts tests/immichClient.test.ts
git commit -m "refactor: add uploadBlob so uploads can stream from disk"
```

---

### Task 5: Ingest

Replaces the first half of `pipeline.ts`: whitelist, dedup, extract, stage, enqueue, react. It does **not** talk to Immich.

**Files:**
- Create: `src/sync/ingest.ts`
- Test: `tests/ingest.test.ts`

**Interfaces:**
- Consumes: `OutboxStore` (Task 2), `stageFile` (Task 3)
- Produces:
  - `type IngestOutcome = 'skipped-not-whitelisted' | 'skipped-no-media' | 'skipped-dedup' | 'queued' | 'error'`
  - `createIngest(deps: IngestDeps)` returning `{ ingest(sock, m): Promise<IngestOutcome>; setGroups(groups: GroupConfig[]): void }`

- [ ] **Step 1: Write the failing test**

Create `tests/ingest.test.ts`:

```ts
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import type { WAMessage } from '@whiskeysockets/baileys';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { createIngest } from '../src/sync/ingest.ts';
import type { AppConfig, MediaItem } from '../src/types.ts';

const GROUP = { jid: 'g@g.us', name: 'Daycare' };

const config: AppConfig = {
  whitelist: ['Daycare'],
  mediaTypes: ['image', 'video'],
  backfill: false,
  albumMode: 'per-group',
  backfillGroupName: 'wa-immich-backfill',
};

const msg = (id = 'A1'): WAMessage =>
  ({ key: { remoteJid: 'g@g.us', id }, message: { imageMessage: {} } }) as WAMessage;

const item = (id = 'A1'): MediaItem => ({
  messageId: `g@g.us:${id}`,
  rawMessageId: id,
  groupJid: 'g@g.us',
  groupName: 'Daycare',
  kind: 'image',
  mimeType: 'image/jpeg',
  fileName: `IMG-${id}.jpg`,
  timestamp: new Date('2026-07-28T08:28:06.000Z'),
  buffer: Buffer.from('photo-bytes'),
});

function setup(overrides: Partial<AppConfig> = {}) {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const dir = mkdtempSync(join(tmpdir(), 'ingest-test-'));
  const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
  const sock = { sendMessage: vi.fn(async () => undefined), updateMediaMessage: vi.fn() };
  const extract = vi.fn(async () => item());

  const ing = createIngest({
    config: { ...config, ...overrides },
    dedup,
    outbox,
    outboxDir: dir,
    logger,
    extract: extract as never,
  });
  ing.setGroups([GROUP]);
  return { ing, outbox, dedup, dir, logger, sock, extract };
}

describe('ingest', () => {
  it('skips messages from non-whitelisted groups without extracting', async () => {
    const { ing, extract, sock } = setup();
    const foreign = { key: { remoteJid: 'other@g.us', id: 'X' } } as WAMessage;
    expect(await ing.ingest(sock as never, foreign)).toBe('skipped-not-whitelisted');
    expect(extract).not.toHaveBeenCalled();
  });

  it('skips an already-synced message before downloading', async () => {
    const { ing, dedup, extract, sock } = setup();
    dedup.markDone('g@g.us:A1', 'g@g.us', 'asset-1', 'created');
    expect(await ing.ingest(sock as never, msg())).toBe('skipped-dedup');
    expect(extract).not.toHaveBeenCalled();
  });

  it('skips a message already sitting in the outbox', async () => {
    const { ing, outbox, extract, sock } = setup();
    outbox.enqueue({
      messageId: 'g@g.us:A1',
      groupJid: 'g@g.us',
      albumName: 'Daycare',
      filePath: '/tmp/x',
      fileName: 'IMG-A1.jpg',
      mimeType: 'image/jpeg',
      capturedAt: 1,
      createdAt: 1,
    });
    expect(await ing.ingest(sock as never, msg())).toBe('skipped-dedup');
    expect(extract).not.toHaveBeenCalled();
  });

  it('skips when the message carries no media', async () => {
    const { ing, sock, extract } = setup();
    extract.mockResolvedValueOnce(null as never);
    expect(await ing.ingest(sock as never, msg())).toBe('skipped-no-media');
  });

  it('writes the bytes to disk and queues a row', async () => {
    const { ing, outbox, sock } = setup();
    expect(await ing.ingest(sock as never, msg())).toBe('queued');

    const rows = outbox.due(Date.now(), 10);
    expect(rows).toHaveLength(1);
    expect(rows[0]!.messageId).toBe('g@g.us:A1');
    expect(rows[0]!.albumName).toBe('Daycare');
    expect(rows[0]!.capturedAt).toBe(new Date('2026-07-28T08:28:06.000Z').getTime());
    expect(readFileSync(rows[0]!.filePath).toString()).toBe('photo-bytes');
  });

  it('records an empty album name when albumMode is "none"', async () => {
    const { ing, outbox, sock } = setup({ albumMode: 'none' });
    await ing.ingest(sock as never, msg());
    expect(outbox.due(Date.now(), 10)[0]!.albumName).toBe('');
  });

  it('uses the single album name when albumMode is "single"', async () => {
    const { ing, outbox, sock } = setup({ albumMode: 'single', singleAlbumName: 'WhatsApp' });
    await ing.ingest(sock as never, msg());
    expect(outbox.due(Date.now(), 10)[0]!.albumName).toBe('WhatsApp');
  });

  it('reacts with the configured emoji once the media is safely captured', async () => {
    const { ing, sock } = setup({ reactionEmoji: 'X' });
    await ing.ingest(sock as never, msg());
    expect(sock.sendMessage).toHaveBeenCalledWith('g@g.us', {
      react: { text: 'X', key: { remoteJid: 'g@g.us', id: 'A1' } },
    });
  });

  it('does not react when no reactionEmoji is configured', async () => {
    const { ing, sock } = setup();
    await ing.ingest(sock as never, msg());
    expect(sock.sendMessage).not.toHaveBeenCalled();
  });

  it('still reports queued when the reaction fails', async () => {
    const { ing, sock } = setup({ reactionEmoji: 'X' });
    sock.sendMessage.mockRejectedValueOnce(new Error('rate limited'));
    expect(await ing.ingest(sock as never, msg())).toBe('queued');
  });

  it('reports error and queues nothing when staging fails', async () => {
    const db = openDb(':memory:');
    const dedup = new DedupStore(db);
    const outbox = new OutboxStore(db);
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const sock = { sendMessage: vi.fn(), updateMediaMessage: vi.fn() };
    const ing = createIngest({
      config,
      dedup,
      outbox,
      // A path under a regular file can never be created as a directory.
      outboxDir: join(__filename, 'not-a-dir'),
      logger,
      extract: (async () => item()) as never,
    });
    ing.setGroups([GROUP]);

    expect(await ing.ingest(sock as never, msg())).toBe('error');
    expect(outbox.depth()).toBe(0);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/ingest.test.ts`
Expected: FAIL — `Failed to load url ../src/sync/ingest.ts`

- [ ] **Step 3: Write minimal implementation**

Create `src/sync/ingest.ts`:

```ts
import type { WAMessage, WASocket } from '@whiskeysockets/baileys';
import type { AppConfig, GroupConfig } from '../types.ts';
import { extractMedia, type ExtractDeps } from '../wa/mediaExtractor.ts';
import type { DedupStore } from './dedupStore.ts';
import type { OutboxStore } from './outboxStore.ts';
import { stageFile } from './staging.ts';

type IngestLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
  error: (...a: unknown[]) => void;
  debug?: (...a: unknown[]) => void;
};

export interface IngestDeps {
  config: AppConfig;
  dedup: Pick<DedupStore, 'has'>;
  outbox: Pick<OutboxStore, 'has' | 'enqueue'>;
  /** Directory staged media is written to. */
  outboxDir: string;
  logger: IngestLogger;
  /** Injectable extractor for tests. */
  extract?: typeof extractMedia;
  extractDeps?: ExtractDeps;
}

export type IngestOutcome =
  | 'skipped-not-whitelisted'
  | 'skipped-no-media'
  | 'skipped-dedup'
  | 'queued'
  | 'error';

type IngestSock = Pick<WASocket, 'updateMediaMessage' | 'sendMessage'>;

/**
 * Capture a WhatsApp message: whitelist, dedup, extract, write bytes to disk,
 * queue it. Deliberately knows nothing about Immich — once this returns
 * 'queued', the media survives an Immich outage, a crash, and a restart.
 */
export function createIngest(deps: IngestDeps) {
  let whitelist = new Map<string, GroupConfig>();
  const extract = deps.extract ?? extractMedia;

  function setGroups(groups: GroupConfig[]): void {
    whitelist = new Map(groups.map((g) => [g.jid, g]));
  }

  function albumNameFor(group: GroupConfig): string {
    switch (deps.config.albumMode) {
      case 'per-group':
        return group.name;
      case 'single':
        return deps.config.singleAlbumName ?? 'WhatsApp';
      case 'none':
        return '';
    }
  }

  /** True when this message is already recorded, synced or still queued. */
  function known(messageId: string): boolean {
    return deps.dedup.has(messageId) || deps.outbox.has(messageId);
  }

  async function ingest(sock: IngestSock, m: WAMessage): Promise<IngestOutcome> {
    const jid = m.key?.remoteJid ?? '';
    const group = whitelist.get(jid);
    if (!group) return 'skipped-not-whitelisted';

    // Dedup BEFORE downloading. History/append batches replay already-handled
    // messages; downloading them first wastes bandwidth and hammers WhatsApp.
    const rawId = m.key?.id ?? '';
    if (rawId && known(`${jid}:${rawId}`)) {
      deps.logger.debug?.({ messageId: `${jid}:${rawId}` }, 'dedup skip (pre-download)');
      return 'skipped-dedup';
    }

    const item = await extract(sock, m, deps.config, group.name, deps.extractDeps);
    if (!item) {
      // Surface WHAT was skipped — silent drops of unsupported media (e.g.
      // images sent as documents) are otherwise indistinguishable from text.
      deps.logger.info(
        {
          messageId: `${jid}:${rawId}`,
          group: group.name,
          contentKeys: Object.keys((m.message ?? {}) as Record<string, unknown>),
          messageTimestamp: Number(m.messageTimestamp ?? 0),
          stubType: m.messageStubType ?? null,
          hasMessage: m.message != null,
        },
        'skipped-no-media',
      );
      return 'skipped-no-media';
    }

    if (known(item.messageId)) {
      deps.logger.debug?.({ messageId: item.messageId }, 'dedup skip');
      return 'skipped-dedup';
    }

    try {
      // Bytes to disk FIRST, row second. A crash between the two leaves an
      // orphan file (swept at startup), never a row without its media.
      const filePath = await stageFile(deps.outboxDir, item.messageId, item.buffer);
      deps.outbox.enqueue({
        messageId: item.messageId,
        groupJid: item.groupJid,
        albumName: albumNameFor(group),
        filePath,
        fileName: item.fileName,
        mimeType: item.mimeType,
        capturedAt: item.timestamp.getTime(),
        createdAt: Date.now(),
      });
      deps.logger.info({ messageId: item.messageId, group: group.name, kind: item.kind }, 'queued');
    } catch (err) {
      deps.logger.error({ err: (err as Error).message }, `ingest failed for ${item.messageId}`);
      return 'error';
    }

    // Mark the message in WhatsApp. This now means "captured safely", not
    // "already in Immich" — drain runs later and holds no socket. A failed
    // reaction must not change the outcome: the media is already durable.
    if (deps.config.reactionEmoji && m.key) {
      try {
        await sock.sendMessage(jid, { react: { text: deps.config.reactionEmoji, key: m.key } });
      } catch (err) {
        deps.logger.warn(
          { messageId: item.messageId, err: (err as Error).message },
          'reaction failed',
        );
      }
    }

    return 'queued';
  }

  return { ingest, setGroups };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/ingest.test.ts`
Expected: PASS (11 tests)

- [ ] **Step 5: Commit**

```bash
git add src/sync/ingest.ts tests/ingest.test.ts
git commit -m "feat: add ingest stage that stages media before any upload"
```

---

### Task 6: Drain

**Files:**
- Create: `src/sync/drain.ts`
- Test: `tests/drain.test.ts`

**Interfaces:**
- Consumes: `OutboxStore` (Task 2), `uploadBlob`/`UploadMeta` (Task 4), `backoffDelayMs` (`src/util/backoff.ts`, already present)
- Produces: `interface DrainTally { uploaded: number; deferred: number }`; `startDrain(deps: DrainDeps): { stop(): void; tick(): Promise<DrainTally>; tickAt(at: number): Promise<DrainTally> }`

- [ ] **Step 1: Write the failing test**

Create `tests/drain.test.ts`:

```ts
import { existsSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { startDrain } from '../src/sync/drain.ts';

function setup(albumName = 'Daycare') {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const dir = mkdtempSync(join(tmpdir(), 'drain-test-'));
  const filePath = join(dir, 'g_g.us_A1');
  writeFileSync(filePath, 'photo-bytes');

  outbox.enqueue({
    messageId: 'g@g.us:A1',
    groupJid: 'g@g.us',
    albumName,
    filePath,
    fileName: 'IMG-A1.jpg',
    mimeType: 'image/jpeg',
    capturedAt: 1_700_000_000_000,
    createdAt: 1_700_000_000_000,
  });

  const immich = {
    uploadBlob: vi.fn(async () => ({ assetId: 'asset-1', status: 'created' as const })),
    ensureAlbum: vi.fn(async () => 'album-1'),
    addToAlbum: vi.fn(async () => undefined),
  };
  const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
  const drain = startDrain({
    immich,
    outbox,
    logger,
    batchSize: 10,
    baseBackoffMs: 1000,
    maxBackoffMs: 60_000,
    now: () => 5_000,
    autoStart: false,
  });
  return { drain, outbox, dedup, immich, filePath, logger };
}

describe('drain', () => {
  it('uploads, files into the album, records synced and deletes the staged file', async () => {
    const { drain, outbox, dedup, immich, filePath } = setup();

    const tally = await drain.tick();

    expect(tally).toEqual({ uploaded: 1, deferred: 0 });
    expect(immich.uploadBlob).toHaveBeenCalledTimes(1);
    expect(immich.ensureAlbum).toHaveBeenCalledWith('Daycare');
    expect(immich.addToAlbum).toHaveBeenCalledWith('album-1', 'asset-1');
    expect(dedup.has('g@g.us:A1')).toBe(true);
    expect(outbox.depth()).toBe(0);
    expect(existsSync(filePath)).toBe(false);
  });

  it('passes the captured timestamp through as the Immich creation date', async () => {
    const { drain, immich } = setup();
    await drain.tick();
    const meta = immich.uploadBlob.mock.calls[0]![1];
    expect(meta.timestamp.getTime()).toBe(1_700_000_000_000);
    expect(meta.messageId).toBe('g@g.us:A1');
    expect(meta.fileName).toBe('IMG-A1.jpg');
  });

  it('skips album calls when the row has no album name', async () => {
    const { drain, immich } = setup('');
    await drain.tick();
    expect(immich.ensureAlbum).not.toHaveBeenCalled();
    expect(immich.addToAlbum).not.toHaveBeenCalled();
  });

  it('keeps the row and the file when the upload fails', async () => {
    const { drain, outbox, dedup, immich, filePath } = setup();
    immich.uploadBlob.mockRejectedValueOnce(new Error('ECONNREFUSED'));

    const tally = await drain.tick();

    expect(tally).toEqual({ uploaded: 0, deferred: 1 });
    expect(outbox.depth()).toBe(1);
    expect(dedup.has('g@g.us:A1')).toBe(false);
    expect(existsSync(filePath)).toBe(true);
  });

  it('backs the row off so it is not retried immediately', async () => {
    const { drain, outbox, immich } = setup();
    immich.uploadBlob.mockRejectedValueOnce(new Error('ECONNREFUSED'));
    await drain.tick();

    expect(outbox.due(5_000, 10)).toHaveLength(0);
    const row = outbox.due(1_000_000, 10)[0]!;
    expect(row.attempts).toBe(1);
    expect(row.lastError).toContain('ECONNREFUSED');
  });

  it('succeeds on a later tick once Immich recovers', async () => {
    const { drain, outbox, dedup, immich } = setup();
    immich.uploadBlob.mockRejectedValueOnce(new Error('ECONNREFUSED'));
    await drain.tick();

    const later = await drain.tickAt(1_000_000);

    expect(later).toEqual({ uploaded: 1, deferred: 0 });
    expect(dedup.has('g@g.us:A1')).toBe(true);
    expect(outbox.depth()).toBe(0);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/drain.test.ts`
Expected: FAIL — `Failed to load url ../src/sync/drain.ts`

- [ ] **Step 3: Write minimal implementation**

Create `src/sync/drain.ts`:

```ts
import { openAsBlob } from 'node:fs';
import { rm } from 'node:fs/promises';
import type { ImmichClient, UploadMeta } from '../immich/client.ts';
import type { OutboxStore } from './outboxStore.ts';
import { backoffDelayMs } from '../util/backoff.ts';

type DrainLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
  error: (...a: unknown[]) => void;
};

export interface DrainDeps {
  immich: Pick<ImmichClient, 'uploadBlob' | 'ensureAlbum' | 'addToAlbum'>;
  outbox: Pick<OutboxStore, 'due' | 'markSyncedAndRemove' | 'defer'>;
  logger: DrainLogger;
  batchSize: number;
  baseBackoffMs: number;
  maxBackoffMs: number;
  /** Loop period. Only used when autoStart is not false. */
  intervalMs?: number;
  /** Injectable clock (tests). */
  now?: () => number;
  /** Start the timer loop. Tests drive tick() by hand instead. */
  autoStart?: boolean;
}

export interface DrainTally {
  uploaded: number;
  deferred: number;
}

/**
 * Move queued media into Immich.
 *
 * Never contacts WhatsApp: every field needed was captured at ingest, so a
 * retry hours later behaves exactly like the first attempt. A failure defers
 * the row with exponential backoff and leaves both the row and its staged file
 * untouched, so nothing is ever lost to an Immich outage.
 */
export function startDrain(deps: DrainDeps) {
  const now = deps.now ?? Date.now;
  const intervalMs = deps.intervalMs ?? 30_000;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;

  async function tickAt(at: number): Promise<DrainTally> {
    const tally: DrainTally = { uploaded: 0, deferred: 0 };

    for (const row of deps.outbox.due(at, deps.batchSize)) {
      try {
        const blob = await openAsBlob(row.filePath, { type: row.mimeType });
        const meta: UploadMeta = {
          messageId: row.messageId,
          fileName: row.fileName,
          mimeType: row.mimeType,
          timestamp: new Date(row.capturedAt),
        };
        const uploaded = await deps.immich.uploadBlob(blob, meta);

        if (row.albumName) {
          const albumId = await deps.immich.ensureAlbum(row.albumName);
          await deps.immich.addToAlbum(albumId, uploaded.assetId);
        }

        // Record and dequeue atomically, then drop the staged bytes. A crash
        // before the unlink leaves an orphan, which the startup sweep clears.
        deps.outbox.markSyncedAndRemove(row, uploaded.assetId, uploaded.status);
        await rm(row.filePath, { force: true });

        tally.uploaded += 1;
        deps.logger.info(
          { messageId: row.messageId, assetId: uploaded.assetId, status: uploaded.status },
          'synced',
        );
      } catch (err) {
        const message = (err as Error).message;
        const delay = backoffDelayMs(row.attempts, {
          baseMs: deps.baseBackoffMs,
          maxMs: deps.maxBackoffMs,
          jitterRatio: 0.2,
        });
        deps.outbox.defer(row.messageId, message, at + delay);
        tally.deferred += 1;
        deps.logger.warn(
          { messageId: row.messageId, attempts: row.attempts + 1, retryInMs: delay, err: message },
          'drain deferred',
        );
      }
    }

    return tally;
  }

  const tick = (): Promise<DrainTally> => tickAt(now());

  function loop(): void {
    if (stopped) return;
    void tick().finally(() => {
      if (!stopped) timer = setTimeout(loop, intervalMs);
    });
  }

  function stop(): void {
    stopped = true;
    if (timer) clearTimeout(timer);
    timer = null;
  }

  if (deps.autoStart !== false) timer = setTimeout(loop, intervalMs);

  return { stop, tick, tickAt };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/drain.test.ts`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add src/sync/drain.ts tests/drain.test.ts
git commit -m "feat: add drain worker that uploads queued media to Immich"
```

---

### Task 7: Configuration

**Files:**
- Modify: `src/config.ts`
- Modify: `.env.example`
- Test: `tests/config.test.ts`

**Interfaces:**
- Consumes: nothing
- Produces: `getOutboxDir(): string`, `getDrainSettings(): { intervalMs: number; batchSize: number; baseBackoffMs: number; maxBackoffMs: number }`

Defaults come from the spec's configuration table.

- [ ] **Step 1: Write the failing test**

Append to `tests/config.test.ts`:

```ts
import { getDrainSettings, getOutboxDir } from '../src/config.ts';

describe('outbox settings', () => {
  it('defaults the outbox directory', () => {
    delete process.env.OUTBOX_DIR;
    expect(getOutboxDir()).toBe('./data/outbox');
  });

  it('honours OUTBOX_DIR when set', () => {
    process.env.OUTBOX_DIR = '/mnt/staging';
    expect(getOutboxDir()).toBe('/mnt/staging');
    delete process.env.OUTBOX_DIR;
  });

  it('uses the documented drain defaults', () => {
    for (const k of [
      'DRAIN_INTERVAL_MS',
      'DRAIN_BATCH_SIZE',
      'DRAIN_BASE_BACKOFF_MS',
      'DRAIN_MAX_BACKOFF_MS',
    ]) delete process.env[k];

    expect(getDrainSettings()).toEqual({
      intervalMs: 30_000,
      batchSize: 10,
      baseBackoffMs: 30_000,
      maxBackoffMs: 3_600_000,
    });
  });

  it('parses drain overrides from the environment', () => {
    process.env.DRAIN_INTERVAL_MS = '5000';
    process.env.DRAIN_BATCH_SIZE = '3';
    expect(getDrainSettings().intervalMs).toBe(5000);
    expect(getDrainSettings().batchSize).toBe(3);
    delete process.env.DRAIN_INTERVAL_MS;
    delete process.env.DRAIN_BATCH_SIZE;
  });

  it('falls back to the default when an override is not a positive number', () => {
    process.env.DRAIN_BATCH_SIZE = 'not-a-number';
    expect(getDrainSettings().batchSize).toBe(10);
    delete process.env.DRAIN_BATCH_SIZE;
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/config.test.ts`
Expected: FAIL — `getOutboxDir is not a function`

- [ ] **Step 3: Write minimal implementation**

Append to `src/config.ts`:

```ts
/** Read a positive-integer env var, falling back to `dflt` when unset or invalid. */
function intEnv(name: string, dflt: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') return dflt;
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : dflt;
}

/** Directory where media is staged between WhatsApp capture and Immich upload. */
export function getOutboxDir(): string {
  ensureDotenv();
  return process.env.OUTBOX_DIR ?? './data/outbox';
}

/** Drain loop tuning. Defaults per the Phase 1 design spec. */
export function getDrainSettings(): {
  intervalMs: number;
  batchSize: number;
  baseBackoffMs: number;
  maxBackoffMs: number;
} {
  ensureDotenv();
  return {
    intervalMs: intEnv('DRAIN_INTERVAL_MS', 30_000),
    batchSize: intEnv('DRAIN_BATCH_SIZE', 10),
    baseBackoffMs: intEnv('DRAIN_BASE_BACKOFF_MS', 30_000),
    maxBackoffMs: intEnv('DRAIN_MAX_BACKOFF_MS', 3_600_000),
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/config.test.ts`
Expected: PASS (9 tests)

- [ ] **Step 5: Document the settings and commit**

Append to `.env.example`:

```bash
# ---- Outbox (media staged on disk until Immich accepts it) ----
# Directory for in-flight media. Files here are deleted once uploaded.
OUTBOX_DIR=./data/outbox
# How often the uploader drains the queue, and how many items per pass.
DRAIN_INTERVAL_MS=30000
DRAIN_BATCH_SIZE=10
# Per-item retry backoff when Immich is unavailable.
DRAIN_BASE_BACKOFF_MS=30000
DRAIN_MAX_BACKOFF_MS=3600000
```

```bash
git add src/config.ts tests/config.test.ts .env.example
git commit -m "feat: add outbox and drain configuration"
```

---

### Task 8: Wire it together and delete the Immich startup gate

**Files:**
- Modify: `src/index.ts`
- Delete: `src/sync/pipeline.ts`, `tests/pipeline.test.ts`

**Interfaces:**
- Consumes: `openDb`, `OutboxStore`, `createIngest`, `startDrain`, `sweepOrphans`, `getOutboxDir`, `getDrainSettings`
- Produces: nothing — this is the composition root

**Why the gate goes.** `src/index.ts:41` blocks the WhatsApp connect on `withRetry(() => immich.ping(), { retries: 120 })`. After roughly 20 minutes it throws, reaches `main().catch`, and **exits the process** — a crash mode that only existed because an in-flight message had nowhere safe to go. With the outbox, ingest queues while Immich is down, so this must be deleted rather than merely shortened.

- [ ] **Step 1: Delete the obsolete pipeline and its tests**

```bash
git rm src/sync/pipeline.ts tests/pipeline.test.ts
```

Confirm every case from `tests/pipeline.test.ts` is present in `tests/ingest.test.ts` or `tests/drain.test.ts` using the migration table in Global Constraints. Do not proceed until each row is accounted for.

- [ ] **Step 2: Rewire `src/index.ts`**

Replace the `createPipeline` and `withRetry` imports with:

```ts
import { getDedupDb, getDrainSettings, getOutboxDir, getWaAuthDir, loadConfig, loadImmichEnv } from './config.ts';
import { openDb } from './sync/db.ts';
import { OutboxStore } from './sync/outboxStore.ts';
import { createIngest } from './sync/ingest.ts';
import { startDrain } from './sync/drain.ts';
import { sweepOrphans } from './sync/staging.ts';
```

Delete the whole `withRetry(() => immich.ping(), …)` block and its `logger.info('Immich reachable')` line (currently `src/index.ts:36-48`), plus the `const dedup = new DedupStore(getDedupDb());` and `const pipeline = createPipeline({...})` lines (currently `src/index.ts:50-51`). Replace all of it with:

```ts
  // No Immich readiness gate: ingest stages media to disk regardless, and drain
  // retries until Immich answers. Blocking startup here used to abort the whole
  // process after ~20 minutes of Immich downtime.
  const db = openDb(getDedupDb());
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const outboxDir = getOutboxDir();

  // A crash between staging a file and inserting its row leaves an orphan.
  const swept = await sweepOrphans(outboxDir, outbox.allFilePaths());
  if (swept > 0) logger.info({ swept }, 'outbox: removed orphaned staged files');

  const ingest = createIngest({ config, dedup, outbox, outboxDir, logger, extractDeps: { logger } });
  const drainSettings = getDrainSettings();
  const drain = startDrain({ immich, outbox, logger, ...drainSettings });
  logger.info({ ...drainSettings, outboxDir, pending: outbox.depth() }, 'drain started');
```

Replace every `pipeline.process(sock, m)` call with `ingest.ingest(sock, m)`, and `pipeline.setGroups(resolved)` with `ingest.setGroups(resolved)`.

In the live message handler, change the success check:

```ts
        const outcome = await ingest.ingest(sock, m);
        if (outcome === 'queued') logger.info({ jid }, 'live queued');
```

Extend `shutdown` so drain stops before the database closes:

```ts
  const shutdown = () => {
    logger.info('shutting down');
    drain.stop();
    dedup.close();
    process.exit(0);
  };
```

- [ ] **Step 3: Typecheck and run the whole suite**

Run: `npm run typecheck && npm test`
Expected: typecheck clean; all tests pass. Fix any lingering reference to the deleted `pipeline.ts`.

- [ ] **Step 4: Verify the daemon boots against an unreachable Immich**

```bash
IMMICH_URL=http://127.0.0.1:9 IMMICH_API_KEY=x WHITELIST_GROUPS=none \
  DEDUP_DB=/tmp/gate-check.db WA_AUTH_DIR=/tmp/gate-check-auth \
  OUTBOX_DIR=/tmp/gate-check-outbox \
  timeout 30 npx tsx src/index.ts 2>&1 | head -20
```

Expected: logs `wa-immich-sync starting` and `drain started`, then proceeds to WhatsApp (QR prompt). It must **not** log `waiting for Immich to come up`, and must **not** exit because Immich is unreachable.

Clean up: `rm -rf /tmp/gate-check.db* /tmp/gate-check-auth /tmp/gate-check-outbox`

- [ ] **Step 5: Commit**

```bash
git add -A src/index.ts src/sync/pipeline.ts tests/pipeline.test.ts
git commit -m "refactor: run sync through the outbox and drop the Immich startup gate"
```

---

### Task 9: Route the zip importer through the outbox

Leaving the importer on a direct upload would rebuild exactly what this phase removes: a second upload path with its own failure behaviour.

**Files:**
- Modify: `src/sync/importFolder.ts:32-137`
- Modify: `src/sync/backfillIngest.ts:18-26, 86-99`
- Modify: `src/index.ts` (backfill deps)
- Test: `tests/importFolder.test.ts`, `tests/backfillIngest.test.ts`

**Interfaces:**
- Consumes: `OutboxStore` (Task 2), `stageFile` (Task 3)
- Produces: `ImportDeps` loses `immich`, gains `outbox: Pick<OutboxStore, 'has' | 'enqueue'>` and `outboxDir: string`. `ImportStats.uploaded` becomes `ImportStats.queued`; `ImportStats.duplicate` is removed, because Immich reports duplicates later, inside drain.

- [ ] **Step 1: Update the importer test**

In `tests/importFolder.test.ts`, replace the fake Immich with a real `OutboxStore` and assert queuing:

```ts
it('queues every supported file instead of uploading directly', async () => {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const outboxDir = mkdtempSync(join(tmpdir(), 'import-outbox-'));
  const folder = mkdtempSync(join(tmpdir(), 'import-src-'));
  writeFileSync(join(folder, 'IMG-20240617-WA0001.jpg'), 'aaa');
  writeFileSync(join(folder, 'notes.txt'), 'ignore me');

  const stats = await importFolder(folder, {
    outbox,
    dedup,
    outboxDir,
    albumName: 'Daycare',
    logger: { info: vi.fn(), warn: vi.fn() },
  });

  expect(stats.queued).toBe(1);
  expect(stats.skippedType).toBe(1);
  const rows = outbox.due(Date.now(), 10);
  expect(rows).toHaveLength(1);
  expect(rows[0]!.albumName).toBe('Daycare');
  expect(readFileSync(rows[0]!.filePath).toString()).toBe('aaa');
});

it('skips a file already queued or synced', async () => {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const outboxDir = mkdtempSync(join(tmpdir(), 'import-outbox-'));
  const folder = mkdtempSync(join(tmpdir(), 'import-src-'));
  writeFileSync(join(folder, 'IMG-20240617-WA0001.jpg'), 'aaa');
  const deps = { outbox, dedup, outboxDir, albumName: 'D', logger: { info: vi.fn(), warn: vi.fn() } };

  await importFolder(folder, deps);
  const second = await importFolder(folder, deps);

  expect(second.queued).toBe(0);
  expect(second.skippedDedup).toBe(1);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/importFolder.test.ts`
Expected: FAIL — `stats.queued` is undefined

- [ ] **Step 3: Update the importer**

In `src/sync/importFolder.ts`, drop the `ImmichClient` and `MediaItem` imports and add:

```ts
import type { OutboxStore } from './outboxStore.ts';
import { stageFile } from './staging.ts';
```

Replace `ImportStats` and `ImportDeps`:

```ts
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
```

Update the stats initialiser to `{ scanned: 0, queued: 0, skippedDedup: 0, skippedType: 0, errors: 0 }` and delete the `let albumId: string | null = null;` line (line 77). Replace the upload block (lines 107-133) with:

```ts
    if (deps.outbox.has(messageId)) {
      stats.skippedDedup += 1;
      continue;
    }

    try {
      const filePath = await stageFile(deps.outboxDir, messageId, buffer);
      deps.outbox.enqueue({
        messageId,
        groupJid: 'import',
        albumName: deps.albumName,
        filePath,
        fileName: path.split('/').pop() ?? rel,
        mimeType: info.mime,
        capturedAt: dateForFile(path).getTime(),
        createdAt: Date.now(),
      });
      stats.queued += 1;
      if (stats.queued % 25 === 0) deps.logger.info(stats, 'import progress');
    } catch (err) {
      stats.errors += 1;
      deps.logger.warn({ path: rel, err: (err as Error).message }, 'import failed for file');
    }
```

Keep the existing sha1 `deps.dedup.has(messageId)` check immediately above; the new `deps.outbox.has` covers items still queued.

- [ ] **Step 4: Update backfillIngest**

In `src/sync/backfillIngest.ts`, add `import type { OutboxStore } from './outboxStore.ts';` and replace the `immich` and `dedup` fields of `BackfillIngestDeps` with:

```ts
  outbox: Pick<OutboxStore, 'has' | 'enqueue'>;
  dedup: Pick<DedupStore, 'has'>;
  outboxDir: string;
```

Update the `importFolder` call (lines 86-91):

```ts
    const stats = await importFolder(tmp, {
      outbox: deps.outbox,
      dedup: deps.dedup,
      outboxDir: deps.outboxDir,
      albumName,
      logger: deps.logger,
    });
```

Update the summary so it reports what actually happened (lines 95-98):

```ts
      const summary =
        `Backfill queued -> album "${albumName}"\n` +
        `queued: ${stats.queued}, already-synced: ${stats.skippedDedup}, ` +
        `non-media: ${stats.skippedType}, errors: ${stats.errors}\n` +
        `Uploading to Immich in the background.`;
```

Update `tests/backfillIngest.test.ts` to pass `outbox`, `dedup`, and `outboxDir` in its deps object, and to assert on the new summary wording.

In `src/index.ts`, update the `handleBackfillMessage` call to pass `{ outbox, dedup, outboxDir, logger, defaultAlbum: backfillDefaultAlbum }` in place of `immich`.

- [ ] **Step 5: Run the full suite and typecheck**

Run: `npm run typecheck && npm test`
Expected: typecheck clean, all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/sync/importFolder.ts src/sync/backfillIngest.ts src/index.ts tests/importFolder.test.ts tests/backfillIngest.test.ts
git commit -m "refactor: route zip imports through the outbox"
```

---

### Task 10: Regression tests for the two real incidents

These encode the failures that motivated the phase.

**Files:**
- Create: `tests/outboxIntegration.test.ts`

**Interfaces:**
- Consumes: everything above
- Produces: nothing

- [ ] **Step 1: Write the tests**

Create `tests/outboxIntegration.test.ts`:

```ts
import { existsSync, mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import type { WAMessage } from '@whiskeysockets/baileys';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { createIngest } from '../src/sync/ingest.ts';
import { startDrain } from '../src/sync/drain.ts';
import { stageFile, sweepOrphans } from '../src/sync/staging.ts';
import type { AppConfig, MediaItem } from '../src/types.ts';

const config: AppConfig = {
  whitelist: ['Daycare'],
  mediaTypes: ['image', 'video'],
  backfill: false,
  albumMode: 'per-group',
  backfillGroupName: 'wa-immich-backfill',
};

const msg = () =>
  ({ key: { remoteJid: 'g@g.us', id: 'A1' }, message: { imageMessage: {} } }) as WAMessage;

const item = (): MediaItem => ({
  messageId: 'g@g.us:A1',
  rawMessageId: 'A1',
  groupJid: 'g@g.us',
  groupName: 'Daycare',
  kind: 'image',
  mimeType: 'image/jpeg',
  fileName: 'IMG-A1.jpg',
  timestamp: new Date('2026-07-28T08:28:06.000Z'),
  buffer: Buffer.from('irreplaceable-photo'),
});

describe('regression: 2026-07-08 — Immich unavailable must not lose a photo', () => {
  it('keeps the media and uploads it once Immich recovers', async () => {
    const db = openDb(':memory:');
    const dedup = new DedupStore(db);
    const outbox = new OutboxStore(db);
    const outboxDir = mkdtempSync(join(tmpdir(), 'integration-'));
    const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
    const sock = { sendMessage: vi.fn(), updateMediaMessage: vi.fn() };

    const ing = createIngest({
      config, dedup, outbox, outboxDir, logger,
      extract: (async () => item()) as never,
    });
    ing.setGroups([{ jid: 'g@g.us', name: 'Daycare' }]);

    // The photo arrives while Immich is down.
    expect(await ing.ingest(sock as never, msg())).toBe('queued');

    let immichUp = false;
    const immich = {
      uploadBlob: vi.fn(async () => {
        if (!immichUp) throw new Error('ECONNREFUSED');
        return { assetId: 'asset-1', status: 'created' as const };
      }),
      ensureAlbum: vi.fn(async () => 'album-1'),
      addToAlbum: vi.fn(async () => undefined),
    };
    const drain = startDrain({
      immich, outbox, logger,
      batchSize: 10, baseBackoffMs: 1000, maxBackoffMs: 60_000, autoStart: false,
    });

    // Three failed attempts across the outage — nothing is lost.
    for (const at of [1_000, 100_000, 200_000]) {
      expect(await drain.tickAt(at)).toEqual({ uploaded: 0, deferred: 1 });
    }
    expect(outbox.depth()).toBe(1);
    expect(dedup.has('g@g.us:A1')).toBe(false);

    // Immich comes back.
    immichUp = true;
    expect(await drain.tickAt(10_000_000)).toEqual({ uploaded: 1, deferred: 0 });

    expect(dedup.has('g@g.us:A1')).toBe(true);
    expect(outbox.depth()).toBe(0);
  });
});

describe('regression: a crash between staging and queuing loses nothing durable', () => {
  it('sweeps a staged file that never got a queue row', async () => {
    const db = openDb(':memory:');
    new DedupStore(db);
    const outbox = new OutboxStore(db);
    const outboxDir = mkdtempSync(join(tmpdir(), 'crash-'));

    // Simulate: bytes landed, then the process died before enqueue.
    const orphan = await stageFile(outboxDir, 'g@g.us:LOST', Buffer.from('bytes'));
    expect(existsSync(orphan)).toBe(true);
    expect(outbox.depth()).toBe(0);

    const removed = await sweepOrphans(outboxDir, outbox.allFilePaths());

    expect(removed).toBe(1);
    expect(existsSync(orphan)).toBe(false);
  });

  it('never removes media that a queue row still references', async () => {
    const db = openDb(':memory:');
    new DedupStore(db);
    const outbox = new OutboxStore(db);
    const outboxDir = mkdtempSync(join(tmpdir(), 'crash2-'));

    const path = await stageFile(outboxDir, 'g@g.us:A1', Buffer.from('bytes'));
    outbox.enqueue({
      messageId: 'g@g.us:A1', groupJid: 'g@g.us', albumName: 'Daycare',
      filePath: path, fileName: 'IMG-A1.jpg', mimeType: 'image/jpeg',
      capturedAt: 1, createdAt: 1,
    });

    await sweepOrphans(outboxDir, outbox.allFilePaths());

    const row = outbox.due(Date.now(), 10)[0]!;
    expect(existsSync(row.filePath)).toBe(true);
    expect(readFileSync(row.filePath).toString()).toBe('bytes');
  });
});
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `npx vitest run tests/outboxIntegration.test.ts`
Expected: PASS (3 tests). If any fail, the outbox guarantee is broken — fix the implementation, not the test.

- [ ] **Step 3: Run the whole suite and typecheck**

Run: `npm run typecheck && npm test`
Expected: typecheck clean; total test count **higher** than the original 69.

- [ ] **Step 4: Commit**

```bash
git add tests/outboxIntegration.test.ts
git commit -m "test: add regression tests for Immich outage and crash safety"
```

---

### Task 11: Deploy and verify against the real stack

**Files:** none (operational)

- [ ] **Step 1: Rebuild the image**

```bash
cd /home/imnot/workspace/wa-immich-sync
docker compose build
```

Expected: `Image wa-immich-sync:local Built`.

- [ ] **Step 2: Recreate the container**

```bash
docker compose up -d
docker inspect wa-immich-sync-wa-immich-sync-1 \
  --format 'Running={{.State.Running}} Restart={{.HostConfig.RestartPolicy.Name}} Init={{.HostConfig.Init}}'
```

Expected: `Running=true Restart=always Init=true`.

- [ ] **Step 3: Confirm startup no longer waits on Immich**

```bash
docker logs --since 2m wa-immich-sync-wa-immich-sync-1 2>&1 \
  | grep -oE '"msg":"[^"]*"' | sort | uniq -c | sort -rn | head
```

Expected present: `wa-immich-sync starting`, `drain started`, `WhatsApp connection open`, `whitelist resolved`.
Expected absent: `waiting for Immich to come up`, `Immich reachable`.

- [ ] **Step 4: Prove the outage guarantee end to end**

```bash
docker stop immich_server
# Post a photo to the whitelisted WhatsApp group, then:
docker logs --since 2m wa-immich-sync-wa-immich-sync-1 2>&1 | grep -E '"msg":"(queued|drain deferred)"'
ls -la data/outbox/
```

Expected: a `queued` line, then `drain deferred` lines, and the staged file present on disk.

```bash
docker start immich_server
# wait ~90s for the next drain tick, then:
docker logs --since 3m wa-immich-sync-wa-immich-sync-1 2>&1 | grep '"msg":"synced"'
ls -la data/outbox/
```

Expected: a `synced` line with an `assetId`, and an empty outbox directory. **This is the 2026-07-08 failure, now survived.**

- [ ] **Step 5: Confirm clean shutdown still exits 0**

```bash
docker stop wa-immich-sync-wa-immich-sync-1
docker inspect wa-immich-sync-wa-immich-sync-1 --format 'ExitCode={{.State.ExitCode}}'
docker start wa-immich-sync-wa-immich-sync-1
```

Expected: `ExitCode=0` (the PID 1 fix from commit `ec8b517` still holds).

- [ ] **Step 6: Push**

```bash
git status --short          # expect clean unless a fix was needed
git push origin main
```

---

## Self-Review

**Spec coverage (Phase 1 scope only):**

| Spec requirement | Task |
|---|---|
| `outbox` table + store | Task 2 |
| Shared connection for the outbox-to-synced transaction | Task 1 |
| Atomic staging write ordering | Task 3 |
| Orphan sweep at startup | Tasks 3, 8 |
| `src/sync/ingest.ts` | Task 5 |
| `src/sync/drain.ts` | Task 6 |
| `next_try_at` via `backoffDelayMs` | Task 6 |
| `uploadBlob` with `fs.openAsBlob` | Task 4 |
| Dedup across `synced` and `outbox` | Task 5 (`known()`) |
| Zip importer rerouted | Task 9 |
| Immich startup gate deleted | Task 8 |
| Configuration defaults | Task 7 |
| Gap B and crash regression tests | Task 10 |

Deliberately absent (Phases 2-3): `captured_at` on `synced`, gap detection, WhatsApp alerting, heartbeat, `HEALTHCHECK`, catch-up traversal.

**Type consistency checked:** `OutboxRow` and `NewOutboxItem` field names match between Task 2's definition and their use in Tasks 5, 6, 9, 10. `UploadMeta` matches between Task 4 and Task 6. `startDrain` returns `{ stop, tick, tickAt }` and the tests use all three. `ImportStats.queued` replaces `uploaded` consistently in Task 9 and in the `backfillIngest` summary.

**Ordering constraint to preserve:** `OutboxStore.markSyncedAndRemove` writes to `synced`, so `DedupStore` must construct that table first. `index.ts` and every test here build `DedupStore` before `OutboxStore`. If that order is ever changed the insert fails loudly rather than silently, which is acceptable.
