# Photo Gap Detection (Phase 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a stalled sync visible in hours instead of days — a WhatsApp alert when the outbox backs up, a group goes silent, or a capture fails, plus a Docker healthcheck that marks a hung container `unhealthy`.

**Architecture:** Phase 1 made loss impossible while the daemon runs; Phase 2 makes *silence* observable. A `synced.captured_at` column gives every row a send-time, so per-group silence can be measured across `synced` ∪ `outbox`. A periodic monitor writes a heartbeat file and evaluates outbox depth/age thresholds. Breaches go to a cooldown-gated alerter that messages the operator over the bot's own WhatsApp socket. Because that channel dies with WhatsApp, a Docker `HEALTHCHECK` reads the heartbeat file as an independent backstop. Nothing added here gates ingest, gates startup, or closes the WhatsApp socket — Immich being down must stay a queueing event, not an availability event.

**Tech Stack:** Node ≥22 ESM, TypeScript via `tsx`, better-sqlite3 (synchronous), pino, zod, vitest, `@whiskeysockets/baileys`, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-08-04-photo-gap-prevention-design.md` (Phase 2 section; Phase 1 already shipped)

## Global Constraints

- Node `>=22`, ESM only, `.ts` extensions in relative imports (e.g. `./config.ts`) — matches every existing import in `src/`.
- better-sqlite3 is **synchronous**. Any promise wrapping a sqlite call must be `void`'d with a `.catch` — an unhandled rejection kills the process, and `docker-compose.yml` sets `restart: always`, so that becomes a crash loop that also drops the WhatsApp socket.
- All timestamps are integer epoch **milliseconds**, matching `synced.created_at`.
- Every new module takes an injectable `now?: () => number` clock, following `startDrain` in `src/sync/drain.ts:137`.
- All 100+ existing tests must stay green. Run `npm test` before each commit.
- **Never** add an Immich readiness gate, and never disconnect WhatsApp because Immich is unreachable. `src/index.ts:47-52` records why that gate was deleted; re-adding either is a Phase 1 regression.
- No `Co-Authored-By` trailer on commits (project rule in `CLAUDE.md`).
- Spec defaults, verbatim: `ALERT_COOLDOWN_MS` 21600000, `ALERT_OUTBOX_DEPTH` 50, `ALERT_OUTBOX_AGE_MS` 7200000, `ALERT_RECONNECT_FAILURES` 10, `HEALTH_STALE_MS` 3600000, `CATCHUP_GAP_THRESHOLD_MS` 3600000 (reused here as the gap-detection threshold).

### Deviations from the spec (deliberate, do not "fix")

1. **Heartbeat lives in a JSON file, not sqlite.** The spec says "stamps `last_ok`" without naming a store. The Docker healthcheck runs every 60s forever; loading better-sqlite3's native binding on each run costs far more than reading one small JSON file, and a second writer on the WAL buys nothing. `HEALTH_FILE` defaults to `./data/health.json`.
2. **Only two heartbeat keys: `daemon` and `wa`.** The spec mentions stamping each successful drain cycle. A drain that cannot reach Immich is *healthy queueing*, not a fault — stamping it would let an Immich outage mark the container `unhealthy`, which is exactly the conflation Phase 1 removed. A wedged drain already surfaces through `ALERT_OUTBOX_AGE_MS`.
3. **A group that has never synced anything is not a gap.** `detectGaps` only reports groups with a known previous send-time. Without this, every newly whitelisted group alerts forever.
4. **Task 8 (ingest capture-failure) is not in the spec.** It was found while reviewing Phase 1: `src/sync/ingest.ts:127-134` catches a `stageFile` failure, logs, and returns `'error'` — no outbox row, no retry, no re-delivery. The outbox protects against Immich failure; nothing protected against *staging* failure, and a full disk is the realistic trigger during a long Immich outage. It belongs in the detection phase because the fix is retry-then-report.

---

### Task 1: `synced.captured_at` — send-time on every row

Gap detection needs when a message was *sent*. `synced.created_at` records when it was *synced*; for zip-imported and backfilled rows those diverge by months.

**Files:**
- Modify: `src/sync/dedupStore.ts:19-40`
- Modify: `src/sync/outboxStore.ts:117-128`
- Test: `tests/dedupStore.test.ts`, `tests/outboxStore.test.ts`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `synced.captured_at INTEGER NULL`, populated by `OutboxStore.markSyncedAndRemove` from `OutboxRow.capturedAt`. `DedupStore.markDone(messageId: string, groupJid: string, immichAssetId: string, status: string, capturedAt?: number): void`. Task 6 reads the column.

- [ ] **Step 1: Write the failing tests**

Append to `tests/dedupStore.test.ts`:

```ts
it('adds captured_at to a pre-existing synced table without losing rows', () => {
  const db = openDb(':memory:');
  // Simulate a database created before this migration existed.
  db.exec(`
    CREATE TABLE synced (
      message_id      TEXT PRIMARY KEY,
      group_jid       TEXT NOT NULL,
      immich_asset_id TEXT,
      status          TEXT NOT NULL,
      created_at      INTEGER NOT NULL
    )
  `);
  db.prepare(
    `INSERT INTO synced (message_id, group_jid, immich_asset_id, status, created_at)
     VALUES ('g@g.us:OLD', 'g@g.us', 'asset-old', 'created', 1000)`,
  ).run();

  const store = new DedupStore(db);

  const cols = db.prepare('PRAGMA table_info(synced)').all() as { name: string }[];
  expect(cols.some((c) => c.name === 'captured_at')).toBe(true);
  expect(store.has('g@g.us:OLD')).toBe(true);
  const old = db.prepare('SELECT captured_at FROM synced WHERE message_id = ?').get('g@g.us:OLD');
  expect(old).toEqual({ captured_at: null });
});

it('is idempotent when captured_at already exists', () => {
  const db = openDb(':memory:');
  new DedupStore(db);
  expect(() => new DedupStore(db)).not.toThrow();
});

it('records captured_at when markDone is given one', () => {
  const db = openDb(':memory:');
  const store = new DedupStore(db);
  store.markDone('g@g.us:A1', 'g@g.us', 'asset-1', 'created', 1_700_000_000_000);
  const row = db.prepare('SELECT captured_at FROM synced WHERE message_id = ?').get('g@g.us:A1');
  expect(row).toEqual({ captured_at: 1_700_000_000_000 });
});
```

Append to `tests/outboxStore.test.ts`:

```ts
it('carries captured_at from the outbox row into synced', () => {
  const db = openDb(':memory:');
  new DedupStore(db);
  const outbox = new OutboxStore(db);
  outbox.enqueue({
    messageId: 'g@g.us:A1',
    groupJid: 'g@g.us',
    albumName: 'Daycare',
    filePath: '/tmp/g_g.us_A1',
    fileName: 'IMG-A1.jpg',
    mimeType: 'image/jpeg',
    capturedAt: 1_700_000_000_000,
    createdAt: 1_700_000_500_000,
  });
  const [row] = outbox.due(Date.now(), 10);
  outbox.markSyncedAndRemove(row, 'asset-1', 'created');

  const synced = db.prepare('SELECT captured_at FROM synced WHERE message_id = ?').get('g@g.us:A1');
  expect(synced).toEqual({ captured_at: 1_700_000_000_000 });
});
```

If `tests/outboxStore.test.ts` does not already import `DedupStore`, add `import { DedupStore } from '../src/sync/dedupStore.ts';` to its imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/dedupStore.test.ts tests/outboxStore.test.ts`
Expected: FAIL — `expect(cols.some(...)).toBe(true)` receives `false`, and the `captured_at` selects throw `SqliteError: no such column: captured_at`.

- [ ] **Step 3: Add the migration and write path**

In `src/sync/dedupStore.ts`, immediately after the `CREATE TABLE IF NOT EXISTS synced` `this.db.exec(...)` call in the constructor, add:

```ts
    // Additive, idempotent migration. `created_at` records when a row was
    // *synced*; gap detection needs when the message was *sent*, and the two
    // diverge sharply for zip-imported and backfilled rows. Pre-existing rows
    // stay NULL and fall back to created_at at query time (see gapDetect.ts).
    // PRAGMA-then-ALTER rather than a caught "duplicate column" error: this
    // runs on every boot, and swallowing SqliteError here would also swallow
    // a genuinely broken schema.
    const columns = this.db.prepare('PRAGMA table_info(synced)').all() as { name: string }[];
    if (!columns.some((c) => c.name === 'captured_at')) {
      this.db.exec('ALTER TABLE synced ADD COLUMN captured_at INTEGER');
    }
```

Replace `markDone` in the same file with:

```ts
  /**
   * `capturedAt` is the WhatsApp send time. Optional because the column is
   * nullable for rows written before the migration; pass it whenever it is
   * known, or gap detection falls back to the (later, less accurate) sync time.
   */
  markDone(
    messageId: string,
    groupJid: string,
    immichAssetId: string,
    status: string,
    capturedAt?: number,
  ): void {
    this.db
      .prepare(
        `INSERT OR REPLACE INTO synced
           (message_id, group_jid, immich_asset_id, status, created_at, captured_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(messageId, groupJid, immichAssetId, status, Date.now(), capturedAt ?? null);
  }
```

In `src/sync/outboxStore.ts`, inside `markSyncedAndRemove`, replace the `INSERT OR REPLACE INTO synced` statement with:

```ts
      this.db
        .prepare(
          `INSERT OR REPLACE INTO synced
             (message_id, group_jid, immich_asset_id, status, created_at, captured_at)
           VALUES (?, ?, ?, ?, ?, ?)`,
        )
        .run(row.messageId, row.groupJid, assetId, status, Date.now(), row.capturedAt);
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npx vitest run tests/dedupStore.test.ts tests/outboxStore.test.ts`
Expected: PASS

Run: `npm test && npm run typecheck`
Expected: all tests pass, no type errors.

- [ ] **Step 5: Commit**

```bash
git add src/sync/dedupStore.ts src/sync/outboxStore.ts tests/dedupStore.test.ts tests/outboxStore.test.ts
git commit -m "feat: record WhatsApp send time as synced.captured_at

Gap detection needs when a message was sent, not when it was synced;
those diverge by months for zip-imported and backfilled rows. Additive,
idempotent migration — existing rows stay NULL and fall back to
created_at at query time."
```

---

### Task 2: Heartbeat file

A small JSON file the daemon rewrites on a timer. Task 3's healthcheck reads it; nothing else in the daemon depends on it.

**Files:**
- Create: `src/health/heartbeat.ts`
- Modify: `src/config.ts` (add `getHealthFile`, `getHealthSettings`, extend `outboxGuards`)
- Test: `tests/heartbeat.test.ts`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `export interface Heartbeat { daemon: number; wa: number | null }`
  - `export async function writeHeartbeat(path: string, beat: Heartbeat): Promise<void>`
  - `export async function readHeartbeat(path: string): Promise<Heartbeat | null>` — `null` when the file is missing or unparseable.
  - `getHealthFile(): string` and `getHealthSettings(): { staleMs: number }` in `src/config.ts`.
  - Task 7 calls `writeHeartbeat`; Task 3 calls `readHeartbeat`.

- [ ] **Step 1: Write the failing test**

Create `tests/heartbeat.test.ts`:

```ts
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { readHeartbeat, writeHeartbeat } from '../src/health/heartbeat.ts';

const dirs: string[] = [];
function tmp(): string {
  const d = mkdtempSync(join(tmpdir(), 'heartbeat-test-'));
  dirs.push(d);
  return d;
}
afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
});

describe('heartbeat', () => {
  it('round-trips a written beat', async () => {
    const path = join(tmp(), 'health.json');
    await writeHeartbeat(path, { daemon: 1000, wa: 900 });
    expect(await readHeartbeat(path)).toEqual({ daemon: 1000, wa: 900 });
  });

  it('round-trips a null wa stamp', async () => {
    const path = join(tmp(), 'health.json');
    await writeHeartbeat(path, { daemon: 1000, wa: null });
    expect(await readHeartbeat(path)).toEqual({ daemon: 1000, wa: null });
  });

  it('creates the parent directory', async () => {
    const path = join(tmp(), 'nested', 'health.json');
    await writeHeartbeat(path, { daemon: 1000, wa: null });
    expect(await readHeartbeat(path)).toEqual({ daemon: 1000, wa: null });
  });

  it('leaves no temp file behind', async () => {
    const dir = tmp();
    const path = join(dir, 'health.json');
    await writeHeartbeat(path, { daemon: 1000, wa: null });
    const { readdir } = await import('node:fs/promises');
    expect(await readdir(dir)).toEqual(['health.json']);
  });

  it('returns null for a missing file', async () => {
    expect(await readHeartbeat(join(tmp(), 'absent.json'))).toBeNull();
  });

  it('returns null for unparseable content rather than throwing', async () => {
    const path = join(tmp(), 'health.json');
    writeFileSync(path, '{ truncated');
    expect(await readHeartbeat(path)).toBeNull();
  });

  it('returns null when daemon is not a number', async () => {
    const path = join(tmp(), 'health.json');
    writeFileSync(path, JSON.stringify({ daemon: 'soon', wa: null }));
    expect(await readHeartbeat(path)).toBeNull();
  });

  it('overwrites a previous beat in place', async () => {
    const path = join(tmp(), 'health.json');
    await writeHeartbeat(path, { daemon: 1000, wa: null });
    await writeHeartbeat(path, { daemon: 2000, wa: 1500 });
    expect(JSON.parse(await readFile(path, 'utf8'))).toEqual({ daemon: 2000, wa: 1500 });
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/heartbeat.test.ts`
Expected: FAIL — `Cannot find module '../src/health/heartbeat.ts'`.

- [ ] **Step 3: Write the implementation**

Create `src/health/heartbeat.ts`:

```ts
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';

/**
 * Liveness stamps the daemon rewrites on every health-monitor tick, and the
 * Docker HEALTHCHECK reads from outside the process.
 *
 * Deliberately a plain JSON file rather than a row in the sqlite db: the
 * healthcheck runs every 60s for the life of the container, and loading
 * better-sqlite3's native binding each time costs far more than reading a
 * hundred bytes. A second writer on the WAL would buy nothing either.
 *
 * Two keys, and no Immich key on purpose. Immich being unreachable is healthy
 * queueing — the outbox exists precisely so it is survivable — and stamping it
 * here would let an Immich outage mark the container `unhealthy`. A wedged
 * drain surfaces through ALERT_OUTBOX_AGE_MS instead.
 */
export interface Heartbeat {
  /** Epoch ms of the last health-monitor tick. Proves the event loop is alive. */
  daemon: number;
  /** Epoch ms of the last WhatsApp message or connection-open; null before the first. */
  wa: number | null;
}

/** Write atomically: a healthcheck reading mid-write must never see half a file. */
export async function writeHeartbeat(path: string, beat: Heartbeat): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const tmpPath = `${path}.tmp`;
  await writeFile(tmpPath, JSON.stringify(beat));
  await rename(tmpPath, path);
}

/**
 * Read the current beat, or null when it is missing, truncated, or malformed.
 * Never throws: the only caller that matters is the healthcheck, and "cannot
 * read the beat" and "the beat is stale" must produce the same verdict —
 * unhealthy — not an unhandled crash inside the healthcheck process.
 */
export async function readHeartbeat(path: string): Promise<Heartbeat | null> {
  let raw: string;
  try {
    raw = await readFile(path, 'utf8');
  } catch {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as Partial<Heartbeat>;
    if (typeof parsed?.daemon !== 'number') return null;
    const wa = typeof parsed.wa === 'number' ? parsed.wa : null;
    return { daemon: parsed.daemon, wa };
  } catch {
    return null;
  }
}
```

In `src/config.ts`, add after `getOutboxDir()`:

```ts
/** Path to the liveness file the Docker healthcheck reads (see src/health/heartbeat.ts). */
export function getHealthFile(): string {
  ensureDotenv();
  return process.env.HEALTH_FILE ?? './data/health.json';
}

/** Healthcheck tuning. Default per the Phase 2 design spec. */
export function getHealthSettings(): { staleMs: number } {
  ensureDotenv();
  return { staleMs: intEnv('HEALTH_STALE_MS', 3_600_000) };
}
```

In the same file, extend `outboxGuards()` to guard the health file too — the startup sweep deletes every unrecognised regular file in `OUTBOX_DIR`, and `./data/health.json` is a sibling of `synced.db` under the same shipped default parent:

```ts
export function outboxGuards(): OverlapGuard[] {
  return [
    { label: 'DEDUP_DB', path: getDedupDb() },
    { label: 'WA_AUTH_DIR', path: getWaAuthDir() },
    { label: 'HEALTH_FILE', path: getHealthFile() },
  ];
}
```

Update the `outboxGuards` doc comment's opening sentence to read `the dedup db file, the WhatsApp auth dir, and the health file` so it does not go stale.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npx vitest run tests/heartbeat.test.ts tests/config.test.ts`
Expected: PASS

Run: `npm test && npm run typecheck`
Expected: all tests pass, no type errors.

- [ ] **Step 5: Commit**

```bash
git add src/health/heartbeat.ts src/config.ts tests/heartbeat.test.ts
git commit -m "feat: add heartbeat file for external liveness checks

Atomic JSON write, read by the Docker healthcheck. A file rather than a
sqlite row so the check does not load a native binding every 60s. No
Immich key: an Immich outage is healthy queueing, not an unhealthy
container. HEALTH_FILE joins the OUTBOX_DIR overlap guards."
```

---

### Task 3: Docker healthcheck

The backstop for the failure the whole spec exists to prevent: a daemon that is silently not running. WhatsApp alerts cannot report a dead WhatsApp link; this can.

**Files:**
- Create: `scripts/healthcheck.ts`
- Modify: `Dockerfile` (add `HEALTHCHECK`)
- Modify: `package.json` (add `healthcheck` script)
- Test: `tests/healthcheck.test.ts`

**Interfaces:**
- Consumes: `readHeartbeat(path)` and `Heartbeat` from Task 2; `getHealthFile()`, `getHealthSettings()` from Task 2.
- Produces: `export function evaluateHealth(beat: Heartbeat | null, now: number, staleMs: number): { ok: boolean; reason: string }` in `scripts/healthcheck.ts`. Nothing else consumes it.

- [ ] **Step 1: Write the failing test**

Create `tests/healthcheck.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { evaluateHealth } from '../scripts/healthcheck.ts';

const STALE = 3_600_000;
const NOW = 10_000_000;

describe('evaluateHealth', () => {
  it('is unhealthy when no beat can be read', () => {
    expect(evaluateHealth(null, NOW, STALE).ok).toBe(false);
  });

  it('is healthy when both stamps are fresh', () => {
    const r = evaluateHealth({ daemon: NOW - 1000, wa: NOW - 2000 }, NOW, STALE);
    expect(r.ok).toBe(true);
  });

  it('is unhealthy when the daemon stamp is stale', () => {
    const r = evaluateHealth({ daemon: NOW - STALE - 1, wa: NOW }, NOW, STALE);
    expect(r.ok).toBe(false);
    expect(r.reason).toContain('daemon');
  });

  it('is unhealthy when the wa stamp is stale', () => {
    const r = evaluateHealth({ daemon: NOW, wa: NOW - STALE - 1 }, NOW, STALE);
    expect(r.ok).toBe(false);
    expect(r.reason).toContain('wa');
  });

  it('is healthy when wa is null — WhatsApp has not connected yet this boot', () => {
    expect(evaluateHealth({ daemon: NOW, wa: null }, NOW, STALE).ok).toBe(true);
  });

  it('is healthy at exactly the staleness boundary', () => {
    expect(evaluateHealth({ daemon: NOW - STALE, wa: NOW - STALE }, NOW, STALE).ok).toBe(true);
  });

  it('is healthy when a stamp is in the future — clock skew is not a fault', () => {
    expect(evaluateHealth({ daemon: NOW + 60_000, wa: NOW + 60_000 }, NOW, STALE).ok).toBe(true);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/healthcheck.test.ts`
Expected: FAIL — `Cannot find module '../scripts/healthcheck.ts'`.

- [ ] **Step 3: Write the implementation**

Create `scripts/healthcheck.ts`:

```ts
import { getHealthFile, getHealthSettings } from '../src/config.ts';
import { readHeartbeat, type Heartbeat } from '../src/health/heartbeat.ts';

/**
 * Docker HEALTHCHECK entrypoint. Exit 0 = healthy, 1 = unhealthy.
 *
 * This is the backstop for the failure mode the design doc opens with: on
 * 2026-07-29 the daemon was simply not running for six days and nothing said
 * so. A WhatsApp alert cannot report a dead WhatsApp link, so this reads the
 * heartbeat file from outside the process instead.
 *
 * It checks liveness only. Immich being unreachable is deliberately NOT
 * unhealthy — media still queues durably to disk, which is the whole point of
 * the outbox. Outbox depth and age raise a WhatsApp alert instead.
 */
export function evaluateHealth(
  beat: Heartbeat | null,
  now: number,
  staleMs: number,
): { ok: boolean; reason: string } {
  if (beat === null) return { ok: false, reason: 'no heartbeat file (daemon never started, or it is unreadable)' };

  const daemonAge = now - beat.daemon;
  if (daemonAge > staleMs) {
    return { ok: false, reason: `daemon heartbeat stale by ${daemonAge - staleMs}ms (age ${daemonAge}ms)` };
  }

  // null means WhatsApp has not connected since this boot — normal during
  // pairing and during a long reconnect backoff. The daemon stamp already
  // proves the process is alive, and failing here would make a first boot
  // (or a QR-pairing wait) look like a fault.
  if (beat.wa !== null) {
    const waAge = now - beat.wa;
    if (waAge > staleMs) {
      return { ok: false, reason: `wa heartbeat stale by ${waAge - staleMs}ms (age ${waAge}ms)` };
    }
  }

  return { ok: true, reason: 'ok' };
}

// Only run when executed directly, so the unit test can import evaluateHealth
// without the import itself calling process.exit and killing the test runner.
if (process.argv[1] && import.meta.url === `file://${process.argv[1]}`) {
  const { staleMs } = getHealthSettings();
  const beat = await readHeartbeat(getHealthFile());
  const result = evaluateHealth(beat, Date.now(), staleMs);
  console.log(result.ok ? `healthy: ${result.reason}` : `unhealthy: ${result.reason}`);
  process.exit(result.ok ? 0 : 1);
}
```

In `Dockerfile`, add immediately before the final `CMD` line:

```dockerfile
# Liveness only. Reads the heartbeat file src/health/monitor.ts rewrites; it
# never contacts Immich, because an Immich outage is healthy queueing and must
# not mark this container unhealthy. start-period covers first-boot QR pairing.
HEALTHCHECK --interval=60s --timeout=20s --start-period=180s --retries=3 \
  CMD ["./node_modules/.bin/tsx", "scripts/healthcheck.ts"]
```

In `package.json`, add to `scripts` after `"status"`:

```json
    "healthcheck": "tsx scripts/healthcheck.ts",
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npx vitest run tests/healthcheck.test.ts`
Expected: PASS

Run: `npm test && npm run typecheck`
Expected: all tests pass, no type errors.

- [ ] **Step 5: Commit**

```bash
git add scripts/healthcheck.ts Dockerfile package.json tests/healthcheck.test.ts
git commit -m "feat: add Docker healthcheck reading the heartbeat file

Backstop for the six-day silent outage: a WhatsApp alert cannot report a
dead WhatsApp link. Liveness only — Immich reachability is deliberately
not part of the verdict."
```

---

### Task 4: Alert cooldown store

Persisted so a bad night cannot produce hundreds of WhatsApp messages, and so a restart loop cannot reset the cooldown and re-alert on every boot.

**Files:**
- Create: `src/alert/alertStore.ts`
- Test: `tests/alertStore.test.ts`

**Interfaces:**
- Consumes: `openDb` from `src/sync/db.ts` (tests only); the production instance shares the daemon's connection.
- Produces:
  - `export interface AlertRecord { condition: string; lastSentAt: number }`
  - `export class AlertStore { constructor(db: Database.Database); lastSentAt(condition: string): number | null; recordSent(condition: string, at: number): void; all(): AlertRecord[] }`
  - Task 5 depends on `lastSentAt` / `recordSent`; Task 11 calls `all()` for `npm run status`.

- [ ] **Step 1: Write the failing test**

Create `tests/alertStore.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { AlertStore } from '../src/alert/alertStore.ts';

describe('AlertStore', () => {
  it('returns null for a condition never sent', () => {
    const store = new AlertStore(openDb(':memory:'));
    expect(store.lastSentAt('outbox-depth')).toBeNull();
  });

  it('records and reads back a send time', () => {
    const store = new AlertStore(openDb(':memory:'));
    store.recordSent('outbox-depth', 1000);
    expect(store.lastSentAt('outbox-depth')).toBe(1000);
  });

  it('keeps conditions independent', () => {
    const store = new AlertStore(openDb(':memory:'));
    store.recordSent('outbox-depth', 1000);
    expect(store.lastSentAt('outbox-age')).toBeNull();
  });

  it('overwrites an earlier send time for the same condition', () => {
    const store = new AlertStore(openDb(':memory:'));
    store.recordSent('outbox-depth', 1000);
    store.recordSent('outbox-depth', 2000);
    expect(store.lastSentAt('outbox-depth')).toBe(2000);
  });

  it('survives reconstruction on the same connection', () => {
    const db = openDb(':memory:');
    new AlertStore(db).recordSent('outbox-depth', 1000);
    expect(new AlertStore(db).lastSentAt('outbox-depth')).toBe(1000);
  });

  it('lists every recorded condition', () => {
    const store = new AlertStore(openDb(':memory:'));
    store.recordSent('outbox-age', 2000);
    store.recordSent('outbox-depth', 1000);
    expect(store.all()).toEqual([
      { condition: 'outbox-age', lastSentAt: 2000 },
      { condition: 'outbox-depth', lastSentAt: 1000 },
    ]);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/alertStore.test.ts`
Expected: FAIL — `Cannot find module '../src/alert/alertStore.ts'`.

- [ ] **Step 3: Write the implementation**

Create `src/alert/alertStore.ts`:

```ts
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
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run tests/alertStore.test.ts`
Expected: PASS

Run: `npm test && npm run typecheck`
Expected: all tests pass, no type errors.

- [ ] **Step 5: Commit**

```bash
git add src/alert/alertStore.ts tests/alertStore.test.ts
git commit -m "feat: persist alert cooldown state in sqlite

Cooldowns must outlive a restart: without that, a crash-looping daemon
re-alerts on every boot, making the condition most likely to be looping
into the loudest possible storm."
```

---

### Task 5: Alerter — cooldown-gated WhatsApp message

**Files:**
- Create: `src/alert/alerter.ts`
- Modify: `src/config.ts` (add `getAlertSettings`)
- Test: `tests/alerter.test.ts`

**Interfaces:**
- Consumes: `AlertStore` (`lastSentAt`, `recordSent`) from Task 4.
- Produces:
  - `export type AlertOutcome = 'sent' | 'cooldown' | 'no-socket' | 'send-failed'`
  - `export interface AlertSock { sendMessage: (jid: string, content: { text: string }) => Promise<unknown>; user?: { id?: string } | null }`
  - `export interface Alerter { raise(condition: string, text: string): Promise<AlertOutcome> }`
  - `export function createAlerter(deps: AlerterDeps): Alerter`
  - `export function selfJid(rawUserId: string | undefined): string | null`
  - `getAlertSettings(): { cooldownMs: number; targetJid?: string; outboxDepth: number; outboxAgeMs: number; reconnectFailures: number; gapThresholdMs: number }` in `src/config.ts`.
  - Tasks 7, 9, and 10 all call `raise`.

- [ ] **Step 1: Write the failing test**

Create `tests/alerter.test.ts`:

```ts
import { describe, expect, it, vi } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { AlertStore } from '../src/alert/alertStore.ts';
import { createAlerter, selfJid, type AlertSock } from '../src/alert/alerter.ts';

const COOLDOWN = 21_600_000;

function silentLogger() {
  return { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
}

function setup(opts: { now: () => number; targetJid?: string; sock?: AlertSock | null }) {
  const store = new AlertStore(openDb(':memory:'));
  const sendMessage = vi.fn(async () => ({}));
  const sock: AlertSock =
    opts.sock === undefined ? { sendMessage, user: { id: '628123456:12@s.whatsapp.net' } } : (opts.sock as AlertSock);
  const alerter = createAlerter({
    store,
    getSock: () => (opts.sock === null ? null : sock),
    targetJid: opts.targetJid,
    cooldownMs: COOLDOWN,
    logger: silentLogger(),
    now: opts.now,
  });
  return { alerter, store, sendMessage };
}

describe('selfJid', () => {
  it('strips the device suffix from a linked-device id', () => {
    expect(selfJid('628123456:12@s.whatsapp.net')).toBe('628123456@s.whatsapp.net');
  });

  it('passes through an id with no device suffix', () => {
    expect(selfJid('628123456@s.whatsapp.net')).toBe('628123456@s.whatsapp.net');
  });

  it('returns null for undefined or malformed input', () => {
    expect(selfJid(undefined)).toBeNull();
    expect(selfJid('')).toBeNull();
    expect(selfJid('no-at-sign')).toBeNull();
  });
});

describe('createAlerter', () => {
  it('sends to the bot own number when no target is configured', async () => {
    const { alerter, sendMessage } = setup({ now: () => 1000 });
    expect(await alerter.raise('outbox-depth', 'queue deep')).toBe('sent');
    expect(sendMessage).toHaveBeenCalledWith('628123456@s.whatsapp.net', { text: 'queue deep' });
  });

  it('sends to an explicit target jid when configured', async () => {
    const { alerter, sendMessage } = setup({ now: () => 1000, targetJid: '628999@s.whatsapp.net' });
    await alerter.raise('outbox-depth', 'queue deep');
    expect(sendMessage).toHaveBeenCalledWith('628999@s.whatsapp.net', { text: 'queue deep' });
  });

  it('suppresses a repeat inside the cooldown window', async () => {
    let now = 1000;
    const { alerter, sendMessage } = setup({ now: () => now });
    expect(await alerter.raise('outbox-depth', 'first')).toBe('sent');
    now += COOLDOWN - 1;
    expect(await alerter.raise('outbox-depth', 'second')).toBe('cooldown');
    expect(sendMessage).toHaveBeenCalledTimes(1);
  });

  it('re-sends once the cooldown has elapsed', async () => {
    let now = 1000;
    const { alerter, sendMessage } = setup({ now: () => now });
    await alerter.raise('outbox-depth', 'first');
    now += COOLDOWN;
    expect(await alerter.raise('outbox-depth', 'second')).toBe('sent');
    expect(sendMessage).toHaveBeenCalledTimes(2);
  });

  it('cools down each condition independently', async () => {
    const { alerter, sendMessage } = setup({ now: () => 1000 });
    await alerter.raise('outbox-depth', 'a');
    expect(await alerter.raise('outbox-age', 'b')).toBe('sent');
    expect(sendMessage).toHaveBeenCalledTimes(2);
  });

  it('reports no-socket and does not start a cooldown when WhatsApp is down', async () => {
    const { alerter, store } = setup({ now: () => 1000, sock: null });
    expect(await alerter.raise('outbox-depth', 'queue deep')).toBe('no-socket');
    expect(store.lastSentAt('outbox-depth')).toBeNull();
  });

  it('does not start a cooldown when the send itself throws', async () => {
    const store = new AlertStore(openDb(':memory:'));
    const alerter = createAlerter({
      store,
      getSock: () => ({
        sendMessage: async () => {
          throw new Error('socket closed');
        },
        user: { id: '628123456:12@s.whatsapp.net' },
      }),
      cooldownMs: COOLDOWN,
      logger: silentLogger(),
      now: () => 1000,
    });
    expect(await alerter.raise('outbox-depth', 'queue deep')).toBe('send-failed');
    expect(store.lastSentAt('outbox-depth')).toBeNull();
  });

  it('reports no-socket when the socket has no resolvable user id', async () => {
    const { alerter } = setup({
      now: () => 1000,
      sock: { sendMessage: vi.fn(async () => ({})), user: null },
    });
    expect(await alerter.raise('outbox-depth', 'queue deep')).toBe('no-socket');
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/alerter.test.ts`
Expected: FAIL — `Cannot find module '../src/alert/alerter.ts'`.

- [ ] **Step 3: Write the implementation**

Create `src/alert/alerter.ts`:

```ts
import type { AlertStore } from './alertStore.ts';

type AlertLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
  error: (...a: unknown[]) => void;
};

/** The slice of a Baileys WASocket this module needs; narrow so tests can fake it. */
export interface AlertSock {
  sendMessage: (jid: string, content: { text: string }) => Promise<unknown>;
  user?: { id?: string } | null;
}

export interface AlerterDeps {
  store: Pick<AlertStore, 'lastSentAt' | 'recordSent'>;
  /**
   * The live socket, or null. A getter rather than a value because the socket
   * is replaced on every reconnect (see startWaClient) — a captured reference
   * would go stale the first time the link drops.
   */
  getSock: () => AlertSock | null;
  /** ALERT_TARGET_JID; falls back to the bot's own number. */
  targetJid?: string;
  cooldownMs: number;
  logger: AlertLogger;
  /** Injectable clock (tests). */
  now?: () => number;
}

export type AlertOutcome = 'sent' | 'cooldown' | 'no-socket' | 'send-failed';

export interface Alerter {
  /**
   * Send `text` unless `condition` fired within the cooldown window.
   * Never throws — every caller is a timer tick or a socket event handler,
   * where an escaping rejection would kill the process under `restart: always`.
   */
  raise(condition: string, text: string): Promise<AlertOutcome>;
}

/**
 * Normalise a Baileys user id to a sendable JID.
 *
 * `sock.user.id` on a linked device carries a device suffix — "628123456:12
 * @s.whatsapp.net" — and sending to that literal string does not reach the
 * account. Strip everything from the ':' up to the '@'.
 */
export function selfJid(rawUserId: string | undefined): string | null {
  if (!rawUserId) return null;
  const at = rawUserId.indexOf('@');
  if (at <= 0) return null;
  const user = rawUserId.slice(0, at).split(':')[0];
  const domain = rawUserId.slice(at + 1);
  if (!user || !domain) return null;
  return `${user}@${domain}`;
}

export function createAlerter(deps: AlerterDeps): Alerter {
  const now = deps.now ?? Date.now;

  async function raise(condition: string, text: string): Promise<AlertOutcome> {
    const at = now();

    const last = deps.store.lastSentAt(condition);
    if (last !== null && at - last < deps.cooldownMs) return 'cooldown';

    const sock = deps.getSock();
    const jid = deps.targetJid ?? selfJid(sock?.user?.id);
    if (!sock || !jid) {
      // Not an error: WhatsApp being down is itself one of the conditions
      // worth alerting about, and the Docker healthcheck covers it. Crucially,
      // no cooldown is recorded, so the alert still lands once the link is back.
      deps.logger.warn({ condition }, 'alert not sent: no WhatsApp socket');
      return 'no-socket';
    }

    try {
      await sock.sendMessage(jid, { text });
    } catch (err) {
      // Also no cooldown recorded — a failed send must be retried on the next
      // tick, not silently swallowed for the next six hours.
      deps.logger.warn(
        { condition, err: err instanceof Error ? err.message : String(err) },
        'alert send failed',
      );
      return 'send-failed';
    }

    deps.store.recordSent(condition, at);
    deps.logger.info({ condition }, 'alert sent');
    return 'sent';
  }

  return { raise };
}
```

In `src/config.ts`, add after `getHealthSettings()`:

```ts
/** Alerting thresholds and cooldown. Defaults per the Phase 2 design spec. */
export function getAlertSettings(): {
  cooldownMs: number;
  /** ALERT_TARGET_JID; undefined means "the bot's own number". */
  targetJid?: string;
  outboxDepth: number;
  outboxAgeMs: number;
  reconnectFailures: number;
  /** Per-group silence before a gap is reported. */
  gapThresholdMs: number;
} {
  ensureDotenv();
  return {
    cooldownMs: intEnv('ALERT_COOLDOWN_MS', 21_600_000),
    targetJid: process.env.ALERT_TARGET_JID?.trim() || undefined,
    outboxDepth: intEnv('ALERT_OUTBOX_DEPTH', 50),
    outboxAgeMs: intEnv('ALERT_OUTBOX_AGE_MS', 7_200_000),
    reconnectFailures: intEnv('ALERT_RECONNECT_FAILURES', 10),
    gapThresholdMs: intEnv('CATCHUP_GAP_THRESHOLD_MS', 3_600_000),
  };
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npx vitest run tests/alerter.test.ts tests/config.test.ts`
Expected: PASS

Run: `npm test && npm run typecheck`
Expected: all tests pass, no type errors.

- [ ] **Step 5: Commit**

```bash
git add src/alert/alerter.ts src/config.ts tests/alerter.test.ts
git commit -m "feat: add cooldown-gated WhatsApp alerter

Sends over the bot's existing socket, so no new infrastructure. A failed
or unsendable alert records no cooldown, so it still lands once the link
is back. sock.user.id carries a device suffix that must be stripped
before it is a sendable JID."
```

---

### Task 6: Gap detection

Answers "which whitelisted group has gone quiet?" — the visibility half of Gap A. Phase 3 will use the same signal to drive recovery; this task only reports.

**Files:**
- Create: `src/sync/gapDetect.ts`
- Test: `tests/gapDetect.test.ts`

**Interfaces:**
- Consumes: `synced.captured_at` from Task 1.
- Produces:
  - `export function lastCapturedByGroup(db: Database.Database): Map<string, number>`
  - `export interface GroupGap { groupJid: string; lastKnownAt: number; silentForMs: number }`
  - `export function detectGaps(opts: { lastKnown: Map<string, number>; groupJids: string[]; now: number; thresholdMs: number }): GroupGap[]`
  - `export function describeGap(gap: GroupGap): string`
  - Task 10 calls all three.

- [ ] **Step 1: Write the failing test**

Create `tests/gapDetect.test.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { describeGap, detectGaps, lastCapturedByGroup } from '../src/sync/gapDetect.ts';

const HOUR = 3_600_000;

function setup() {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  return { db, dedup, outbox };
}

describe('lastCapturedByGroup', () => {
  it('is empty when nothing has been seen', () => {
    const { db } = setup();
    expect(lastCapturedByGroup(db).size).toBe(0);
  });

  it('uses captured_at from synced rows', () => {
    const { db, dedup } = setup();
    dedup.markDone('g@g.us:A1', 'g@g.us', 'asset-1', 'created', 5000);
    expect(lastCapturedByGroup(db).get('g@g.us')).toBe(5000);
  });

  it('falls back to created_at for pre-migration rows with a null captured_at', () => {
    const { db } = setup();
    db.prepare(
      `INSERT INTO synced (message_id, group_jid, immich_asset_id, status, created_at, captured_at)
       VALUES ('g@g.us:OLD', 'g@g.us', 'asset-old', 'created', 7000, NULL)`,
    ).run();
    expect(lastCapturedByGroup(db).get('g@g.us')).toBe(7000);
  });

  it('counts still-pending outbox rows, so a stuck queue is not read as silence', () => {
    const { db, outbox } = setup();
    outbox.enqueue({
      messageId: 'g@g.us:A1',
      groupJid: 'g@g.us',
      albumName: '',
      filePath: '/tmp/x',
      fileName: 'IMG.jpg',
      mimeType: 'image/jpeg',
      capturedAt: 9000,
      createdAt: 9000,
    });
    expect(lastCapturedByGroup(db).get('g@g.us')).toBe(9000);
  });

  it('takes the newest across both tables', () => {
    const { db, dedup, outbox } = setup();
    dedup.markDone('g@g.us:A1', 'g@g.us', 'asset-1', 'created', 5000);
    outbox.enqueue({
      messageId: 'g@g.us:A2',
      groupJid: 'g@g.us',
      albumName: '',
      filePath: '/tmp/x',
      fileName: 'IMG.jpg',
      mimeType: 'image/jpeg',
      capturedAt: 9000,
      createdAt: 9000,
    });
    expect(lastCapturedByGroup(db).get('g@g.us')).toBe(9000);
  });

  it('keeps groups separate', () => {
    const { db, dedup } = setup();
    dedup.markDone('a@g.us:1', 'a@g.us', 'asset-1', 'created', 5000);
    dedup.markDone('b@g.us:1', 'b@g.us', 'asset-2', 'created', 8000);
    const map = lastCapturedByGroup(db);
    expect(map.get('a@g.us')).toBe(5000);
    expect(map.get('b@g.us')).toBe(8000);
  });
});

describe('detectGaps', () => {
  const groupJids = ['a@g.us', 'b@g.us'];

  it('reports a group silent for longer than the threshold', () => {
    const lastKnown = new Map([['a@g.us', 0]]);
    const gaps = detectGaps({ lastKnown, groupJids, now: HOUR + 1, thresholdMs: HOUR });
    expect(gaps).toEqual([{ groupJid: 'a@g.us', lastKnownAt: 0, silentForMs: HOUR + 1 }]);
  });

  it('does not report a group inside the threshold', () => {
    const lastKnown = new Map([['a@g.us', 0]]);
    expect(detectGaps({ lastKnown, groupJids, now: HOUR, thresholdMs: HOUR })).toEqual([]);
  });

  it('never reports a group that has never synced anything', () => {
    // A newly whitelisted group has no baseline, so infinite silence is not
    // evidence of a gap — it would otherwise alert forever.
    expect(detectGaps({ lastKnown: new Map(), groupJids, now: HOUR * 100, thresholdMs: HOUR })).toEqual([]);
  });

  it('ignores groups that are no longer whitelisted', () => {
    const lastKnown = new Map([['gone@g.us', 0]]);
    expect(detectGaps({ lastKnown, groupJids, now: HOUR * 100, thresholdMs: HOUR })).toEqual([]);
  });

  it('reports the longest silence first', () => {
    const lastKnown = new Map([
      ['a@g.us', HOUR * 5],
      ['b@g.us', 0],
    ]);
    const gaps = detectGaps({ lastKnown, groupJids, now: HOUR * 10, thresholdMs: HOUR });
    expect(gaps.map((g) => g.groupJid)).toEqual(['b@g.us', 'a@g.us']);
  });
});

describe('describeGap', () => {
  it('renders hours and an ISO last-seen time', () => {
    const text = describeGap({ groupJid: 'a@g.us', lastKnownAt: 0, silentForMs: HOUR * 3 });
    expect(text).toContain('a@g.us');
    expect(text).toContain('3h');
    expect(text).toContain(new Date(0).toISOString());
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/gapDetect.test.ts`
Expected: FAIL — `Cannot find module '../src/sync/gapDetect.ts'`.

- [ ] **Step 3: Write the implementation**

Create `src/sync/gapDetect.ts`:

```ts
import type Database from 'better-sqlite3';

/**
 * Newest known WhatsApp send-time per group, across `synced` ∪ `outbox`.
 *
 * Both tables must be counted. A message sitting in the outbox during an
 * Immich outage has been *received* — reading only `synced` would call that
 * silence and report a gap that does not exist, which is precisely the
 * conflation Phase 1 removed from the startup path.
 *
 * Takes the raw connection rather than living on DedupStore or OutboxStore:
 * the query spans both tables and belongs to neither. Requires both stores to
 * have been constructed on this connection first (src/index.ts does that).
 *
 * `COALESCE(captured_at, created_at)` covers rows written before the
 * captured_at migration; their sync time is later than their send time, which
 * biases toward under-reporting a gap rather than inventing one.
 */
export function lastCapturedByGroup(db: Database.Database): Map<string, number> {
  const rows = db
    .prepare(
      `SELECT group_jid, MAX(at) AS at FROM (
         SELECT group_jid, COALESCE(captured_at, created_at) AS at FROM synced
         UNION ALL
         SELECT group_jid, captured_at AS at FROM outbox
       )
       GROUP BY group_jid`,
    )
    .all() as { group_jid: string; at: number }[];
  return new Map(rows.map((r) => [r.group_jid, r.at]));
}

/** A whitelisted group that used to deliver media and has since gone quiet. */
export interface GroupGap {
  groupJid: string;
  /** Newest send-time known for this group. */
  lastKnownAt: number;
  /** How long the group has been silent, in ms. */
  silentForMs: number;
}

/**
 * Whitelisted groups whose silence exceeds `thresholdMs`, longest first.
 *
 * A group with no entry in `lastKnown` is never reported. It has no baseline —
 * a newly whitelisted group, or one that has genuinely never posted media,
 * would otherwise look infinitely silent and alert on every reconnect forever.
 * A gap means "this used to work and stopped", which requires a before.
 */
export function detectGaps(opts: {
  lastKnown: Map<string, number>;
  groupJids: string[];
  now: number;
  thresholdMs: number;
}): GroupGap[] {
  const gaps: GroupGap[] = [];
  for (const groupJid of opts.groupJids) {
    const lastKnownAt = opts.lastKnown.get(groupJid);
    if (lastKnownAt === undefined) continue;
    const silentForMs = opts.now - lastKnownAt;
    if (silentForMs <= opts.thresholdMs) continue;
    gaps.push({ groupJid, lastKnownAt, silentForMs });
  }
  return gaps.sort((a, b) => b.silentForMs - a.silentForMs);
}

/** One human-readable line for an alert body. */
export function describeGap(gap: GroupGap): string {
  const hours = Math.floor(gap.silentForMs / 3_600_000);
  return `${gap.groupJid}: no media for ${hours}h (last seen ${new Date(gap.lastKnownAt).toISOString()})`;
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npx vitest run tests/gapDetect.test.ts`
Expected: PASS

Run: `npm test && npm run typecheck`
Expected: all tests pass, no type errors.

- [ ] **Step 5: Commit**

```bash
git add src/sync/gapDetect.ts tests/gapDetect.test.ts
git commit -m "feat: detect per-group silence across synced and outbox

Counting both tables matters: a message queued during an Immich outage
has been received, and reading only synced would report a gap that does
not exist. A group with no history is never a gap — it has no baseline."
```

---

### Task 7: Health monitor

The timer that ties the previous tasks together: writes the heartbeat and evaluates outbox thresholds.

**Files:**
- Create: `src/health/monitor.ts`
- Modify: `src/config.ts` (add `getHealthMonitorSettings`)
- Test: `tests/healthMonitor.test.ts`

**Interfaces:**
- Consumes: `writeHeartbeat` (Task 2), `Alerter.raise` (Task 5), `OutboxStore.snapshot` (existing, `src/sync/outboxStore.ts:164`).
- Produces:
  - `export interface HealthMonitor { stop(): void; tick(): Promise<void> }`
  - `export function startHealthMonitor(deps: HealthMonitorDeps): HealthMonitor`
  - `getHealthMonitorSettings(): { intervalMs: number }` in `src/config.ts`.
  - Task 10 constructs it.

- [ ] **Step 1: Write the failing test**

Create `tests/healthMonitor.test.ts`:

```ts
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { readHeartbeat } from '../src/health/heartbeat.ts';
import { startHealthMonitor } from '../src/health/monitor.ts';
import type { OutboxSnapshot } from '../src/sync/outboxStore.ts';

const dirs: string[] = [];
function tmp(): string {
  const d = mkdtempSync(join(tmpdir(), 'monitor-test-'));
  dirs.push(d);
  return d;
}
afterEach(() => {
  for (const d of dirs.splice(0)) rmSync(d, { recursive: true, force: true });
});

const EMPTY: OutboxSnapshot = { depth: 0, oldestPendingAgeMs: null, maxAttempts: 0, lastError: null };

function setup(snapshot: OutboxSnapshot, opts: { now?: number; waActivity?: number | null } = {}) {
  const heartbeatPath = join(tmp(), 'health.json');
  const raise = vi.fn(async () => 'sent' as const);
  const monitor = startHealthMonitor({
    outbox: { snapshot: vi.fn(() => snapshot) },
    alerter: { raise },
    heartbeatPath,
    waActivity: () => (opts.waActivity === undefined ? 500 : opts.waActivity),
    thresholds: { outboxDepth: 50, outboxAgeMs: 7_200_000 },
    intervalMs: 60_000,
    logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
    now: () => opts.now ?? 1000,
    autoStart: false,
  });
  return { monitor, raise, heartbeatPath };
}

describe('startHealthMonitor', () => {
  it('writes both heartbeat stamps on every tick', async () => {
    const { monitor, heartbeatPath } = setup(EMPTY, { now: 1000, waActivity: 500 });
    await monitor.tick();
    expect(await readHeartbeat(heartbeatPath)).toEqual({ daemon: 1000, wa: 500 });
  });

  it('writes a null wa stamp before WhatsApp has ever connected', async () => {
    const { monitor, heartbeatPath } = setup(EMPTY, { now: 1000, waActivity: null });
    await monitor.tick();
    expect(await readHeartbeat(heartbeatPath)).toEqual({ daemon: 1000, wa: null });
  });

  it('raises no alert on an empty outbox', async () => {
    const { monitor, raise } = setup(EMPTY);
    await monitor.tick();
    expect(raise).not.toHaveBeenCalled();
  });

  it('raises outbox-depth once the depth threshold is reached', async () => {
    const { monitor, raise } = setup({ depth: 50, oldestPendingAgeMs: 1000, maxAttempts: 2, lastError: 'ECONNREFUSED' });
    await monitor.tick();
    expect(raise).toHaveBeenCalledTimes(1);
    expect(raise.mock.calls[0][0]).toBe('outbox-depth');
    expect(raise.mock.calls[0][1]).toContain('50');
    expect(raise.mock.calls[0][1]).toContain('ECONNREFUSED');
  });

  it('raises outbox-age once the age threshold is reached', async () => {
    const { monitor, raise } = setup({ depth: 1, oldestPendingAgeMs: 7_200_000, maxAttempts: 9, lastError: '503' });
    await monitor.tick();
    expect(raise).toHaveBeenCalledTimes(1);
    expect(raise.mock.calls[0][0]).toBe('outbox-age');
  });

  it('raises both conditions when both are breached', async () => {
    const { monitor, raise } = setup({ depth: 500, oldestPendingAgeMs: 99_000_000, maxAttempts: 30, lastError: '503' });
    await monitor.tick();
    expect(raise.mock.calls.map((c) => c[0]).sort()).toEqual(['outbox-age', 'outbox-depth']);
  });

  it('still writes the heartbeat when reading the outbox throws', async () => {
    const heartbeatPath = join(tmp(), 'health.json');
    const monitor = startHealthMonitor({
      outbox: {
        snapshot: () => {
          throw new Error('database is locked');
        },
      },
      alerter: { raise: vi.fn(async () => 'sent' as const) },
      heartbeatPath,
      waActivity: () => 500,
      thresholds: { outboxDepth: 50, outboxAgeMs: 7_200_000 },
      intervalMs: 60_000,
      logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
      now: () => 1000,
      autoStart: false,
    });
    await expect(monitor.tick()).resolves.toBeUndefined();
    expect(await readHeartbeat(heartbeatPath)).toEqual({ daemon: 1000, wa: 500 });
  });

  it('does not reject when the alerter throws', async () => {
    const heartbeatPath = join(tmp(), 'health.json');
    const monitor = startHealthMonitor({
      outbox: { snapshot: () => ({ depth: 99, oldestPendingAgeMs: 1, maxAttempts: 1, lastError: null }) },
      alerter: {
        raise: async () => {
          throw new Error('boom');
        },
      },
      heartbeatPath,
      waActivity: () => 500,
      thresholds: { outboxDepth: 50, outboxAgeMs: 7_200_000 },
      intervalMs: 60_000,
      logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn() },
      now: () => 1000,
      autoStart: false,
    });
    await expect(monitor.tick()).resolves.toBeUndefined();
  });

  it('stop() is safe to call when autoStart is false', () => {
    const { monitor } = setup(EMPTY);
    expect(() => monitor.stop()).not.toThrow();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/healthMonitor.test.ts`
Expected: FAIL — `Cannot find module '../src/health/monitor.ts'`.

- [ ] **Step 3: Write the implementation**

Create `src/health/monitor.ts`:

```ts
import type { Alerter } from '../alert/alerter.ts';
import type { OutboxStore } from '../sync/outboxStore.ts';
import { writeHeartbeat } from './heartbeat.ts';

type MonitorLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
  error: (...a: unknown[]) => void;
};

export interface HealthMonitorDeps {
  outbox: Pick<OutboxStore, 'snapshot'>;
  alerter: Pick<Alerter, 'raise'>;
  heartbeatPath: string;
  /** Epoch ms of the last WhatsApp activity, or null before the first connect. */
  waActivity: () => number | null;
  thresholds: { outboxDepth: number; outboxAgeMs: number };
  intervalMs: number;
  logger: MonitorLogger;
  /** Injectable clock (tests). */
  now?: () => number;
  /** Start the timer loop. Tests drive tick() by hand instead. */
  autoStart?: boolean;
}

export interface HealthMonitor {
  stop(): void;
  tick(): Promise<void>;
}

/**
 * Periodic liveness and backlog check.
 *
 * The heartbeat is written FIRST and unconditionally, before anything that can
 * throw. A wedged sqlite read or a failing alert must not also make the
 * container look dead — those are separate faults with separate signals.
 *
 * Nothing here touches Immich or the WhatsApp connection state. Depth and age
 * are read from columns the outbox already maintains, so a stuck drain is
 * inferred from its backlog rather than probed for.
 */
export function startHealthMonitor(deps: HealthMonitorDeps): HealthMonitor {
  const now = deps.now ?? Date.now;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let stopped = false;

  async function tick(): Promise<void> {
    const at = now();

    try {
      await writeHeartbeat(deps.heartbeatPath, { daemon: at, wa: deps.waActivity() });
    } catch (err) {
      deps.logger.warn(
        { err: err instanceof Error ? err.message : String(err), path: deps.heartbeatPath },
        'health: heartbeat write failed',
      );
    }

    try {
      const snapshot = deps.outbox.snapshot(at);

      if (snapshot.depth >= deps.thresholds.outboxDepth) {
        await deps.alerter.raise(
          'outbox-depth',
          `wa-immich-sync: ${snapshot.depth} items queued and not yet in Immich ` +
            `(threshold ${deps.thresholds.outboxDepth}). Nothing is lost — they retry with backoff — ` +
            `but Immich has not been accepting uploads. Last error: ${snapshot.lastError ?? 'none'}`,
        );
      }

      const ageMs = snapshot.oldestPendingAgeMs;
      if (ageMs !== null && ageMs >= deps.thresholds.outboxAgeMs) {
        await deps.alerter.raise(
          'outbox-age',
          `wa-immich-sync: oldest queued item is ${Math.round(ageMs / 3_600_000)}h old ` +
            `(${snapshot.depth} queued, ${snapshot.maxAttempts} attempts). ` +
            `Last error: ${snapshot.lastError ?? 'none'}`,
        );
      }
    } catch (err) {
      // better-sqlite3 is synchronous, so snapshot() throws in-band; the
      // alerter is async but documented never to throw. Contain both here so
      // the timer loop below always re-arms.
      deps.logger.error(
        { err: err instanceof Error ? err.message : String(err) },
        'health: monitor tick failed',
      );
    }
  }

  function loop(): void {
    if (stopped) return;
    void tick().finally(() => {
      if (!stopped) timer = setTimeout(loop, deps.intervalMs);
    });
  }

  function stop(): void {
    stopped = true;
    if (timer) clearTimeout(timer);
    timer = null;
  }

  if (deps.autoStart !== false) timer = setTimeout(loop, deps.intervalMs);

  return { stop, tick };
}
```

In `src/config.ts`, add after `getHealthSettings()`:

```ts
/**
 * How often the health monitor stamps the heartbeat and checks the outbox.
 * Must be comfortably below HEALTH_STALE_MS, or a healthy daemon looks stale
 * between ticks.
 */
export function getHealthMonitorSettings(): { intervalMs: number } {
  ensureDotenv();
  return { intervalMs: intEnv('HEALTH_INTERVAL_MS', 60_000) };
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npx vitest run tests/healthMonitor.test.ts`
Expected: PASS

Run: `npm test && npm run typecheck`
Expected: all tests pass, no type errors.

- [ ] **Step 5: Commit**

```bash
git add src/health/monitor.ts src/config.ts tests/healthMonitor.test.ts
git commit -m "feat: add health monitor for heartbeat and outbox backlog

Heartbeat is written first and unconditionally: a wedged sqlite read or a
failing alert must not also make the container look dead. A stuck drain
is inferred from outbox depth and age, never by probing Immich."
```

---

### Task 8: Stop losing a message when staging fails

Not in the spec — found while reviewing Phase 1. `src/sync/ingest.ts:127-134` catches a `stageFile` failure, logs it, returns `'error'`, and the message is gone: no outbox row, no retry, and nothing re-delivers it. A full disk is the realistic trigger, and a long Immich outage is what fills the disk.

**Files:**
- Modify: `src/sync/ingest.ts:16-26` (deps), `:104-134` (the staging block)
- Test: `tests/ingest.test.ts`

**Interfaces:**
- Consumes: `withRetry` from `src/util/retry.ts` (existing).
- Produces: two new optional `IngestDeps` fields —
  - `stageRetries?: number` (default 3)
  - `onCaptureFailed?: (info: { messageId: string; groupJid: string; error: string }) => void`
  - Task 10 wires `onCaptureFailed` to the alerter.

- [ ] **Step 1: Write the failing test**

In `tests/ingest.test.ts`, add this mock at the top of the file, directly below the existing imports. Without it, `vi.spyOn` on a plain ESM export has no effect:

```ts
vi.mock('../src/sync/staging.ts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../src/sync/staging.ts')>();
  return { ...actual, stageFile: vi.fn(actual.stageFile) };
});
```

Then extend the file's existing `setup()` helper — it currently takes only `Partial<AppConfig>` — so tests can inject the new deps. Replace it with:

```ts
interface SetupOpts {
  config?: Partial<AppConfig>;
  stageRetries?: number;
  onCaptureFailed?: (info: { messageId: string; groupJid: string; error: string }) => void;
  /** Replaces the real OutboxStore, for forcing an enqueue failure. */
  outboxOverride?: { has: (id: string) => boolean; enqueue: (item: never) => void };
}

function setup(opts: SetupOpts | Partial<AppConfig> = {}) {
  // Keep the old call shape working: every existing test passes a bare
  // Partial<AppConfig>, and none of them sets any SetupOpts key.
  const o: SetupOpts =
    'config' in opts || 'stageRetries' in opts || 'onCaptureFailed' in opts || 'outboxOverride' in opts
      ? (opts as SetupOpts)
      : { config: opts as Partial<AppConfig> };

  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const dir = mkdtempSync(join(tmpdir(), 'ingest-test-'));
  const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
  const sock = { sendMessage: vi.fn(async () => undefined), updateMediaMessage: vi.fn() };
  const extract = vi.fn(async () => item());

  const ing = createIngest({
    config: { ...config, ...o.config },
    dedup,
    outbox: (o.outboxOverride ?? outbox) as never,
    outboxDir: dir,
    logger,
    extract: extract as never,
    stageRetries: o.stageRetries,
    onCaptureFailed: o.onCaptureFailed,
  });
  ing.setGroups([GROUP]);
  return { ing, outbox, dedup, dir, logger, sock, extract };
}
```

Then append these tests:

```ts
describe('ingest staging failures', () => {
  it('retries a transient staging failure and still queues the media', async () => {
    const staging = await import('../src/sync/staging.ts');
    const spy = vi.spyOn(staging, 'stageFile');
    spy.mockRejectedValueOnce(Object.assign(new Error('ENOSPC: no space left on device'), { code: 'ENOSPC' }));

    const { ing, outbox, sock } = setup({ stageRetries: 3 });
    const outcome = await ing.ingest(sock as never, msg());

    expect(outcome).toBe('queued');
    expect(spy).toHaveBeenCalledTimes(2);
    expect(outbox.has('g@g.us:A1')).toBe(true);
    spy.mockRestore();
  });

  it('reports a capture failure once every retry is exhausted', async () => {
    const staging = await import('../src/sync/staging.ts');
    const spy = vi
      .spyOn(staging, 'stageFile')
      .mockRejectedValue(Object.assign(new Error('ENOSPC: no space left on device'), { code: 'ENOSPC' }));

    const onCaptureFailed = vi.fn();
    const { ing, outbox, sock } = setup({ onCaptureFailed, stageRetries: 1 });
    const outcome = await ing.ingest(sock as never, msg());

    expect(outcome).toBe('error');
    expect(spy).toHaveBeenCalledTimes(2);
    expect(outbox.has('g@g.us:A1')).toBe(false);
    expect(onCaptureFailed).toHaveBeenCalledTimes(1);
    expect(onCaptureFailed.mock.calls[0][0]).toMatchObject({
      messageId: 'g@g.us:A1',
      groupJid: 'g@g.us',
    });
    expect(onCaptureFailed.mock.calls[0][0].error).toContain('ENOSPC');
    spy.mockRestore();
  });

  it('does not retry an enqueue failure, and reports it', async () => {
    const onCaptureFailed = vi.fn();
    const { ing, sock } = setup({
      onCaptureFailed,
      outboxOverride: {
        has: () => false,
        enqueue: () => {
          throw new Error('NOT NULL constraint failed: outbox.captured_at');
        },
      },
    });
    expect(await ing.ingest(sock as never, msg())).toBe('error');
    expect(onCaptureFailed).toHaveBeenCalledTimes(1);
  });

  it('does not let a throwing onCaptureFailed escape ingest', async () => {
    const staging = await import('../src/sync/staging.ts');
    const spy = vi.spyOn(staging, 'stageFile').mockRejectedValue(new Error('ENOSPC'));
    const { ing, sock } = setup({
      stageRetries: 0,
      onCaptureFailed: () => {
        throw new Error('alerter exploded');
      },
    });
    await expect(ing.ingest(sock as never, msg())).resolves.toBe('error');
    spy.mockRestore();
  });
});
```

Note the retry timing: `withRetry` sleeps `baseDelayMs` (500ms) between attempts, so the `stageRetries: 3` test takes ~3.5s of real time. That is within vitest's default 5s per-test timeout, but do not raise `stageRetries` in a test without also raising the timeout.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npx vitest run tests/ingest.test.ts`
Expected: FAIL — the retry test sees `stageFile` called once and the outcome `'error'`; `onCaptureFailed` is never called.

- [ ] **Step 3: Write the implementation**

In `src/sync/ingest.ts`, change the staging import so a test spy is observable:

```ts
import * as staging from './staging.ts';
```

(remove the existing `import { stageFile } from './staging.ts';`)

Add to `IngestDeps`:

```ts
  /**
   * Extra attempts for the staging write. ENOSPC, EACCES on a not-yet-ready
   * bind mount, and EBUSY are all commonly transient; a single attempt turned
   * every one of them into permanent media loss. Defaults to 3.
   */
  stageRetries?: number;
  /**
   * Called when a message could not be captured at all — the bytes were
   * downloaded but never reached disk or the queue, so nothing will retry and
   * nothing will re-deliver it. This is the one loss the outbox cannot absorb,
   * so it must be reported rather than logged and forgotten. Passed as a
   * callback so ingest keeps knowing nothing about WhatsApp alerting.
   */
  onCaptureFailed?: (info: { messageId: string; groupJid: string; error: string }) => void;
```

Add `import { withRetry } from '../util/retry.ts';` to the imports.

Replace the `try { ... } catch (err) { ... return 'error'; }` block (currently `src/sync/ingest.ts:104-134`) with:

```ts
    try {
      // Bytes to disk FIRST, row second. A crash between the two leaves an
      // orphan file (swept at startup), never a row without its media.
      //
      // Retried, unlike the enqueue below: a staging write fails on ENOSPC,
      // on a bind mount that is not ready yet, or on a transient EBUSY, and
      // all three clear on their own. Before this, a single failure returned
      // 'error' and the message was gone — no outbox row, so nothing retried
      // it, and nothing re-delivers it either (live upserts carry only new
      // traffic, and startBackfill pages away from it). The bytes are already
      // in memory here, so retrying costs no bandwidth.
      const filePath = await withRetry(() => staging.stageFile(deps.outboxDir, item.messageId, item.buffer), {
        retries: deps.stageRetries ?? 3,
        baseDelayMs: 500,
        maxDelayMs: 5_000,
        onRetry: (err, attempt) =>
          deps.logger.warn(
            { messageId: item.messageId, attempt, err: (err as Error).message },
            'staging failed, retrying',
          ),
      });
      try {
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
      } catch (err) {
        // The row never landed, so nothing will ever reference these bytes.
        // Drop them now rather than leaving an orphan for the startup sweep —
        // this daemon is meant to run unattended for weeks. Not retried: an
        // enqueue failure is a schema or constraint fault, and repeating an
        // identical INSERT cannot change the outcome.
        await rm(filePath, { force: true }).catch(() => {});
        throw err;
      }
      deps.logger.info({ messageId: item.messageId, group: group.name, kind: item.kind }, 'queued');
    } catch (err) {
      // Pass the whole error so pino's serializer keeps the type and stack.
      deps.logger.error(
        { err, code: (err as NodeJS.ErrnoException).code },
        `ingest failed for ${item.messageId}`,
      );
      // Report it: this is the one loss path the outbox cannot absorb.
      try {
        deps.onCaptureFailed?.({
          messageId: item.messageId,
          groupJid: item.groupJid,
          error: err instanceof Error ? err.message : String(err),
        });
      } catch (notifyErr) {
        // A broken notifier must not also swallow the outcome below.
        deps.logger.warn(
          { err: notifyErr instanceof Error ? notifyErr.message : String(notifyErr) },
          'onCaptureFailed threw',
        );
      }
      return 'error';
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npx vitest run tests/ingest.test.ts`
Expected: PASS

Run: `npm test && npm run typecheck`
Expected: all tests pass, no type errors.

- [ ] **Step 5: Commit**

```bash
git add src/sync/ingest.ts tests/ingest.test.ts
git commit -m "fix: retry and report a failed media capture instead of dropping it

A stageFile failure returned 'error' and lost the message outright: no
outbox row, so nothing retried, and nothing re-delivers it. ENOSPC,
EACCES on a cold bind mount and EBUSY all clear on their own, and the
bytes are already in memory, so retrying is free. Exhausted retries now
call onCaptureFailed rather than only writing a log line."
```

---

### Task 9: Alert on repeated WhatsApp reconnect failures

The one condition the WhatsApp channel itself can still report — a link that keeps bouncing rather than one that is fully down.

**Files:**
- Modify: `src/wa/client.ts:32-54` (options), `:117-142` (the close branch)
- Test: `tests/waClient.test.ts` (create if absent)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: one new optional `WaClientOptions` field —
  `onReconnectScheduled?: (info: { attempt: number; delayMs: number; statusCode?: number }) => void`
  Task 10 wires it to the alerter.

- [ ] **Step 1: Write the failing test**

Create `tests/waClient.test.ts` (or append to it if it exists):

```ts
import { describe, expect, it } from 'vitest';
import type { WaClientOptions } from '../src/wa/client.ts';

describe('WaClientOptions', () => {
  it('accepts an onReconnectScheduled callback', () => {
    const seen: number[] = [];
    const opts: Pick<WaClientOptions, 'onReconnectScheduled'> = {
      onReconnectScheduled: ({ attempt }) => seen.push(attempt),
    };
    opts.onReconnectScheduled?.({ attempt: 3, delayMs: 1000, statusCode: 428 });
    expect(seen).toEqual([3]);
  });
});
```

This is a type-level contract test — `npm run typecheck` is the real gate. The reconnect path itself needs a live Baileys socket to exercise and is covered by the manual verification in Task 11 rather than by a unit test.

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/waClient.test.ts && npm run typecheck`
Expected: FAIL — `Object literal may only specify known properties, and 'onReconnectScheduled' does not exist in type ...`.

- [ ] **Step 3: Write the implementation**

In `src/wa/client.ts`, add to `WaClientOptions`, immediately after `reconnectAttempt`:

```ts
  /**
   * Called each time a reconnect is scheduled, with the 1-based consecutive
   * attempt number. A link that keeps bouncing is the one WhatsApp-side fault
   * the WhatsApp alert channel can still report — a link that is fully down
   * cannot send anything, which is what the Docker healthcheck covers instead.
   */
  onReconnectScheduled?: (info: { attempt: number; delayMs: number; statusCode?: number }) => void;
```

In the `connection === 'close'` branch, immediately after the existing `opts.logger.info({ attempt: nextAttempt, delayMs }, 'scheduling WhatsApp reconnect');` line, add:

```ts
      try {
        opts.onReconnectScheduled?.({ attempt: nextAttempt, delayMs, statusCode });
      } catch (err) {
        // This handler is synchronous and inside Baileys' event emitter; a
        // throw here would escape into the emitter and skip the reconnect
        // scheduled below, turning an alerting failure into a dead link.
        opts.logger.warn(
          { err: err instanceof Error ? err.message : String(err) },
          'onReconnectScheduled threw',
        );
      }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npx vitest run tests/waClient.test.ts && npm run typecheck`
Expected: PASS, no type errors.

Run: `npm test`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/wa/client.ts tests/waClient.test.ts
git commit -m "feat: expose a reconnect-scheduled hook on the WA client

A link that keeps bouncing is the one WhatsApp-side fault the WhatsApp
alert channel can still report. The callback is contained so an alerting
failure cannot skip the reconnect itself."
```

---

### Task 10: Wire it all into the daemon

**Files:**
- Modify: `src/index.ts`
- Test: `tests/gapAlertIntegration.test.ts`

**Interfaces:**
- Consumes: everything from Tasks 1–9.
- Produces: no new exports. The daemon constructs `AlertStore`, `createAlerter`, `startHealthMonitor`, tracks `currentSock` and `lastWaActivityAt`, runs gap detection in `onReady`, and stops the monitor during shutdown.

- [ ] **Step 1: Write the failing test**

Create `tests/gapAlertIntegration.test.ts` — an end-to-end check over the real stores that gap detection and alerting agree, without needing a WhatsApp socket:

```ts
import { describe, expect, it, vi } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { AlertStore } from '../src/alert/alertStore.ts';
import { createAlerter } from '../src/alert/alerter.ts';
import { describeGap, detectGaps, lastCapturedByGroup } from '../src/sync/gapDetect.ts';

const HOUR = 3_600_000;

function silentLogger() {
  return { info: vi.fn(), warn: vi.fn(), error: vi.fn() };
}

describe('gap detection to alert, end to end', () => {
  it('alerts once for a silent group and then respects the cooldown', async () => {
    const db = openDb(':memory:');
    const dedup = new DedupStore(db);
    new OutboxStore(db);
    dedup.markDone('a@g.us:1', 'a@g.us', 'asset-1', 'created', 0);

    const sendMessage = vi.fn(async () => ({}));
    const alerter = createAlerter({
      store: new AlertStore(db),
      getSock: () => ({ sendMessage, user: { id: '628123:12@s.whatsapp.net' } }),
      cooldownMs: 6 * HOUR,
      logger: silentLogger(),
      now: () => HOUR * 10,
    });

    const gaps = detectGaps({
      lastKnown: lastCapturedByGroup(db),
      groupJids: ['a@g.us'],
      now: HOUR * 10,
      thresholdMs: HOUR,
    });
    expect(gaps).toHaveLength(1);

    expect(await alerter.raise(`gap:${gaps[0].groupJid}`, describeGap(gaps[0]))).toBe('sent');
    expect(await alerter.raise(`gap:${gaps[0].groupJid}`, describeGap(gaps[0]))).toBe('cooldown');
    expect(sendMessage).toHaveBeenCalledTimes(1);
  });

  it('does not alert when a queued outbox row proves the group is still active', async () => {
    const db = openDb(':memory:');
    new DedupStore(db);
    const outbox = new OutboxStore(db);
    outbox.enqueue({
      messageId: 'a@g.us:2',
      groupJid: 'a@g.us',
      albumName: '',
      filePath: '/tmp/x',
      fileName: 'IMG.jpg',
      mimeType: 'image/jpeg',
      capturedAt: HOUR * 10,
      createdAt: HOUR * 10,
    });

    const gaps = detectGaps({
      lastKnown: lastCapturedByGroup(db),
      groupJids: ['a@g.us'],
      now: HOUR * 10 + 1000,
      thresholdMs: HOUR,
    });
    expect(gaps).toEqual([]);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npx vitest run tests/gapAlertIntegration.test.ts`
Expected: PASS if Tasks 1, 4, 5, and 6 are complete — this test guards their composition, which is what Step 3 depends on. If it fails, fix the composition before wiring the daemon.

- [ ] **Step 3: Wire the daemon**

In `src/index.ts`, add to the config import list: `getAlertSettings`, `getHealthFile`, `getHealthMonitorSettings`. Add these imports:

```ts
import type { WASocket } from '@whiskeysockets/baileys';
import { AlertStore } from './alert/alertStore.ts';
import { createAlerter } from './alert/alerter.ts';
import { startHealthMonitor } from './health/monitor.ts';
import { describeGap, detectGaps, lastCapturedByGroup } from './sync/gapDetect.ts';
```

**3a.** After `const outbox = new OutboxStore(db);`, add:

```ts
  const alertStore = new AlertStore(db);
```

**3b.** After the `sweepOrphans` block, add the socket/activity tracking and the alerter:

```ts
  // The socket is replaced on every reconnect, so the alerter reads it through
  // a getter rather than capturing one instance.
  let currentSock: WASocket | null = null;
  // Last WhatsApp activity, in-memory. The monitor copies it into the
  // heartbeat file once per tick; stamping the file per message would mean an
  // fsync-adjacent write on every photo for no extra signal.
  let lastWaActivityAt: number | null = null;
  const noteWaActivity = (): void => {
    lastWaActivityAt = Date.now();
  };

  const alertSettings = getAlertSettings();
  const alerter = createAlerter({
    store: alertStore,
    getSock: () => currentSock,
    targetJid: alertSettings.targetJid,
    cooldownMs: alertSettings.cooldownMs,
    logger,
  });
```

**3c.** `createIngest` gains the capture-failure reporter:

```ts
  const ingest = createIngest({
    config,
    dedup,
    outbox,
    outboxDir,
    logger,
    extractDeps: { logger },
    onCaptureFailed: ({ messageId, groupJid, error }) => {
      // Fire-and-forget: ingest is on the message hot path and must not wait
      // on a WhatsApp round-trip. raise() never throws, but .catch anyway —
      // an unhandled rejection here would kill the process under
      // `restart: always`.
      void alerter
        .raise(
          'capture-failed',
          `wa-immich-sync: could not capture media from ${groupJid} (${messageId}). ` +
            `This one is NOT queued and will not retry. Error: ${error}`,
        )
        .catch((err) => logger.warn({ err: (err as Error).message }, 'capture-failed alert threw'));
    },
  });
```

**3d.** After the `drain` block and its initial `tick()`, start the monitor:

```ts
  const healthMonitorSettings = getHealthMonitorSettings();
  const healthFile = getHealthFile();
  const healthMonitor = startHealthMonitor({
    outbox,
    alerter,
    heartbeatPath: healthFile,
    waActivity: () => lastWaActivityAt,
    thresholds: {
      outboxDepth: alertSettings.outboxDepth,
      outboxAgeMs: alertSettings.outboxAgeMs,
    },
    intervalMs: healthMonitorSettings.intervalMs,
    logger,
  });
  logger.info(
    { ...healthMonitorSettings, healthFile, outboxDepth: alertSettings.outboxDepth, outboxAgeMs: alertSettings.outboxAgeMs },
    'health monitor started',
  );
  // Stamp the heartbeat immediately: the Dockerfile's start-period covers the
  // boot window, but a first tick a full interval later leaves the file absent
  // (and therefore "unhealthy") for no reason.
  void healthMonitor.tick().catch((err) => {
    logger.warn({ err: (err as Error).message }, 'health: initial tick failed');
  });
```

**3e.** In `startWaClient`'s `onMessage`, add `noteWaActivity();` as the first statement, before `await whitelistGate.wait();`.

**3f.** In `onReady`, add `currentSock = sock;` and `noteWaActivity();` as the first two statements, before the `logger.info('ready — resolving groups')` line.

**3g.** In `onReady`, after `whitelistGate.open();` and before the `if (!config.backfill) return;` line, add gap detection:

```ts
      // Gap detection. Reports only — Phase 3 adds the catch-up traversal that
      // recovers the window. Runs after the whitelist is resolved so
      // whitelistJids is populated, and before the early return below, so it
      // still runs with BACKFILL=false.
      try {
        const gaps = detectGaps({
          lastKnown: lastCapturedByGroup(db),
          groupJids: [...whitelistJids],
          now: Date.now(),
          thresholdMs: alertSettings.gapThresholdMs,
        });
        for (const gap of gaps) {
          logger.warn({ ...gap }, 'gap detected');
          // Keyed per group so one quiet group cannot cool down the alert for
          // every other group.
          await alerter.raise(
            `gap:${gap.groupJid}`,
            `wa-immich-sync: ${describeGap(gap)}. If the daemon was down, that window ` +
              'is not recovered automatically — re-import a chat export to fill it.',
          );
        }
      } catch (err) {
        logger.warn({ err: (err as Error).message }, 'gap detection failed');
      }
```

**3h.** Add the reconnect-failure alert to the `startWaClient` options object (alongside `onReady`):

```ts
    onReconnectScheduled: ({ attempt, delayMs, statusCode }) => {
      if (attempt < alertSettings.reconnectFailures) return;
      void alerter
        .raise(
          'reconnect-failures',
          `wa-immich-sync: WhatsApp has failed to reconnect ${attempt} times in a row ` +
            `(status ${statusCode ?? 'unknown'}, next try in ${Math.round(delayMs / 1000)}s). ` +
            'Media sent during this window may not be recoverable.',
        )
        .catch((err) => logger.warn({ err: (err as Error).message }, 'reconnect alert threw'));
    },
```

**3i.** In `shutdown`, add `healthMonitor.stop();` immediately after `logger.info('shutting down');`.

- [ ] **Step 4: Verify**

Run: `npm test && npm run typecheck`
Expected: all tests pass, no type errors.

Manual smoke test — confirms the wiring works against a real Immich outage without a WhatsApp pairing:

```bash
IMMICH_URL=http://127.0.0.1:9 \
IMMICH_API_KEY=dummy \
WHITELIST_GROUPS=nonexistent \
HEALTH_FILE=./data/health.json \
HEALTH_INTERVAL_MS=2000 \
npm start
```

Expected: `health monitor started` in the log; `./data/health.json` appears within ~2s containing `{"daemon":<epoch>,"wa":null}`; **no** startup abort on the unreachable Immich. Then, in a second terminal:

```bash
HEALTH_FILE=./data/health.json HEALTH_STALE_MS=3600000 npm run healthcheck; echo "exit=$?"
```

Expected: `healthy: ok` and `exit=0`. Stop the daemon, wait, and re-run with `HEALTH_STALE_MS=1000`:

Expected: `unhealthy: daemon heartbeat stale by ...` and `exit=1`.

- [ ] **Step 5: Commit**

```bash
git add src/index.ts tests/gapAlertIntegration.test.ts
git commit -m "feat: wire health monitor, alerting, and gap detection into the daemon

Gap detection runs on each reconnect, keyed per group so one quiet group
cannot cool down the alert for the others. Nothing added here gates
startup or the WhatsApp connection: an Immich outage still queues."
```

---

### Task 11: Surface it in `npm run status`, and document it

**Files:**
- Modify: `scripts/status.ts`
- Modify: `.env.example`
- Modify: `README.md`
- Test: manual (`npm run status`)

**Interfaces:**
- Consumes: `AlertStore.all()` (Task 4), `readHeartbeat` (Task 2), `getHealthFile`/`getHealthSettings` (Task 2), `lastCapturedByGroup` (Task 6).
- Produces: nothing consumed by other tasks.

- [ ] **Step 1: Extend `scripts/status.ts`**

Add to its imports:

```ts
import { getDedupDb, getHealthFile, getHealthSettings } from '../src/config.ts';
import { AlertStore } from '../src/alert/alertStore.ts';
import { readHeartbeat } from '../src/health/heartbeat.ts';
import { lastCapturedByGroup } from '../src/sync/gapDetect.ts';
```

(replace the existing `import { getDedupDb } from '../src/config.ts';` line with the first of these)

Add `const alerts = new AlertStore(db);` next to the other store constructions.

Insert before the final `dedup.close();`:

```ts
console.log();
console.log('Health');
console.log('------');
const beat = await readHeartbeat(getHealthFile());
const { staleMs } = getHealthSettings();
if (beat === null) {
  console.log('Heartbeat:            none (daemon not running, or HEALTH_FILE is elsewhere)');
} else {
  const daemonAge = Date.now() - beat.daemon;
  console.log(`Daemon heartbeat:     ${Math.round(daemonAge / 1000)}s ago${daemonAge > staleMs ? '  <-- STALE' : ''}`);
  if (beat.wa === null) {
    console.log('WhatsApp activity:    never this boot');
  } else {
    const waAge = Date.now() - beat.wa;
    console.log(`WhatsApp activity:    ${Math.round(waAge / 1000)}s ago${waAge > staleMs ? '  <-- STALE' : ''}`);
  }
}

console.log();
console.log('Last media per group (send time)');
console.log('---------------------------------');
const lastSeen = [...lastCapturedByGroup(db).entries()].sort((a, b) => a[1] - b[1]);
if (lastSeen.length === 0) console.log('  (nothing captured yet)');
for (const [jid, at] of lastSeen) {
  console.log(`  ${jid}: ${new Date(at).toISOString()} (${Math.round((Date.now() - at) / 3_600_000)}h ago)`);
}

console.log();
console.log('Alerts sent');
console.log('-----------');
const sent = alerts.all();
if (sent.length === 0) console.log('  (none)');
for (const a of sent) console.log(`  ${a.condition}: ${new Date(a.lastSentAt).toISOString()}`);
```

`readHeartbeat` is async and this script is a top-level ESM module, so top-level `await` works as written — no wrapper function needed.

- [ ] **Step 2: Run it**

Run: `npm run status`
Expected: the existing synced/outbox sections, followed by Health, per-group last-media, and Alerts sections. With no daemon running it prints `Heartbeat: none`; with the Task 10 smoke test running it prints a fresh daemon age.

- [ ] **Step 3: Document the new environment variables**

Append to `.env.example`:

```dotenv
# --- Health and alerting (Phase 2) ---
# Liveness file the Docker HEALTHCHECK reads. Must NOT be inside OUTBOX_DIR.
HEALTH_FILE=./data/health.json
# How often the daemon stamps the heartbeat and checks the outbox backlog.
HEALTH_INTERVAL_MS=60000
# Heartbeat age at which the healthcheck reports unhealthy (1 hour).
HEALTH_STALE_MS=3600000

# Where alerts are sent. Unset = the bot's own number.
# ALERT_TARGET_JID=628123456789@s.whatsapp.net
# Minimum gap between repeats of one condition (6 hours).
ALERT_COOLDOWN_MS=21600000
# Queued items before alerting that Immich is not accepting uploads.
ALERT_OUTBOX_DEPTH=50
# Age of the oldest queued item before alerting (2 hours).
ALERT_OUTBOX_AGE_MS=7200000
# Consecutive WhatsApp reconnect failures before alerting.
ALERT_RECONNECT_FAILURES=10
# Per-group silence before a gap is reported (1 hour).
CATCHUP_GAP_THRESHOLD_MS=3600000
```

- [ ] **Step 4: Document the behaviour**

Add to `README.md`, after the existing durability paragraph (the one beginning "Media is durable on disk"):

```markdown
### Knowing when it stops

Durability answers "can a photo be lost while the daemon runs". It says nothing
about the daemon not running — an outage on 2026-07-29 went unnoticed for six
days. Three signals cover that:

- **Docker healthcheck.** The daemon rewrites `HEALTH_FILE` every
  `HEALTH_INTERVAL_MS`; the container is marked `unhealthy` once that stamp is
  older than `HEALTH_STALE_MS`. Check it with `docker ps` or run it by hand with
  `npm run healthcheck`. It reports liveness only — **an unreachable Immich is
  deliberately not unhealthy**, because media still queues durably to disk.
- **WhatsApp alerts.** The bot messages `ALERT_TARGET_JID` (its own number by
  default) when the outbox passes `ALERT_OUTBOX_DEPTH` or `ALERT_OUTBOX_AGE_MS`,
  when a whitelisted group has been silent longer than
  `CATCHUP_GAP_THRESHOLD_MS`, when WhatsApp has failed to reconnect
  `ALERT_RECONNECT_FAILURES` times in a row, or when a message could not be
  captured to disk at all. Each condition is rate-limited to one message per
  `ALERT_COOLDOWN_MS`, and that cooldown is persisted, so a restart loop cannot
  turn into a notification storm.
- **`npm run status`.** Adds a Health section, last-media-per-group send times,
  and which alerts have fired.

A detected gap is **reported, not recovered** — WhatsApp does not reliably
re-deliver a window a linked device missed. Fill it by re-importing a chat
export into the backfill group.
```

Add these rows to the README environment-variable table, after the `DRAIN_MAX_DROPS_PER_TICK` row (`README.md:117`). The table's columns are `| name | required | default | description |`, with an empty second column for optional settings:

```markdown
| `HEALTH_FILE` | | `./data/health.json` | Liveness file the Docker healthcheck reads. Subject to the same overlap guard as `DEDUP_DB` and `WA_AUTH_DIR`: it must not sit inside `OUTBOX_DIR` |
| `HEALTH_INTERVAL_MS` | | `60000` | How often the daemon stamps the heartbeat and checks the outbox backlog |
| `HEALTH_STALE_MS` | | `3600000` | Heartbeat age at which the container is reported `unhealthy` |
| `ALERT_TARGET_JID` | | own number | Where WhatsApp alerts are sent |
| `ALERT_COOLDOWN_MS` | | `21600000` | Minimum gap between repeats of one alert condition |
| `ALERT_OUTBOX_DEPTH` | | `50` | Queued items before alerting that Immich is not accepting uploads |
| `ALERT_OUTBOX_AGE_MS` | | `7200000` | Age of the oldest queued item before alerting |
| `ALERT_RECONNECT_FAILURES` | | `10` | Consecutive WhatsApp reconnect failures before alerting |
| `CATCHUP_GAP_THRESHOLD_MS` | | `3600000` | Per-group silence before a gap is reported |
```

Also update the `npm run status` row in the commands table (`README.md:181`) to:

```markdown
| `npm run status` | Synced counts, outbox depth/age/errors, daemon health, last media per group, and which alerts have fired |
```

and add a row for the new script:

```markdown
| `npm run healthcheck` | Run the Docker healthcheck by hand; exits non-zero when the heartbeat is stale |
```

- [ ] **Step 5: Verify and commit**

Run: `npm test && npm run typecheck && npm run status`
Expected: all tests pass, no type errors, status prints the new sections.

```bash
git add scripts/status.ts .env.example README.md
git commit -m "docs: surface health, gap, and alert state in status and docs

Documents that a detected gap is reported rather than recovered, and that
an unreachable Immich is deliberately not an unhealthy container."
```

---

## Verification

Run after the final task:

```bash
npm test
npm run typecheck
docker compose build
docker compose up -d
sleep 200 && docker ps --format '{{.Names}}\t{{.Status}}'
```

Expected: all tests pass; no type errors; `docker ps` shows `(healthy)` once the 180s start-period elapses.

Then confirm an Immich outage does **not** make the container unhealthy — the single most important regression this phase must not introduce:

```bash
# Point IMMICH_URL at a closed port in .env, then:
docker compose up -d --force-recreate
sleep 240 && docker ps --format '{{.Names}}\t{{.Status}}'
docker compose logs --tail=40 wa-immich-sync
```

Expected: still `(healthy)`. Logs show `drain deferred` warnings and no startup abort. **If the container reports `unhealthy` here, the healthcheck has been wired to Immich and Task 3 must be corrected.**

## Out of scope (Phase 3)

Catch-up traversal — `src/sync/catchup.ts`, the separate backward cursor, generalised stall detection, and undecryptable-message reporting. This plan detects and reports a gap; it does not recover one.
