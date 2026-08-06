import { mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { openDb } from '../src/sync/db.ts';
import { DedupStore } from '../src/sync/dedupStore.ts';
import { OutboxStore } from '../src/sync/outboxStore.ts';
import { importFolder, type ImportStats } from '../src/sync/importFolder.ts';

const logger = { info: vi.fn(), warn: vi.fn() };

function makeDeps(overrides: Partial<{ albumName: string }> = {}) {
  const db = openDb(':memory:');
  const dedup = new DedupStore(db);
  const outbox = new OutboxStore(db);
  const outboxDir = mkdtempSync(join(tmpdir(), 'import-outbox-'));
  dirs.push(outboxDir);
  return { outbox, dedup, outboxDir, albumName: 'A', logger, ...overrides };
}

let dirs: string[] = [];
function tmp(files: Record<string, Buffer | string>): string {
  const d = mkdtempSync(join(tmpdir(), 'imp-'));
  dirs.push(d);
  for (const [name, content] of Object.entries(files)) writeFileSync(join(d, name), content);
  return d;
}
afterEach(() => {
  for (const d of dirs) rmSync(d, { recursive: true, force: true });
  dirs = [];
});

describe('importFolder', () => {
  it('queues every supported file instead of uploading directly', async () => {
    const db = openDb(':memory:');
    const dedup = new DedupStore(db);
    const outbox = new OutboxStore(db);
    const outboxDir = mkdtempSync(join(tmpdir(), 'import-outbox-'));
    const folder = mkdtempSync(join(tmpdir(), 'import-src-'));
    dirs.push(folder, outboxDir);
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
    dirs.push(folder, outboxDir);
    writeFileSync(join(folder, 'IMG-20240617-WA0001.jpg'), 'aaa');
    const deps = { outbox, dedup, outboxDir, albumName: 'D', logger: { info: vi.fn(), warn: vi.fn() } };

    await importFolder(folder, deps);
    const second = await importFolder(folder, deps);

    expect(second.queued).toBe(0);
    // Not yet in `synced` — only staged in the outbox from the first run.
    expect(second.skippedQueued).toBe(1);
    expect(second.skippedSynced).toBe(0);
  });

  it('skips identical content under a different filename (re-export from another person)', async () => {
    const deps = makeDeps();
    const dir = tmp({
      'IMG-20240101-WA0001.jpg': Buffer.from('PHOTO-A'),
      'IMG-20240101-WA0002.jpg': Buffer.from('PHOTO-A'), // same bytes, different name
      'IMG-20240101-WA0003.jpg': Buffer.from('PHOTO-B'),
      'notes.txt': 'ignored',
    });
    const stats = await importFolder(dir, deps);
    expect(stats.queued).toBe(2); // A once, B once
    expect(stats.skippedQueued).toBe(1);
    expect(stats.skippedType).toBe(1); // notes.txt
  });

  it('skips everything on a re-run (content already recorded)', async () => {
    const deps = makeDeps();
    const dir = tmp({ 'IMG-20240101-WA0001.jpg': Buffer.from('X') });
    await importFolder(dir, deps);
    const stats2 = await importFolder(dir, deps);
    expect(stats2.queued).toBe(0);
    expect(stats2.skippedQueued).toBe(1);
  });

  it('imports a file whose name defeats the date parser instead of erroring', async () => {
    // Neither the IMG-/VID- pattern nor the "WhatsApp Image ..." pattern
    // matches this name, so dateForFile() falls through to statSync(path).mtime.
    const deps = makeDeps();
    const dir = tmp({ 'totally-unparseable-name.jpg': Buffer.from('bytes') });
    const filePath = join(dir, 'totally-unparseable-name.jpg');
    const mtimeMs = statSync(filePath).mtimeMs;

    const stats = await importFolder(dir, deps);

    expect(stats.errors).toBe(0);
    expect(stats.queued).toBe(1);
    const rows = deps.outbox.due(Date.now(), 10);
    expect(rows).toHaveLength(1);
    // capturedAt must be a valid, finite timestamp — never NaN — and close to
    // the file's own mtime, since that's the fallback source.
    expect(Number.isFinite(rows[0]!.capturedAt)).toBe(true);
    expect(Math.abs(rows[0]!.capturedAt - mtimeMs)).toBeLessThan(60_000);
  });

  it('drops the staged file and continues to the next file when the row insert fails', async () => {
    // Ported from tests/ingest.test.ts's "deletes the staged file when the
    // row insert fails" — importFolder has the identical stage-then-enqueue
    // cleanup path (src/sync/importFolder.ts), but unlike ingest.ts had no
    // test covering it: the whole cleanup block could be deleted and this
    // suite would stay green.
    const deps = makeDeps();
    const dir = tmp({
      'IMG-20240101-WA0001.jpg': Buffer.from('first'),
      'IMG-20240101-WA0002.jpg': Buffer.from('second'),
    });

    const enqueueSpy = vi.spyOn(OutboxStore.prototype, 'enqueue').mockImplementationOnce(() => {
      throw new Error('boom');
    });

    let stats: ImportStats;
    try {
      stats = await importFolder(dir, deps);
    } finally {
      enqueueSpy.mockRestore();
    }

    expect(stats.errors).toBe(1);
    // The failure on the first file must not abort the walk — the second
    // file is still queued.
    expect(stats.queued).toBe(1);

    // The failed file's bytes must not survive as an orphan: nothing will
    // ever reference them since its row never landed. Every file actually
    // left in the staging dir must be one the outbox still references.
    const rows = deps.outbox.due(Date.now(), 10);
    const leftover = readdirSync(deps.outboxDir).filter((f) => f !== 'tmp');
    expect(leftover).toHaveLength(rows.length);
  });
});

// The `safeCapturedAtMs` unit-test block that used to live here was removed:
// its two tests restated the function's one-line body and could only fail if
// the function were deleted outright, so they added no coverage beyond what
// "imports a file whose name defeats the date parser" above already
// exercises end-to-end, through the real
// dateForFile() -> safeCapturedAtMs() -> enqueue() path.
