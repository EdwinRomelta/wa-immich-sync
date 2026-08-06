import {
  chmodSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  OUTBOX_MARKER_FILE,
  ensureOutboxDirWritable,
  stageFile,
  stagedPathFor,
  sweepOrphans,
} from '../src/sync/staging.ts';

const newDir = () => mkdtempSync(join(tmpdir(), 'outbox-test-'));

/** Simulates a directory `ensureOutboxDirWritable` has already prepared. */
const markDir = (dir: string) => writeFileSync(join(dir, OUTBOX_MARKER_FILE), '');

// Restores permissions on anything chmod'd to 0o555 during a test, so vitest's
// own cleanup of the tmp dir doesn't fail trying to unlink inside it.
const readOnlyDirs: string[] = [];
afterEach(() => {
  for (const dir of readOnlyDirs.splice(0)) chmodSync(dir, 0o755);
});

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
    expect(dirname(path)).toBe(dir);
    expect(basename(path)).toMatch(/^[A-Za-z0-9@._-]+$/);
  });

  it('gives distinct message ids distinct paths even when they sanitise alike', () => {
    const dir = newDir();
    // ':' and '/' both map to '_', so the sanitised prefixes are identical.
    // Message ids come from the sending peer, so this collision is reachable.
    expect(stagedPathFor(dir, 'g@g.us:A+1')).not.toBe(stagedPathFor(dir, 'g@g.us:A/1'));
  });

  it('keeps the filename within the filesystem name limit', () => {
    const dir = newDir();
    const path = stagedPathFor(dir, `g@g.us:${'A'.repeat(4_000)}`);
    expect(basename(path).length).toBeLessThan(255);
  });

  it('leaves no temp file behind after a successful stage', async () => {
    const dir = newDir();
    await stageFile(dir, 'g@g.us:A1', Buffer.from('hello'));
    expect(readdirSync(join(dir, 'tmp'))).toEqual([]);
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
    markDir(dir);

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
    markDir(dir);

    await sweepOrphans(dir, []);

    expect(existsSync(partial)).toBe(false);
  });

  it('returns 0 on a directory that does not exist yet', async () => {
    expect(await sweepOrphans(join(newDir(), 'missing'), [])).toBe(0);
  });

  it('propagates a readdir failure that is not "not created yet"', async () => {
    const dir = newDir();
    const notADir = join(dir, 'file');
    writeFileSync(notADir, 'x');
    // A silent 0 here would let the staging directory grow forever while
    // every startup reported "nothing to sweep".
    await expect(sweepOrphans(notADir, [])).rejects.toThrow();
  });

  it('leaves a stray subdirectory alone instead of throwing', async () => {
    const dir = newDir();
    // Synology writes @eaDir into media folders; Syncthing writes .stfolder.
    const stray = join(dir, '@eaDir');
    mkdirSync(stray);
    const orphan = await stageFile(dir, 'g@g.us:GONE', Buffer.from('y'));
    markDir(dir);

    const removed = await sweepOrphans(dir, []);

    expect(removed).toBe(1);
    expect(existsSync(orphan)).toBe(false);
    expect(existsSync(stray)).toBe(true);
  });

  it('does not let a keep path from another directory shield an orphan', async () => {
    const dir = newDir();
    const other = newDir();
    const orphan = await stageFile(dir, 'g@g.us:SAME', Buffer.from('y'));
    const staleRow = join(other, basename(orphan));
    markDir(dir);

    expect(await sweepOrphans(dir, [staleRow])).toBe(1);
    expect(existsSync(orphan)).toBe(false);
  });

  it('refuses to sweep a non-empty directory with no marker', async () => {
    const dir = newDir();
    // A directory OUTBOX_DIR was accidentally repointed at — never prepared
    // by ensureOutboxDirWritable, so it has no marker — must not have its
    // contents treated as sweepable, even though sweepOrphans only deletes
    // regular files: this is the second line of defense behind the overlap
    // check, for directories the overlap check doesn't know about.
    writeFileSync(join(dir, 'some-users-file.txt'), 'not ours');

    await expect(sweepOrphans(dir, [])).rejects.toThrow(/marker/);
    expect(existsSync(join(dir, 'some-users-file.txt'))).toBe(true);
  });

  it('sweeps normally once the marker is present', async () => {
    const dir = newDir();
    const orphan = await stageFile(dir, 'g@g.us:GONE', Buffer.from('y'));
    markDir(dir);

    expect(await sweepOrphans(dir, [])).toBe(1);
    expect(existsSync(orphan)).toBe(false);
  });

  it('sweeps an empty directory even without a marker', async () => {
    const dir = newDir();
    expect(await sweepOrphans(dir, [])).toBe(0);
  });
});

describe('ensureOutboxDirWritable', () => {
  it('creates a nested directory that does not exist yet', async () => {
    const dir = join(newDir(), 'nested', 'outbox');
    await ensureOutboxDirWritable(dir);
    expect(existsSync(dir)).toBe(true);
  });

  it('leaves a marker file behind so sweepOrphans recognises the directory later', async () => {
    const dir = newDir();
    await ensureOutboxDirWritable(dir);
    expect(existsSync(join(dir, OUTBOX_MARKER_FILE))).toBe(true);
  });

  it('does not leave the write probe behind', async () => {
    const dir = newDir();
    await ensureOutboxDirWritable(dir);
    expect(existsSync(join(dir, '.write-probe'))).toBe(false);
  });

  it('rejects when the parent directory is unwritable', async () => {
    const parent = newDir();
    chmodSync(parent, 0o555);
    readOnlyDirs.push(parent);

    await expect(ensureOutboxDirWritable(join(parent, 'outbox'))).rejects.toThrow();
  });

  it('rejects a staging dir that is the same as a guarded directory', async () => {
    const dir = newDir();
    await expect(
      ensureOutboxDirWritable(dir, [{ label: 'DEDUP_DB', path: dir }]),
    ).rejects.toThrow(/overlaps/);
  });

  it('rejects OUTBOX_DIR=./data when DEDUP_DB is guarded by its own file path (real danger case)', async () => {
    // Mirrors the actual review scenario: OUTBOX_DIR pointed straight at the
    // directory the dedup db file lives in. The guard is passed the dedup
    // db's own file path (as src/index.ts does), not its parent directory.
    const dir = newDir();
    const dedupDbFile = join(dir, 'synced.db');
    writeFileSync(dedupDbFile, 'pretend sqlite bytes');

    await expect(
      ensureOutboxDirWritable(dir, [{ label: 'DEDUP_DB', path: dedupDbFile }]),
    ).rejects.toThrow(/overlaps/);
  });

  it('accepts the real shipped defaults: OUTBOX_DIR, DEDUP_DB, and WA_AUTH_DIR as siblings under ./data', async () => {
    // Regression test: an earlier version of this guard compared against
    // dirname(DEDUP_DB) instead of the dedup db's own file path, which made
    // it flag the actual default configuration (OUTBOX_DIR=./data/outbox,
    // DEDUP_DB=./data/synced.db, WA_AUTH_DIR=./data/auth — all siblings
    // under ./data) as an overlap and refuse to boot out of the box.
    const root = newDir();
    const stagingDir = join(root, 'outbox');
    const dedupDbFile = join(root, 'synced.db');
    const authDir = join(root, 'auth');
    mkdirSync(authDir);
    writeFileSync(dedupDbFile, 'pretend sqlite bytes');

    await expect(
      ensureOutboxDirWritable(stagingDir, [
        { label: 'DEDUP_DB', path: dedupDbFile },
        { label: 'WA_AUTH_DIR', path: authDir },
      ]),
    ).resolves.toBeUndefined();
  });

  it('rejects a staging dir that contains a guarded directory (OUTBOX_DIR=./data case)', async () => {
    const dir = newDir();
    const authDir = join(dir, 'auth');
    mkdirSync(authDir);

    await expect(
      ensureOutboxDirWritable(dir, [{ label: 'WA_AUTH_DIR', path: authDir }]),
    ).rejects.toThrow(/overlaps/);
  });

  it('rejects a staging dir nested inside a guarded directory (OUTBOX_DIR=./data/auth case)', async () => {
    const parent = newDir();
    const stagingDir = join(parent, 'auth');

    await expect(
      ensureOutboxDirWritable(stagingDir, [{ label: 'WA_AUTH_DIR', path: parent }]),
    ).rejects.toThrow(/overlaps/);
  });

  it('does not create the directory when an overlap is detected', async () => {
    const dir = join(newDir(), 'never-created');
    await expect(
      ensureOutboxDirWritable(dir, [{ label: 'WA_AUTH_DIR', path: dir }]),
    ).rejects.toThrow();
    expect(existsSync(dir)).toBe(false);
  });

  it('accepts a staging dir that is a sibling of guarded directories', async () => {
    const root = newDir();
    const stagingDir = join(root, 'outbox');
    const authDir = join(root, 'auth');
    mkdirSync(authDir);

    await expect(
      ensureOutboxDirWritable(stagingDir, [{ label: 'WA_AUTH_DIR', path: authDir }]),
    ).resolves.toBeUndefined();
  });
});
