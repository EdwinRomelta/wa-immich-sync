import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
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

    expect(await sweepOrphans(dir, [staleRow])).toBe(1);
    expect(existsSync(orphan)).toBe(false);
  });
});
