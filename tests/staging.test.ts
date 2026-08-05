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
