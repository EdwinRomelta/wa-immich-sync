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
