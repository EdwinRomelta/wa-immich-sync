import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { importFolder } from '../src/sync/importFolder.ts';

const logger = { info: vi.fn(), warn: vi.fn() };

function makeDeps() {
  const seen = new Set<string>();
  const immich = {
    uploadAsset: vi.fn(async () => ({ assetId: `a${Math.random()}`, status: 'created' as const })),
    ensureAlbum: vi.fn(async () => 'album1'),
    addToAlbum: vi.fn(async () => {}),
  };
  const dedup = {
    has: (id: string) => seen.has(id),
    markDone: (id: string) => void seen.add(id),
  };
  return { immich, dedup, albumName: 'A', logger };
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

describe('importFolder content-hash dedup', () => {
  it('skips identical content under a different filename (re-export from another person)', async () => {
    const deps = makeDeps();
    const dir = tmp({
      'IMG-20240101-WA0001.jpg': Buffer.from('PHOTO-A'),
      'IMG-20240101-WA0002.jpg': Buffer.from('PHOTO-A'), // same bytes, different name
      'IMG-20240101-WA0003.jpg': Buffer.from('PHOTO-B'),
      'notes.txt': 'ignored',
    });
    const stats = await importFolder(dir, deps);
    expect(deps.immich.uploadAsset).toHaveBeenCalledTimes(2); // A once, B once
    expect(stats.uploaded).toBe(2);
    expect(stats.skippedDedup).toBe(1);
    expect(stats.skippedType).toBe(1); // notes.txt
  });

  it('skips everything on a re-run (content already recorded)', async () => {
    const deps = makeDeps();
    const dir = tmp({ 'IMG-20240101-WA0001.jpg': Buffer.from('X') });
    await importFolder(dir, deps);
    deps.immich.uploadAsset.mockClear();
    const stats2 = await importFolder(dir, deps);
    expect(deps.immich.uploadAsset).not.toHaveBeenCalled();
    expect(stats2.skippedDedup).toBe(1);
  });
});
