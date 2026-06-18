import { describe, expect, it, vi } from 'vitest';
import { createPipeline } from '../src/sync/pipeline.ts';
import type { AppConfig, MediaItem } from '../src/types.ts';

const baseConfig: AppConfig = {
  whitelist: ['g@g.us'],
  mediaTypes: ['image', 'video'],
  backfill: true,
  albumMode: 'per-group',
  backfillGroupName: 'wa-immich-backfill',
};

const groups = [{ jid: 'g@g.us', name: 'Fam' }];
const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
const sock = { updateMediaMessage: vi.fn(), sendMessage: vi.fn(async () => {}) } as never;
const msg = (jid: string) => ({ key: { remoteJid: jid, id: '1' } }) as never;

function makePipeline(cfg: AppConfig, m: ReturnType<typeof mocks>) {
  // No-op sleep keeps retry-backed paths instant in tests.
  const p = createPipeline({ config: cfg, ...m, logger, retry: { sleep: async () => {} } });
  p.setGroups(groups);
  return p;
}

function item(): MediaItem {
  return {
    messageId: 'g@g.us:1',
    rawMessageId: '1',
    groupJid: 'g@g.us',
    groupName: 'Fam',
    kind: 'image',
    mimeType: 'image/jpeg',
    fileName: '1.jpg',
    timestamp: new Date(),
    buffer: Buffer.from('x'),
  };
}

function mocks(extractReturn: MediaItem | null) {
  return {
    immich: {
      uploadAsset: vi.fn(async () => ({ assetId: 'a1', status: 'created' as const })),
      ensureAlbum: vi.fn(async () => 'al1'),
      addToAlbum: vi.fn(async () => {}),
    },
    dedup: { has: vi.fn(() => false), markDone: vi.fn() },
    extract: vi.fn(async () => extractReturn),
  };
}

describe('pipeline', () => {
  it('skips messages from non-whitelisted groups (without extracting)', async () => {
    const m = mocks(item());
    const p = makePipeline(baseConfig, m);
    expect(await p.process(sock, msg('other@g.us'))).toBe('skipped-not-whitelisted');
    expect(m.extract).not.toHaveBeenCalled();
  });

  it('skips when the message carries no media', async () => {
    const m = mocks(null);
    const p = makePipeline(baseConfig, m);
    expect(await p.process(sock, msg('g@g.us'))).toBe('skipped-no-media');
  });

  it('skips when already deduped — before downloading (no extract)', async () => {
    const m = mocks(item());
    m.dedup.has = vi.fn(() => true);
    const p = makePipeline(baseConfig, m);
    expect(await p.process(sock, msg('g@g.us'))).toBe('skipped-dedup');
    expect(m.extract).not.toHaveBeenCalled();
    expect(m.immich.uploadAsset).not.toHaveBeenCalled();
  });

  it('uploads, adds to the per-group album, and marks done', async () => {
    const m = mocks(item());
    const p = makePipeline(baseConfig, m);
    expect(await p.process(sock, msg('g@g.us'))).toBe('uploaded');
    expect(m.immich.ensureAlbum).toHaveBeenCalledWith('Fam');
    expect(m.immich.addToAlbum).toHaveBeenCalledWith('al1', 'a1');
    expect(m.dedup.markDone).toHaveBeenCalledWith('g@g.us:1', 'g@g.us', 'a1', 'created');
  });

  it('does NOT mark done on upload error (so it retries later)', async () => {
    const m = mocks(item());
    m.immich.uploadAsset = vi.fn(async () => {
      throw new Error('immich down');
    });
    const p = makePipeline(baseConfig, m);
    expect(await p.process(sock, msg('g@g.us'))).toBe('error');
    expect(m.dedup.markDone).not.toHaveBeenCalled();
  });

  it('retries a transient upload failure then marks done', async () => {
    const m = mocks(item());
    let calls = 0;
    m.immich.uploadAsset = vi.fn(async () => {
      calls += 1;
      if (calls < 2) throw new Error('Timed Out');
      return { assetId: 'a1', status: 'created' as const };
    });
    const p = makePipeline(baseConfig, m);
    expect(await p.process(sock, msg('g@g.us'))).toBe('uploaded');
    expect(m.immich.uploadAsset).toHaveBeenCalledTimes(2);
    expect(m.dedup.markDone).toHaveBeenCalledWith('g@g.us:1', 'g@g.us', 'a1', 'created');
  });

  it('albumMode "none" skips album calls', async () => {
    const m = mocks(item());
    const p = makePipeline({ ...baseConfig, albumMode: 'none' }, m);
    expect(await p.process(sock, msg('g@g.us'))).toBe('uploaded');
    expect(m.immich.ensureAlbum).not.toHaveBeenCalled();
    expect(m.dedup.markDone).toHaveBeenCalled();
  });

  it('reacts with the configured emoji after a successful sync', async () => {
    const m = mocks(item());
    const reactSock = { updateMediaMessage: vi.fn(), sendMessage: vi.fn(async () => {}) } as never;
    const p = makePipeline({ ...baseConfig, reactionEmoji: '🔄' }, m);
    expect(await p.process(reactSock, msg('g@g.us'))).toBe('uploaded');
    expect((reactSock as { sendMessage: ReturnType<typeof vi.fn> }).sendMessage).toHaveBeenCalledWith(
      'g@g.us',
      { react: { text: '🔄', key: { remoteJid: 'g@g.us', id: '1' } } },
    );
  });

  it('does NOT react when no reactionEmoji is configured', async () => {
    const m = mocks(item());
    const reactSock = { updateMediaMessage: vi.fn(), sendMessage: vi.fn(async () => {}) } as never;
    const p = makePipeline(baseConfig, m);
    expect(await p.process(reactSock, msg('g@g.us'))).toBe('uploaded');
    expect((reactSock as { sendMessage: ReturnType<typeof vi.fn> }).sendMessage).not.toHaveBeenCalled();
  });

  it('still reports "uploaded" when the reaction fails', async () => {
    const m = mocks(item());
    const reactSock = {
      updateMediaMessage: vi.fn(),
      sendMessage: vi.fn(async () => {
        throw new Error('react down');
      }),
    } as never;
    const p = makePipeline({ ...baseConfig, reactionEmoji: '🔄' }, m);
    expect(await p.process(reactSock, msg('g@g.us'))).toBe('uploaded');
    expect(logger.warn).toHaveBeenCalled();
    expect(m.dedup.markDone).toHaveBeenCalled();
  });
});
