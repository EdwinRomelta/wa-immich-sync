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
