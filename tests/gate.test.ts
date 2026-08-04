import { describe, expect, it } from 'vitest';
import { createGate } from '../src/util/gate.ts';

describe('createGate', () => {
  it('holds waiters until opened', async () => {
    const gate = createGate();
    let passed = false;
    const waiter = gate.wait().then(() => {
      passed = true;
    });

    await Promise.resolve();
    expect(passed).toBe(false);
    expect(gate.isOpen).toBe(false);

    gate.open();
    await waiter;
    expect(passed).toBe(true);
    expect(gate.isOpen).toBe(true);
  });

  it('releases multiple waiters and resolves immediately once open', async () => {
    const gate = createGate();
    const results: number[] = [];
    const w1 = gate.wait().then(() => results.push(1));
    const w2 = gate.wait().then(() => results.push(2));

    gate.open();
    gate.open(); // idempotent
    await Promise.all([w1, w2]);
    expect(results.sort()).toEqual([1, 2]);

    // Late waiter passes straight through.
    let late = false;
    await gate.wait().then(() => {
      late = true;
    });
    expect(late).toBe(true);
  });
});
