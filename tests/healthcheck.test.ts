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
