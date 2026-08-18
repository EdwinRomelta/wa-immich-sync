import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';

/**
 * Liveness stamps the daemon rewrites on every health-monitor tick, and the
 * Docker HEALTHCHECK reads from outside the process.
 *
 * Deliberately a plain JSON file rather than a row in the sqlite db: the
 * healthcheck runs every 60s for the life of the container, and loading
 * better-sqlite3's native binding each time costs far more than reading a
 * hundred bytes. A second writer on the WAL would buy nothing either.
 *
 * Two keys, and no Immich key on purpose. Immich being unreachable is healthy
 * queueing — the outbox exists precisely so it is survivable — and stamping it
 * here would let an Immich outage mark the container `unhealthy`. A wedged
 * drain surfaces through ALERT_OUTBOX_AGE_MS instead.
 */
export interface Heartbeat {
  /** Epoch ms of the last health-monitor tick. Proves the event loop is alive. */
  daemon: number;
  /** Epoch ms of the last WhatsApp message or connection-open; null before the first. */
  wa: number | null;
}

/** Write atomically: a healthcheck reading mid-write must never see half a file. */
export async function writeHeartbeat(path: string, beat: Heartbeat): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const tmpPath = `${path}.tmp`;
  await writeFile(tmpPath, JSON.stringify(beat));
  await rename(tmpPath, path);
}

/**
 * Read the current beat, or null when it is missing, truncated, or malformed.
 * Never throws: the only caller that matters is the healthcheck, and "cannot
 * read the beat" and "the beat is stale" must produce the same verdict —
 * unhealthy — not an unhandled crash inside the healthcheck process.
 */
export async function readHeartbeat(path: string): Promise<Heartbeat | null> {
  let raw: string;
  try {
    raw = await readFile(path, 'utf8');
  } catch {
    return null;
  }
  try {
    const parsed = JSON.parse(raw) as Partial<Heartbeat>;
    if (typeof parsed?.daemon !== 'number') return null;
    const wa = typeof parsed.wa === 'number' ? parsed.wa : null;
    return { daemon: parsed.daemon, wa };
  } catch {
    return null;
  }
}
