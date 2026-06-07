import type { WAMessageKey, WASocket } from '@whiskeysockets/baileys';

export interface AnchorEntry {
  key: WAMessageKey;
  /** Message timestamp in seconds. */
  tsSec: number;
}

/**
 * Tracks the oldest message seen per chat. This is the cursor used to page
 * WhatsApp history backwards via `fetchMessageHistory`.
 */
export class OldestAnchors {
  private readonly map = new Map<string, AnchorEntry>();

  /** Record a message; keeps the oldest (smallest timestamp) per jid. */
  note(jid: string, key: WAMessageKey | null | undefined, tsSec: number): void {
    if (!jid || !key?.id || !(tsSec > 0)) return;
    const cur = this.map.get(jid);
    if (!cur || tsSec < cur.tsSec) this.map.set(jid, { key, tsSec });
  }

  get(jid: string): AnchorEntry | undefined {
    return this.map.get(jid);
  }
}

export type BackfillStep =
  | { action: 'wait' } // no anchor yet — waiting for a seed message
  | { action: 'request'; anchor: AnchorEntry }
  | { action: 'done' };

/**
 * Per-chat backfill state machine. Each `step` is fed the chat's current
 * oldest anchor; it decides whether to request an older page, keep waiting
 * for a seed, or stop because history is exhausted (the anchor stopped
 * moving for `maxStalls` consecutive steps).
 */
export class ChatBackfill {
  private lastTs = Infinity;
  private stalls = 0;
  done = false;

  constructor(private readonly maxStalls = 3) {}

  step(anchor: AnchorEntry | undefined): BackfillStep {
    if (this.done) return { action: 'done' };
    if (!anchor) return { action: 'wait' };

    if (anchor.tsSec < this.lastTs) {
      this.lastTs = anchor.tsSec;
      this.stalls = 0;
    } else {
      this.stalls += 1;
    }

    if (this.stalls >= this.maxStalls) {
      this.done = true;
      return { action: 'done' };
    }
    return { action: 'request', anchor };
  }
}

type FetchSock = Pick<WASocket, 'fetchMessageHistory'>;

type BackfillLogger = {
  info: (...a: unknown[]) => void;
  warn: (...a: unknown[]) => void;
};

export interface BackfillOpts {
  sock: FetchSock;
  groupJids: string[];
  anchors: OldestAnchors;
  logger: BackfillLogger;
  /** Messages requested per page. */
  pageSize?: number;
  /** Delay between pump ticks (ms). Should exceed typical phone response time. */
  intervalMs?: number;
  /** Consecutive no-progress steps before a chat is declared exhausted. */
  maxStalls?: number;
  /** Safety cap on total ticks. */
  maxPages?: number;
  /** Injectable timer for tests. */
  setTimer?: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (t: ReturnType<typeof setTimeout>) => void;
}

/**
 * Drive history backfill for the given groups. Returns a handle to stop early.
 * The pump only requests pages; the resulting messages arrive asynchronously
 * via the socket's `messaging-history.set` event (wired in the caller), which
 * must call `anchors.note(...)` so the cursor advances.
 */
export function startBackfill(opts: BackfillOpts): { stop: () => void } {
  const pageSize = opts.pageSize ?? 50;
  const intervalMs = opts.intervalMs ?? 10_000;
  const maxStalls = opts.maxStalls ?? 3;
  const maxPages = opts.maxPages ?? 5000;
  const setTimer = opts.setTimer ?? setTimeout;
  const clearTimer = opts.clearTimer ?? clearTimeout;

  const chats = new Map<string, ChatBackfill>();
  for (const jid of opts.groupJids) chats.set(jid, new ChatBackfill(maxStalls));

  let pages = 0;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | null = null;

  const stop = (): void => {
    stopped = true;
    if (timer) clearTimer(timer);
    timer = null;
  };

  const tick = async (): Promise<void> => {
    if (stopped) return;
    let active = 0;

    for (const jid of opts.groupJids) {
      const chat = chats.get(jid)!;
      const step = chat.step(opts.anchors.get(jid));

      if (step.action === 'done') continue;
      active += 1;
      if (step.action === 'wait') continue; // no seed yet

      try {
        await opts.sock.fetchMessageHistory(pageSize, step.anchor.key, step.anchor.tsSec * 1000);
        opts.logger.info(
          { jid, anchor: new Date(step.anchor.tsSec * 1000).toISOString(), page: pages },
          'backfill: requested older page',
        );
      } catch (err) {
        opts.logger.warn({ jid, err: (err as Error).message }, 'backfill: fetch failed');
      }
    }

    pages += 1;
    if (active === 0) {
      opts.logger.info({ pages }, 'backfill: complete — all chats exhausted');
      stop();
      return;
    }
    if (pages >= maxPages) {
      opts.logger.warn({ pages }, 'backfill: hit max pages cap, stopping');
      stop();
      return;
    }
    timer = setTimer(() => void tick(), intervalMs);
  };

  timer = setTimer(() => void tick(), intervalMs);
  return { stop };
}
