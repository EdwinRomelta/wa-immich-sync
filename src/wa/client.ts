import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
  type WAMessage,
  type WASocket,
} from '@whiskeysockets/baileys';
import pino from 'pino';
import qrcode from 'qrcode-terminal';
import type { Logger } from '../logger.ts';
import { startStallWatchdog } from '../util/stallWatchdog.ts';
import { backoffDelayMs } from '../util/backoff.ts';

/**
 * A healthy Baileys link exchanges keepalive frames every ~30s. Total inbound
 * silence for this long means the socket died without a close event (zombie);
 * the server keeps pushing messages into it and they are lost forever.
 */
const STALL_TIMEOUT_MS = 10 * 60_000;
const STALL_CHECK_MS = 60_000;

/**
 * Reconnect backoff. A flat retry interval turns a sustained WhatsApp-side
 * rejection (e.g. the 405 storm on 2026-07-28) into a tight loop hammering the
 * account — which this bot number shares with other services. Back off
 * exponentially with jitter instead, and reset once a link actually opens.
 */
const RECONNECT_BASE_MS = 3_000;
const RECONNECT_MAX_MS = 5 * 60_000;
const RECONNECT_JITTER = 0.3;

export interface WaClientOptions {
  authDir: string;
  /** Request WhatsApp history sync on link (enables best-effort backfill). */
  syncFullHistory: boolean;
  logger: Logger;
  /** Called for every live message (messages.upsert, type 'notify'). */
  onMessage: (sock: WASocket, m: WAMessage) => Promise<void> | void;
  /** Called with each batch of synced history messages (messaging-history.set). */
  onHistory?: (sock: WASocket, messages: WAMessage[]) => Promise<void> | void;
  /**
   * Called once the connection opens. May be async (src/index.ts's resolves
   * groups and seeds the backfill cursor) — the call site awaits nothing, so
   * a returned promise is handled the same way `tick()` is in drain.ts and
   * src/index.ts: `void`'d with a `.catch`, not left to become an unhandled
   * rejection.
   */
  onReady?: (sock: WASocket) => void | Promise<void>;
  /**
   * Consecutive failed connect attempts, used to space out reconnects.
   * Set internally when re-entering after a disconnect; callers pass nothing.
   */
  reconnectAttempt?: number;
  /**
   * Called each time a reconnect is scheduled, with the 1-based consecutive
   * attempt number. A link that keeps bouncing is the one WhatsApp-side fault
   * the WhatsApp alert channel can still report — a link that is fully down
   * cannot send anything, which is what the Docker healthcheck covers instead.
   */
  onReconnectScheduled?: (info: { attempt: number; delayMs: number; statusCode?: number }) => void;
  /**
   * Called on every inbound WebSocket frame — keepalives included, which
   * Baileys exchanges roughly every 30s on a healthy link. This is a
   * link-liveness signal, distinct from onMessage/onReady: those fire only
   * on actual chat traffic or a fresh connection, so a quiet whitelisted
   * chat makes them go stale even while the link itself is fine. The stall
   * watchdog above still forces a reconnect after 10 minutes of total frame
   * silence, so a genuinely dead link lets this go stale too.
   */
  onFrame?: () => void;
}

/**
 * Start (and auto-reconnect) a Baileys WhatsApp socket as an additional
 * linked device. Auth state is persisted under `authDir`.
 */
export async function startWaClient(opts: WaClientOptions): Promise<WASocket> {
  // Reset by a successful 'open' so a healthy link always reconnects promptly.
  let reconnectAttempt = opts.reconnectAttempt ?? 0;

  const { state, saveCreds } = await useMultiFileAuthState(opts.authDir);
  // Keep Baileys' own logging quiet; our app logger handles the useful events.
  const waLogger = pino({ level: 'warn' });

  // Use the current WhatsApp-web protocol version; a stale one is rejected (405).
  const { version } = await fetchLatestBaileysVersion();
  opts.logger.info({ waVersion: version.join('.') }, 'using WhatsApp web version');

  const sock = makeWASocket({
    version,
    auth: state,
    logger: waLogger,
    syncFullHistory: opts.syncFullHistory,
    markOnlineOnConnect: false,
  });

  sock.ev.on('creds.update', saveCreds);

  const watchdog = startStallWatchdog({
    timeoutMs: STALL_TIMEOUT_MS,
    checkEveryMs: STALL_CHECK_MS,
    onStall: (idleMs) => {
      opts.logger.warn({ idleMs }, 'stall watchdog: no inbound frames — forcing reconnect');
      sock.end(new Error('stall watchdog'));
    },
  });
  sock.ws.on('message', () => {
    watchdog.touch();
    try {
      opts.onFrame?.();
    } catch (err) {
      // Synchronous handler on the raw WebSocket, outside Baileys' own
      // event emitter — a throw here would escape uncaught. Same shape as
      // onReconnectScheduled's guard below: an alerting/stamping failure
      // must not turn into a dead link.
      opts.logger.warn(
        { err: err instanceof Error ? err.message : String(err) },
        'onFrame threw',
      );
    }
  });

  sock.ev.on('connection.update', (update) => {
    const { connection, qr, lastDisconnect } = update;

    if (qr) {
      opts.logger.info('Scan this QR in WhatsApp → Linked Devices → Link a device');
      qrcode.generate(qr, { small: true });
    }

    if (connection === 'open') {
      reconnectAttempt = 0;
      opts.logger.info('WhatsApp connection open');
      // This 'connection.update' handler is synchronous, and onReady's own
      // internal try/catch (group resolution, in src/index.ts) does not
      // cover everything it does afterwards — e.g. dedup.newestByGroup() is
      // a synchronous better-sqlite3 call outside any try. An uncaught throw
      // there would reject the promise onReady returns; left unhandled here,
      // that becomes an unhandled promise rejection that kills the process
      // under `restart: always` (docker-compose.yml). Same fix shape as
      // drain's loop() wrapping tick(): void the call, catch and log.
      void Promise.resolve(opts.onReady?.(sock)).catch((err) => {
        opts.logger.error(err, 'onReady handler failed');
      });
    }

    if (connection === 'close') {
      watchdog.stop();
      const statusCode = (lastDisconnect?.error as { output?: { statusCode?: number } } | undefined)
        ?.output?.statusCode;
      const loggedOut = statusCode === DisconnectReason.loggedOut;
      opts.logger.warn({ statusCode, loggedOut }, 'WhatsApp connection closed');

      if (loggedOut) {
        opts.logger.error('Logged out by WhatsApp. Delete the auth dir and re-run `npm run pair`.');
        return;
      }
      // Transient disconnect: recreate the socket (listeners re-register),
      // spacing attempts out so a persistent rejection cannot become a storm.
      const nextAttempt = reconnectAttempt + 1;
      const delayMs = backoffDelayMs(reconnectAttempt, {
        baseMs: RECONNECT_BASE_MS,
        maxMs: RECONNECT_MAX_MS,
        jitterRatio: RECONNECT_JITTER,
      });
      opts.logger.info({ attempt: nextAttempt, delayMs }, 'scheduling WhatsApp reconnect');
      try {
        opts.onReconnectScheduled?.({ attempt: nextAttempt, delayMs, statusCode });
      } catch (err) {
        // This handler is synchronous and inside Baileys' event emitter; a
        // throw here would escape into the emitter and skip the reconnect
        // scheduled below, turning an alerting failure into a dead link.
        opts.logger.warn(
          { err: err instanceof Error ? err.message : String(err) },
          'onReconnectScheduled threw',
        );
      }
      setTimeout(() => {
        startWaClient({ ...opts, reconnectAttempt: nextAttempt }).catch((err) =>
          opts.logger.error(err, 'reconnect failed'),
        );
      }, delayMs);
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    // 'notify' = fresh live messages. 'append' = messages delivered on
    // (re)connect / buffer flush — offline-queued and recent items that the
    // timing-out initial history sync never delivers. Process both so a missed
    // window self-heals; dedup (by key, pre-download) skips already-synced ones.
    if (type !== 'notify' && type !== 'append') return;
    for (const m of messages) {
      try {
        await opts.onMessage(sock, m);
      } catch (err) {
        opts.logger.error(err, 'onMessage handler failed');
      }
    }
  });

  if (opts.onHistory) {
    sock.ev.on('messaging-history.set', async ({ messages }) => {
      try {
        await opts.onHistory!(sock, messages);
      } catch (err) {
        opts.logger.error(err, 'onHistory handler failed');
      }
    });
  }

  return sock;
}
