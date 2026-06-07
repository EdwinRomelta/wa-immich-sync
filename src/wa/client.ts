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

export interface WaClientOptions {
  authDir: string;
  /** Request WhatsApp history sync on link (enables best-effort backfill). */
  syncFullHistory: boolean;
  logger: Logger;
  /** Called for every live message (messages.upsert, type 'notify'). */
  onMessage: (sock: WASocket, m: WAMessage) => Promise<void> | void;
  /** Called with each batch of synced history messages (messaging-history.set). */
  onHistory?: (sock: WASocket, messages: WAMessage[]) => Promise<void> | void;
  /** Called once the connection opens. */
  onReady?: (sock: WASocket) => void;
}

/**
 * Start (and auto-reconnect) a Baileys WhatsApp socket as an additional
 * linked device. Auth state is persisted under `authDir`.
 */
export async function startWaClient(opts: WaClientOptions): Promise<WASocket> {
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

  sock.ev.on('connection.update', (update) => {
    const { connection, qr, lastDisconnect } = update;

    if (qr) {
      opts.logger.info('Scan this QR in WhatsApp → Linked Devices → Link a device');
      qrcode.generate(qr, { small: true });
    }

    if (connection === 'open') {
      opts.logger.info('WhatsApp connection open');
      opts.onReady?.(sock);
    }

    if (connection === 'close') {
      const statusCode = (lastDisconnect?.error as { output?: { statusCode?: number } } | undefined)
        ?.output?.statusCode;
      const loggedOut = statusCode === DisconnectReason.loggedOut;
      opts.logger.warn({ statusCode, loggedOut }, 'WhatsApp connection closed');

      if (loggedOut) {
        opts.logger.error('Logged out by WhatsApp. Delete the auth dir and re-run `npm run pair`.');
        return;
      }
      // Transient disconnect: recreate the socket (listeners re-register).
      setTimeout(() => {
        startWaClient(opts).catch((err) => opts.logger.error(err, 'reconnect failed'));
      }, 3000);
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type !== 'notify') return; // only fresh live messages; history handled separately
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
