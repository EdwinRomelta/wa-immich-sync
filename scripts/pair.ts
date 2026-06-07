import { getWaAuthDir } from '../src/config.ts';
import { logger } from '../src/logger.ts';
import { startWaClient } from '../src/wa/client.ts';

/**
 * One-time pairing: prints a QR to link this tool as an additional WhatsApp
 * device. Scan it from the bot phone (WhatsApp → Linked Devices → Link a
 * device). Auth state is saved under WA_AUTH_DIR; afterwards the daemon
 * reconnects silently.
 */
async function main(): Promise<void> {
  const authDir = getWaAuthDir();
  logger.info({ authDir }, 'Pairing — open WhatsApp → Linked Devices, then scan the QR below');

  await startWaClient({
    authDir,
    syncFullHistory: false,
    logger,
    onMessage: () => {},
    onReady: () => {
      logger.info('Paired and connected. Auth saved — press Ctrl+C, then run the sync.');
    },
  });
}

main().catch((err) => {
  logger.error(err, 'pair failed');
  process.exit(1);
});
