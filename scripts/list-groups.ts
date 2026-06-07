import { getWaAuthDir } from '../src/config.ts';
import { logger } from '../src/logger.ts';
import { startWaClient } from '../src/wa/client.ts';

/**
 * Connect with the existing paired session and print all participating groups
 * as JSON ready to paste into config.json's "groups". Requires `npm run pair`
 * first.
 */
async function main(): Promise<void> {
  await startWaClient({
    authDir: getWaAuthDir(),
    syncFullHistory: false,
    logger,
    onMessage: () => {},
    onReady: async (sock) => {
      try {
        const groups = await sock.groupFetchAllParticipating();
        const list = Object.values(groups).map((g) => ({ jid: g.id, name: g.subject }));
        logger.info({ count: list.length }, 'groups fetched — copy desired entries into config.json');
        // Print raw JSON to stdout for easy copy/paste.
        console.log(JSON.stringify(list, null, 2));
      } catch (err) {
        logger.error(err, 'failed to fetch groups');
      } finally {
        process.exit(0);
      }
    },
  });
}

main().catch((err) => {
  logger.error(err, 'list-groups failed');
  process.exit(1);
});
