# wa-immich-sync

Automatically copy photos and videos shared in **whitelisted WhatsApp groups** into a
self-hosted **[Immich](https://immich.app)** server — one album per group, no manual
download/upload. Plus a **backfill** path for older media via WhatsApp's "Export chat".

It links as an *additional* WhatsApp device (like WhatsApp Web) using
[Baileys](https://github.com/WhiskeySockets/Baileys), watches the groups you whitelist, and
pushes every new image/video to Immich. Uploads are idempotent, so it is safe to restart anytime.

> [!WARNING]
> **Unofficial client.** Baileys logs in as a linked device; this is not an official WhatsApp
> API and carries a small risk of the number being flagged. Use a number you control. The daemon
> only reads media — with one exception: it posts a short text **summary reply** in the dedicated
> backfill group after importing a zip.

## Features

- Live sync of images/videos from any number of whitelisted groups
- One Immich album per group (or a single album, or none)
- Whitelist groups by **name or JID**
- Backfill older media by dropping a WhatsApp chat-export `.zip` into a dedicated group
- Idempotent: local content-hash dedup **+** Immich's checksum dedup
- Runs as a small Node daemon or a Docker container

## How it works

```
WhatsApp (linked device)
   │  messages.upsert (live)  +  messaging-history.set (best-effort backfill)
   ▼
src/wa/client.ts ──▶ src/sync/pipeline.ts
                        │ 1. filter to whitelisted groups (resolved from names/JIDs)
                        │ 2. extract image/video       (src/wa/mediaExtractor.ts)
                        │ 3. dedup check               (src/sync/dedupStore.ts, sqlite)
                        │ 4. upload to Immich          (src/immich/client.ts)
                        │ 5. add to album
                        └ 6. mark done
                        ▼
                  Immich server

Backfill group ──▶ src/sync/backfillIngest.ts ──▶ src/sync/importFolder.ts ──▶ Immich
   (a .zip document is downloaded, unzipped, and every photo/video imported)
```

## Prerequisites

- Node.js **≥ 22** (uses built-in `fetch`/`FormData` and `process.loadEnvFile`)
- A running Immich server and an **API key** (Immich → Account Settings → API Keys).
  The key needs `asset.upload`, `album.read`, `album.create`, `albumAsset.create` (or full access).
- A WhatsApp account with a free linked-device slot (WhatsApp allows ~4)

## Quick start

```bash
git clone https://github.com/<your-username>/wa-immich-sync.git
cd wa-immich-sync
npm install

cp .env.example .env
#   edit .env: IMMICH_URL, IMMICH_API_KEY, WHITELIST_GROUPS
```

### 1. Pair the device (one time)

```bash
npm run pair
```

Scan the QR: **WhatsApp → Linked Devices → Link a device**. Auth is saved under `data/auth/`.
Press `Ctrl+C` once connected.

### 2. Find your group names

```bash
npm run list-groups
```

Copy the group names (or JIDs) you want into `WHITELIST_GROUPS` in `.env`.

### 3. Run

```bash
npm run dev            # foreground (development)
# or
docker compose up -d   # background daemon (restart: unless-stopped)
```

Post a photo in a whitelisted group and watch it appear in Immich (album + main timeline).

## Configuration

All configuration is via environment variables (see `.env.example`):

| Var | Required | Default | Meaning |
|---|---|---|---|
| `IMMICH_URL` | ✓ | — | Immich base URL |
| `IMMICH_API_KEY` | ✓ | — | Immich API key |
| `WHITELIST_GROUPS` | ✓ | — | Comma-separated group **names or JIDs** to sync |
| `BACKFILL_GROUP_NAME` | | `wa-immich-backfill` | Group whose `.zip` uploads get imported |
| `ALBUM_MODE` | | `per-group` | `per-group` \| `single` \| `none` |
| `SINGLE_ALBUM_NAME` | | `WhatsApp` | Album name when `ALBUM_MODE=single` |
| `MEDIA_TYPES` | | `image,video` | Subset of `image,video` |
| `BACKFILL` | | `true` | Request WhatsApp history sync on link |
| `WA_AUTH_DIR` | | `./data/auth` | Where Baileys stores auth |
| `DEDUP_DB` | | `./data/synced.db` | sqlite dedup database |

**Whitelist by name or JID.** Each `WHITELIST_GROUPS` entry is matched by group name, or treated
as an exact JID if it contains `@g.us`. If a name matches **multiple** groups, all of them are
synced and a warning is logged — use a JID to target exactly one. A JID is also stable if the
group is later renamed.

## Backfill (older media)

WhatsApp only delivers a limited, recent slice of history to a newly linked device, and it
**never** gives a member messages sent *before that member joined*. So media older than the bot's
membership cannot be pulled through the live connection at all. Two reliable options:

### Option A — drop a chat export into the backfill group (recommended)

1. Create a group named `wa-immich-backfill` (or your `BACKFILL_GROUP_NAME`) and add the bot.
2. On a phone that **has** the photos: WhatsApp → the source chat → **Export chat → Attach Media**.
3. Send the resulting `.zip` into the backfill group **as a Document** (not as photos — documents
   are not recompressed, so original files and dates are preserved). Optionally set the message
   **caption** to the target album name (otherwise a default album is used).
4. The bot downloads, unzips, imports every photo/video, and replies with a summary.

### Option B — import a folder directly (CLI)

```bash
npm run import -- /path/to/exported/folder --album "My Album"
```

Both paths parse dates from WhatsApp filenames (`IMG-YYYYMMDD-WA####`), falling back to file mtime.

> WhatsApp's "Export with media" caps the number of exported files. For very large chats, copy the
> phone's `WhatsApp/Media/WhatsApp Images` (and `WhatsApp Video`) folders and import those instead.

## Deduplication

Two layers make every path idempotent:

- **Local (sqlite):** import dedup is keyed by the **SHA-1 of the file content**, so the same photo
  re-exported by a different person (different filename) is skipped. Live sync dedups by WhatsApp
  message id.
- **Immich:** the server dedups by checksum, so a duplicate upload never creates a second asset.

A re-compressed or edited copy is genuinely different bytes and is treated as a new asset.

## Docker

```bash
docker compose up -d      # start
docker compose logs -f    # watch
docker compose down       # stop
```

`./data` is mounted so WhatsApp auth + the dedup db survive restarts. To reach an Immich server on
the host, set `IMMICH_URL=http://host.docker.internal:2283`. If Immich runs in its own Docker
stack, see the commented `networks:` block in `docker-compose.yml`.

## Scripts

| Command | Purpose |
|---|---|
| `npm run pair` | One-time device pairing (QR) |
| `npm run list-groups` | Print the groups you are in (names + JIDs) |
| `npm run dev` / `npm start` | Run the sync daemon |
| `npm run import -- <folder>` | Import an exported-chat folder |
| `npm run status` | Show how many assets have been synced |
| `npm test` / `npm run typecheck` | Tests (all I/O mocked) / type check |

## Caveats

- Best-effort live backfill: WhatsApp only syncs recent, post-join history to a linked device.
- The daemon sends one summary message per imported zip in the backfill group.
- `.env` and `data/` (WhatsApp credentials) are gitignored — never commit them.

## License

[MIT](./LICENSE)
