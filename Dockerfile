FROM node:22-bookworm-slim

WORKDIR /app

# Build toolchain for better-sqlite3 native bindings (used if no prebuilt binary).
RUN apt-get update \
  && apt-get install -y --no-install-recommends python3 make g++ \
  && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

ENV NODE_ENV=production
CMD ["npx", "tsx", "src/index.ts"]
