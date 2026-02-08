FROM node:20-bullseye

WORKDIR /app

COPY smart-label/package.json smart-label/package-lock.json* ./
RUN npm ci

COPY shared-types.ts ../shared-types.ts
COPY smart-label/scrape.ts smart-label/config.json ./

CMD ["npx", "tsx", "scrape.ts", "--config", "./config.json"]
