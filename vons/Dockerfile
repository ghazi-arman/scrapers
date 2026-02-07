FROM --platform=linux/amd64 mcr.microsoft.com/playwright:v1.58.0-jammy

WORKDIR /app

COPY vons/package.json vons/package-lock.json* ./
RUN npm ci

COPY shared-types.ts ../shared-types.ts
COPY vons/scrape.ts vons/config.json ./

CMD ["npx", "tsx", "scrape.ts", "--config", "./config.json"]
