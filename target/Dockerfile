FROM --platform=linux/amd64 mcr.microsoft.com/playwright:v1.58.0-jammy

WORKDIR /app

COPY target/package.json target/package-lock.json* ./
RUN npm ci

COPY shared-types.ts ../shared-types.ts
COPY target/scrape.ts target/config.json ./

CMD ["npx", "tsx", "scrape.ts", "--config", "./config.json"]
