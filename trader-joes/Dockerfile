FROM node:20-slim

WORKDIR /app

COPY trader-joes/package.json trader-joes/package-lock.json* ./
RUN npm ci

COPY shared-types.ts ../shared-types.ts
COPY trader-joes/scrape.ts ./scrape.ts

# Default: read from /app/traderjoes_products.json (mount at runtime)
CMD ["npx", "tsx", "scrape.ts", "--input", "/app/traderjoes_products.json"]
