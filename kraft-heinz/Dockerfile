# Playwright base image includes browsers + system deps
# Explicitly use linux/amd64 platform to match ECS task definition
FROM --platform=linux/amd64 mcr.microsoft.com/playwright:v1.58.0-jammy

WORKDIR /app

# Install dependencies first (better caching)
# Include devDependencies for tsx to run TypeScript directly
COPY kraft-heinz/package.json kraft-heinz/package-lock.json* ./
RUN npm ci

# Copy shared types, scraper code and config
# Build context is the scrapers directory, so paths are relative to that
# Maintain directory structure so imports work correctly
COPY shared-types.ts ../shared-types.ts
COPY kraft-heinz/scrape.ts kraft-heinz/brands.config.json ./

# Run the scraper using tsx (TypeScript executor)
CMD ["npx", "tsx", "scrape.ts", "./brands.config.json"]
