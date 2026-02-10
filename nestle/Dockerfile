# Playwright base image includes browsers + system deps
# Explicitly use linux/amd64 platform to match ECS task definition
FROM --platform=linux/amd64 mcr.microsoft.com/playwright:v1.58.0-jammy

WORKDIR /app

# Install dependencies first (better caching)
# Include devDependencies for tsx to run TypeScript directly
COPY smuckers/package.json smuckers/package-lock.json* ./
RUN npm ci

# Copy shared types and scraper code
# Build context is the scrapers directory, so paths are relative to that
# Maintain directory structure so imports work correctly
COPY shared-types.ts ../shared-types.ts
COPY smuckers/scrape.ts ./scrape.ts

# Run the scraper using tsx (TypeScript executor)
CMD ["npx", "tsx", "scrape.ts"]
