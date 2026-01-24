# Playwright base image includes browsers + system deps
# Explicitly use linux/amd64 platform to match ECS task definition
FROM --platform=linux/amd64 mcr.microsoft.com/playwright:v1.42.0-jammy

WORKDIR /app

# Install dependencies first (better caching)
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev

# Copy your scraper code
COPY . .

# Optional: if you run chromium only, you can set:
# ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

CMD ["node", "scrape.js"]
