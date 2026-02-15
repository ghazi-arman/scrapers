# Scrapers

This directory contains web scrapers for various brands.

## Local Development Setup

### Prerequisites

1. **AWS Credentials**: Configure AWS credentials for local development. The AWS SDK will automatically use:
   - Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
   - AWS credentials file: `~/.aws/credentials` (use `aws configure`)
   - IAM role (if running on EC2/ECS)

   **Recommended**: Use AWS CLI to configure credentials:
   ```bash
   aws configure
   ```

2. **Environment Variables**: Create a `.env` file in the scraper directory (e.g., `kraft-heinz/.env`) with the following variables:

   ```bash
   # Copy the example file
   cp .env.example kraft-heinz/.env
   
   # Edit with your values
   ```

   Required variables:
   - `SCRAPER_OUTPUTS_BUCKET`: S3 bucket name for scraper outputs
   - `SCRAPER_JOB_STATUS_TABLE_NAME`: DynamoDB table name for job status
   - `PENDING_PRODUCTS_QUEUE_URL`: SQS queue URL (optional, only if scraper submits directly to queue)
   - `API_BASE_URL`: API Gateway endpoint (has default)
   - `API_KEYS_PARAMETER_NAME`: SSM parameter name for API keys (has default: `/tummi/api-keys`)
   - `JOB_NAME`: Scraper name (optional, defaults to directory name)

3. **Install Dependencies**: 
   ```bash
   cd kraft-heinz
   npm install
   ```

4. **Install Playwright Browsers** (for Playwright-based scrapers):
   ```bash
   npx playwright install chromium
   ```

### Running Locally

You can run any scraper **locally** (no AWS/API) and optionally cap how many products are scraped with **`--limit N`**.

**Local mode (`--local`):** Skips only DynamoDB (job status) and S3 (upload of products.json to the scraper-outputs bucket). API submission still runs—use your AWS profile so the scraper can read the service token from SSM and submit products for review.

**Limit (`--limit N` or `-l N`):** Scrape at most N products. Discovery still runs fully; only the first N product URLs are detail-scraped. Useful for quick runs.

**CLI options (all scrapers):**

| Option | Description |
|--------|-------------|
| `--limit N` or `-l N` | Scrape at most N products (after discovery). |
| `--local` | Skip DynamoDB job status and S3 upload; API submission still runs (use AWS profile for SSM). |

### Debug, Headless, and Config

**Debug mode** – Enable verbose logging for any scraper with `--debug`:

Example:
```bash
npx tsx scrape.ts --config config.json --local --limit 1 --debug
```

**Headless** – By default, Playwright runs headless (no browser window). To see the browser window:

| Scraper | Headless env var | Example (headed = show browser) |
|---------|------------------|---------------------------------|
| Amazon | `AMAZON_HEADLESS=0` | `AMAZON_HEADLESS=0 npx tsx scrape.ts --config config.json --local --limit 1` |
| Trader Joe's | `TJ_HEADLESS=0` | `TJ_HEADLESS=0 npx tsx scrape.ts --config config.json --local --limit 1` |
| Vons | `VONS_HEADLESS=0` | `VONS_HEADLESS=0 npx tsx scrape.ts --config config.json --local --limit 1` |

**Slow motion** (Amazon, Vons) – Add delay between actions (milliseconds) for easier observation:

```bash
AMAZON_SLOWMO=500 AMAZON_HEADLESS=0 npx tsx scrape.ts --config config.json --local --limit 1
VONS_SLOWMO=500 VONS_HEADLESS=0 npx tsx scrape.ts --config config.json --local --limit 1
```

**Playwright Inspector** – Use Playwright’s built‑in debugger (all Playwright scrapers):

```bash
PWDEBUG=1 npx tsx scrape.ts --config config.json --local --limit 1
```

This opens the Playwright Inspector so you can step through actions and inspect the page.

**Config** – Each scraper’s config path:

| Scraper | Config option | Default / example |
|---------|---------------|-------------------|
| Amazon | `--config <path>` or `-c` | `--config config.json` |
| Trader Joe's | `--config <path>` | `--config ./config.json` (default) |
| Vons | `--config <path>` or `-c` | `--config ./config.json` |
| Target | `--config <path>` or `-c` | `--config ./config.json` |
| Kraft-Heinz | Positional path | `./brands.config.json` |

**Example – debug + headed + custom config:**

```bash
cd scrapers/amazon
AMAZON_HEADLESS=0 AMAZON_SLOWMO=300 \
  npx tsx scrape.ts --config config.json --local --limit 1 --debug
```

**Target scraper options:**
| Option | Description |
|--------|-------------|
| `--url <URL>` or `-u` | Scrape a single Target product URL. |
| `--config <path>` or `-c` | Path to JSON config with `urls` array. |

**Quick start (local + limit):**

```bash
# Kraft-Heinz: local run, limit 5 products
cd scrapers/kraft-heinz && npm run scrape:local:limit

# Kraft-Heinz: local run, custom limit (e.g. 10)
cd scrapers/kraft-heinz && npm run scrape:local -- --limit 10

# Smuckers: local run, limit 5 products
cd scrapers/smuckers && npm run scrape:local:limit

# Smuckers: local run, custom limit (e.g. 10)
cd scrapers/smuckers && npm run scrape:local -- --limit 10

# Target: scrape single product URL
cd scrapers/target && npx tsx scrape.ts --url "https://www.target.com/p/ritz-herb-fresh-stacks-crackers-11-8oz/-/A-92270376" --local

# Target: discover & scrape Good & Gather brand (from config.json), limit 5
cd scrapers/target && npm run scrape:local:limit

# Trader Joe's: local run, limit 5 (reads config.json)
cd scrapers/trader-joes && npm run scrape:local:limit
```

**Full CLI (when not using npm scripts):**

- **Kraft-Heinz:** `npx tsx scrape.ts ./brands.config.json [--limit N] [--local]`
- **Smuckers:** `npx tsx scrape.ts [--concurrency N] [--limit N] [--local]` (default concurrency: 5)
- **Target:** `npx tsx scrape.ts --url <URL>` or `npx tsx scrape.ts --config ./config.json [--limit N] [--local]` (config can include `brands` with `listingUrl` for discovery, e.g. Good & Gather)
- **Trader Joe's:** `npx tsx scrape.ts [--config ./config.json] [--limit N] [--local]`

For full runs (with AWS and API), the AWS SDK uses the default credential chain. Configure AWS credentials and set the required environment variables (see Prerequisites above).

#### Option 1: Using dotenv (Recommended for local development)

Install dotenv:
```bash
cd kraft-heinz
npm install --save-dev dotenv
```

Create a `.env` file in the scraper directory:
```bash
# kraft-heinz/.env
SCRAPER_OUTPUTS_BUCKET=your-bucket-name
SCRAPER_JOB_STATUS_TABLE_NAME=your-table-name
API_BASE_URL=https://your-api-endpoint.amazonaws.com
API_KEYS_PARAMETER_NAME=/tummi/api-keys
JOB_NAME=kraft-heinz
```

Run with dotenv-cli:
```bash
# Full run (requires AWS + API)
npx dotenv -e .env -- npx tsx scrape.ts ./brands.config.json

# Local only: scrape 5 products, no AWS/API (kraft-heinz)
npx dotenv -e .env -- npx tsx scrape.ts ./brands.config.json --local --limit 5

# Smuckers: local, limit 3 (no config path)
npx tsx scrape.ts --local --limit 3
```

Or use the built-in scripts (no dotenv needed for local-only runs):

- **Kraft-Heinz:** `npm run scrape:local` or `npm run scrape:local:limit` (limit 5), or `npm run scrape:local -- --limit 10`
- **Smuckers:** `npm run scrape:local` or `npm run scrape:local:limit` (limit 5), or `npm run scrape:local -- --limit 10`

With dotenv for env-backed runs, you can add to `package.json`:
```json
{
  "scripts": {
    "start": "npx tsx scrape.ts ./brands.config.json",
    "start:local": "dotenv -e .env -- npx tsx scrape.ts ./brands.config.json --local",
    "start:local:limit": "dotenv -e .env -- npx tsx scrape.ts ./brands.config.json --local --limit 5"
  }
}
```

Then run: `npm run start:local` or `npm run start:local:limit`

#### Option 2: Export Environment Variables

```bash
export SCRAPER_OUTPUTS_BUCKET=your-bucket-name
export SCRAPER_JOB_STATUS_TABLE_NAME=your-table-name
export API_BASE_URL=https://your-api-endpoint.amazonaws.com
npx tsx scrape.ts ./brands.config.json
```

#### Option 3: Inline Environment Variables

```bash
SCRAPER_OUTPUTS_BUCKET=your-bucket-name \
SCRAPER_JOB_STATUS_TABLE_NAME=your-table-name \
npx tsx scrape.ts ./brands.config.json
```

### Getting AWS Resource Names

To find the actual values for your environment variables, check:

1. **S3 Bucket**: Look in CDK outputs or AWS Console → S3 → `scraper-outputs-*`
2. **DynamoDB Table**: Look in CDK outputs or AWS Console → DynamoDB → `scraper-job-status-*`
3. **SQS Queue**: Look in CDK outputs or AWS Console → SQS → `pending-products`
4. **API Base URL**: Your API Gateway endpoint (check CDK outputs or API Gateway console)
5. **SSM Parameter**: `/tummi/api-keys` (default, or check Parameter Store console)

### What `--local` skips

When you use `--local`, the scraper skips only:

1. **DynamoDB** – Creating/updating the job record in the scraper job status table.
2. **S3** – Uploading the run’s products JSON to the scraper-outputs bucket (`s3://{SCRAPER_OUTPUTS_BUCKET}/{SCRAPER_NAME}/{runDateTime}/products.json`).

API submission to submit-product-for-review still runs locally; configure your AWS profile so the scraper can read the service token from SSM.

If you don’t use `--local`, you can still leave some env vars unset:
- Leave `SCRAPER_OUTPUTS_BUCKET` unset (S3 upload will be skipped)
- Leave `SCRAPER_JOB_STATUS_TABLE_NAME` unset (DynamoDB updates will be skipped)
- The scraper will still attempt API submission if `API_BASE_URL` and API keys are configured

### Troubleshooting

**"Access Denied" errors**: 
- Verify your AWS credentials have permissions for S3, DynamoDB, SSM, and SQS
- Check that your IAM user/role has the same permissions as the ECS task role

**"Parameter not found" errors**:
- Verify `API_KEYS_PARAMETER_NAME` points to the correct SSM parameter
- Ensure your AWS credentials have `ssm:GetParameter` permission
- If the parameter is encrypted, ensure you have `kms:Decrypt` permission

**"Table not found" errors**:
- Verify `SCRAPER_JOB_STATUS_TABLE_NAME` matches your DynamoDB table name
- Ensure the table exists in the same region as your AWS credentials
