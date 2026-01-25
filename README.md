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

The AWS SDK automatically uses the default credential chain, so you just need to:
1. Configure AWS credentials (see Prerequisites above)
2. Set the required environment variables

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
npx dotenv -e .env -- npx tsx scrape.ts ./brands.config.json
```

Or add a script to `package.json`:
```json
{
  "scripts": {
    "start": "npx tsx scrape.ts ./brands.config.json",
    "start:local": "dotenv -e .env -- npx tsx scrape.ts ./brands.config.json"
  }
}
```

Then run: `npm run start:local`

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

### Optional: Skip AWS Operations for Testing

If you just want to test scraping without AWS operations, you can:
- Leave `SCRAPER_OUTPUTS_BUCKET` unset (S3 upload will be skipped)
- Leave `SCRAPER_JOB_STATUS_TABLE_NAME` unset (DynamoDB updates will be skipped)
- The scraper will still write to the local JSONL file specified in `brands.config.json`

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
