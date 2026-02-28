import * as crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import { chromium, Browser, Page } from "playwright";
import * as cheerio from "cheerio";
import pLimit from "p-limit";
import axios from "axios";
import * as dotenv from "dotenv";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import * as nutritionUtils from "../nutrition-utils";
import * as servingSizeUtils from "../serving-size-utils";
import * as nameUtils from "../name-utils";
import * as productIdUtils from "../product-id-utils";
import { fileURLToPath } from "url";

dotenv.config();

const parseNutrientAmountWithQualifier =
  (nutritionUtils as any).parseNutrientAmountWithQualifier ??
  (nutritionUtils as any).default?.parseNutrientAmountWithQualifier;
const normalizeNutritionData =
  (nutritionUtils as any).normalizeNutritionData ??
  (nutritionUtils as any).default?.normalizeNutritionData;
const parseServingSizeFromText =
  (servingSizeUtils as any).parseServingSizeFromText ??
  (servingSizeUtils as any).default?.parseServingSizeFromText;
const cleanProductName =
  (nameUtils as any).cleanProductName ?? (nameUtils as any).default?.cleanProductName;
const generateDeterministicProductId =
  (productIdUtils as any).generateDeterministicProductId ??
  (productIdUtils as any).default?.generateDeterministicProductId;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

type BrandConfig = {
  brand: string;
  source: string;
  listingUrl: string;
};

type AppConfig = {
  urls?: Array<string | { url: string; brand?: string; source?: string }>;
  searchUrls?: Array<string | { url: string; brand?: string; source?: string }>;
  brand?: string;
  source?: string;
  concurrency?: number;
};

type NutritionNutrient = {
  name: string;
  amount: string | null;
  dailyValuePercent: string | null;
};

type Nutrition = {
  servingSize: string | null;
  servingsPerContainer: string | null;
  calories: string | null;
  nutrients: NutritionNutrient[];
  rawText: string | null; // always keep a raw fallback for debugging
};

type ScrapedProduct = {
  brand: string;
  source: string;
  listingUrl: string;
  productUrl: string;

  name: string | null;
  ingredients: string | null;
  allergens: string | null;
  upc12: string | null;
  nutrition: Nutrition | null;
  nutritionData?: ScraperNutritionData | null;
  nutritionImageUrl?: string | null;
  imageUrl: string | null;

  scrapedAt: string;
  sourceCreatedAt: string | null;
  sourceLastUpdatedAt: string | null;
  fingerprint: string;
};

// AWS clients
const s3Client = new S3Client({});
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);
const ssmClient = new SSMClient({});

// Environment variables
const SCRAPER_NAME = process.env.JOB_NAME || "kraftheinz";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://api.mytummi.app";
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
let DEBUG_KH = process.env.DEBUG_KH === "1";
let KRAFT_HEADLESS = process.env.KRAFT_HEADLESS !== "0";
const SUBMIT_NUTRITION_PARSE_JOB_URL =
  process.env.SUBMIT_NUTRITION_PARSE_JOB_URL ||
  `${API_BASE_URL}/submit-nutrition-parse-job`;
const GET_NUTRITION_PARSE_RESULT_URL =
  process.env.GET_NUTRITION_PARSE_RESULT_URL ||
  `${API_BASE_URL}/get-nutrition-parse-result`;
const PARSE_NUTRITION_SERVICE_TOKEN = process.env.PARSE_NUTRITION_SERVICE_TOKEN || "12345";
const NUTRITION_POLL_INTERVAL_MS = parseInt(process.env.NUTRITION_POLL_INTERVAL_MS || "2000", 10);
const NUTRITION_POLL_TIMEOUT_MS = parseInt(process.env.NUTRITION_POLL_TIMEOUT_MS || "60000", 10);
const PRODUCTS_API_URL = `${API_BASE_URL}/products`;

// Cache for service token
let serviceTokenCache: string | null = null;

function ensureDirForFile(filePath: string): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function normalizeWhitespace(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

async function checkProductExists(params: {
  name: string | null;
  brand: string | null;
  upc: string | null;
}): Promise<boolean> {
  const { name, brand, upc } = params;
  if (!name || typeof generateDeterministicProductId !== "function") return false;
  const productId = generateDeterministicProductId(name, brand || undefined, upc || undefined);
  try {
    const token = await getServiceToken();
    const res = await axios.get(`${PRODUCTS_API_URL}/${productId}`, {
      headers: { "X-Service-Token": token },
      timeout: 10_000,
    });
    return !!res?.data;
  } catch (e: any) {
    if (e?.response?.status === 404) return false;
    if (DEBUG_KH) console.log("[DEBUG] product exists check failed:", e);
    return false;
  }
}

function sha256(input: string): string {
  return crypto.createHash("sha256").update(input).digest("hex");
}

/**
 * Parse serving size text (e.g., "1 Tbsp (20g)", "8 fl oz", "240ml", "1/4 cup", "1/2 cup") into value and unit
 */
function parseServingSize(servingSizeText: string | null): { value: number | null; unit: string | null } {
  if (typeof parseServingSizeFromText !== "function") {
    throw new Error("parseServingSizeFromText import failed");
  }
  return parseServingSizeFromText(servingSizeText);
}



/**
 * Map nutrient name to database column name
 */
function mapNutrientNameToColumn(name: string): string | null {
  const nameLower = name.toLowerCase().trim();
  
  const mapping: { [key: string]: string } = {
    'total fat': 'total_fat_g',
    'saturated fat': 'saturated_fat_g',
    'trans fat': 'trans_fat_g',
    'polyunsaturated fat': 'polyunsaturated_fat_g',
    'monounsaturated fat': 'monounsaturated_fat_g',
    'cholesterol': 'cholesterol_mg',
    'sodium': 'sodium_mg',
    'total carbohydrate': 'total_carbs_g',
    'dietary fiber': 'fiber_g',
    'total sugars': 'sugars_g',
    'added sugars': 'added_sugars_g',
    'protein': 'protein_g',
    'calcium': 'calcium_mg',
    'iron': 'iron_mg',
    'potassium': 'potassium_mg',
    'vitamin d': 'vitamin_d_mcg',
    'vitamin a': 'vitamin_a_mcg',
    'vitamin c': 'vitamin_c_mg',
    'vitamin e': 'vitamin_e_mg',
    'vitamin k': 'vitamin_k_mcg',
    'thiamin': 'thiamin_mg',
    'riboflavin': 'riboflavin_mg',
    'niacin': 'niacin_mg',
    'vitamin b6': 'vitamin_b6_mg',
    'folate': 'folate_mcg',
    'vitamin b12': 'vitamin_b12_mcg',
    'biotin': 'biotin_mcg',
    'pantothenic acid': 'pantothenic_acid_mg',
    'magnesium': 'magnesium_mg',
    'phosphorus': 'phosphorus_mg',
    'zinc': 'zinc_mg',
  };
  
  return mapping[nameLower] || null;
}

/**
 * Transform scraped nutrition data to standard scraper format
 */
function transformNutritionToDbFormat(nutrition: Nutrition | null): ScraperNutritionData | null {
  if (!nutrition) return null;
  
  // Parse serving size
  let servingSize = parseServingSize(nutrition.servingSize);
  
  // If serving size parsing failed but we have nutrition data, use default
  if (!servingSize.value || !servingSize.unit) {
    // Check if we have any nutrition data worth saving
    const hasCalories = nutrition.calories && !isNaN(parseFloat(nutrition.calories.trim()));
    const hasNutrients = nutrition.nutrients && nutrition.nutrients.length > 0;
    
    if (hasCalories || hasNutrients) {
      console.log(`Warning: Could not parse serving size "${nutrition.servingSize}", using default 1 serving`);
      servingSize = { value: 1, unit: 'serving' };
    } else {
      // No nutrition data to save
      return null;
    }
  }
  
  // Parse calories
  const calories = nutrition.calories ? parseFloat(nutrition.calories.trim()) : null;
  
  // Build nutrition object with serving size
  const result: any = {
    serving_size_value: servingSize.value,
    serving_size_unit_text: servingSize.unit,
    serving_size_text: nutrition.servingSize || null, // Store original text
  };
  
  if (calories !== null && !isNaN(calories)) {
    result.calories = calories;
  }
  
  // Map nutrients to database columns (with qualifier support for <1g, >5mg, ~10g, etc.)
  for (const nutrient of nutrition.nutrients) {
    const columnName = mapNutrientNameToColumn(nutrient.name);
    if (columnName) {
      if (typeof parseNutrientAmountWithQualifier !== "function") {
        throw new Error("parseNutrientAmountWithQualifier import failed");
      }
      const parsed = parseNutrientAmountWithQualifier(nutrient.amount);
      if (parsed !== null) {
        result[columnName] = parsed.value;
        if (parsed.qualifier) {
          result[`${columnName}_qualifier`] = parsed.qualifier;
        }
      }
    }
  }
  
  return result;
}

/**
 * Extract createdAt and updatedAt from __NEXT_DATA__
 */
function extractSourceDates($: cheerio.CheerioAPI): { createdAt: string | null; updatedAt: string | null } {
  try {
    const nextDataScript = $('script#__NEXT_DATA__').html();
    if (nextDataScript) {
      const nextData = JSON.parse(nextDataScript);
      
      // Recursively search for createdAt and updatedAt fields
      const findDates = (obj: any): { createdAt: string | null; updatedAt: string | null } => {
        if (!obj || typeof obj !== 'object') return {createdAt: null, updatedAt: null};
        
        // Check if this object has both createdAt and updatedAt
        if (obj.createdAt && obj.updatedAt) {
          return {createdAt: typeof obj.createdAt === 'string' ? obj.createdAt : null,
            updatedAt: typeof obj.updatedAt === 'string' ? obj.updatedAt : null};
        }
        
        // Recursively search nested objects/arrays
        for (const key of Object.keys(obj)) {
          if (obj[key] && typeof obj[key] === 'object') {
            const found = findDates(obj[key]);
            if (found.createdAt || found.updatedAt) {
              return found;
            }
          }
        }
        return {createdAt: null, updatedAt: null};
      };
      
      return findDates(nextData);
    }
  } catch (e) {
    // Ignore parsing errors
  }
  
  return {createdAt: null, updatedAt: null};
}

/**
 * Get service token from SSM Parameter Store
 */
async function getServiceToken(): Promise<string> {
  if (serviceTokenCache) {
    return serviceTokenCache;
  }

  try {
    const command = new GetParameterCommand({
      Name: API_KEYS_PARAMETER_NAME,
      WithDecryption: true,
    });

    const response = await ssmClient.send(command);
    if (!response.Parameter?.Value) {
      throw new Error(`Parameter "${API_KEYS_PARAMETER_NAME}" not found or has no value`);
    }

    const parameter = JSON.parse(response.Parameter.Value);
    const token = parameter.InternalServiceToken;
    
    if (!token) {
      throw new Error('InternalServiceToken not found in parameter');
    }

    serviceTokenCache = token;
    return token;
  } catch (error) {
    console.error('Error getting service token:', error);
    throw error;
  }
}

/**
 * Submit product for review via API
 */
async function submitProductForReview(product: ScrapedProduct): Promise<boolean> {
  if (!product.name || !product.ingredients) {
    console.log(`Skipping product ${product.name || product.productUrl}: missing name or ingredients`);
    return false;
  }

  try {
    const serviceToken = await getServiceToken();
    const productOutput = transformProductToApiRequest(product);
    // Remove scraper_job_id for API submission (it's only for S3)
    const { scraper_job_id, ...requestBody } = productOutput;
    if (DEBUG_KH) {
      console.log("[DEBUG] submit body:");
      console.log(JSON.stringify(requestBody, null, 2));
    }

    const response = await axios.post(
      `${API_BASE_URL}/submit-product-for-review`,
      requestBody,
      {
        headers: {
          'Content-Type': 'application/json',
          'X-Service-Token': serviceToken,
        },
      }
    );

    if (response.status === 200) {
      console.log(`✅ Submitted product "${product.name}"` + (response.data?.data?.job_id ? ` for review (job_id: ${response.data.data.job_id})` : ''));
      return true;
    } else {
      console.error(`❌ Failed to submit product "${product.name}": HTTP ${response.status}`);
      return false;
    }
  } catch (error: any) {
    if (error.response) {
      console.error(`❌ Failed to submit product "${product.name}": ${error.response.status} ${error.response.statusText} - ${JSON.stringify(error.response.data)}`);
    } else {
      console.error(`❌ Failed to submit product "${product.name}": ${error.message || error}`);
    }
    return false;
  }
}

/**
 * Upload results to S3 (skipped when running with --local).
 * S3 operation: PutObject of products.json to scraper-outputs bucket at
 * s3://{SCRAPER_OUTPUTS_BUCKET}/{SCRAPER_NAME}/{runDateTime}/products.json
 */
async function uploadToS3(results: ScraperProductOutput[], jobId: string, runDateTime: string): Promise<void> {
  if (!SCRAPER_OUTPUTS_BUCKET) {
    console.log("S3 bucket not configured, skipping upload");
    return;
  }

  const key = `${SCRAPER_NAME}/${runDateTime}/products.json`;
  const body = JSON.stringify(results, null, 2);

  const putCommand = new PutObjectCommand({
    Bucket: SCRAPER_OUTPUTS_BUCKET,
    Key: key,
    Body: body,
    ContentType: "application/json",
  });

  await s3Client.send(putCommand);
  console.log(`Uploaded results to s3://${SCRAPER_OUTPUTS_BUCKET}/${key}`);
}

/**
 * Update job status in DynamoDB
 */
async function updateJobStatus(jobId: string, status: string, error: string | null = null): Promise<void> {
  if (!SCRAPER_JOB_STATUS_TABLE_NAME) return;

  const now = new Date().toISOString();
  const expressionAttributeNames: { [key: string]: string } = {
    "#status": "status",
  };
  const expressionAttributeValues: { [key: string]: any } = {
    ":status": status,
    ":updated_at": now,
  };
  
  let updateExpression = "SET #status = :status, updated_at = :updated_at";
  
  if (error) {
    updateExpression += ", error = :error";
    expressionAttributeValues[":error"] = String(error);
  }

  const updateCommand = new UpdateCommand({
    TableName: SCRAPER_JOB_STATUS_TABLE_NAME,
    Key: { job_id: jobId },
    UpdateExpression: updateExpression,
    ExpressionAttributeNames: expressionAttributeNames,
    ExpressionAttributeValues: expressionAttributeValues,
  });

  await docClient.send(updateCommand);
  console.log(`Updated job ${jobId} status to ${status}`);
}

/**
 * Transform scraped product to standard scraper output format
 */
function transformProductToApiRequest(product: ScrapedProduct): ScraperProductOutput {
  // Use source dates if available, otherwise fall back to current timestamp
  const now = new Date().toISOString();
  const sourceCreatedAt = product.sourceCreatedAt || now;
  const sourceLastUpdatedAt = product.sourceLastUpdatedAt || now;
  
  // Parse serving size from nutrition data (for backward compatibility with serving_size_value/unit)
  const servingSize = product.nutrition?.servingSize 
    ? parseServingSize(product.nutrition.servingSize)
    : { value: null, unit: null };
  
  // Transform nutrition data to database format
  const nutrition = product.nutritionData || transformNutritionToDbFormat(product.nutrition);
  
  return {product_name: product.name || '',
    brand: product.brand,
    upc: product.upc12 || undefined,
    ingredients_text: product.ingredients || '',
    allergen_statement: product.allergens || undefined,
    serving_size_value: servingSize.value ?? undefined,
    serving_size_unit: servingSize.unit ?? undefined,
    serving_size_text: product.nutrition?.servingSize ?? undefined,
    source: product.source,
    source_id: product.productUrl,
    source_created_at: sourceCreatedAt,
    source_last_updated_at: sourceLastUpdatedAt,
    image_url: product.imageUrl || undefined,
    nutrition: nutrition || undefined};
}

/**
 * KraftHeinz product detail URLs typically include a 12–14 digit GTIN/UPC-like prefix in the slug.
 * We derive upc12 by taking the last 12 digits of the first 12–14 digit run found in the path.
 */
function extractUpc12FromUrl(productUrl: string): string | null {
  try {
    const u = new URL(productUrl);
    const m = u.pathname.match(/(\d{12,14})/);
    if (!m?.[1]) return null;
    return m[1].slice(-12);
  } catch {
    return null;
  }
}

function extractUpc12FromText(text: string): string | null {
  // Look for explicit "UPC" patterns first
  const upcLabel = text.match(/UPC\s*(?:Code)?\s*[:#]?\s*(\d{12})/i);
  if (upcLabel?.[1]) return upcLabel[1];

  // Otherwise any standalone 12-digit run (avoid capturing years etc. by requiring word boundaries)
  const any = text.match(/\b(\d{12})\b/);
  return any?.[1] ?? null;
}

/**
 * Load more expansion: click until "Showing X of Y products" reaches total,
 * or until button disappears, or count stops increasing.
 */
async function expandAllProducts(page: Page): Promise<void> {
  const maxClicks = 250;
  let lastCount = -1;

  for (let i = 0; i < maxClicks; i++) {
    // Read "Showing X of Y products"
    const showingText = await page
      .locator("text=/Showing\\s+\\d+\\s+of\\s+\\d+\\s+products/i")
      .first()
      .textContent()
      .catch(() => null);

    if (showingText) {
      const m = showingText.match(/Showing\s+(\d+)\s+of\s+(\d+)\s+products/i);
      if (m) {
        const shown = Number(m[1]);
        const total = Number(m[2]);
        if (Number.isFinite(shown) && Number.isFinite(total) && total > 0 && shown >= total) return;
      }
    }

    // Look for a "Load More" button
    const loadMore = page.getByRole("button", { name: /load more/i });
    const visible = await loadMore.isVisible().catch(() => false);
    if (!visible) return;

    // Count product card links that look like detail pages
    const count = await page.locator('a[href*="/products/"]').count().catch(() => 0);

    if (count === lastCount) {
      // give it one more moment; if still stuck, stop
      await page.waitForTimeout(800);
      const count2 = await page.locator('a[href*="/products/"]').count().catch(() => 0);
      if (count2 === lastCount) return;
    }

    lastCount = count;

    await loadMore.click({ timeout: 10_000 }).catch(() => null);
    // Use domcontentloaded instead of networkidle for faster loading
    await page.waitForLoadState("domcontentloaded", { timeout: 10_000 }).catch(() => null);
    await page.waitForTimeout(200);
  }
}

/**
 * Extract product detail URLs from the listing page HTML.
 * Tightened filters:
 * - must contain "/products/" and have a trailing segment after "products"
 * - that trailing segment must contain a 12–14 digit run OR look like a product slug
 * - must be same origin as listingUrl (kraftheinz.com)
 */
function extractProductDetailUrls(listingUrl: string, html: string): string[] {
  const $ = cheerio.load(html);
  const base = new URL(listingUrl);
  const out = new Set<string>();

  $('a[href*="/products/"]').each((_, el) => {
    const href = $(el).attr("href");
    if (!href) return;

    let abs: string;
    try {
      abs = new URL(href, base).toString();
    } catch {
      return;
    }

    try {
      const u = new URL(abs);

      // Keep within same host as listing (reduces footer links to other sites)
      if (u.host !== base.host) return;

      // Require .../<brand>/products/<slug>
      const parts = u.pathname.split("/").filter(Boolean);
      const idx = parts.indexOf("products");
      if (idx < 0) return;
      if (parts.length <= idx + 1) return; // this would be the listing itself

      // Exclude HeinzSeed products (seed products, not food products)
      if (u.pathname.toLowerCase().includes("/heinzseed/")) return;

      const slug = parts[idx + 1];
      if (!slug) return;

      // Most detail slugs include a 12–14 digit prefix; prefer those.
      // If not, still allow but require it to be "sluggy" (letters/hyphens) and not generic.
      const hasDigits = /\d{12,14}/.test(slug);
      const isSluggy = /^[a-z0-9-]{6,}$/i.test(slug);

      if (!hasDigits && !isSluggy) return;

      // Avoid query-only variants
      u.search = "";
      u.hash = "";

      out.add(u.toString());
    } catch {
      return;
    }
  });

  return Array.from(out);
}

/**
 * Prefer deterministic extraction:
 * - Name: h1
 * - Ingredients: find "Ingredients" section label and read nearest value container
 * - Nutrition: parse "Nutrition Facts" section into structured fields + raw
 */
function parseName($: cheerio.CheerioAPI): string | null {
  const h1 = normalizeWhitespace($("h1").first().text());
  if (h1) return h1;

  const title = normalizeWhitespace($("title").first().text());
  if (title) return title;

  return null;
}

function parseNameFromNextData($: cheerio.CheerioAPI): string | null {
  try {
    const nextDataScript = $('script#__NEXT_DATA__').html();
    if (!nextDataScript) return null;
    const jsonData = JSON.parse(nextDataScript);
    const looksLikeSlug = (value: string): boolean => {
      const v = value.trim();
      if (!v) return true;
      if (/\s/.test(v)) return false;
      if (/[A-Z]/.test(v)) return false;
      return /-/.test(v) && /\d/.test(v);
    };

    const productFields = extractNextDataProductFields(jsonData);
    if (productFields) {
      const directCandidates = [
        productFields.name,
        productFields.displayName,
        productFields.productName,
        productFields.title,
        productFields.entryTitle,
      ].filter((v: any) => typeof v === "string") as string[];
      for (const candidate of directCandidates) {
        const v = normalizeWhitespace(candidate);
        if (
          v.length > 3 &&
          v.length < 200 &&
          !looksLikeSlug(v) &&
          !/navbar|menu|mega menu|nav bar/i.test(v) &&
          !/^\s*>/.test(v)
        ) {
          return v;
        }
      }
    }

    const findName = (obj: any): string | null => {
      if (!obj || typeof obj !== "object") return null;
      if (typeof obj.name === "string") {
        const v = normalizeWhitespace(obj.name);
        if (
          v.length > 3 &&
          v.length < 200 &&
          !looksLikeSlug(v) &&
          !/navbar|menu|mega menu|nav bar/i.test(v) &&
          !/^\s*>/.test(v)
        ) {
          return v;
        }
      }
      if (obj.fields?.name && typeof obj.fields.name === "string") {
        const v = normalizeWhitespace(obj.fields.name);
        if (
          v.length > 3 &&
          v.length < 200 &&
          !looksLikeSlug(v) &&
          !/navbar|menu|mega menu|nav bar/i.test(v) &&
          !/^\s*>/.test(v)
        ) {
          return v;
        }
      }
      if (obj.fields?.entryTitle && typeof obj.fields.entryTitle === "string") {
        const v = normalizeWhitespace(obj.fields.entryTitle);
        if (
          v.length > 3 &&
          v.length < 200 &&
          !looksLikeSlug(v) &&
          !/navbar|menu|mega menu|nav bar/i.test(v) &&
          !/^\s*>/.test(v)
        ) {
          return v;
        }
      }
      if (Array.isArray(obj)) {
        for (const item of obj) {
          const found = findName(item);
          if (found) return found;
        }
      } else {
        for (const key of Object.keys(obj)) {
          if (obj[key] && typeof obj[key] === "object") {
            const found = findName(obj[key]);
            if (found) return found;
          }
        }
      }
      return null;
    };
    return findName(jsonData);
  } catch {
    return null;
  }
}

function extractNextData($: cheerio.CheerioAPI): any | null {
  try {
    const nextDataScript = $('script#__NEXT_DATA__').html();
    if (!nextDataScript) return null;
    return JSON.parse(nextDataScript);
  } catch {
    return null;
  }
}

function extractNextDataProductFields(nextData: any): any | null {
  if (!nextData || typeof nextData !== "object") return null;
  const productRefs = nextData?.props?.pageProps?.template?.product?.references;
  if (Array.isArray(productRefs) && productRefs.length > 0) {
    // Prefer the actual product entry by content type when available
    for (const ref of productRefs) {
      const entry = ref?.entry ?? ref;
      const contentTypeId = entry?.sys?.contentType?.sys?.id;
      if (contentTypeId === "ct-product" && entry?.fields && typeof entry.fields === "object") {
        return entry.fields;
      }
    }
    for (const ref of productRefs) {
      const entry = ref?.entry ?? ref;
      const fields = entry?.fields;
      if (fields && typeof fields === "object") {
        const hasProductSignal =
          typeof fields.ingredients === "string" ||
          typeof fields.allergenInformation === "string" ||
          typeof fields.allergens === "string" ||
          typeof fields.name === "string" ||
          typeof fields.displayName === "string" ||
          typeof fields.productName === "string" ||
          typeof fields.title === "string";
        if (hasProductSignal) return fields;
      }
    }
  }
  return null;
}

function extractNextDataProductEntry(nextData: any): any | null {
  if (!nextData || typeof nextData !== "object") return null;
  const productRefs = nextData?.props?.pageProps?.template?.product?.references;
  if (Array.isArray(productRefs) && productRefs.length > 0) {
    for (const ref of productRefs) {
      const entry = ref?.entry ?? ref;
      const contentTypeId = entry?.sys?.contentType?.sys?.id;
      if (contentTypeId === "ct-product") return entry;
    }
    for (const ref of productRefs) {
      const entry = ref?.entry ?? ref;
      if (entry && typeof entry === "object") return entry;
    }
  }
  return null;
}

function extractImageUrlFromNextData(nextData: any, productUrl: string): string | null {
  const fields = extractNextDataProductFields(nextData);
  const candidates: any[] = [];
  if (!fields || typeof fields !== "object") return null;

  const pushMaybe = (value: any) => {
    if (value == null) return;
    if (Array.isArray(value)) {
      candidates.push(...value);
    } else {
      candidates.push(value);
    }
  };

  pushMaybe(fields.image);
  pushMaybe(fields.images);
  pushMaybe(fields.primaryImage);
  pushMaybe(fields.productImage);
  pushMaybe(fields.heroImage);
  pushMaybe(fields.packshot);

  for (const candidate of candidates) {
    if (!candidate) continue;
    if (typeof candidate === "string") {
      try {
        const url = candidate.startsWith("//") ? `https:${candidate}` : candidate;
        return new URL(url, productUrl).toString();
      } catch {
        continue;
      }
    }
    const url =
      candidate?.fields?.file?.url ??
      candidate?.file?.url ??
      candidate?.fields?.url ??
      candidate?.url ??
      null;
    if (typeof url === "string") {
      try {
        const resolved = url.startsWith("//") ? `https:${url}` : url;
        return new URL(resolved, productUrl).toString();
      } catch {
        continue;
      }
    }
  }

  return null;
}

function extractUpc12FromNextData($: cheerio.CheerioAPI): string | null {
  const nextData = extractNextData($);
  if (!nextData) return null;

  const entry = extractNextDataProductEntry(nextData);
  const fields = extractNextDataProductFields(nextData);
  const candidates: string[] = [];

  const pushCandidate = (value: any) => {
    if (typeof value === "string") candidates.push(value);
  };

  pushCandidate(fields?.upc);
  pushCandidate(fields?.upc12);
  pushCandidate(fields?.gtin);
  pushCandidate(fields?.gtin12);
  pushCandidate(fields?.gtin13);
  pushCandidate(fields?.gtin14);
  pushCandidate(fields?.productId);
  pushCandidate(fields?.productID);
  pushCandidate(fields?.sku);
  pushCandidate(fields?.itemNumber);
  pushCandidate(fields?.entryTitle);
  pushCandidate(entry?.sys?.id);

  for (const candidate of candidates) {
    const m = candidate.match(/(\d{12,14})/);
    if (m?.[1]) return m[1].slice(-12);
  }

  return null;
}

function findSectionValueByHeading($: cheerio.CheerioAPI, headingRegex: RegExp): string | null {
  // Look for headings/labels that match the section name and then read nearby content
  const candidates = $("h1,h2,h3,h4,h5,strong,span,div,p")
    .filter((_, el) => headingRegex.test(normalizeWhitespace($(el).text())))
    .toArray();

  for (const el of candidates) {
    const $el = $(el);

    // 1) If there's a sibling immediately after with meaningful text
    const nextText = normalizeWhitespace($el.next().text());
    if (nextText && nextText.length > 10 && nextText.length < 5000) return nextText;

    // 2) If the parent container has a distinct value node (common pattern)
    const parent = $el.parent();
    const parentText = normalizeWhitespace(parent.text());
    // If parent text is "Ingredients: blah", extract after label
    const mInline = parentText.match(new RegExp(`${headingRegex.source}\\s*:?\\s*(.+)$`, "i"));
    if (mInline?.[1]) {
      const v = normalizeWhitespace(mInline[1]);
      if (v.length > 10 && v.length < 5000) return v;
    }

    // 3) Try parent's next sibling
    const parentNextText = normalizeWhitespace(parent.next().text());
    if (parentNextText && parentNextText.length > 10 && parentNextText.length < 5000) return parentNextText;
  }

  return null;
}

function parseIngredients($: cheerio.CheerioAPI): string | null {
  // First, try to extract from __NEXT_DATA__ (primary source)
  try {
    const nextData = extractNextData($);
    const fields = extractNextDataProductFields(nextData);
    const direct = fields?.ingredients;
    if (typeof direct === "string") {
      const ingredients = normalizeWhitespace(direct);
      if (ingredients.length > 10 && ingredients.length < 2000) return ingredients;
    }
  } catch {
    // Continue to other methods
  }

  // Second, try to extract from JSON-LD structured data
  try {
    const scripts = $('script[type="application/ld+json"]').toArray();
    for (const el of scripts) {
      const jsonText = $(el).html();
      if (!jsonText) continue;
      
      try {
        const jsonData = JSON.parse(jsonText);
        
        // Recursively search for ingredients field
        const findIngredients = (obj: any): string | null => {
          if (!obj || typeof obj !== 'object') return null;
          
          // Check if this object has ingredients
          if (obj.ingredients && typeof obj.ingredients === 'string') {
            const ingredients = normalizeWhitespace(obj.ingredients);
            if (ingredients.length > 10 && ingredients.length < 2000) return ingredients;
          }
          
          // Check if this is an array of product objects
          if (Array.isArray(obj)) {
            for (const item of obj) {
              const found = findIngredients(item);
              if (found) return found;
            }
          }
          
          // Recursively search nested objects
          for (const key of Object.keys(obj)) {
            if (obj[key] && typeof obj[key] === 'object') {
              const found = findIngredients(obj[key]);
              if (found) return found;
            }
          }
          return null;
        };
        
        const ingredients = findIngredients(jsonData);
        if (ingredients) return ingredients;
      } catch {
        // Invalid JSON, continue
      }
    }
  } catch {
    // Continue to other methods
  }
  
  // Third, prefer a labeled "Ingredients" section in the DOM.
  // Some pages use "INGREDIENTS" uppercase; match exact-ish.
  const v =
    findSectionValueByHeading($, /^ingredients$/i) ??
    findSectionValueByHeading($, /^ingredients\s*(and|&)\s*allergens?$/i);

  if (v) return v;

  // Fallback: find "Ingredients:" anywhere but keep it bounded with stricter limits
  // Stop at common page boundaries to avoid capturing footer/navigation
  const body = normalizeWhitespace($("body").text());
  const m = body.match(/Ingredients\s*:?\s*([^]{20,1000}?)(?:Nutrition Facts|Allergens?|Contains:|Buy Online|Other Products|Follow Us|©\d{4}|Terms and Conditions|Privacy Notice|script|__NEXT_DATA__)/i);
  if (m?.[1]) {
    const ingredients = normalizeWhitespace(m[1]);
    // Additional validation: should not contain common footer/nav text
    if (!ingredients.includes('©') && !ingredients.includes('Terms and Conditions') && 
        !ingredients.includes('Privacy Notice') && !ingredients.includes('Follow Us') &&
        ingredients.length > 10 && ingredients.length < 2000) {
      return ingredients;
    }
  }

  return null;
}

function parseAllergens($: cheerio.CheerioAPI): string | null {
  // Try __NEXT_DATA__ first (primary source)
  try {
    const nextData = extractNextData($);
    const fields = extractNextDataProductFields(nextData);
    const direct = fields?.allergenInformation ?? fields?.allergens;
    if (typeof direct === "string") {
      const v = normalizeWhitespace(direct);
      if (v.length > 3 && v.length < 500) return v;
    }
  } catch {
    // ignore
  }

  // Try JSON-LD / embedded JSON next
  try {
    const scripts = $('script[type="application/ld+json"]').toArray();
    for (const el of scripts) {
      const jsonText = $(el).html();
      if (!jsonText) continue;
      try {
        const jsonData = JSON.parse(jsonText);
        const findAllergens = (obj: any): string | null => {
          if (!obj || typeof obj !== "object") return null;
          if (typeof obj.allergenInformation === "string") {
            const v = normalizeWhitespace(obj.allergenInformation);
            if (v.length > 3 && v.length < 500) return v;
          }
          if (typeof obj.allergens === "string") {
            const v = normalizeWhitespace(obj.allergens);
            if (v.length > 3 && v.length < 500) return v;
          }
          if (obj.fields?.allergenInformation && typeof obj.fields.allergenInformation === "string") {
            const v = normalizeWhitespace(obj.fields.allergenInformation);
            if (v.length > 3 && v.length < 500) return v;
          }
          if (Array.isArray(obj)) {
            for (const item of obj) {
              const found = findAllergens(item);
              if (found) return found;
            }
          } else {
            for (const key of Object.keys(obj)) {
              if (obj[key] && typeof obj[key] === "object") {
                const found = findAllergens(obj[key]);
                if (found) return found;
              }
            }
          }
          return null;
        };
        const allergens = findAllergens(jsonData);
        if (allergens) return allergens;
      } catch {
        // ignore JSON errors
      }
    }
  } catch {
    // ignore
  }

  const isValidAllergenText = (text: string): boolean => {
    if (!text || text.length > 300) return false;
    const lower = text.toLowerCase();
    if (lower.includes("buy online") || lower.includes("other products") || lower.includes("you may like")) {
      return false;
    }
    if (lower.includes("contains")) return true;
    return /(milk|egg|eggs|wheat|soy|peanut|tree nut|shellfish|fish|sesame)/i.test(text);
  };

  const v =
    findSectionValueByHeading($, /^allergens?$/i) ??
    findSectionValueByHeading($, /^contains:/i) ??
    findSectionValueByHeading($, /^ingredients\s*(and|&)\s*allergens?$/i);
  if (v && isValidAllergenText(v)) return v;

  const body = normalizeWhitespace($("body").text());
  const m = body.match(/Allergens?\s*:?\s*([^]{3,300})/i) || body.match(/Contains:\s*([^]{3,300})/i);
  if (m?.[1]) {
    const candidate = normalizeWhitespace(m[1]);
    if (isValidAllergenText(candidate)) return candidate;
  }

  return null;
}

async function submitNutritionParseJob(imageUrl: string): Promise<string | null> {
  try {
    const res = await axios.post(
      SUBMIT_NUTRITION_PARSE_JOB_URL,
      { image_url: imageUrl },
      {
        headers: {
          "Content-Type": "application/json",
          "X-Service-Token": PARSE_NUTRITION_SERVICE_TOKEN,
        },
        timeout: 60_000,
      }
    );
    const jobId = res?.data?.job_id;
    if (DEBUG_KH) {
      console.log("[DEBUG] submit nutrition parse response:");
      console.log(JSON.stringify(res?.data ?? null, null, 2));
    }
    return typeof jobId === "string" ? jobId : null;
  } catch (err) {
    if (DEBUG_KH) console.log("[DEBUG] nutrition API error:", err);
  }
  return null;
}

async function pollNutritionParseResult(jobId: string): Promise<any | null> {
  const start = Date.now();
  while (Date.now() - start < NUTRITION_POLL_TIMEOUT_MS) {
    try {
      const res = await axios.get(GET_NUTRITION_PARSE_RESULT_URL, {
        params: { job_id: jobId },
        headers: { "X-Service-Token": PARSE_NUTRITION_SERVICE_TOKEN },
        timeout: 30_000,
      });
      const status = res?.data?.status;
      if (DEBUG_KH) {
        console.log(`[DEBUG] nutrition parse status: ${status}`);
      }
      if (status === "completed") return res?.data?.result ?? null;
      if (status === "failed") return null;
    } catch (err) {
      if (DEBUG_KH) console.log("[DEBUG] nutrition poll error:", err);
    }
    await new Promise((r) => setTimeout(r, NUTRITION_POLL_INTERVAL_MS));
  }
  return null;
}

async function fetchNutritionFromImage(imageUrl: string): Promise<ScraperNutritionData | null> {
  const jobId = await submitNutritionParseJob(imageUrl);
  if (!jobId) return null;
  const result = await pollNutritionParseResult(jobId);
  const nutrition = result?.nutrition;
  if (DEBUG_KH) {
    console.log("[DEBUG] nutrition parse result:");
    console.log(JSON.stringify(result ?? null, null, 2));
  }
  if (nutrition && typeof nutrition === "object") {
    if (typeof normalizeNutritionData === "function") {
      return normalizeNutritionData(nutrition as ScraperNutritionData);
    }
    return nutrition as ScraperNutritionData;
  }
  return null;
}

function parseImageUrl($: cheerio.CheerioAPI, productUrl: string): string | null {
  // Try multiple strategies to find the product image
  
  // 0. Look for image in __NEXT_DATA__ (primary source)
  try {
    const nextData = extractNextData($);
    const nextImage = extractImageUrlFromNextData(nextData, productUrl);
    if (nextImage) return nextImage;
  } catch {
    // Continue to next strategy
  }

  // 1. Look for JSON-LD structured data with image
  try {
    let foundImage: string | null = null;
    $('script[type="application/ld+json"]').each((_, el) => {
      if (foundImage) return; // Already found, skip
      const jsonText = $(el).html();
      if (jsonText) {
        try {
          const jsonData = JSON.parse(jsonText);
          // Check if this has an image field
          if (jsonData.image) {
            const imageUrl = Array.isArray(jsonData.image) ? jsonData.image[0] : jsonData.image;
            if (imageUrl && typeof imageUrl === 'string') {
              foundImage = imageUrl;
              return;
            }
          }
        } catch {
          // Invalid JSON, continue
        }
      }
    });
    if (foundImage) {
      try {
        return new URL(foundImage, productUrl).toString();
      } catch {
        // Invalid URL, continue to next strategy
      }
    }
  } catch {
    // Continue to next strategy
  }
  
  // 2. Look for og:image meta tag (most reliable for product pages)
  const ogImage = $('meta[property="og:image"]').attr('content');
  if (ogImage) {
    try {
      return new URL(ogImage, productUrl).toString();
    } catch {
      // Invalid URL, continue
    }
  }
  
  // 3. Look for twitter:image meta tag
  const twitterImage = $('meta[name="twitter:image"]').attr('content');
  if (twitterImage) {
    try {
      return new URL(twitterImage, productUrl).toString();
    } catch {
      // Invalid URL, continue
    }
  }
  
  // 4. Look for the main product image (common patterns)
  const mainImage = $('img[class*="product"], img[class*="hero"], img[data-testid*="product"]').first().attr('src');
  if (mainImage) {
    try {
      return new URL(mainImage, productUrl).toString();
    } catch {
      // Invalid URL, continue
    }
  }
  
  // 5. Look for the first large image (heuristic: images that are likely product photos)
  const largeImage = $('img[src*="product"], img[src*="image"]').first().attr('src');
  if (largeImage && !largeImage.includes('logo') && !largeImage.includes('icon')) {
    try {
      return new URL(largeImage, productUrl).toString();
    } catch {
      // Invalid URL, continue
    }
  }
  
  return null;
}

function extractUpc12FromJsonLd($: cheerio.CheerioAPI): string | null {
  // Look for JSON-LD structured data with gtin12, gtin, or similar fields
  let foundUpc: string | null = null;
  
  $('script[type="application/ld+json"]').each((_, el) => {
    if (foundUpc) return; // Already found, skip
    
    const jsonText = $(el).html();
    if (!jsonText) return;
    
    try {
      const jsonData = JSON.parse(jsonText);
      
      // Check for gtin12, gtin13, gtin, sku fields
      const candidates = [
        jsonData.gtin12,
        jsonData.gtin,
        jsonData.gtin13,
        jsonData.productID,
        jsonData.sku,
      ];
      
      for (const candidate of candidates) {
        if (candidate && typeof candidate === 'string') {
          // Check if it's a valid 12-digit UPC
          const match = candidate.match(/(\d{12})/);
          if (match) {
            foundUpc = match[1];
            return false; // Break out of .each()
          }
        }
      }
    } catch {
      // Invalid JSON, continue
    }
  });
  
  return foundUpc;
}

function parseNutritionFromNextData(nextData: any): Nutrition | null {
  if (!nextData || typeof nextData !== "object") return null;
  try {
    // Search recursively for objects containing nutrition data (with items array and servingSize)
    const findNutritionData = (obj: any, path: string = 'root'): { items: any[]; servingSize?: string; servingsPerContainer?: string } | null => {
      if (!obj || typeof obj !== 'object') return null;
      
      // Check if this is an object with an items array containing nutrition fields
      if (!Array.isArray(obj) && obj.items && Array.isArray(obj.items)) {
        // Check if first item has the nutrition fields structure
        if (obj.items.length > 0 && obj.items[0]?.fields?.externalId) {
          const externalId = obj.items[0].fields.externalId;
          if (externalId.includes('NUTRSRV1')) {
            return {items: obj.items,
              servingSize: obj.servingSize || undefined,
              servingsPerContainer: obj.servingsPerContainer || undefined};
          }
        }
      }
      
      // Check if this is an object with fields.items array (alternative structure)
      if (!Array.isArray(obj) && obj.fields?.items && Array.isArray(obj.fields.items)) {
        // Check if first item has the nutrition fields structure
        if (obj.fields.items.length > 0 && obj.fields.items[0]?.fields?.externalId) {
          const externalId = obj.fields.items[0].fields.externalId;
          if (externalId.includes('NUTRSRV1')) {
            return {items: obj.fields.items,
              servingSize: obj.servingSize || obj.fields.servingSize || undefined,
              servingsPerContainer: obj.servingsPerContainer || obj.fields.servingsPerContainer || undefined};
          }
        }
      }
      
      // Check if this is an array of nutrition objects with fields (fallback for different structure)
      if (Array.isArray(obj)) {
        if (obj.length > 0 && obj[0]?.fields?.externalId) {
          const externalId = obj[0].fields.externalId;
          if (externalId.includes('NUTRSRV1')) {
            return {items: obj};
          }
        }
        // Also recursively search within array elements
        for (let i = 0; i < Math.min(obj.length, 10); i++) { // Limit to first 10 to avoid performance issues
          if (obj[i] && typeof obj[i] === 'object') {
            const found = findNutritionData(obj[i], `${path}[${i}]`);
            if (found) return found;
          }
        }
        return null; // Don't continue with object key iteration for arrays
      }
      
      // Recursively search nested objects/arrays
      for (const key of Object.keys(obj)) {
        if (obj[key] && typeof obj[key] === 'object') {
          const found = findNutritionData(obj[key], `${path}.${key}`);
          if (found) return found;
        }
      }
      return null;
    };
    
    const nutritionData = findNutritionData(nextData);
    
    if (nutritionData && nutritionData.items && nutritionData.items.length > 0) {
      const nutritionArray = nutritionData.items;
      let servingSize: string | null = nutritionData.servingSize || null;
      let servingsPerContainer: string | null = nutritionData.servingsPerContainer || null;
      let calories: string | null = null;
      const nutrients: NutritionNutrient[] = [];
      
      // Map of externalId to nutrient name
      const nutrientNameMap: { [key: string]: string } = {
        'F_NUTRSRV1_FAT': 'Total Fat',
        'F_NUTRSRV1_TOTFAT': 'Total Fat',
        'F_NUTRSRV1_SATFAT': 'Saturated Fat',
        'F_NUTRSRV1_FASAT': 'Saturated Fat',
        'F_NUTRSRV1_TRNFAT': 'Trans Fat',
        'F_NUTRSRV1_FATRN': 'Trans Fat',
        'F_NUTRSRV1_FAPU': 'Polyunsaturated Fat',
        'F_NUTRSRV1_FAMS': 'Monounsaturated Fat',
        'F_NUTRSRV1_CHOL': 'Cholesterol',
        'F_NUTRSRV1_SOD': 'Sodium',
        'F_NUTRSRV1_NA': 'Sodium',
        'F_NUTRSRV1_TOTCARB': 'Total Carbohydrate',
        'F_NUTRSRV1_CARB': 'Total Carbohydrate',
        'F_NUTRSRV1_CHO': 'Total Carbohydrate',
        'F_NUTRSRV1_FIBTSW': 'Dietary Fiber',
        'F_NUTRSRV1_FIB': 'Dietary Fiber',
        'F_NUTRSRV1_TOTSUG': 'Total Sugars',
        'F_NUTRSRV1_SUGAR': 'Total Sugars',
        'F_NUTRSRV1_ADDSUG': 'Added Sugars',
        'F_NUTRSRV1_PRO': 'Protein',
        'F_NUTRSRV1_VITD': 'Vitamin D',
        'F_NUTRSRV1_CA': 'Calcium',
        'F_NUTRSRV1_FE': 'Iron',
        'F_NUTRSRV1_K': 'Potassium',
      };
      
      // Process each nutrition field
      for (const item of nutritionArray) {
        const fields = item.fields || item;
        const externalId = fields.externalId || fields.id;
        const amount = fields.amount;
        const dailyPercent = fields.dailyPercent || fields.dailyValue;
        
        if (!externalId) continue;
        
        // Check for serving size
        if (externalId.includes('SERVSIZE') || externalId.includes('SERVSIZ')) {
          servingSize = amount || null;
          continue;
        }
        
        // Check for servings per container
        if (externalId.includes('SERVCON')) {
          servingsPerContainer = amount || null;
          continue;
        }
        
        // Check for calories
        if (externalId.includes('CALORIES') || externalId.includes('CAL') || externalId.includes('ENER')) {
          calories = amount || null;
          continue;
        }
        
        // Check if this is a known nutrient
        const nutrientName = nutrientNameMap[externalId];
        if (nutrientName && amount) {
          nutrients.push({
            name: nutrientName,
            amount: String(amount),
            dailyValuePercent: dailyPercent ? String(dailyPercent) : null,
          });
        }
      }
        
      if (servingSize || calories || nutrients.length > 0) {
        return {
          servingSize: servingSize ? normalizeWhitespace(servingSize) : null,
          servingsPerContainer: servingsPerContainer ? normalizeWhitespace(servingsPerContainer) : null,
          calories,
          nutrients,
          rawText: `Found ${nutrients.length} nutrients from __NEXT_DATA__`,
        };
      }
    }
  } catch (e) {
    console.log('Error in __NEXT_DATA__ nutrition extraction:', e);
  }
  return null;
}

function parseNutrition($: cheerio.CheerioAPI): Nutrition | null {
  // First, try to extract from __NEXT_DATA__
  const nextData = extractNextData($);
  const nextNutrition = parseNutritionFromNextData(nextData);
  if (nextNutrition) return nextNutrition;

  // Second, try to extract from JSON-LD structured data
  try {
    const scripts = $('script[type="application/ld+json"]').toArray();
    
    for (let scriptIdx = 0; scriptIdx < scripts.length; scriptIdx++) {
      const el = scripts[scriptIdx];
      const jsonText = $(el).html();
      if (!jsonText) continue;
      
      try {
        const jsonData = JSON.parse(jsonText);
        
        // Check if this JSON-LD has nutrition information
        // Look for arrays with nutrition field objects
        const findNutritionArray = (obj: any, path: string = 'root'): any[] | null => {
          if (!obj || typeof obj !== 'object') return null;
          
          // Check if this is an array of nutrition objects with fields
          if (Array.isArray(obj)) {
            // Check if first item has the nutrition fields structure
            if (obj.length > 0 && obj[0]?.fields?.externalId) {
              const externalId = obj[0].fields.externalId;
              if (externalId.includes('NUTRSRV1')) {
                return obj;
              }
            }
          }
          
          // Recursively search nested objects/arrays
          for (const key of Object.keys(obj)) {
            if (obj[key] && typeof obj[key] === 'object') {
              const found = findNutritionArray(obj[key], `${path}.${key}`);
              if (found) return found;
            }
          }
          return null;
        };
        
        const nutritionArray = findNutritionArray(jsonData);
        
        if (nutritionArray && nutritionArray.length > 0) {
          let servingSize: string | null = null;
          let servingsPerContainer: string | null = null;
          let calories: string | null = null;
          const nutrients: NutritionNutrient[] = [];
          
          // Map of externalId to nutrient name
        const nutrientNameMap: { [key: string]: string } = {
          'F_NUTRSRV1_FAT': 'Total Fat',
          'F_NUTRSRV1_TOTFAT': 'Total Fat',
          'F_NUTRSRV1_SATFAT': 'Saturated Fat',
          'F_NUTRSRV1_FASAT': 'Saturated Fat',
          'F_NUTRSRV1_TRNFAT': 'Trans Fat',
          'F_NUTRSRV1_FATRN': 'Trans Fat',
          'F_NUTRSRV1_FAPU': 'Polyunsaturated Fat',
          'F_NUTRSRV1_FAMS': 'Monounsaturated Fat',
          'F_NUTRSRV1_CHOL': 'Cholesterol',
          'F_NUTRSRV1_SOD': 'Sodium',
          'F_NUTRSRV1_TOTCARB': 'Total Carbohydrate',
          'F_NUTRSRV1_CARB': 'Total Carbohydrate',
          'F_NUTRSRV1_CHO': 'Total Carbohydrate',
          'F_NUTRSRV1_FIBTSW': 'Dietary Fiber',
          'F_NUTRSRV1_FIB': 'Dietary Fiber',
          'F_NUTRSRV1_TOTSUG': 'Total Sugars',
          'F_NUTRSRV1_SUGAR': 'Total Sugars',
          'F_NUTRSRV1_ADDSUG': 'Added Sugars',
          'F_NUTRSRV1_PRO': 'Protein',
          'F_NUTRSRV1_VITD': 'Vitamin D',
          'F_NUTRSRV1_CA': 'Calcium',
          'F_NUTRSRV1_FE': 'Iron',
          'F_NUTRSRV1_K': 'Potassium',
        };
          
          // Process each nutrition field
          for (const item of nutritionArray) {
            const fields = item.fields || item;
            const externalId = fields.externalId || fields.id;
            const amount = fields.amount;
            const dailyPercent = fields.dailyPercent || fields.dailyValue;
            
            if (!externalId) continue;
            
            // Check for serving size
            if (externalId.includes('SERVSIZE') || externalId.includes('SERVSIZ')) {
              servingSize = amount || null;
              continue;
            }
            
            // Check for servings per container
            if (externalId.includes('SERVCON')) {
              servingsPerContainer = amount || null;
              continue;
            }
            
            // Check for calories
          if (externalId.includes('CALORIES') || externalId.includes('CAL') || externalId.includes('ENER')) {
            calories = amount || null;
            continue;
          }
            
            // Check if this is a known nutrient
            const nutrientName = nutrientNameMap[externalId];
            if (nutrientName && amount) {
              nutrients.push({
                name: nutrientName,
                amount: String(amount),
                dailyValuePercent: dailyPercent ? String(dailyPercent) : null,
              });
            }
          }
          
          if (servingSize || calories || nutrients.length > 0) {
            return {
              servingSize,
              servingsPerContainer,
              calories,
              nutrients,
              rawText: `Found ${nutrients.length} nutrients from structured data`,
            };
          }
        }
      } catch (e) {
        // Invalid JSON, continue to next script tag
        continue;
      }
    }
  } catch (e) {
    // Continue to DOM parsing
  }
  
  // Fallback: Try to locate a "Nutrition Facts" block and parse common fields.
  // Keep a rawText fallback to avoid losing data if structure changes.

  // Find an element that contains "Nutrition Facts" and use its nearest container.
  const nfEl = $("*:contains('Nutrition Facts')")
    .filter((_, el) => {
      const t = normalizeWhitespace($(el).text());
      return /^nutrition facts$/i.test(t) || t.toLowerCase().includes("nutrition facts");
    })
    .first();

  if (!nfEl.length) return null;

  // Container: go up a bit (heuristic) to capture the facts table/lines.
  const container = nfEl.closest("section,article,div").first();
  const rawText = normalizeWhitespace(container.text());
  if (!rawText) return null;

  const servingSize =
    rawText.match(/Serving Size\s*[: ]\s*([^\n\r]+?)(?:Servings Per|Calories|$)/i)?.[1]?.trim() ?? null;

  const servingsPerContainer =
    rawText.match(/Servings Per (?:Container|Package)\s*[: ]\s*([^\n\r]+?)(?:Calories|$)/i)?.[1]?.trim() ?? null;

  const calories =
    rawText.match(/Calories\s*[: ]\s*(\d+)\b/i)?.[1]?.trim() ??
    rawText.match(/\bCalories\s+(\d+)\b/i)?.[1]?.trim() ??
    null;

  // Parse nutrient lines heuristically
  // Example patterns:
  // "Total Fat 0g 0%"
  // "Sodium 150mg 7%"
  // "Total Sugars 10g" (no DV)
  const nutrients: NutritionNutrient[] = [];
  const lines = rawText
    .split(/(?:(?:\r?\n)+)|(?:\s{2,})/)
    .map((l) => normalizeWhitespace(l))
    .filter((l) => l.length > 0);

  const seen = new Set<string>();

  for (const line of lines) {
    if (/nutrition facts/i.test(line)) continue;
    if (/serving size/i.test(line)) continue;
    if (/servings per/i.test(line)) continue;

    // Nutrient line capture: label + amount + optional dv%
    const m = line.match(
      /^(Total Fat|Saturated Fat|Trans Fat|Cholesterol|Sodium|Total Carbohydrate|Dietary Fiber|Total Sugars|Includes\s+Added\s+Sugars|Added Sugars|Protein|Vitamin D|Calcium|Iron|Potassium)\s+([0-9][^%]*?)\s*(\d+%|%DV\s*\d+%|)?$/i
    );
    if (m) {
      const name = normalizeWhitespace(m[1]);
      const amount = normalizeWhitespace(m[2]);
      const dv = m[3] ? normalizeWhitespace(m[3]).replace(/^%DV\s*/i, "") : null;

      const key = name.toLowerCase();
      if (!seen.has(key)) {
        nutrients.push({ name, amount, dailyValuePercent: dv });
        seen.add(key);
      }
    }
  }

  return {servingSize: servingSize ? normalizeWhitespace(servingSize) : null,
    servingsPerContainer: servingsPerContainer ? normalizeWhitespace(servingsPerContainer) : null,
    calories,
    nutrients,
    rawText};
}

async function scrapeListing(browser: Browser, brandCfg: BrandConfig): Promise<string[]> {
  const page = await browser.newPage();
  try {
    // Use domcontentloaded instead of networkidle for better reliability
    // Many modern sites continuously make network requests
    await page.goto(brandCfg.listingUrl, { 
      waitUntil: "domcontentloaded", 
      timeout: 60_000 
    });
    
    // Wait a bit for dynamic content to load (reduced from 2000ms)
    await page.waitForTimeout(1000);

    // Expand via Load More if present
    await expandAllProducts(page);

    const html = await page.content();
    const urls = extractProductDetailUrls(brandCfg.listingUrl, html);

    return urls;
  } finally {
    await page.close().catch(() => null);
  }
}

async function scrapeProductDetail(browser: Browser, brandCfg: BrandConfig, productUrl: string): Promise<ScrapedProduct | null> {
  const page = await browser.newPage();
  try {
    // Use domcontentloaded for better reliability
    await page.goto(productUrl, { 
      waitUntil: "domcontentloaded", 
      timeout: 60_000 
    });
    // Wait for page to be ready (reduced timeout since we're using domcontentloaded)
    await page.waitForTimeout(1000);

    // Early existence check before expanding accordions or fetching nutrition images
    try {
      const earlyHtml = await page.content();
      const $early = cheerio.load(earlyHtml);
      const earlyRawName = parseName($early);
      const earlyName =
        typeof cleanProductName === "function"
          ? cleanProductName(earlyRawName, {
              brand: brandCfg.brand,
              decodeHtml: true,
              stripBrandPrefix: true,
              stripPipe: true,
              stripTrailingDashSize: true,
              stripTrailingCommaSize: true,
            })
          : earlyRawName;
      const earlyUpc12 = extractUpc12FromNextData($early) || extractUpc12FromUrl(productUrl);

      const exists = await checkProductExists({
        name: earlyName,
        brand: brandCfg.brand,
        upc: earlyUpc12,
      });
      if (exists) {
        console.log(`[SKIP] ${brandCfg.brand} ${earlyName ?? "(no name)"}: already exists`);
        return null;
      }
    } catch (e) {
      if (DEBUG_KH) console.log("[DEBUG] early existence check failed:", e);
    }

    // Try to click on accordion/tab sections to reveal hidden content
    // Look for "Nutrition", "Ingredients", "Allergen" buttons/links
    // Use Promise.all to try both simultaneously to save time
    await Promise.all([
      (async () => {
        try {
          const nutritionButton = page.locator('button:has-text("Nutrition"), a:has-text("Nutrition"), [role="button"]:has-text("Nutrition")').first();
          if (await nutritionButton.isVisible({ timeout: 2000 }).catch(() => false)) {
            await nutritionButton.click({ timeout: 2000 }).catch(() => null);
          }
        } catch (e) {
          // Ignore if nutrition section not found or clickable
        }
      })(),
      (async () => {
        try {
          const ingredientsButton = page.locator('button:has-text("Ingredients"), a:has-text("Ingredients"), [role="button"]:has-text("Ingredients")').first();
          if (await ingredientsButton.isVisible({ timeout: 2000 }).catch(() => false)) {
            await ingredientsButton.click({ timeout: 2000 }).catch(() => null);
          }
        } catch (e) {
          // Ignore if ingredients section not found or clickable
        }
      })(),
    ]);
    
    // Brief wait after clicking to allow content to load
    await page.waitForTimeout(1000);
    await page
      .waitForSelector("img[data-testid='or-product-accordion-nutritional-image']", { timeout: 1500 })
      .catch(() => {});

    const html = await page.content();
    const $ = cheerio.load(html);

    const rawName = parseName($);
    const name =
      typeof cleanProductName === "function"
        ? cleanProductName(rawName, {
            brand: brandCfg.brand,
            decodeHtml: true,
            stripBrandPrefix: true,
            stripPipe: true,
            stripTrailingDashSize: true,
            stripTrailingCommaSize: true,
          })
        : rawName;
    const ingredients = parseIngredients($);
    const allergens = parseAllergens($);
    const imageUrl = parseImageUrl($, productUrl);
    const nutritionImageUrl = $("img[data-testid='or-product-accordion-nutritional-image']").attr("src") || null;
    if (DEBUG_KH) {
      console.log(`[DEBUG] nutrition image url: ${nutritionImageUrl ?? "(null)"}`);
    }

    // UPC: Try __NEXT_DATA__ first, then JSON-LD, then URL, then fallback to page text
    const upc12FromNextData = extractUpc12FromNextData($);
    const upc12FromJsonLd = upc12FromNextData ? null : extractUpc12FromJsonLd($);
    const upc12FromUrl = (upc12FromNextData || upc12FromJsonLd) ? null : extractUpc12FromUrl(productUrl);
    const upc12FromPage = (upc12FromNextData || upc12FromJsonLd || upc12FromUrl)
      ? null
      : extractUpc12FromText(normalizeWhitespace($("body").text()));
    const upc12 = upc12FromNextData ?? upc12FromJsonLd ?? upc12FromUrl ?? upc12FromPage;

    const nutrition = parseNutrition($);
    const nutritionFromDom = transformNutritionToDbFormat(nutrition);
    const shouldUseImage =
      !!nutritionImageUrl &&
      (!nutritionFromDom || nutritionFromDom.added_sugars_g == null);
    const nutritionData = shouldUseImage ? await fetchNutritionFromImage(nutritionImageUrl!) : null;

    // Extract source dates from __NEXT_DATA__
    const sourceDates = extractSourceDates($);

    const scrapedAt = new Date().toISOString();

    return {
      brand: brandCfg.brand,
      source: brandCfg.source,
      listingUrl: brandCfg.listingUrl,
      productUrl,

      name,
      ingredients,
      allergens,
      upc12,
      nutrition,
      nutritionData,
      nutritionImageUrl,
      imageUrl,

      scrapedAt,
      sourceCreatedAt: sourceDates.createdAt,
      sourceLastUpdatedAt: sourceDates.updatedAt,
      fingerprint: sha256(
        JSON.stringify({
          source: brandCfg.source,
          brand: brandCfg.brand,
          productUrl,
          upc12,
          name,
        })
      ),
    };
  } finally {
    await page.close().catch(() => null);
  }
}

/** Parse CLI args: similar to Amazon scraper style */
function parseKraftHeinzArgs(): {
  configPath?: string;
  url?: string;
  searchUrl?: string;
  limit?: number;
  offset: number;
  local: boolean;
  debug: boolean;
  noHeadless: boolean;
  headless: boolean;
  concurrency: number;
} {
  const argv = process.argv.slice(2);
  const defaultConfig = path.resolve(__dirname, "./config.json");
  let configPath: string | undefined = defaultConfig;
  let url: string | undefined;
  let searchUrl: string | undefined;
  let limit: number | undefined;
  let offset = 0;
  let local = false;
  let debug = false;
  let noHeadless = false;
  let headless = false;
  let concurrency = 6;
  for (let i = 0; i < argv.length; i++) {
    if ((argv[i] === "--url" || argv[i] === "-u") && argv[i + 1]) {
      url = argv[i + 1];
      i++;
    } else if ((argv[i] === "--search" || argv[i] === "-s") && argv[i + 1]) {
      searchUrl = argv[i + 1];
      i++;
    } else if ((argv[i] === "--config" || argv[i] === "-c") && argv[i + 1]) {
      configPath = path.resolve(argv[i + 1]);
      i++;
    } else if (argv[i] === "--limit" || argv[i] === "-l") {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n > 0) {
        limit = n;
        i++;
      }
    } else if ((argv[i] === "--offset" || argv[i] === "-o") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n >= 0) offset = n;
      i++;
    } else if (argv[i] === "--local") {
      local = true;
    } else if ((argv[i] === "--debug" || argv[i] === "-d")) {
      debug = true;
    } else if (argv[i] === "--no-headless") {
      noHeadless = true;
    } else if (argv[i] === "--headless") {
      headless = true;
    } else if ((argv[i] === "--concurrency" || argv[i] === "-n") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n > 0) concurrency = n;
      i++;
    }
  }
  return { configPath, url, searchUrl, limit, offset, local, debug, noHeadless, headless, concurrency };
}

async function main(): Promise<void> {
  const { configPath, url, searchUrl, limit: productLimit, offset, local, debug, noHeadless, headless, concurrency } =
    parseKraftHeinzArgs();
  if (!configPath && !url && !searchUrl) {
    console.error("Usage: npx tsx scrape.ts --config ./config.json [--limit N] [--offset N] [--local]");
    console.error("   or: npx tsx scrape.ts --url <productUrl> [--local]");
    console.error("   or: npx tsx scrape.ts --search <listingUrl> [--local]");
    process.exit(1);
  }

  const cfg = configPath ? (JSON.parse(fs.readFileSync(configPath, "utf8")) as AppConfig) : null;

  DEBUG_KH = debug || DEBUG_KH;
  if (noHeadless) KRAFT_HEADLESS = false;
  if (headless) KRAFT_HEADLESS = true;

  if (local) {
    console.log("Running in local mode: skipping DynamoDB job status and S3 upload; API submission still runs (use AWS profile for SSM).");
  }
  if (productLimit != null) {
    console.log(`Limit: scraping at most ${productLimit} products.`);
  }
  if (offset > 0) {
    console.log(`Offset: skipping first ${offset} products.`);
  }

  const jobId = uuidv4();
  const runDateTime = new Date().toISOString().replace(/[:.]/g, "-").slice(0, -5);

  // Create job status entry (skip when running locally)
  if (!local && SCRAPER_JOB_STATUS_TABLE_NAME) {
    const putCommand = new PutCommand({
      TableName: SCRAPER_JOB_STATUS_TABLE_NAME,
      Item: {
        job_id: jobId,
        scraper_name: SCRAPER_NAME,
        status: "active",
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      },
    });
    await docClient.send(putCommand);
    console.log(`Created job ${jobId} with status: active`);
  }

  // Optimize browser launch for Docker/ECS environment
  const browser = await chromium.launch({
    headless: KRAFT_HEADLESS,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage', // Overcome limited resource problems in Docker
      '--disable-accelerated-2d-canvas',
      '--disable-gpu',
      '--disable-software-rasterizer',
      '--disable-background-networking',
      '--disable-background-timer-throttling',
      '--disable-renderer-backgrounding',
      '--disable-backgrounding-occluded-windows',
      '--disable-breakpad',
      '--disable-component-extensions-with-background-pages',
      '--disable-extensions',
      '--disable-features=TranslateUI',
      '--disable-ipc-flooding-protection',
      '--disable-sync',
      '--metrics-recording-only',
      '--mute-audio',
      '--no-first-run',
      '--safebrowsing-disable-auto-update',
      '--enable-automation',
      '--password-store=basic',
      '--use-mock-keychain',
    ],
  });

  try {
    // 1) Discover product detail URLs
    const allTargets: Array<{ brandCfg: BrandConfig; url: string }> = [];

    const defaultBrand = cfg?.brand || "Kraft Heinz";
    const defaultSource = cfg?.source || "kraftheinz";

    if (url) {
      allTargets.push({
        brandCfg: { brand: defaultBrand, source: defaultSource, listingUrl: "" },
        url,
      });
    } else {
      const searchTargets: Array<{ url: string; brand: string; source: string }> = [];
      if (searchUrl) {
        searchTargets.push({ url: searchUrl, brand: defaultBrand, source: defaultSource });
      } else if (cfg?.searchUrls) {
        for (const entry of cfg.searchUrls) {
          if (typeof entry === "string") {
            searchTargets.push({ url: entry, brand: defaultBrand, source: defaultSource });
          } else if (entry && typeof entry.url === "string") {
            searchTargets.push({
              url: entry.url,
              brand: entry.brand || defaultBrand,
              source: entry.source || defaultSource,
            });
          }
        }
      }

      if (cfg?.urls) {
        for (const entry of cfg.urls) {
          if (typeof entry === "string") {
            allTargets.push({
              brandCfg: { brand: defaultBrand, source: defaultSource, listingUrl: "" },
              url: entry,
            });
          } else if (entry && typeof entry.url === "string") {
            allTargets.push({
              brandCfg: { brand: entry.brand || defaultBrand, source: entry.source || defaultSource, listingUrl: "" },
              url: entry.url,
            });
          }
        }
      }

      for (const target of searchTargets) {
        if (productLimit != null && allTargets.length >= productLimit + offset) break;
        const brandCfg: BrandConfig = { brand: target.brand, source: target.source, listingUrl: target.url };
        console.log(`\n[DISCOVER] ${brandCfg.brand}: ${brandCfg.listingUrl}`);
        const urls = await scrapeListing(browser, brandCfg);
        console.log(`[DISCOVER] ${brandCfg.brand}: ${urls.length} product URLs`);
        for (const u of urls) {
          allTargets.push({ brandCfg, url: u });
          if (productLimit != null && allTargets.length >= productLimit + offset) break;
        }
      }
    }

    const slicedTargets = allTargets.slice(offset, productLimit ? offset + productLimit : undefined);
    if (productLimit != null && allTargets.length > productLimit) {
      console.log(`[LIMIT] Scraping ${slicedTargets.length} of ${allTargets.length} discovered URLs`);
    }

    const concurrencyLimit = pLimit(Math.max(1, concurrency || cfg?.concurrency || 4));

    const results = await Promise.all(
      slicedTargets.map(({ brandCfg, url }) =>
        concurrencyLimit(async () => {
          try {
            const product = await scrapeProductDetail(browser, brandCfg, url);
            if (!product) return null;
            console.log(`[OK] ${brandCfg.brand} ${product.upc12 ?? ""} ${product.name ?? ""}`.trim());
            return product;
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            console.error(`[ERR] ${brandCfg.brand} ${url}: ${msg}`);
            return { error: msg, url, brandCfg } as any;
          }
        })
      )
    );

    // Filter out errors and products without required fields
    const validProducts = results.filter(
      (r) => r && !(r as any).error && (r as ScrapedProduct).name && (r as ScrapedProduct).ingredients
    ) as ScrapedProduct[];

    console.log(`\nScraped ${validProducts.length} valid products out of ${results.length} total`);

    // Transform valid products to API format for S3 upload (for reference/backup)
    const transformedProducts = validProducts.map((product) => {
      const apiRequest = transformProductToApiRequest(product);
      // Add job_id for S3 backup (scraper job ID, not product review job ID)
      return {...apiRequest, scraper_job_id: jobId};
    });

    // Upload results to S3 (skip when running locally). S3: PutObject of products.json to scraper-outputs bucket.
    if (!local) {
      await uploadToS3(transformedProducts, jobId, runDateTime);
    }

    // Submit each valid product via API (runs locally too; use AWS profile for SSM API key).
    console.log(`\n📤 Submitting products for review via API...`);
    const apiLimit = pLimit(10); // Submit 10 products concurrently to avoid overwhelming the API

    const apiResults = await Promise.all(
      validProducts.map((product) =>
        apiLimit(async () => {
          const success = await submitProductForReview(product);
          return success;
        })
      )
    );

    const apiSuccessCount = apiResults.filter((r) => r === true).length;
    const apiFailureCount = apiResults.filter((r) => r === false).length;

    console.log(`\n📊 API Submission Summary: ${apiSuccessCount} submitted successfully, ${apiFailureCount} failed`);

    // Update job status (skip when running locally)
    if (!local) {
      if (validProducts.length === 0) {
        await updateJobStatus(jobId, "error", "No products were scraped");
        process.exit(1);
      } else {
        await updateJobStatus(jobId, "complete");
        console.log(`Job ${jobId} completed successfully with ${validProducts.length} products`);
      }
    } else {
      if (validProducts.length === 0) {
        console.error("No products were scraped.");
        process.exit(1);
      }
      console.log(`\n✅ Local run complete: ${validProducts.length} products scraped; API submissions ran (DynamoDB/S3 skipped).`);
    }
  } catch (error: any) {
    console.error("Fatal error:", error);
    if (!local) {
      await updateJobStatus(jobId, "failure", error?.message || String(error));
    }
    process.exit(1);
  } finally {
    await browser.close().catch(() => null);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
