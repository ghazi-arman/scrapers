import * as crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import { chromium, Browser, Page } from "playwright";
import * as cheerio from "cheerio";
import pLimit from "p-limit";
import axios from "axios";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import { parseNutrientAmountWithQualifier } from "../nutrition-utils";

type BrandConfig = {
  brand: string;
  source: string;
  listingUrl: string;
};

type AppConfig = {
  outputPath: string;
  concurrency: number;
  brands: BrandConfig[];
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
  upc12: string | null;
  nutrition: Nutrition | null;
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
const API_BASE_URL = process.env.API_BASE_URL || "https://it7rdy3qbh.execute-api.us-west-2.amazonaws.com";
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";

// Cache for service token
let serviceTokenCache: string | null = null;

function ensureDirForFile(filePath: string): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function normalizeWhitespace(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

function sha256(input: string): string {
  return crypto.createHash("sha256").update(input).digest("hex");
}

/**
 * Parse serving size text (e.g., "1 Tbsp (20g)", "8 fl oz", "240ml", "1/4 cup", "1/2 cup") into value and unit
 */
function parseServingSize(servingSizeText: string | null): { value: number | null; unit: string | null } {
  if (!servingSizeText || typeof servingSizeText !== 'string') {
    return { value: null, unit: null };
  }

  // Clean the text - remove parentheses content and extra whitespace
  const cleaned = servingSizeText.trim().replace(/\([^)]*\)/g, '').trim();
  
  // Try multiple patterns to be more lenient
  // Pattern 1: Fraction like "1/4 cup" or "1/2 cup" or "3/4 cup"
  let match = cleaned.match(/^(\d+)\/(\d+)\s+(Tbsp|tbsp|TSP|tsp|cup|cups|oz|fl\s*oz|floz|ml|g|kg|lb|lbs|serving|servings)\b/i);
  if (match) {
    const numerator = parseFloat(match[1]);
    const denominator = parseFloat(match[2]);
    const value = numerator / denominator;
    let unit = match[3].trim();
    
    // Normalize unit
    if (unit.toLowerCase() === 'tbsp') unit = 'Tbsp';
    else if (unit.toLowerCase() === 'tsp') unit = 'tsp';
    else if (unit.toLowerCase() === 'cups' || unit.toLowerCase() === 'cup') unit = 'cup';
    else if (unit.toLowerCase() === 'lbs') unit = 'lb';
    else if (unit.toLowerCase() === 'fl oz' || unit.toLowerCase() === 'floz') unit = 'fl oz';
    else if (unit.toLowerCase() === 'servings') unit = 'serving';
    else unit = unit.charAt(0).toUpperCase() + unit.slice(1).toLowerCase();
    
    return { value, unit };
  }
  
  // Pattern 2: "1 Tbsp" or "8 fl oz" (with space, whole number or decimal)
  match = cleaned.match(/^(\d+(?:\.\d+)?)\s+(Tbsp|tbsp|TSP|tsp|cup|cups|oz|fl\s*oz|floz|ml|g|kg|lb|lbs|serving|servings)\b/i);
  
  // Pattern 3: "8fl oz" or "240ml" (without space, but with unit)
  if (!match) {
    match = cleaned.match(/^(\d+(?:\.\d+)?)(Tbsp|tbsp|TSP|tsp|cup|cups|oz|fl\s*oz|floz|ml|g|kg|lb|lbs|serving|servings)\b/i);
  }
  
  // Pattern 4: Just a number (default to "serving")
  if (!match) {
    const numMatch = cleaned.match(/^(\d+(?:\.\d+)?)/);
    if (numMatch) {
      return { value: parseFloat(numMatch[1]), unit: 'serving' };
    }
  }
  
  if (match) {
    const value = parseFloat(match[1]);
    let unit = match[2].trim();
    
    // Normalize unit to standard form
    if (unit.toLowerCase() === 'tbsp') unit = 'Tbsp';
    else if (unit.toLowerCase() === 'tsp') unit = 'tsp';
    else if (unit.toLowerCase() === 'cups' || unit.toLowerCase() === 'cup') unit = 'cup';
    else if (unit.toLowerCase() === 'lbs') unit = 'lb';
    else if (unit.toLowerCase() === 'fl oz' || unit.toLowerCase() === 'floz') unit = 'fl oz';
    else if (unit.toLowerCase() === 'servings') unit = 'serving';
    else unit = unit.charAt(0).toUpperCase() + unit.slice(1).toLowerCase();
    
    return { value, unit };
  }

  console.log(`Warning: Could not parse serving size: "${servingSizeText}"`);
  return { value: null, unit: null };
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
        if (!obj || typeof obj !== 'object') return { createdAt: null, updatedAt: null };
        
        // Check if this object has both createdAt and updatedAt
        if (obj.createdAt && obj.updatedAt) {
          return {
            createdAt: typeof obj.createdAt === 'string' ? obj.createdAt : null,
            updatedAt: typeof obj.updatedAt === 'string' ? obj.updatedAt : null,
          };
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
        return { createdAt: null, updatedAt: null };
      };
      
      return findDates(nextData);
    }
  } catch (e) {
    // Ignore parsing errors
  }
  
  return { createdAt: null, updatedAt: null };
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
  const nutrition = transformNutritionToDbFormat(product.nutrition);
  
  return {
    product_name: product.name || '',
    brand: product.brand,
    upc: product.upc12 || undefined,
    ingredients_text: product.ingredients || '',
    serving_size_value: servingSize.value ?? undefined,
    serving_size_unit: servingSize.unit ?? undefined,
    serving_size_text: product.nutrition?.servingSize ?? undefined,
    source: product.source,
    source_id: product.productUrl,
    source_created_at: sourceCreatedAt,
    source_last_updated_at: sourceLastUpdatedAt,
    image_url: product.imageUrl || undefined,
    nutrition: nutrition || undefined,
  };
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
  // First, try to extract from JSON-LD structured data
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
  
  // Second, try to extract from __NEXT_DATA__
  try {
    const nextDataScript = $('script#__NEXT_DATA__').html();
    if (nextDataScript) {
      const nextData = JSON.parse(nextDataScript);
      
      // Recursively search for ingredients field
      const findIngredients = (obj: any): string | null => {
        if (!obj || typeof obj !== 'object') return null;
        
        // Check if this object has ingredients
        if (obj.ingredients && typeof obj.ingredients === 'string') {
          const ingredients = normalizeWhitespace(obj.ingredients);
          if (ingredients.length > 10 && ingredients.length < 2000) return ingredients;
        }
        
        // Check for fields.ingredients (common in Next.js data structures)
        if (obj.fields?.ingredients && typeof obj.fields.ingredients === 'string') {
          const ingredients = normalizeWhitespace(obj.fields.ingredients);
          if (ingredients.length > 10 && ingredients.length < 2000) return ingredients;
        }
        
        // Recursively search nested objects/arrays
        for (const key of Object.keys(obj)) {
          if (obj[key] && typeof obj[key] === 'object') {
            const found = findIngredients(obj[key]);
            if (found) return found;
          }
        }
        return null;
      };
      
      const ingredients = findIngredients(nextData);
      if (ingredients) return ingredients;
    }
  } catch {
    // Continue to DOM parsing
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

function parseImageUrl($: cheerio.CheerioAPI, productUrl: string): string | null {
  // Try multiple strategies to find the product image
  
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

function parseNutrition($: cheerio.CheerioAPI): Nutrition | null {
  // First, try to extract from JSON-LD structured data
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
            'F_NUTRSRV1_TRNFAT': 'Trans Fat',
            'F_NUTRSRV1_FATRN': 'Trans Fat',
            'F_NUTRSRV1_CHOL': 'Cholesterol',
            'F_NUTRSRV1_SOD': 'Sodium',
            'F_NUTRSRV1_TOTCARB': 'Total Carbohydrate',
            'F_NUTRSRV1_CARB': 'Total Carbohydrate',
            'F_NUTRSRV1_FIBTSW': 'Dietary Fiber',
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
            if (externalId.includes('CALORIES') || externalId.includes('CAL')) {
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
    // Continue to __NEXT_DATA__
  }
  
  // Second, try to extract from __NEXT_DATA__
  // Kraft Heinz uses Next.js and embeds product data (including nutrition) in the page
  try {
    const nextDataScript = $('script#__NEXT_DATA__').html();
    if (nextDataScript) {
      const nextData = JSON.parse(nextDataScript);
      
      // Search recursively for objects containing nutrition data (with items array and servingSize)
      const findNutritionData = (obj: any, path: string = 'root'): { items: any[]; servingSize?: string; servingsPerContainer?: string } | null => {
        if (!obj || typeof obj !== 'object') return null;
        
        // Check if this is an object with an items array containing nutrition fields
        if (!Array.isArray(obj) && obj.items && Array.isArray(obj.items)) {
          // Check if first item has the nutrition fields structure
          if (obj.items.length > 0 && obj.items[0]?.fields?.externalId) {
            const externalId = obj.items[0].fields.externalId;
            if (externalId.includes('NUTRSRV1')) {
              return {
                items: obj.items,
                servingSize: obj.servingSize || undefined,
                servingsPerContainer: obj.servingsPerContainer || undefined,
              };
            }
          }
        }
        
        // Check if this is an object with fields.items array (alternative structure)
        if (!Array.isArray(obj) && obj.fields?.items && Array.isArray(obj.fields.items)) {
          // Check if first item has the nutrition fields structure
          if (obj.fields.items.length > 0 && obj.fields.items[0]?.fields?.externalId) {
            const externalId = obj.fields.items[0].fields.externalId;
            if (externalId.includes('NUTRSRV1')) {
              return {
                items: obj.fields.items,
                servingSize: obj.servingSize || obj.fields.servingSize || undefined,
                servingsPerContainer: obj.servingsPerContainer || obj.fields.servingsPerContainer || undefined,
              };
            }
          }
        }
        
        // Check if this is an array of nutrition objects with fields (fallback for different structure)
        if (Array.isArray(obj)) {
          if (obj.length > 0 && obj[0]?.fields?.externalId) {
            const externalId = obj[0].fields.externalId;
            if (externalId.includes('NUTRSRV1')) {
              return { items: obj };
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
          'F_NUTRSRV1_CHOL': 'Cholesterol',
          'F_NUTRSRV1_SOD': 'Sodium',
          'F_NUTRSRV1_NA': 'Sodium',
          'F_NUTRSRV1_TOTCARB': 'Total Carbohydrate',
          'F_NUTRSRV1_CARB': 'Total Carbohydrate',
          'F_NUTRSRV1_CHO': 'Total Carbohydrate',
          'F_NUTRSRV1_FIBTSW': 'Dietary Fiber',
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
          if (externalId.includes('CALORIES') || externalId.includes('CAL')) {
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
    }
  } catch (e) {
    console.log('Error in __NEXT_DATA__ nutrition extraction:', e);
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

  return {
    servingSize: servingSize ? normalizeWhitespace(servingSize) : null,
    servingsPerContainer: servingsPerContainer ? normalizeWhitespace(servingsPerContainer) : null,
    calories,
    nutrients,
    rawText,
  };
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

async function scrapeProductDetail(browser: Browser, brandCfg: BrandConfig, productUrl: string): Promise<ScrapedProduct> {
  const page = await browser.newPage();
  try {
    // Use domcontentloaded for better reliability
    await page.goto(productUrl, { 
      waitUntil: "domcontentloaded", 
      timeout: 60_000 
    });
    // Wait for page to be ready (reduced timeout since we're using domcontentloaded)
    await page.waitForTimeout(1000);

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
    await page.waitForTimeout(300);

    const html = await page.content();
    const $ = cheerio.load(html);

    const name = parseName($);
    const ingredients = parseIngredients($);
    const imageUrl = parseImageUrl($, productUrl);

    // UPC: Try JSON-LD first (most reliable), then URL, then fallback to page text
    const upc12FromJsonLd = extractUpc12FromJsonLd($);
    const upc12FromUrl = upc12FromJsonLd ? null : extractUpc12FromUrl(productUrl);
    const upc12FromPage = (upc12FromJsonLd || upc12FromUrl) ? null : extractUpc12FromText(normalizeWhitespace($("body").text()));
    const upc12 = upc12FromJsonLd ?? upc12FromUrl ?? upc12FromPage;

    const nutrition = parseNutrition($);

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
      upc12,
      nutrition,
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

/** Parse CLI args: config path (required), --limit N, --local */
function parseKraftHeinzArgs(): { configPath: string; limit?: number; local: boolean } {
  const argv = process.argv.slice(2);
  let configPath: string | undefined;
  let limit: number | undefined;
  let local = false;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--limit" || argv[i] === "-l") {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n > 0) {
        limit = n;
        i++;
      }
    } else if (argv[i] === "--local") {
      local = true;
    } else if (!argv[i].startsWith("-")) {
      configPath = argv[i];
    }
  }
  return { configPath: configPath!, limit, local };
}

async function main(): Promise<void> {
  const { configPath, limit: productLimit, local } = parseKraftHeinzArgs();
  if (!configPath) {
    console.error("Usage: npx tsx scrape.ts ./brands.config.json [--limit N] [--local]");
    console.error("  --limit N   Scrape at most N products (default: no limit)");
    console.error("  --local     Skip AWS (DynamoDB, S3) and API submission; run scraping only");
    process.exit(1);
  }

  const cfg = JSON.parse(fs.readFileSync(configPath, "utf8")) as AppConfig;

  if (!Array.isArray(cfg.brands) || cfg.brands.length === 0) {
    throw new Error("Invalid config: must include non-empty brands[]");
  }

  if (local) {
    console.log("Running in local mode: skipping DynamoDB job status and S3 upload; API submission still runs (use AWS profile for SSM).");
  }
  if (productLimit != null) {
    console.log(`Limit: scraping at most ${productLimit} products.`);
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
    headless: true,
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
    // 1) Discover product detail URLs for each brand
    const allTargets: Array<{ brandCfg: BrandConfig; url: string }> = [];

    for (const brandCfg of cfg.brands) {
      if (productLimit != null && allTargets.length >= productLimit) break;
      console.log(`\n[DISCOVER] ${brandCfg.brand}: ${brandCfg.listingUrl}`);
      const urls = await scrapeListing(browser, brandCfg);
      console.log(`[DISCOVER] ${brandCfg.brand}: ${urls.length} product URLs`);
      for (const url of urls) {
        allTargets.push({ brandCfg, url });
        if (productLimit != null && allTargets.length >= productLimit) break;
      }
    }

    // Apply --limit: only scrape first N product URLs
    const targets = productLimit != null ? allTargets.slice(0, productLimit) : allTargets;
    if (productLimit != null && allTargets.length > productLimit) {
      console.log(`[LIMIT] Scraping ${targets.length} of ${allTargets.length} discovered URLs`);
    }

    // 2) Scrape details with bounded concurrency
    const concurrencyLimit = pLimit(Math.max(1, cfg.concurrency ?? 4));

    const results = await Promise.all(
      targets.map(({ brandCfg, url }) =>
        concurrencyLimit(async () => {
          try {
            const product = await scrapeProductDetail(browser, brandCfg, url);
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
      (r) => !(r as any).error && r.name && r.ingredients
    ) as ScrapedProduct[];

    console.log(`\nScraped ${validProducts.length} valid products out of ${results.length} total`);

    // Transform valid products to API format for S3 upload (for reference/backup)
    const transformedProducts = validProducts.map((product) => {
      const apiRequest = transformProductToApiRequest(product);
      // Add job_id for S3 backup (scraper job ID, not product review job ID)
      return { ...apiRequest, scraper_job_id: jobId };
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
