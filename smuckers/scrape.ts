import axios from "axios";
import * as cheerio from "cheerio";
import pLimit from "p-limit";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import { parseNutrientAmountWithQualifier } from "../nutrition-utils";

const PRODUCTS_PAGE = "https://www.smuckers.com/products";

// AWS clients
const s3Client = new S3Client({});
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);
const ssmClient = new SSMClient({});

// Environment variables
const SCRAPER_NAME = process.env.JOB_NAME || "smuckers";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://it7rdy3qbh.execute-api.us-west-2.amazonaws.com";
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";

// Cache for service token
let serviceTokenCache: string | null = null;

type ScrapedProduct = {
  url: string;
  name: string | null;
  image: string | null;
  ingredientsText: string | null;
  nutrition: {
    servingSize: string | null;
    calories: number | null;
    nutrients: { [key: string]: { label: string; amount: string; dailyValue: string | null } };
  };
  upc12: string | null;
};

type NutrientMapping = {
  key: string;
  label: string;
  dbField: string;
};

function cleanText(s: string | null | undefined): string {
  return (s ?? "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
}

function absUrl(base: string, href: string): string | null {
  try {
    return new URL(href, base).toString();
  } catch {
    return null;
  }
}

async function fetchHtml(url: string): Promise<string> {
  const res = await axios.get(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
      Accept: "text/html,application/xhtml+xml",
    },
    timeout: 30000,
  });
  return res.data;
}

/**
 * ✅ FIXED: Only keep *real* Smuckers product detail URLs.
 * This prevents junk like OneTrust from being scraped.
 */
function extractSmuckersProductLinks(listingUrl: string, html: string): string[] {
  const $ = cheerio.load(html);
  const out = new Set<string>();

  $("a[href]").each((_, a) => {
    const href = $(a).attr("href");
    if (!href) return;

    const full = absUrl(listingUrl, href);
    if (!full) return;

    let u: URL;
    try {
      u = new URL(full);
    } catch {
      return;
    }

    // ✅ Only Smuckers
    if (u.hostname !== "www.smuckers.com") return;

    const path = u.pathname.replace(/\/+$/, "");
    if (!path || path === "/") return;

    // ✅ Exclude obvious non-product areas
    const bannedPrefixes = [
      "/products",
      "/recipes",
      "/articles",
      "/about",
      "/faqs",
      "/search",
      "/where-to-buy",
      "/privacy",
      "/terms",
    ];
    if (bannedPrefixes.some((p) => path === p || path.startsWith(p + "/"))) return;

    // ✅ Exclude "View All ..." category links (these are usually category landing pages)
    const linkText = cleanText($(a).text()).toLowerCase();
    if (linkText.startsWith("view all")) return;

    // ✅ Heuristic: product detail pages are "deeper" than category pages.
    // Examples on /products include /fruit-spreads/jam/strawberry-jam (3 segments),
    // and /ice-cream-toppings/magic-shell/magic-shell-chocolate-topping (3 segments).
    const segments = path.split("/").filter(Boolean);
    if (segments.length < 3) return;

    out.add(u.toString());
  });

  return Array.from(out);
}

function extractJsonLd($: cheerio.CheerioAPI): any[] {
  const out: any[] = [];
  $('script[type="application/ld+json"]').each((_, el) => {
    const raw = $(el).text();
    if (!raw) return;
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) out.push(...parsed);
      else out.push(parsed);
    } catch {
      // ignore malformed json-ld
    }
  });
  return out;
}

function extractOgImage($: cheerio.CheerioAPI, pageUrl: string): string | null {
  const og = $('meta[property="og:image"]').attr("content");
  const tw = $('meta[name="twitter:image"]').attr("content");
  const raw = og || tw || null;
  return raw ? absUrl(pageUrl, raw) : null;
}

function extractNutritionFromText(fullText: string): {
  servingSize: string | null;
  calories: number | null;
  nutrients: { [key: string]: { label: string; amount: string; dailyValue: string | null } };
} {
  // Normalize whitespace
  const t = cleanText(fullText);

  // Isolate nutrition segment (stop before Ingredients if present)
  const stopIdx = t.search(/\bIngredients\b/i);
  const nutritionOnly = stopIdx >= 0 ? t.slice(0, stopIdx) : t;

  // Serving size: capture from "Serving Size" up to "Amount Per Serving" or "Calories"
  let servingSize: string | null = null;
  {
    const m = nutritionOnly.match(/Serving Size\s*(.+?)(?:Amount Per Serving|Calories)\b/i);
    if (m) servingSize = cleanText(m[1]);
  }

  // Calories: capture number after "Calories"
  let calories: number | null = null;
  {
    const m = nutritionOnly.match(/\bCalories\s*(\d+)\b/i);
    if (m) calories = Number(m[1]);
  }

  // Nutrients: parse common FDA-style labels.
  // We'll look for patterns like "Total Fat 0g 0%" or "Protein 0g" etc.
  const nutrients: { [key: string]: { label: string; amount: string; dailyValue: string | null } } = {};

  const NUTRIENTS = [
    { key: "total_fat", label: "Total Fat" },
    { key: "saturated_fat", label: "Saturated Fat" },
    { key: "trans_fat", label: "Trans Fat" },
    { key: "cholesterol", label: "Cholesterol" },
    { key: "sodium", label: "Sodium" },
    { key: "total_carbohydrate", label: "Total Carbohydrate" },
    { key: "dietary_fiber", label: "Dietary Fiber" },
    { key: "total_sugars", label: "Total Sugars" },
    { key: "added_sugars", label: "Total Added Sugars" },
    { key: "protein", label: "Protein" },
    { key: "vitamin_d", label: "Vitamin D" },
    { key: "calcium", label: "Calcium" },
    { key: "iron", label: "Iron" },
    { key: "potassium", label: "Potassium" },
  ];

  // Helper regex for amount like "0g", "10g", "0mg", "0µg"
  const amtRe = /(\d+(?:\.\d+)?)(g|mg|µg)\b/i;
  const dvRe = /(\d+)%\b/;

  for (const n of NUTRIENTS) {
    // Find the substring starting at label
    const idx = nutritionOnly.toLowerCase().indexOf(n.label.toLowerCase());
    if (idx < 0) continue;

    // Take a limited window after the label so we don't accidentally capture the whole page
    const window = nutritionOnly.slice(idx, idx + 120);

    // Examples:
    // "Total Fat 0g 0%"
    // "Trans Fat 0g"
    // "Total Sugars 8g Total Added Sugars 4g 7%"
    // We'll pull first amount after label, and first %DV after that if present.
    const afterLabel = window.slice(n.label.length);
    const amtMatch = afterLabel.match(amtRe);
    if (!amtMatch) continue;

    const amount = `${amtMatch[1]}${amtMatch[2]}`;

    // %DV might be absent (e.g., Trans Fat, Total Sugars on many labels)
    const afterAmt = afterLabel.slice(afterLabel.indexOf(amtMatch[0]) + amtMatch[0].length);
    const dvMatch = afterAmt.match(dvRe);
    const dailyValue = dvMatch ? `${dvMatch[1]}%` : null;

    nutrients[n.key] = { label: n.label, amount, dailyValue };
  }

  return { servingSize, calories, nutrients };
}

function extractNutritionAndIngredients(url: string, html: string): ScrapedProduct {
  const $ = cheerio.load(html);

  const name = cleanText($("h1").first().text()) || null;
  const image = extractOgImage($, url);

  // Ingredients: find exact "Ingredients" label and take next nearby text block.
  let ingredientsText: string | null = null;
  const ingredientsLabel = $("*")
    .filter((_, el) => cleanText($(el).text()) === "Ingredients")
    .first();

  if (ingredientsLabel.length) {
    let cursor = ingredientsLabel;
    for (let i = 0; i < 12; i++) {
      cursor = cursor.next();
      if (!cursor || !cursor.length) break;
      const t = cleanText(cursor.text());
      if (t && t !== "Ingredients") {
        ingredientsText = t;
        break;
      }
    }
  }

  // Nutrition: get a text blob from the nutrition section or fallback to body text
  let nutritionText = "";
  const nutritionHeader = $("*")
    .filter((_, el) => cleanText($(el).text()) === "Nutrition Information")
    .first();

  if (nutritionHeader.length) {
    let cursor = nutritionHeader;
    for (let i = 0; i < 60; i++) {
      cursor = cursor.next();
      if (!cursor || !cursor.length) break;
      const chunk = cleanText(cursor.text());
      if (chunk) nutritionText += chunk + " ";
      if (/^Ingredients$/i.test(chunk)) break;
    }
  } else {
    nutritionText = cleanText($("body").text());
  }

  const parsedNutrition = extractNutritionFromText(nutritionText);

  const nutrition = {
    servingSize: parsedNutrition.servingSize,
    calories: parsedNutrition.calories,
    nutrients: parsedNutrition.nutrients,
  };

  const servingMatch =
    nutritionText.match(/Serving Size\s*:? *([^\n]+)/i) ||
    nutritionText.match(/Serving size\s*:? *([^\n]+)/i);
  if (servingMatch) nutrition.servingSize = cleanText(servingMatch[1]);

  const calMatch =
    nutritionText.match(/Calories\s*:? *(\d+)/i) ||
    nutritionText.match(/Calories\s*\n(\d+)/i);
  if (calMatch) nutrition.calories = Number(calMatch[1]);

  // Conservative nutrient parsing: label / amount / %DV triplets
  const lines = nutritionText
    .split("\n")
    .map((l) => cleanText(l))
    .filter(Boolean);

  for (let i = 0; i < lines.length - 2; i++) {
    const label = lines[i];
    const amount = lines[i + 1];
    const dv = lines[i + 2];

    const looksLikeAmount = /(\d+(\.\d+)?)(g|mg|µg)\b/i.test(amount);
    const looksLikeDV = /^\d+%$/.test(dv);

    if (looksLikeAmount && looksLikeDV) {
      const banned = new Set([
        "Nutrition Facts",
        "Amount Per Serving",
        "% Daily Value*",
        "Serving Size",
        "Calories",
      ]);
      if (!banned.has(label)) {
        nutrition.nutrients[label] = { label, amount, dailyValue: dv };
      }
    }
  }

  // UPC12 best-effort: JSON-LD gtin12, then HTML regex.
  let upc12: string | null = null;
  const jsonLd = extractJsonLd($);

  const scanObj = (obj: any): string | null => {
    if (!obj || typeof obj !== "object") return null;
    const cands = [obj.gtin12, obj.gtin, obj.sku, obj.productID, obj.mpn];
    for (const c of cands) {
      if (c && /^\d{12}$/.test(String(c))) return String(c);
    }
    return null;
  };

  for (const obj of jsonLd) {
    if (Array.isArray(obj?.["@graph"])) {
      for (const g of obj["@graph"]) {
        const hit = scanObj(g);
        if (hit) {
          upc12 = hit;
          break;
        }
      }
      if (upc12) break;
    }
    const hit = scanObj(obj);
    if (hit) {
      upc12 = hit;
      break;
    }
  }

  if (!upc12) {
    const m = html.match(/\b(\d{12})\b/);
    if (m) upc12 = m[1];
  }

  return { url, name, image, ingredientsText, nutrition, upc12 };
}

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
 * Parse serving size text to extract value and unit
 * Example: "1 Tbsp (20g)" -> { value: 1, unit: "Tbsp" }
 */
function parseServingSize(servingSizeText: string | null): { value: number | null; unit: string | null } {
  if (!servingSizeText || typeof servingSizeText !== "string") {
    return { value: null, unit: null };
  }

  // Match pattern like "1 Tbsp (20g)" or "2 Tbsp" or "1 cup"
  // Look for number followed by unit (Tbsp, tsp, cup, etc.)
  const match = servingSizeText.match(/^(\d+(?:\.\d+)?)\s+(Tbsp|tbsp|TSP|tsp|cup|cups|oz|fl\s*oz|ml|g|kg|lb|lbs)\b/i);

  if (match) {
    const value = parseFloat(match[1]);
    let unit = match[2];

    // Normalize unit to standard form
    if (unit.toLowerCase() === "tbsp") unit = "Tbsp";
    else if (unit.toLowerCase() === "tsp") unit = "tsp";
    else if (unit.toLowerCase() === "cups") unit = "cup";
    else if (unit.toLowerCase() === "lbs") unit = "lb";
    else if (unit.toLowerCase() === "fl oz" || unit.toLowerCase() === "floz") unit = "fl oz";
    else unit = unit.charAt(0).toUpperCase() + unit.slice(1).toLowerCase();

    return { value, unit };
  }

  return { value: null, unit: null };
}


/**
 * Map nutrient key to database column name
 */
function mapNutrientKeyToColumn(key: string): string | null {
  const mapping: { [key: string]: string } = {
    total_fat: "total_fat_g",
    saturated_fat: "saturated_fat_g",
    trans_fat: "trans_fat_g",
    cholesterol: "cholesterol_mg",
    sodium: "sodium_mg",
    total_carbohydrate: "total_carbs_g",
    dietary_fiber: "fiber_g",
    total_sugars: "sugars_g",
    added_sugars: "added_sugars_g",
    protein: "protein_g",
    calcium: "calcium_mg",
    iron: "iron_mg",
    potassium: "potassium_mg",
    vitamin_d: "vitamin_d_mcg",
  };

  return mapping[key] || null;
}

/**
 * Transform scraped nutrition data to standard scraper format
 */
function transformNutritionToDbFormat(nutrition: ScrapedProduct["nutrition"]): ScraperNutritionData | null {
  if (!nutrition) return null;

  // Parse serving size
  let servingSize = parseServingSize(nutrition.servingSize);

  // If serving size parsing failed but we have nutrition data, use default
  if (!servingSize.value || !servingSize.unit) {
    const hasCalories = nutrition.calories !== null;
    const hasNutrients = Object.keys(nutrition.nutrients).length > 0;

    if (hasCalories || hasNutrients) {
      console.log(`Warning: Could not parse serving size "${nutrition.servingSize}", using default 1 serving`);
      servingSize = { value: 1, unit: "serving" };
    } else {
      // No nutrition data to save
      return null;
    }
  }

  // Build nutrition object with serving size
  const result: any = {
    serving_size_value: servingSize.value,
    serving_size_unit_text: servingSize.unit,
    serving_size_text: nutrition.servingSize || null,
  };

  if (nutrition.calories !== null && !isNaN(nutrition.calories)) {
    result.calories = nutrition.calories;
  }

  // Map nutrients to database columns (with qualifier support for <1g, >5mg, ~10g, etc.)
  for (const [key, nutrient] of Object.entries(nutrition.nutrients)) {
    const columnName = mapNutrientKeyToColumn(key);
    if (columnName) {
      const parsed = parseNutrientAmountWithQualifier(nutrient.amount);
      if (parsed !== null) {
        result[columnName] = parsed.value;
        if (parsed.qualifier) result[`${columnName}_qualifier`] = parsed.qualifier;
      }
    }
  }

  return result;
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
      throw new Error("InternalServiceToken not found in parameter");
    }

    serviceTokenCache = token;
    return token;
  } catch (error) {
    console.error("Error getting service token:", error);
    throw error;
  }
}

/**
 * Transform scraped product to standard scraper output format
 */
function transformProductToApiRequest(product: ScrapedProduct): ScraperProductOutput {
  const now = new Date().toISOString();

  // Parse serving size from nutrition data
  const servingSize = product.nutrition?.servingSize ? parseServingSize(product.nutrition.servingSize) : { value: null, unit: null };

  // Transform nutrition data to standard format
  const nutrition = transformNutritionToDbFormat(product.nutrition);

  return {
    product_name: product.name || "",
    brand: "Smucker's",
    upc: product.upc12 || undefined,
    ingredients_text: product.ingredientsText || "",
    serving_size_value: servingSize.value ?? undefined,
    serving_size_unit: servingSize.unit ?? undefined,
    serving_size_text: product.nutrition?.servingSize ?? undefined,
    source: SCRAPER_NAME,
    source_id: product.url,
    source_created_at: now,
    source_last_updated_at: now,
    image_url: product.image || undefined,
    nutrition: nutrition || undefined,
  };
}

/**
 * Submit product for review via API
 */
async function submitProductForReview(product: ScrapedProduct): Promise<boolean> {
  if (!product.name || !product.ingredientsText) {
    console.log(`Skipping product ${product.name || product.url}: missing name or ingredients`);
    return false;
  }

  try {
    const serviceToken = await getServiceToken();
    const productOutput = transformProductToApiRequest(product);
    // Remove scraper_job_id for API submission (it's only for S3)
    const { scraper_job_id, ...requestBody } = productOutput;

    const response = await axios.post(`${API_BASE_URL}/submit-product-for-review`, requestBody, {
      headers: {
        "Content-Type": "application/json",
        "X-Service-Token": serviceToken,
      },
    });

    if (response.status === 200) {
      console.log(`✅ Submitted product "${product.name}"` + (response.data?.data?.job_id ? ` for review (job_id: ${response.data.data.job_id})` : ""));
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

/** Parse CLI args: --concurrency N, --limit N, --local */
function parseSmuckersArgs(): { concurrency: number; limit?: number; local: boolean } {
  const argv = process.argv.slice(2);
  let concurrency = 5;
  let limit: number | undefined;
  let local = false;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--concurrency" && argv[i + 1] != null) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n > 0) concurrency = n;
      i++;
    } else if (argv[i] === "--limit" || argv[i] === "-l") {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n > 0) {
        limit = n;
        i++;
      }
    } else if (argv[i] === "--local") {
      local = true;
    }
  }
  return { concurrency, limit, local };
}

async function main(): Promise<void> {
  const { concurrency, limit: productLimit, local } = parseSmuckersArgs();
  const jobId = uuidv4();
  const runDateTime = new Date().toISOString().replace(/[:.]/g, "-").slice(0, -5);

  if (local) {
    console.log("Running in local mode: skipping DynamoDB job status and S3 upload; API submission still runs (use AWS profile for SSM).");
  }
  if (productLimit != null) {
    console.log(`Limit: scraping at most ${productLimit} products.`);
  }

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

  try {
    console.log(`Fetching listing: ${PRODUCTS_PAGE}`);
    const listingHtml = await fetchHtml(PRODUCTS_PAGE);

    const productLinks = extractSmuckersProductLinks(PRODUCTS_PAGE, listingHtml);

    console.log(`Found ${productLinks.length} Smuckers product links`);
    console.log("Sample:", productLinks.slice(0, 10));

    // Apply --limit: only scrape first N product URLs
    const linksToScrape = productLimit != null ? productLinks.slice(0, productLimit) : productLinks;
    if (productLimit != null && productLinks.length > productLimit) {
      console.log(`[LIMIT] Scraping ${linksToScrape.length} of ${productLinks.length} discovered URLs`);
    }

    const concurrencyLimit = pLimit(concurrency);

    const results = await Promise.all(
      linksToScrape.map((u) =>
        concurrencyLimit(async () => {
          try {
            const html = await fetchHtml(u);
            const parsed = extractNutritionAndIngredients(u, html);
            console.log(`✅ ${parsed.name ?? "(no name)"} | ${u}`);
            return parsed;
          } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            console.error(`❌ Failed ${u}:`, msg);
            return { url: u, error: msg } as any;
          }
        })
      )
    );

    // Filter out errors and products without required fields
    const validProducts = results.filter((r) => !(r as any).error && r.name && r.ingredientsText) as ScrapedProduct[];

    console.log(`Scraped ${validProducts.length} valid products out of ${results.length} total`);

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
    let apiSuccessCount = 0;
    let apiFailureCount = 0;
    for (const product of validProducts) {
      const success = await submitProductForReview(product);
      if (success) {
        apiSuccessCount++;
      } else {
        apiFailureCount++;
      }
    }

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
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
