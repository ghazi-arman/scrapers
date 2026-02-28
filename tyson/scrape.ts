import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import axios from "axios";
import * as cheerio from "cheerio";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import * as nutritionUtils from "../nutrition-utils";
import { chromium } from "playwright";
import * as servingSizeUtils from "../serving-size-utils";
import * as productIdUtils from "../product-id-utils";
import * as nameUtils from "../name-utils";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const parseNutrientAmountWithQualifier =
  (nutritionUtils as any).parseNutrientAmountWithQualifier ??
  (nutritionUtils as any).default?.parseNutrientAmountWithQualifier;
const parseServingSizeFromText =
  (servingSizeUtils as any).parseServingSizeFromText ??
  (servingSizeUtils as any).default?.parseServingSizeFromText;
const generateDeterministicProductId =
  (productIdUtils as any).generateDeterministicProductId ??
  (productIdUtils as any).default?.generateDeterministicProductId;
const cleanProductName =
  (nameUtils as any).cleanProductName ?? (nameUtils as any).default?.cleanProductName;

const SOURCE = "tyson.com";
const SCRAPER_NAME = process.env.JOB_NAME || "tyson";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://api.mytummi.app";
const PRODUCTS_API_URL = `${API_BASE_URL}/products`;
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
let DEBUG_TYSON = false;

const s3Client = new S3Client({});
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);
const ssmClient = new SSMClient({});

let serviceTokenCache: string | null = null;

type AppConfig = {
  urls?: string[];
  searchUrls?: string[];
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
};

type ScrapedProduct = {
  productUrl: string;
  name: string | null;
  brand: string | null;
  ingredients: string | null;
  allergenStatement: string | null;
  upc12: string | null;
  nutrition: Nutrition | null;
  imageUrl: string | null;
  scrapedAt: string;
  sourceCreatedAt: string | null;
  sourceLastUpdatedAt: string | null;
};

const NUTRIENT_COLUMN_MAP: Record<string, string> = {
  "total fat": "total_fat_g",
  "saturated fat": "saturated_fat_g",
  trans: "trans_fat_g",
  "trans fat": "trans_fat_g",
  cholesterol: "cholesterol_mg",
  sodium: "sodium_mg",
  "total carbohydrate": "total_carbs_g",
  "dietary fiber": "fiber_g",
  fiber: "fiber_g",
  "total sugars": "sugars_g",
  sugars: "sugars_g",
  "added sugars": "added_sugars_g",
  protein: "protein_g",
  calcium: "calcium_mg",
  iron: "iron_mg",
  potassium: "potassium_mg",
  "vitamin d": "vitamin_d_mcg",
  "vitamin a": "vitamin_a_mcg",
  "vitamin c": "vitamin_c_mg",
  "vitamin e": "vitamin_e_mg",
  "vitamin k": "vitamin_k_mcg",
  thiamin: "thiamin_mg",
  riboflavin: "riboflavin_mg",
  niacin: "niacin_mg",
  "vitamin b6": "vitamin_b6_mg",
  folate: "folate_mcg",
  "folic acid": "folic_acid_mcg",
  "vitamin b12": "vitamin_b12_mcg",
  biotin: "biotin_mcg",
  "pantothenic acid": "pantothenic_acid_mg",
  magnesium: "magnesium_mg",
  phosphorus: "phosphorus_mg",
  zinc: "zinc_mg",
};

const NUTRIENT_DV_COLUMN_MAP: Record<string, string> = {
  "vitamin a": "vitamin_a_dv_pct",
  "vitamin c": "vitamin_c_dv_pct",
  "vitamin d": "vitamin_d_dv_pct",
  "vitamin e": "vitamin_e_dv_pct",
  "vitamin k": "vitamin_k_dv_pct",
  thiamin: "thiamin_dv_pct",
  riboflavin: "riboflavin_dv_pct",
  niacin: "niacin_dv_pct",
  "vitamin b6": "vitamin_b6_dv_pct",
  folate: "folate_dv_pct",
  "folic acid": "folic_acid_dv_pct",
  "vitamin b12": "vitamin_b12_dv_pct",
  biotin: "biotin_dv_pct",
  "pantothenic acid": "pantothenic_acid_dv_pct",
  calcium: "calcium_dv_pct",
  iron: "iron_dv_pct",
  magnesium: "magnesium_dv_pct",
  phosphorus: "phosphorus_dv_pct",
  potassium: "potassium_dv_pct",
  zinc: "zinc_dv_pct",
};

function mapNutrientToColumn(name: string): string | null {
  const lower = name.toLowerCase().trim();
  if (lower.includes("folic acid")) return "folic_acid_mcg";
  if (lower.includes("monounsaturated")) return "monounsaturated_fat_g";
  if (lower.includes("polyunsaturated")) return "polyunsaturated_fat_g";
  if (lower.includes("folate")) return "folate_mcg";
  for (const [key, col] of Object.entries(NUTRIENT_COLUMN_MAP)) {
    if (lower.includes(key)) return col;
  }
  return null;
}

function mapNutrientToDvColumn(name: string): string | null {
  const lower = name.toLowerCase().trim();
  for (const [key, col] of Object.entries(NUTRIENT_DV_COLUMN_MAP)) {
    if (lower.includes(key)) return col;
  }
  return null;
}
function normalizeWhitespace(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

function cleanText(s: string | null | undefined): string | null {
  if (!s) return null;
  return normalizeWhitespace(s.replace(/\u00A0/g, " "));
}

function parseServingSize(servingSizeText: string | null): { value: number | null; unit: string | null } {
  if (typeof parseServingSizeFromText !== "function") {
    throw new Error("parseServingSizeFromText import failed");
  }
  return parseServingSizeFromText(servingSizeText);
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
      // ignore
    }
  });
  return out;
}

function extractOgImage($: cheerio.CheerioAPI, pageUrl: string): string | null {
  const og = $('meta[property="og:image"]').attr("content");
  const tw = $('meta[name="twitter:image"]').attr("content");
  const raw = og || tw || null;
  if (!raw) return null;
  try {
    return new URL(raw, pageUrl).toString();
  } catch {
    return raw;
  }
}

function extractUpcFromText(text: string): string | null {
  const match = text.match(/\b(\d{12}|\d{13}|\d{14})\b/);
  if (!match) return null;
  const raw = match[1];
  if (raw.length === 12) return raw;
  if (raw.length === 13) return raw.startsWith("0") ? raw.slice(1) : raw;
  if (raw.length === 14) return raw.startsWith("00") ? raw.slice(2) : raw.slice(2);
  return raw;
}

function extractIngredientsFromHtml($: cheerio.CheerioAPI): string | null {
  const selectors = [
    "[data-testid='ingredients']",
    "[data-automation-id='ingredients']",
    "#ingredients",
    "[aria-label*='Ingredients']",
    "[id*='ingredients']",
  ];
  for (const sel of selectors) {
    const text = cleanText($(sel).text());
    if (text && /ingredient/i.test(text)) return text.replace(/^ingredients:?\s*/i, "");
  }
  const label = $("*:contains('Ingredients')").filter((_, el) => {
    const t = cleanText($(el).text()) || "";
    return /^ingredients\b/i.test(t);
  }).first();
  if (label.length) {
    const text = cleanText(label.text());
    if (text) return text.replace(/^ingredients:?\s*/i, "");
  }
  return null;
}

function extractAllergenFromHtml($: cheerio.CheerioAPI): string | null {
  const selectors = [
    "[data-testid='allergens']",
    "[data-automation-id='allergens']",
    "#allergens",
    "[aria-label*='Allergen']",
    "[id*='allergen']",
  ];
  for (const sel of selectors) {
    const text = cleanText($(sel).text());
    if (text && /(contains|may contain|allergen)/i.test(text)) return text;
  }
  const candidates = $("p, li, div, span")
    .map((_, el) => cleanText($(el).text()))
    .get()
    .filter((t): t is string => !!t && /(?:contains|may contain|allergen)/i.test(t) && t.length <= 240);
  const direct = candidates.find((t) => /^(contains|may contain|allergen)/i.test(t));
  return cleanText(direct || candidates[0] || null);
}

function extractNutritionFromTable($: cheerio.CheerioAPI): Nutrition | null {
  const table = $("table.pdp-retail__nutritional-facts-table").first().length
    ? $("table.pdp-retail__nutritional-facts-table").first()
    : $("table").filter((_, el) => /nutrition/i.test($(el).text())).first();
  if (!table.length) return null;

  let servingSize: string | null = null;
  let servingsPerContainer: string | null = null;
  let calories: string | null = null;
  const nutrients: NutritionNutrient[] = [];

  const tableText = cleanText(table.text());
  if (tableText) {
    const spc = tableText.match(/servings per container\s*:?\s*([^\n]+)/i);
    if (spc) servingsPerContainer = cleanText(spc[1]);
    const cal = tableText.match(/calories\s*:?\s*(\d+)/i);
    if (cal) calories = cal[1];
  }

  const extractAmountFromCell = (cell: cheerio.Cheerio<cheerio.Element>): string | null => {
    const bold = cleanText(cell.find("b").first().text());
    if (bold && /[a-z]/i.test(bold)) return bold;
    const text = cleanText(cell.text()) || "";
    const unitMatch = text.match(/([<>~]?\d+(?:\.\d+)?\s*(?:mg|mcg|g|kg|iu|niu))\b/i);
    if (unitMatch) return unitMatch[1];
    return null;
  };

  const extractAmountFromCells = (cells: cheerio.Cheerio<cheerio.Element>): string | null => {
    for (let i = 0; i < cells.length; i += 1) {
      const amt = extractAmountFromCell(cells.eq(i));
      if (amt) return amt;
    }
    return null;
  };

  const stripAmountFromLabel = (label: string, amount: string | null): string => {
    let out = label;
    if (amount) {
      out = out.replace(amount, "");
    }
    out = out.replace(/\s*\d+(?:\.\d+)?\s*%/g, "");
    return normalizeWhitespace(out);
  };

  table.find("tr").each((_, row) => {
    const cells = $(row).find("td, th");
    if (cells.length < 1) return;
    const left = cleanText($(cells[0]).text()) || "";
    const right = cells.length > 1 ? cleanText($(cells[1]).text()) : null;

    if (/^serving size/i.test(left)) {
      const ss = left.replace(/^serving size/i, "").trim();
      if (ss) servingSize = ss;
      return;
    }

    if (!left) return;
    const rawLeft = left;
    const amountFromLeft = extractAmountFromCell($(cells[0]));
    const amountFromAny = extractAmountFromCells(cells);
    const rightHasPercent = right ? right.includes("%") : false;
    const rightHasUnit = right ? /[a-z]/i.test(right) : false;

    const amount =
      amountFromLeft ||
      amountFromAny ||
      (rightHasUnit && !rightHasPercent ? right : null);

    const leftPercent = rawLeft.match(/(\d+(?:\.\d+)?\s*%)/)?.[1] || null;
    const dailyValue =
      (right && rightHasPercent ? right : null) ||
      leftPercent ||
      (cells.length > 2 ? cleanText($(cells[2]).text()) : null);

    const name = stripAmountFromLabel(rawLeft, amount);
    if (!name) return;

    if (/includes/i.test(name) && /added sugars/i.test(name)) {
      const cleaned = "Added Sugars";
      if (amount) nutrients.push({ name: cleaned, amount, dailyValuePercent: dailyValue });
      return;
    }

    const hasDvNumber = !!(dailyValue && /\d/.test(dailyValue));
    if (!amount && !hasDvNumber) return;
    nutrients.push({ name, amount: amount || null, dailyValuePercent: dailyValue });
  });

  if (!servingSize && !calories && nutrients.length === 0) return null;
  return {servingSize, servingsPerContainer, calories, nutrients};
}

function nutritionFromJsonLd(nutrition: any): Nutrition | null {
  if (!nutrition || typeof nutrition !== "object") return null;

  const nutrients: NutritionNutrient[] = [];
  const add = (name: string, amount: any) => {
    const amt = cleanText(typeof amount === "string" ? amount : String(amount ?? ""));
    if (!amt) return;
    nutrients.push({ name, amount: amt, dailyValuePercent: null });
  };

  const servingSize = cleanText(nutrition.servingSize || null);
  const calories = cleanText(nutrition.calories || nutrition.calorieContent || null);

  add("Total Fat", nutrition.fatContent);
  add("Saturated Fat", nutrition.saturatedFatContent);
  add("Trans Fat", nutrition.transFatContent);
  add("Cholesterol", nutrition.cholesterolContent);
  add("Sodium", nutrition.sodiumContent);
  add("Total Carbohydrate", nutrition.carbohydrateContent);
  add("Dietary Fiber", nutrition.fiberContent);
  add("Total Sugars", nutrition.sugarContent);
  add("Protein", nutrition.proteinContent);

  return {servingSize: servingSize || null,
    servingsPerContainer: null,
    calories: calories || null,
    nutrients};
}

function transformNutritionToDb(nutrition: Nutrition | null): ScraperNutritionData | null {
  if (!nutrition) return null;

  const serving = parseServingSize(nutrition.servingSize);
  const hasData = nutrition.calories || (nutrition.nutrients && nutrition.nutrients.length > 0);
  if (!hasData) return null;

  const value = serving.value ?? 1;
  const unit = serving.unit ?? "serving";

  const result: ScraperNutritionData = {
    serving_size_value: value,
    serving_size_unit_text: unit,
    serving_size_text: nutrition.servingSize || null,
  };

  const calories = nutrition.calories ? parseFloat(String(nutrition.calories).replace(/[^\d.]/g, "")) : null;
  if (calories !== null && !isNaN(calories)) result.calories = calories;

  for (const n of nutrition.nutrients || []) {
    const col = mapNutrientToColumn(n.name);
    if (typeof parseNutrientAmountWithQualifier !== "function") {
      throw new Error("parseNutrientAmountWithQualifier import failed");
    }
    if (col && n.amount) {
      const amountHasUnit = /(?:mg|mcg|g|kg|iu|niu)\b/i.test(n.amount);
      const percentOnly = /%/.test(n.amount) && !amountHasUnit;
      if (!percentOnly) {
        const parsed = parseNutrientAmountWithQualifier(n.amount);
        if (parsed !== null) {
          const existing = (result as unknown as Record<string, number>)[col];
          if (existing == null || (existing === 0 && parsed.value > 0)) {
            (result as unknown as Record<string, number>)[col] = parsed.value;
          }
          if (parsed.qualifier) (result as unknown as Record<string, string>)[`${col}_qualifier`] = parsed.qualifier;
        }
      }
    }

    const dvCol = mapNutrientToDvColumn(n.name);
    if (dvCol && n.dailyValuePercent) {
      const pct = parseFloat(String(n.dailyValuePercent).replace(/[^\d.]/g, ""));
      if (!isNaN(pct)) {
        (result as unknown as Record<string, number>)[dvCol] = pct;
      }
    }
  }

  return result;
}

function transformToOutput(p: ScrapedProduct): ScraperProductOutput {
  const now = new Date().toISOString();
  const serving = p.nutrition?.servingSize ? parseServingSize(p.nutrition.servingSize) : { value: null, unit: null };

  return {product_name: p.name || "",
    brand: p.brand || "Tyson",
    upc: p.upc12 || undefined,
    ingredients_text: p.ingredients || "",
    allergen_statement: p.allergenStatement || undefined,
    serving_size_value: serving.value ?? undefined,
    serving_size_unit: serving.unit ?? undefined,
    serving_size_text: p.nutrition?.servingSize ?? undefined,
    source: SOURCE,
    source_id: p.productUrl,
    source_created_at: p.sourceCreatedAt || now,
    source_last_updated_at: p.sourceLastUpdatedAt || now,
    image_url: p.imageUrl || undefined,
    nutrition: transformNutritionToDb(p.nutrition) || undefined};
}

async function fetchHtml(url: string): Promise<string> {
  const res = await axios.get(url, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
      Accept: "text/html,application/xhtml+xml",
      "Accept-Language": "en-US,en;q=0.9",
    },
    timeout: 30_000,
  });
  return res.data;
}

async function discoverTysonProductUrls(listingUrl: string, limit?: number): Promise<string[]> {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
  });
  try {
    await page.goto(listingUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForLoadState("networkidle", { timeout: 8000 }).catch(() => {});

    const seen = new Set<string>();
    while (true) {
      const hrefs = await page.$$eval("a[href*='/products/']", (els) =>
        els.map((e) => (e as HTMLAnchorElement).href)
      );
      for (const h of hrefs) {
        if (!h || !h.includes("/products/")) continue;
        try {
          const u = new URL(h);
          if (u.pathname === "/products" || u.pathname === "/products/") continue;
          seen.add(u.toString());
        } catch {
          continue;
        }
      }
      if (limit && seen.size >= limit) break;

      const button = await page.$("button.lazyProductsButton");
      if (!button) break;
      const visible = await button.isVisible().catch(() => false);
      if (!visible) break;
      await button.click().catch(() => null);
      await page.waitForTimeout(1500);
    }

    return Array.from(seen).slice(0, limit ?? seen.size);
  } finally {
    await page.close().catch(() => null);
    await browser.close().catch(() => null);
  }
}

async function scrapeProduct(url: string): Promise<ScrapedProduct | null> {
  const html = await fetchHtml(url);
  if (DEBUG_TYSON) {
    try {
      fs.writeFileSync("/tmp/tyson-rendered.html", html);
    } catch {
      // ignore debug write errors
    }
  }
  const $ = cheerio.load(html);

  const jsonLd = extractJsonLd($);
  const jsonLdProduct = jsonLd.find((o) => o && (o["@type"] === "Product" || o["@type"] === "ProductGroup")) || null;

  const rawName = cleanText(jsonLdProduct?.name || $("h1").first().text() || null);
  const name = cleanProductName(rawName, {
    decodeHtml: true,
    stripTrailingWeight: true,
    stripTrailingCount: true,
    stripTrailingDashSize: true,
    stripParenAtEnd: true,
  });
  const brand = cleanText(
    (typeof jsonLdProduct?.brand === "string" ? jsonLdProduct.brand : jsonLdProduct?.brand?.name) ||
      $("[data-testid='brand']").text() ||
      "Tyson"
  );
  const ingredients =
    cleanText(jsonLdProduct?.ingredients || jsonLdProduct?.ingredientStatement || null) ||
    extractIngredientsFromHtml($);
  const allergenStatement =
    cleanText(jsonLdProduct?.allergenStatement || jsonLdProduct?.allergens || null) || extractAllergenFromHtml($);
  const nutrition =
    extractNutritionFromTable($) ||
    nutritionFromJsonLd(jsonLdProduct?.nutrition || jsonLdProduct?.nutritionInformation || null);
  const imageUrl =
    cleanText($(".pdp-retail__image img").first().attr("src")) ||
    cleanText(jsonLdProduct?.image || jsonLdProduct?.imageUrl || null) ||
    extractOgImage($, url);
  const upc12 =
    extractUpcFromText(JSON.stringify(jsonLdProduct || {})) ||
    extractUpcFromText(html);

  if (DEBUG_TYSON) {
    console.log(`[DEBUG] name=${name ?? "(null)"}`);
    console.log(`[DEBUG] brand=${brand ?? "(null)"}`);
    console.log(`[DEBUG] ingredients=${ingredients ?? "(null)"}`);
    console.log(`[DEBUG] allergens=${allergenStatement ?? "(null)"}`);
    console.log(`[DEBUG] upc=${upc12 ?? "(null)"}`);
    console.log(`[DEBUG] image=${imageUrl ?? "(null)"}`);
    console.log(`[DEBUG] nutrition=${nutrition ? "yes" : "no"}`);
  }

  const now = new Date().toISOString();
  return {
    productUrl: url,
    name,
    brand: brand || "Tyson",
    ingredients,
    allergenStatement,
    upc12,
    nutrition,
    imageUrl: typeof imageUrl === "string" ? imageUrl : null,
    scrapedAt: now,
    sourceCreatedAt: now,
    sourceLastUpdatedAt: now,
  };
}

async function getServiceToken(): Promise<string> {
  if (serviceTokenCache) return serviceTokenCache;
  const cmd = new GetParameterCommand({ Name: API_KEYS_PARAMETER_NAME, WithDecryption: true });
  const res = await ssmClient.send(cmd);
  const param = JSON.parse(res.Parameter?.Value || "{}");
  serviceTokenCache = param.InternalServiceToken;
  if (!serviceTokenCache) throw new Error("InternalServiceToken not found");
  return serviceTokenCache;
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
  } catch (err: any) {
    if (err?.response?.status === 404) return false;
    if (DEBUG_TYSON) console.log("[DEBUG] product exists check failed:", err?.message || err);
    return false;
  }
}

async function submitProductForReview(productOutput: ScraperProductOutput): Promise<boolean> {
  if (!productOutput.product_name || !productOutput.ingredients_text) return false;
  try {
    const token = await getServiceToken();
    const { scraper_job_id: _, ...body } = productOutput as any;
    if (DEBUG_TYSON) {
      console.log(`[DEBUG] submit body: ${JSON.stringify(body, null, 2)}`);
    }
    const res = await axios.post(`${API_BASE_URL}/submit-product-for-review`, body, {
      headers: { "Content-Type": "application/json", "X-Service-Token": token },
    });
    if (res.status === 200) {
      console.log(`✅ Submitted "${productOutput.product_name}"` + (res.data?.data?.job_id ? ` (job_id: ${res.data.data.job_id})` : ""));
      return true;
    }
    console.error(`❌ Failed "${productOutput.product_name}": HTTP ${res.status}`);
    return false;
  } catch (err: unknown) {
    const e = err as { response?: { status?: number; statusText?: string }; message?: string };
    console.error(`❌ Failed "${productOutput.product_name}":`, e.response ? `${e.response.status} ${e.response.statusText}` : e.message);
    return false;
  }
}

async function uploadToS3(results: ScraperProductOutput[], jobId: string, runDateTime: string): Promise<void> {
  if (!SCRAPER_OUTPUTS_BUCKET) return;
  const key = `${SCRAPER_NAME}/${runDateTime}/products.json`;
  await s3Client.send(
    new PutObjectCommand({
      Bucket: SCRAPER_OUTPUTS_BUCKET,
      Key: key,
      Body: JSON.stringify(results, null, 2),
      ContentType: "application/json",
    })
  );
  console.log(`Uploaded to s3://${SCRAPER_OUTPUTS_BUCKET}/${key}`);
}

async function updateJobStatus(jobId: string, status: string, error: string | null = null): Promise<void> {
  if (!SCRAPER_JOB_STATUS_TABLE_NAME) return;
  const now = new Date().toISOString();
  await docClient.send(
    new UpdateCommand({
      TableName: SCRAPER_JOB_STATUS_TABLE_NAME,
      Key: { job_id: jobId },
      UpdateExpression: "SET #status = :status, updated_at = :updated_at" + (error ? ", error = :error" : ""),
      ExpressionAttributeNames: { "#status": "status" },
      ExpressionAttributeValues: {
        ":status": status,
        ":updated_at": now,
        ...(error && { ":error": error }),
      },
    })
  );
}

function parseArgs() {
  const argv = process.argv.slice(2);
  const defaultConfig = path.resolve(__dirname, "./config.json");
  let configPath = defaultConfig;
  let url: string | undefined;
  let limit: number | undefined;
  let offset: number | undefined;
  let local = false;
  let debug = false;
  let concurrency = 5;

  for (let i = 0; i < argv.length; i++) {
    if ((argv[i] === "--url" || argv[i] === "-u") && argv[i + 1]) {
      url = argv[i + 1];
      i++;
    } else if ((argv[i] === "--config" || argv[i] === "-c") && argv[i + 1]) {
      configPath = path.resolve(argv[i + 1]);
      i++;
    } else if ((argv[i] === "--limit" || argv[i] === "-l") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n > 0) limit = n;
      i++;
    } else if ((argv[i] === "--offset" || argv[i] === "-o") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n >= 0) offset = n;
      i++;
    } else if ((argv[i] === "--concurrency" || argv[i] === "-n") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n > 0) concurrency = n;
      i++;
    } else if (argv[i] === "--local") {
      local = true;
    } else if ((argv[i] === "--debug" || argv[i] === "-d")) {
      debug = true;
    }
  }

  return { url, configPath, limit, offset, local, concurrency, debug };
}

async function main(): Promise<void> {
  const { url, configPath, limit, offset, local, concurrency, debug } = parseArgs();

  DEBUG_TYSON = debug;

  if (local) {
    console.log("Running in local mode: skipping DynamoDB and S3; API submission still runs.");
  }
  if (limit != null) {
    console.log(`Limit: processing at most ${limit} products.`);
  }
  if (offset != null && offset > 0) {
    console.log(`Offset: skipping first ${offset} products.`);
  }
  console.log(`Concurrency: ${concurrency}`);

  const jobId = uuidv4();
  const runDateTime = new Date().toISOString().replace(/[:.]/g, "-").slice(0, -5);

  if (!local && SCRAPER_JOB_STATUS_TABLE_NAME) {
    await docClient.send(
      new PutCommand({
        TableName: SCRAPER_JOB_STATUS_TABLE_NAME,
        Item: {
          job_id: jobId,
          scraper_name: SCRAPER_NAME,
          status: "active",
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        },
      })
    );
    console.log(`Created job ${jobId}`);
  }

  let urls: string[] = [];
  if (url) {
    urls = [url];
  } else {
    try {
      const raw = await fs.readFileSync(configPath, "utf-8");
      const cfg = JSON.parse(raw) as AppConfig;
      const searchUrls = (cfg.searchUrls || []).filter((u) => typeof u === "string");
      if (searchUrls.length) {
        for (const su of searchUrls) {
          const targetCount = limit != null ? limit + (offset ?? 0) : undefined;
          const found = await discoverTysonProductUrls(su, targetCount);
          urls.push(...found);
          if (limit && urls.length >= limit + (offset ?? 0)) break;
        }
      } else {
        urls = (cfg.urls || []).filter((u) => typeof u === "string");
      }
    } catch {
      // ignore
    }
  }

  if (offset != null && offset > 0) {
    urls = urls.slice(offset);
  }
  if (limit != null) urls = urls.slice(0, limit);
  if (urls.length === 0) {
    console.error("No Tyson product URLs found. Provide --url or config.json with urls.");
    process.exit(1);
  }
  console.log(`Processing ${urls.length} product URLs`);

  const results: ScraperProductOutput[] = [];
  const submitBatch: ScraperProductOutput[] = [];
  const SUBMIT_BATCH_SIZE = 10;
  let scrapedCount = 0;
  let skippedCount = 0;
  let success = 0;
  let fail = 0;

  const flushSubmitBatch = async (force: boolean) => {
    if (!force && submitBatch.length < SUBMIT_BATCH_SIZE) return;
    if (!submitBatch.length) return;
    const take = force ? submitBatch.length : Math.min(SUBMIT_BATCH_SIZE, submitBatch.length);
    const batch = submitBatch.splice(0, take);
    console.log(`\n➡️  Submitting batch (${batch.length} items)`);
    for (const r of batch) {
      const { scraper_job_id: _, ...body } = r;
      const ok = await submitProductForReview(body);
      if (ok) success++;
      else fail++;
    }
  };
  let flushChain: Promise<void> = Promise.resolve();
  const queueFlush = (force: boolean) => {
    flushChain = flushChain.then(() => flushSubmitBatch(force)).catch((err) => {
      console.error("[SUBMIT] batch flush failed:", err);
    });
    return flushChain;
  };

  const queue = [...urls];
  const workerCount = Math.max(1, Math.min(concurrency, queue.length));
  const workers = Array.from({ length: workerCount }, async () => {
    while (queue.length) {
      const u = queue.shift();
      if (!u) break;
      let p: ScrapedProduct | null = null;
      try {
        p = await scrapeProduct(u);
      } catch (err) {
        console.error(`❌ Failed ${u}:`, err);
        skippedCount++;
        continue;
      }
      if (!p || !p.name || !p.ingredients) {
        console.log(`[SKIP] ${u}: missing name or ingredients`);
        skippedCount++;
        continue;
      }
      const output = transformToOutput(p);
      const exists = await checkProductExists({
        name: output.product_name || null,
        brand: output.brand || null,
        upc: output.upc || output.upcs?.[0] || null,
      });
      if (exists) {
        console.log(`[SKIP] ${output.product_name || "(no name)"}: already exists`);
        skippedCount++;
        continue;
      }
      scrapedCount++;
      console.log(`[PRODUCT] OK | ${output.product_name || "(no name)"} | ${u}`);
      const out = { ...output, scraper_job_id: jobId };
      results.push(out);
      submitBatch.push(out);
      if (submitBatch.length >= SUBMIT_BATCH_SIZE) void queueFlush(false);
    }
  });
  await Promise.all(workers);
  await queueFlush(true);
  await flushChain;

  if (!local) {
    await uploadToS3(results, jobId, runDateTime);
  }

  console.log(`\nScraped ${scrapedCount} valid products (skipped ${skippedCount}) out of ${urls.length} total`);
  console.log(`\n📊 API: ${success} submitted, ${fail} failed`);

  if (!local && SCRAPER_JOB_STATUS_TABLE_NAME) {
    if (results.length === 0) {
      await updateJobStatus(jobId, "error", "No products processed");
      process.exit(1);
    } else {
      await updateJobStatus(jobId, "complete");
    }
  }
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
