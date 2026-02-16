import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import axios from "axios";
import * as cheerio from "cheerio";
import pLimit from "p-limit";
import dotenv from "dotenv";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import * as nutritionUtils from "../nutrition-utils";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.resolve(__dirname, ".env") });

const parseNutrientAmountWithQualifier =
  (nutritionUtils as any).parseNutrientAmountWithQualifier ??
  (nutritionUtils as any).default?.parseNutrientAmountWithQualifier;

const SOURCE = "samsclub.com";
const SCRAPER_NAME = process.env.JOB_NAME || "sams-club";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://it7rdy3qbh.execute-api.us-west-2.amazonaws.com";
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
const SCRAPEDO_TOKEN = process.env.SCRAPEDO_TOKEN || "";
const SUBMIT_NUTRITION_PARSE_JOB_URL =
  process.env.SUBMIT_NUTRITION_PARSE_JOB_URL ||
  `${API_BASE_URL}/submit-nutrition-parse-job`;
const GET_NUTRITION_PARSE_RESULT_URL =
  process.env.GET_NUTRITION_PARSE_RESULT_URL ||
  `${API_BASE_URL}/get-nutrition-parse-result`;
const PARSE_NUTRITION_SERVICE_TOKEN = process.env.PARSE_NUTRITION_SERVICE_TOKEN || "12345";
const NUTRITION_POLL_INTERVAL_MS = parseInt(process.env.NUTRITION_POLL_INTERVAL_MS || "2000", 10);
const NUTRITION_POLL_TIMEOUT_MS = parseInt(process.env.NUTRITION_POLL_TIMEOUT_MS || "60000", 10);
let DEBUG_SAMS = false;

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
  upc12: string | null;
  nutrition: Nutrition | null;
  nutritionData: ScraperNutritionData | null;
  nutritionImageUrl: string | null;
  imageUrl: string | null;
  scrapedAt: string;
  sourceCreatedAt: string | null;
  sourceLastUpdatedAt: string | null;
};

const NUTRIENT_COLUMN_MAP: Record<string, string> = {
  "total fat": "total_fat_g",
  saturated: "saturated_fat_g",
  trans: "trans_fat_g",
  "polyunsaturated": "polyunsaturated_fat_g",
  "monounsaturated": "monounsaturated_fat_g",
  cholesterol: "cholesterol_mg",
  sodium: "sodium_mg",
  "total carbohydrate": "total_carbs_g",
  "dietary fiber": "fiber_g",
  "total sugars": "sugars_g",
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
  if (!servingSizeText || typeof servingSizeText !== "string") {
    return {value: null, unit: null};
  }
  const cleaned = servingSizeText.trim().replace(/\([^)]*\)/g, "").trim();

  let match = cleaned.match(
    /^(\d+)\/(\d+)\s+(Tbsp|tbsp|TSP|tsp|cup|cups|oz|fl\s*oz|floz|ml|g|kg|lb|lbs|serving|servings|pieces?|packets?)\b/i
  );
  if (match) {
    const value = parseFloat(match[1]) / parseFloat(match[2]);
    let unit = match[3].trim();
    if (unit.toLowerCase() === "pieces") unit = "piece";
    if (unit.toLowerCase() === "packets") unit = "packet";
    return {value, unit};
  }

  match = cleaned.match(
    /^(\d+(?:\.\d+)?)\s+(Tbsp|tbsp|TSP|tsp|cup|cups|oz|fl\s*oz|floz|ml|g|kg|lb|lbs|serving|servings|pieces?|packets?)\b/i
  );
  if (!match) match = cleaned.match(/^(\d+(?:\.\d+)?)(g|oz|ml)\b/i);
  if (match) {
    const value = parseFloat(match[1]);
    let unit = (match[2] || "g").trim();
    if (unit.toLowerCase() === "pieces") unit = "piece";
    if (unit.toLowerCase() === "packets") unit = "packet";
    return {value, unit};
  }

  const numMatch = cleaned.match(/^(\d+(?:\.\d+)?)/);
  if (numMatch) return {value: parseFloat(numMatch[1]), unit: "serving"};

  return {value: null, unit: null};
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

function extractNextData($: cheerio.CheerioAPI): any | null {
  const raw = $("#__NEXT_DATA__").text();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function extractNextDataFromHtml(html: string): any | null {
  const match = html.match(/<script[^>]+id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
  if (!match) return null;
  try {
    return JSON.parse(match[1]);
  } catch {
    return null;
  }
}

function extractNextDataRoot(nextData: any): any | null {
  if (!nextData || typeof nextData !== "object") return null;
  return nextData?.props?.pageProps?.initialData?.data || null;
}

function extractSearchResultRoot(nextData: any): any | null {
  if (!nextData || typeof nextData !== "object") return null;
  return nextData?.props?.pageProps?.initialData?.searchResult || null;
}

function normalizeProductName(name: string | null): string | null {
  if (!name) return null;
  return name
    .replace(/,\s*\d+\s*(pk|ct|count|pack)s?\.?$/i, "")
    .replace(/,\s*\d+(?:\.\d+)?\s*(oz|fl\s*oz|lb|lbs|g|kg|ml)\b.*$/i, "")
    .trim();
}

function findObjectWithKeys(obj: any, keys: string[]): any | null {
  if (!obj || typeof obj !== "object") return null;
  const hasKeys = keys.every((k) => Object.prototype.hasOwnProperty.call(obj, k));
  if (hasKeys) return obj;
  if (Array.isArray(obj)) {
    for (const item of obj) {
      const found = findObjectWithKeys(item, keys);
      if (found) return found;
    }
    return null;
  }
  for (const val of Object.values(obj)) {
    const found = findObjectWithKeys(val, keys);
    if (found) return found;
  }
  return null;
}

function findNamedValue(obj: any, needle: string): string | null {
  if (!obj || typeof obj !== "object") return null;
  const lowerNeedle = needle.toLowerCase();
  const stack: any[] = [obj];
  const visited = new Set<any>();
  while (stack.length) {
    const curr = stack.pop();
    if (!curr || typeof curr !== "object") continue;
    if (visited.has(curr)) continue;
    visited.add(curr);
    if (Array.isArray(curr)) {
      for (const item of curr) stack.push(item);
      continue;
    }
    for (const [key, value] of Object.entries(curr)) {
      if (typeof value === "string") {
        if (key.toLowerCase().includes(lowerNeedle) && value.startsWith("http")) {
          return value;
        }
      } else if (value && typeof value === "object") {
        stack.push(value);
      }
    }
  }
  return null;
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
  return null;
}

function extractIngredientsFromSpecifications(specs: any[]): string | null {
  if (!Array.isArray(specs)) return null;
  for (const spec of specs) {
    const value = cleanText(spec?.value || null);
    if (!value || !/ingredients/i.test(value)) continue;
    const $ = cheerio.load(value);
    const text = normalizeWhitespace($.text());
    const match = text.match(/Ingredients:\s*(.+)$/i);
    if (match) return match[1].trim();
  }
  return null;
}

function cleanIngredientsText(text: string | null): string | null {
  if (!text) return null;
  let out = text;
  // Remove leading flavor/header labels like "Mixed Berry Ingredients:"
  out = out.replace(/^[^:]{0,80}\bingredients\s*:\s*/i, "");
  // Remove trailing store/order instructions
  out = out.replace(/place your club pickup order.*$/i, "");
  // Remove safe handling guidance
  out = out.replace(/\b(safe handling|safe handling instructions)\b.*$/i, "");
  out = out.replace(/this product was prepared.*$/i, "");
  out = out.replace(/\bkeep refrigerated\b.*$/i, "");
  // Trim anything after allergen "Contains:" statements
  out = out.replace(/\bcontains\s*:\s*.*$/i, "");
  out = out.replace(/\bcontains\s+[a-z0-9 ,&-]+\.\s*$/i, "");
  out = normalizeWhitespace(out);
  return out || null;
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

function nutritionFromFactsList(facts: any[]): Nutrition | null {
  if (!Array.isArray(facts) || facts.length === 0) return null;
  const nutrients: NutritionNutrient[] = [];
  let calories: string | null = null;
  let servingSize: string | null = null;
  let servingsPerContainer: string | null = null;

  for (const item of facts) {
    const name = cleanText(item?.name || item?.label || item?.nutrientName || null);
    const amount = cleanText(item?.amount || item?.value || null);
    const unit = cleanText(item?.unit || item?.uom || null);
    const dv = cleanText(item?.dailyValuePercent || item?.dailyValue || item?.percentDailyValue || null);

    if (name && /serving size/i.test(name) && !servingSize) {
      servingSize = cleanText(item?.value || item?.amount || item?.text || null);
      continue;
    }
    if (name && /servings per/i.test(name) && !servingsPerContainer) {
      servingsPerContainer = cleanText(item?.value || item?.amount || item?.text || null);
      continue;
    }
    if (name && /calories/i.test(name)) {
      calories = amount || cleanText(item?.value || null);
      continue;
    }

    if (!name) continue;
    const amt = amount && unit ? `${amount}${unit}` : amount;
    nutrients.push({ name, amount: amt || null, dailyValuePercent: dv || null });
  }

  return { servingSize, servingsPerContainer, calories, nutrients };
}

function stripQueryParams(url: string | null): string | null {
  if (!url) return null;
  try {
    const u = new URL(url);
    u.search = "";
    return u.toString();
  } catch {
    return url.split("?")[0] || null;
  }
}

function extractCarouselImagesFromNextData(nextData: any): string[] {
  const root = extractNextDataRoot(nextData);
  const images = root?.product?.imageInfo?.allImages;
  if (Array.isArray(images)) {
    return images
      .map((img) => cleanText(img?.url || null))
      .filter((u): u is string => Boolean(u));
  }
  return [];
}

async function submitNutritionParseJob(imageUrl: string, mode?: string): Promise<string | null> {
  try {
    if (DEBUG_SAMS) {
      console.log(`[DEBUG] submit nutrition parse job: ${SUBMIT_NUTRITION_PARSE_JOB_URL}`);
      console.log(`[DEBUG] nutrition api image_url=${imageUrl}`);
      if (mode) console.log(`[DEBUG] nutrition api mode=${mode}`);
    }
    const res = await axios.post(
      SUBMIT_NUTRITION_PARSE_JOB_URL,
      { image_url: imageUrl, ...(mode ? { mode } : {}) },
      {
        headers: {
          "Content-Type": "application/json",
          "X-Service-Token": PARSE_NUTRITION_SERVICE_TOKEN,
        },
        timeout: 60_000,
      }
    );
    const jobId = res?.data?.job_id;
    if (DEBUG_SAMS) {
      console.log("[DEBUG] submit nutrition parse response:");
      console.log(JSON.stringify(res?.data ?? null, null, 2));
    }
    return typeof jobId === "string" ? jobId : null;
  } catch (err) {
    if (DEBUG_SAMS) console.log("[DEBUG] nutrition API error:", err);
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
      if (DEBUG_SAMS) {
        console.log(`[DEBUG] nutrition parse status: ${status}`);
      }
      if (status === "completed") return res?.data?.result ?? null;
      if (status === "failed") return null;
    } catch (err) {
      if (DEBUG_SAMS) console.log("[DEBUG] nutrition poll error:", err);
    }
    await new Promise((r) => setTimeout(r, NUTRITION_POLL_INTERVAL_MS));
  }
  return null;
}

function normalizeNutritionData(data: ScraperNutritionData): ScraperNutritionData {
  const result: ScraperNutritionData = { ...data };
  for (const [key, value] of Object.entries(result)) {
    if (key.endsWith("_qualifier")) continue;
    if (key === "serving_size_text" || key === "serving_size_unit_text" || key === "serving_size_unit_id") continue;
    if (typeof value === "string") {
      if (typeof parseNutrientAmountWithQualifier !== "function") {
        throw new Error("parseNutrientAmountWithQualifier import failed");
      }
      const parsed = parseNutrientAmountWithQualifier(value);
      if (parsed) {
        (result as unknown as Record<string, number>)[key] = parsed.value;
        const qualifierKey = `${key}_qualifier`;
        if (!(qualifierKey in result) && parsed.qualifier) {
          (result as unknown as Record<string, string>)[qualifierKey] = parsed.qualifier;
        }
      }
    }
  }
  return result;
}

async function fetchNutritionFromImage(imageUrl: string): Promise<ScraperNutritionData | null> {
  const jobId = await submitNutritionParseJob(imageUrl);
  if (!jobId) return null;
  const result = await pollNutritionParseResult(jobId);
  const nutrition = result?.nutrition;
  if (DEBUG_SAMS) {
    console.log("[DEBUG] nutrition parse result:");
    console.log(JSON.stringify(result ?? null, null, 2));
  }
  if (nutrition && typeof nutrition === "object") {
    return normalizeNutritionData(nutrition as ScraperNutritionData);
  }
  return null;
}

async function fetchNutritionOcrText(imageUrl: string): Promise<string | null> {
  try {
    const jobId = await submitNutritionParseJob(imageUrl, "ocr_only");
    if (!jobId) return null;
    const result = await pollNutritionParseResult(jobId);
    const text = result?.ocr_text;
    if (DEBUG_SAMS) {
      console.log("[DEBUG] nutrition api (ocr_only) result:");
      console.log(JSON.stringify(result ?? null, null, 2));
    }
    return typeof text === "string" ? text : null;
  } catch (err) {
    if (DEBUG_SAMS) console.log("[DEBUG] nutrition api (ocr_only) error:", err);
  }
  return null;
}

async function detectNutritionImageFromCarousel(imageUrls: string[]): Promise<string | null> {
  if (DEBUG_SAMS) {
    console.log(`[DEBUG] scanning ${imageUrls.length} carousel images for nutrition label`);
  }
  for (const url of imageUrls) {
    const stripped = stripQueryParams(url);
    if (!stripped) continue;
    const ocrText = await fetchNutritionOcrText(stripped);
    if (DEBUG_SAMS) {
      const snippet = ocrText ? ocrText.replace(/\s+/g, " ").slice(0, 120) : "(no text)";
      console.log(`[DEBUG] carousel image: ${stripped}`);
      console.log(`[DEBUG] ocr snippet: ${snippet}`);
    }
    if (ocrText) {
      const lower = ocrText.toLowerCase();
      if ((lower.includes("nutrition") && lower.includes("facts")) || lower.includes("supplement facts")) {
        return stripped;
      }
    }
  }
  return null;
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
  const serving = p.nutritionData?.serving_size_value != null
    ? { value: p.nutritionData.serving_size_value, unit: p.nutritionData.serving_size_unit_text || "serving" }
    : (p.nutrition?.servingSize ? parseServingSize(p.nutrition.servingSize) : { value: null, unit: null });

  return {product_name: p.name || "",
    brand: p.brand || "",
    upc: p.upc12 || undefined,
    ingredients_text: p.ingredients || "",
    serving_size_value: serving.value ?? undefined,
    serving_size_unit: serving.unit ?? undefined,
    serving_size_text: p.nutritionData?.serving_size_text || p.nutrition?.servingSize || undefined,
    source: SOURCE,
    source_id: p.productUrl,
    source_created_at: p.sourceCreatedAt || now,
    source_last_updated_at: p.sourceLastUpdatedAt || now,
    image_url: p.imageUrl || undefined,
    nutrition: p.nutritionData || transformNutritionToDb(p.nutrition) || undefined};
}

async function fetchHtml(url: string): Promise<string> {
  if (!SCRAPEDO_TOKEN) {
    throw new Error("SCRAPEDO_TOKEN is required to fetch Sam's Club pages via scrape.do");
  }
  const targetUrl = encodeURIComponent(url);
  const requestUrl = `http://api.scrape.do/?url=${targetUrl}&token=${SCRAPEDO_TOKEN}`;
  const res = await axios.get(requestUrl, {
    headers: {
      "User-Agent":
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
      Accept: "text/html,application/xhtml+xml",
      "Accept-Language": "en-US,en;q=0.9",
    },
    timeout: 60_000,
  });
  if (typeof res.data === "string") return res.data;
  if (res.data && typeof res.data === "object") {
    const html =
      (res.data as any).body ||
      (res.data as any).content ||
      (res.data as any).html ||
      (res.data as any).data ||
      null;
    if (typeof html === "string") return html;
  }
  throw new Error("Unexpected scrape.do response format");
}

async function getProductHtml(url: string): Promise<string> {
  return await fetchHtml(url);
}

function normalizeListingUrl(url: string, page: number): string {
  try {
    const u = new URL(url);
    if (page <= 1) {
      u.searchParams.delete("page");
      return u.toString();
    }
    u.searchParams.set("page", String(page));
    return u.toString();
  } catch {
    if (page <= 1) return url;
    const joiner = url.includes("?") ? "&" : "?";
    return `${url}${joiner}page=${page}`;
  }
}

function extractProductLinksFromListing(listingUrl: string, html: string): string[] {
  const $ = cheerio.load(html);
  const out = new Set<string>();
  $("a[href]").each((_, el) => {
    const href = $(el).attr("href");
    if (!href) return;
    let abs: string;
    try {
      abs = new URL(href, listingUrl).toString();
    } catch {
      return;
    }
    if (!abs.includes("/ip/")) return;
    try {
      const u = new URL(abs);
      const parts = u.pathname.split("/").filter(Boolean);
      const last = parts[parts.length - 1] || "";
      if (!/^\d+$/.test(last)) return;
      out.add(u.toString());
    } catch {
      return;
    }
  });
  // Parse JSON-LD ItemList links if present
  $('script[type="application/ld+json"]').each((_, el) => {
    const raw = $(el).text();
    if (!raw) return;
    try {
      const parsed = JSON.parse(raw);
      const list = parsed?.mainEntity?.itemListElement;
      if (Array.isArray(list)) {
        for (const item of list) {
          const url = cleanText(item?.url || null);
          if (!url) continue;
          try {
            const abs = new URL(url, listingUrl).toString();
            if (abs.includes("/ip/")) out.add(abs);
          } catch {
            // ignore
          }
        }
      }
    } catch {
      // ignore
    }
  });
  if (out.size === 0) {
    const nextData = extractNextDataFromHtml(html);
    const fromNext = extractProductLinksFromNextData(listingUrl, nextData);
    for (const link of fromNext) out.add(link);
  }
  return Array.from(out);
}

function extractProductLinksFromNextData(listingUrl: string, nextData: any): string[] {
  const out = new Set<string>();
  if (!nextData) return [];
  const roots = [
    extractNextDataRoot(nextData),
    extractSearchResultRoot(nextData),
  ].filter(Boolean);
  if (!roots.length) return [];

  const stack: any[] = [...roots];
  const visited = new Set<any>();
  while (stack.length) {
    const curr = stack.pop();
    if (!curr || typeof curr !== "object") continue;
    if (visited.has(curr)) continue;
    visited.add(curr);

    const maybeItems: any[] = [];
    if ((curr as any).items && Array.isArray((curr as any).items)) maybeItems.push(...(curr as any).items);
    if ((curr as any).itemsV2 && Array.isArray((curr as any).itemsV2)) maybeItems.push(...(curr as any).itemsV2);
    if (maybeItems.length) {
      for (const item of maybeItems) {
        const canonical = cleanText(item?.canonicalUrl || null);
        const usItemId = cleanText(item?.usItemId || null);
        if (canonical && canonical.includes("/ip/")) {
          try {
            const abs = new URL(canonical, listingUrl).toString();
            out.add(abs);
          } catch {
            // ignore
          }
        } else if (usItemId && /^\d+$/.test(usItemId)) {
          try {
            const abs = new URL(`/ip/${usItemId}`, listingUrl).toString();
            out.add(abs);
          } catch {
            // ignore
          }
        }
      }
    }

    if (Array.isArray(curr)) {
      for (const item of curr) stack.push(item);
      continue;
    }

    const canonical = cleanText((curr as any).canonicalUrl || null);
    const usItemId = cleanText((curr as any).usItemId || null);
    if (canonical && canonical.includes("/ip/")) {
      try {
        const abs = new URL(canonical, listingUrl).toString();
        out.add(abs);
      } catch {
        // ignore
      }
    } else if (usItemId && /^\d+$/.test(usItemId)) {
      try {
        const abs = new URL(`/ip/${usItemId}`, listingUrl).toString();
        out.add(abs);
      } catch {
        // ignore
      }
    }

    for (const val of Object.values(curr)) {
      if (val && typeof val === "object") stack.push(val);
    }
  }

  return Array.from(out);
}
async function fetchListingHtml(url: string): Promise<string> {
  return await fetchHtml(url);
}

async function discoverProductUrlsFromSearch(listingUrl: string, limit?: number): Promise<string[]> {
  const urls: string[] = [];
  const seen = new Set<string>();
  let page = 1;
  const targetCount = limit;
  while (true) {
    const pageUrl = normalizeListingUrl(listingUrl, page);
    if (DEBUG_SAMS) console.log(`[DISCOVER] page ${page}: ${pageUrl}`);
    let html = await fetchListingHtml(pageUrl);
    let links = extractProductLinksFromListing(pageUrl, html);
    if (DEBUG_SAMS) console.log(`[DEBUG] listing links from html: ${links.length}`);
    const needMoreFromPage1 = page === 1 && targetCount && links.length < targetCount;
    if (!links.length || needMoreFromPage1) {
      if (DEBUG_SAMS) console.log("[DISCOVER] no links from initial html; trying next data");
      const nextData = extractNextDataFromHtml(html);
      const fromNext = extractProductLinksFromNextData(pageUrl, nextData);
      for (const link of fromNext) links.push(link);
      links = Array.from(new Set(links));
      if (DEBUG_SAMS) console.log(`[DEBUG] listing links from next data: ${fromNext.length} (merged total ${links.length})`);
    }
    if (DEBUG_SAMS) {
      try {
        fs.writeFileSync(`/tmp/samsclub-listing-page-${page}.html`, html);
      } catch {
        // ignore
      }
    }
    if (!links.length) break;

    // If the first page already has enough items for the requested limit, stop paginating.
    if (page === 1 && targetCount && links.length >= targetCount) {
      links = links.slice(0, targetCount);
    }

    let added = 0;
    for (const link of links) {
      if (seen.has(link)) continue;
      seen.add(link);
      urls.push(link);
      added += 1;
      if (targetCount && urls.length >= targetCount) return urls;
    }
    if (added === 0) break;
    if (targetCount && urls.length >= targetCount) break;
    page += 1;
  }
  return urls;
}

function extractFromNextData(nextData: any): { name?: string; brand?: string; upc?: string; image?: string; ingredients?: string; nutrition?: Nutrition | null } {
  if (!nextData) return {};
  const productCandidate =
    findObjectWithKeys(nextData, ["product"])?.product ||
    findObjectWithKeys(nextData, ["item"])?.item ||
    findObjectWithKeys(nextData, ["productName"]) ||
    null;

  const rawProduct = productCandidate && typeof productCandidate === "object" ? productCandidate : null;
  const name = cleanText(rawProduct?.name || rawProduct?.productName || rawProduct?.title || null) || undefined;
  const brand = cleanText(rawProduct?.brand || rawProduct?.brandName || null) || undefined;
  const upc = cleanText(rawProduct?.upc || rawProduct?.upc12 || rawProduct?.gtin || null) || undefined;
  const image = cleanText(rawProduct?.image || rawProduct?.imageUrl || rawProduct?.primaryImage || null) || undefined;
  const ingredients = cleanText(rawProduct?.ingredients || rawProduct?.ingredientStatement || null) || undefined;

  const nutritionFacts =
    rawProduct?.nutritionFacts ||
    rawProduct?.nutrition ||
    rawProduct?.nutrients ||
    rawProduct?.nutritionInfo ||
    null;

  let nutrition: Nutrition | null = null;
  if (Array.isArray(nutritionFacts)) nutrition = nutritionFromFactsList(nutritionFacts);
  else if (nutritionFacts && typeof nutritionFacts === "object") {
    const factsArray = (nutritionFacts.facts || nutritionFacts.items || nutritionFacts.nutrients) as any;
    if (Array.isArray(factsArray)) nutrition = nutritionFromFactsList(factsArray);
  }

  return { name, brand, upc, image, ingredients, nutrition };
}

async function scrapeProduct(url: string): Promise<ScrapedProduct | null> {
  const html = await getProductHtml(url);
  if (DEBUG_SAMS) {
    try {
      fs.writeFileSync("/tmp/samsclub-rendered.html", html);
    } catch {
      // ignore debug write errors
    }
  }
  const $ = cheerio.load(html);

  const jsonLd = extractJsonLd($);
  const jsonLdProduct = jsonLd.find((o) => o && (o["@type"] === "Product" || o["@type"] === "ProductGroup")) || null;
  const nextData = extractNextData($);
  const nextExtracted = extractFromNextData(nextData);

  const nameRaw =
    cleanText(nextExtracted.name) ||
    cleanText(jsonLdProduct?.name || $("h1").first().text() || null);
  const name = normalizeProductName(nameRaw);
  const brand =
    cleanText(nextExtracted.brand) ||
    cleanText(
      (typeof jsonLdProduct?.brand === "string" ? jsonLdProduct.brand : jsonLdProduct?.brand?.name) ||
        $('meta[property="og:site_name"]').attr("content") ||
        null
    );
  const specs = extractNextDataRoot(nextData)?.idml?.specifications || [];
  const ingredientsRaw =
    cleanText(nextExtracted.ingredients) ||
    extractIngredientsFromSpecifications(specs) ||
    cleanText(jsonLdProduct?.ingredients || jsonLdProduct?.ingredientStatement || null) ||
    extractIngredientsFromHtml($);
  const ingredients = cleanIngredientsText(ingredientsRaw);
  const nutrition =
    nextExtracted.nutrition ||
    nutritionFromJsonLd(jsonLdProduct?.nutrition || jsonLdProduct?.nutritionInformation || null);
  let nutritionImageUrl = findNamedValue(extractNextDataRoot(nextData), "nutrition") || null;
  const carouselImages = extractCarouselImagesFromNextData(nextData);
  if (!nutritionImageUrl && carouselImages.length > 0) {
    if (DEBUG_SAMS) console.log("[DEBUG] no nutrition image in data; probing carousel images via OCR");
    nutritionImageUrl = await detectNutritionImageFromCarousel(carouselImages);
  }
  const nutritionData = nutritionImageUrl ? await fetchNutritionFromImage(nutritionImageUrl) : null;
  const imageUrl =
    cleanText(nextExtracted.image) ||
    cleanText(jsonLdProduct?.image || jsonLdProduct?.imageUrl || null) ||
    extractOgImage($, url);
  const upc12 =
    cleanText(nextExtracted.upc) ||
    extractUpcFromText(JSON.stringify(jsonLdProduct || {})) ||
    extractUpcFromText(html);

  if (DEBUG_SAMS) {
    console.log(`[DEBUG] name=${name ?? "(null)"}`);
    console.log(`[DEBUG] brand=${brand ?? "(null)"}`);
    console.log(`[DEBUG] ingredients=${ingredients ?? "(null)"}`);
    console.log(`[DEBUG] upc=${upc12 ?? "(null)"}`);
    console.log(`[DEBUG] image=${imageUrl ?? "(null)"}`);
    console.log(`[DEBUG] nutritionImageUrl=${nutritionImageUrl ?? "(null)"}`);
    console.log(`[DEBUG] nutrition=${nutritionData ? "image-api" : nutrition ? "yes" : "no"}`);
  }

  const now = new Date().toISOString();
  return {
    productUrl: url,
    name,
    brand,
    ingredients,
    upc12,
    nutrition,
    nutritionData,
    nutritionImageUrl,
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

async function submitProductForReview(productOutput: ScraperProductOutput): Promise<boolean> {
  if (!productOutput.product_name || !productOutput.ingredients_text) return false;
  try {
    const token = await getServiceToken();
    const { scraper_job_id: _, ...body } = productOutput as any;
    if (DEBUG_SAMS) {
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
      UpdateExpression: "SET #status = :status, #updated_at = :updated_at, #error = :error",
      ExpressionAttributeNames: { "#status": "status", "#updated_at": "updated_at", "#error": "error" },
      ExpressionAttributeValues: { ":status": status, ":updated_at": now, ":error": error },
    })
  );
}

function loadConfig(configPath?: string): AppConfig {
  const p = configPath || path.resolve(__dirname, "config.json");
  if (!fs.existsSync(p)) return { urls: [] };
  const raw = fs.readFileSync(p, "utf8");
  return JSON.parse(raw) as AppConfig;
}

function parseArgs() {
  const argv = process.argv.slice(2);
  let url: string | undefined;
  let configPath: string | undefined;
  let limit: number | undefined;
  let offset: number | undefined;
  let local = false;
  let concurrency = 5;
  let debug = false;

  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === "--url" || argv[i] === "-u") {
      url = argv[i + 1];
      i += 1;
    } else if (argv[i] === "--config" || argv[i] === "-c") {
      configPath = argv[i + 1];
      i += 1;
    } else if ((argv[i] === "--limit" || argv[i] === "-l") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n)) limit = n;
      i += 1;
    } else if ((argv[i] === "--offset" || argv[i] === "-o") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n >= 0) offset = n;
      i += 1;
    } else if (argv[i] === "--local") {
      local = true;
    } else if (argv[i] === "--concurrency" && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n)) concurrency = n;
      i += 1;
    } else if (argv[i] === "--debug") {
      debug = true;
    }
  }
  return { url, configPath, limit, offset, local, concurrency, debug };
}

async function main() {
  const { url, configPath, limit, offset, local, concurrency, debug } = parseArgs();
  DEBUG_SAMS = debug || process.env.DEBUG_SAMS === "1";

  if (local) {
    console.log("Running in local mode: skipping DynamoDB and S3; API submission still runs.");
  }
  if (limit != null) {
    console.log(`Limit: processing at most ${limit} products.`);
  }
  if (offset != null && offset > 0) {
    console.log(`Offset: skipping first ${offset} products.`);
  }

  const config = loadConfig(configPath);
  let urls: string[] = [];
  if (url) {
    urls = [url];
  } else if (config.urls?.length) {
    urls = config.urls;
  } else if (config.searchUrls?.length) {
    const targetCount = limit != null ? limit + (offset ?? 0) : undefined;
    for (const searchUrl of config.searchUrls) {
      const found = await discoverProductUrlsFromSearch(searchUrl, targetCount);
      urls.push(...found);
      if (targetCount && urls.length >= targetCount) break;
    }
  }

  if (offset != null && offset > 0) {
    urls = urls.slice(offset);
  }
  if (limit != null) {
    urls = urls.slice(0, limit);
  }

  if (!urls.length) {
    console.error("No URLs found. Provide --url or config.json with urls.");
    process.exit(1);
  }

  console.log(`Processing ${urls.length} product URLs`);
  const limiter = pLimit(concurrency);
  const results = await Promise.all(
    urls.map((u) =>
      limiter(async () => {
        const product = await scrapeProduct(u);
        if (!product) return null;
        const output = transformToOutput(product);
        if (!output.product_name) {
          console.log(`[PRODUCT] SKIP | ${u}`);
          return null;
        }
        console.log(`[PRODUCT] OK | ${output.product_name} | ${u}`);
        return output;
      })
    )
  );

  const filtered = results.filter(Boolean) as ScraperProductOutput[];
  const jobId = uuidv4();
  const runDateTime = new Date().toISOString();

  if (!local) {
    await updateJobStatus(jobId, "RUNNING");
  }

  console.log(`\n📤 Submitting ${filtered.length} products for review...`);
  let submitted = 0;
  let failed = 0;
  for (const prod of filtered) {
    const ok = await submitProductForReview(prod);
    if (ok) submitted += 1;
    else failed += 1;
  }

  if (!local) {
    await uploadToS3(filtered, jobId, runDateTime);
    await updateJobStatus(jobId, "SUCCESS", failed ? `${failed} failed` : null);
  }

  console.log(`\n📊 API: ${submitted} submitted, ${failed} failed`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
