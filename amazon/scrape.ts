import * as fs from "fs";
import { chromium, type Browser, type BrowserContext } from "playwright";
import axios from "axios";
import * as cheerio from "cheerio";
import pLimit from "p-limit";
import * as dotenv from "dotenv";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import * as nameUtils from "../name-utils";
import * as nutritionUtils from "../nutrition-utils";
import * as servingSizeUtils from "../serving-size-utils";
import * as productIdUtils from "../product-id-utils";

dotenv.config();

const parseNutrientAmountWithQualifier =
  (nutritionUtils as any).parseNutrientAmountWithQualifier ??
  (nutritionUtils as any).default?.parseNutrientAmountWithQualifier;
const cleanProductName =
  (nameUtils as any).cleanProductName ?? (nameUtils as any).default?.cleanProductName;
const parseServingSizeFromText =
  (servingSizeUtils as any).parseServingSizeFromText ??
  (servingSizeUtils as any).default?.parseServingSizeFromText;
const generateDeterministicProductId =
  (productIdUtils as any).generateDeterministicProductId ??
  (productIdUtils as any).default?.generateDeterministicProductId;

type AppConfig = {
  urls?: string[];
  searchUrls?: string[];
  brand?: string;
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
  brand: string;
  source: string;
  productUrl: string;
  name: string | null;
  ingredients: string | null;
  allergens: string | null;
  upc12: string | null;
  upcs?: string[];
  nutrition: Nutrition | null;
  nutritionData: ScraperNutritionData | null;
  nutritionImageUrl: string | null;
  imageUrl: string | null;
  scrapedAt: string;
  sourceCreatedAt: string | null;
  sourceLastUpdatedAt: string | null;
}

// AMAZON_USER_DATA_DIR=/tmp/amazon-profile npx tsx scrape.ts --config config.json --local --limit 1

const SOURCE = "amazon.com";
const SCRAPER_NAME = process.env.JOB_NAME || "amazon";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://api.mytummi.app";
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
const AMAZON_BRAND_OVERRIDE = "365 by Whole Foods Market";
let DEBUG_AMAZON = process.env.DEBUG_AMAZON === "1";
let AMAZON_HEADLESS = process.env.AMAZON_HEADLESS !== "0";
const AMAZON_SLOWMO = process.env.AMAZON_SLOWMO ? parseInt(process.env.AMAZON_SLOWMO, 10) : undefined;
const AMAZON_USER_DATA_DIR = process.env.AMAZON_USER_DATA_DIR;
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
const PRODUCTS_API_URL = `${API_BASE_URL}/products`;

const s3Client = new S3Client({});
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);
const ssmClient = new SSMClient({});

let serviceTokenCache: string | null = null;

function normalizeWhitespace(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

function decodeHtmlEntities(input: string | null): string | null {
  if (!input) return null;
  return input
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&#(\d+);/g, (_, num) => {
      const code = parseInt(num, 10);
      return Number.isFinite(code) ? String.fromCharCode(code) : _;
    })
    .replace(/\s+/g, " ")
    .trim();
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
    if (DEBUG_AMAZON) console.log("[DEBUG] product exists check failed:", e);
    return false;
  }
}

function buildScrapeDoUrl(url: string): string {
  if (!SCRAPEDO_TOKEN) {
    throw new Error("SCRAPEDO_TOKEN is required to fetch Amazon pages via scrape.do");
  }
  const targetUrl = encodeURIComponent(url);
  return `http://api.scrape.do/?url=${targetUrl}&token=${SCRAPEDO_TOKEN}`;
}

function parseServingSize(servingSizeText: string | null): { value: number | null; unit: string | null } {
  if (typeof parseServingSizeFromText !== "function") {
    throw new Error("parseServingSizeFromText import failed");
  }
  return parseServingSizeFromText(servingSizeText);
}



const NUTRIENT_COLUMN_MAP: Record<string, string> = {
  "total fat": "total_fat_g",
  saturated: "saturated_fat_g",
  trans: "trans_fat_g",
  cholesterol: "cholesterol_mg",
  sodium: "sodium_mg",
  "total carbohydrate": "total_carbs_g",
  "dietary fiber": "fiber_g",
  sugars: "sugars_g",
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
  "vitamin b1": "thiamin_mg",
  thiamin: "thiamin_mg",
  "vitamin b2": "riboflavin_mg",
  riboflavin: "riboflavin_mg",
  "vitamin b3": "niacin_mg",
  niacin: "niacin_mg",
  "vitamin b6": "vitamin_b6_mg",
  folate: "folate_mcg",
  "folate dfe": "folate_mcg",
  "folic acid": "folic_acid_mcg",
  "vitamin b12": "vitamin_b12_mcg",
  biotin: "biotin_mcg",
  "pantothenic acid": "pantothenic_acid_mg",
  magnesium: "magnesium_mg",
  phosphorus: "phosphorus_mg",
  zinc: "zinc_mg",
};

function mapNutrientToColumn(name: string): string | null {
  const lower = name.toLowerCase().trim();
  if (lower.includes("added sugars")) return "added_sugars_g";
  if (lower.includes("total sugars")) return "sugars_g";
  if (lower.includes("folic acid")) return "folic_acid_mcg";
  if (lower.includes("folate")) return "folate_mcg";
  for (const [key, col] of Object.entries(NUTRIENT_COLUMN_MAP)) {
    if (lower.includes(key)) return col;
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

  const calories = nutrition.calories ? parseFloat(nutrition.calories.trim()) : null;
  if (calories !== null && !isNaN(calories)) result.calories = calories;

  for (const n of nutrition.nutrients || []) {
    const col = mapNutrientToColumn(n.name);
    if (DEBUG_AMAZON) {
      console.log(`[DEBUG] nutrition row: label="${n.name}" amount="${n.amount}" mapped="${col ?? "(none)"}"`);
    }
    if (col) {
      if (typeof parseNutrientAmountWithQualifier !== "function") {
        throw new Error("parseNutrientAmountWithQualifier import failed");
      }
      const parsed = parseNutrientAmountWithQualifier(n.amount);
      if (parsed !== null) {
        (result as unknown as Record<string, number>)[col] = parsed.value;
        if (parsed.qualifier) (result as unknown as Record<string, string>)[`${col}_qualifier`] = parsed.qualifier;
      }
    }
  }

  return result;
}

function mergeNutritionData(primary: ScraperNutritionData | null, secondary: ScraperNutritionData | null): ScraperNutritionData | null {
  if (primary && !secondary) return primary;
  if (!primary && secondary) return secondary;
  if (!primary && !secondary) return null;
  const merged: ScraperNutritionData = { ...(secondary as ScraperNutritionData) };
  for (const [key, value] of Object.entries(primary as ScraperNutritionData)) {
    if (value !== undefined && value !== null && value !== "") {
      (merged as any)[key] = value;
    }
  }
  return merged;
}

function extractBrandFromName(name: string | null): string | null {
  if (!name) return null;
  const commaIdx = name.indexOf(",");
  if (commaIdx > 0) return name.slice(0, commaIdx).trim();
  const dashIdx = name.indexOf(" - ");
  if (dashIdx > 0) return name.slice(0, dashIdx).trim();
  const pipeIdx = name.indexOf("|");
  if (pipeIdx > 0) return name.slice(0, pipeIdx).trim();
  return null;
}

function normalizeProductName(name: string | null, brand: string | null): string | null {
  return cleanProductName(name, {
    brand,
    decodeHtml: true,
    stripBrandPrefix: true,
    stripPipe: true,
    stripTrailingDashSize: true,
    stripTrailingCommaSize: true,
  });
}

function hasIngredientsOrNutrition(p: ScrapedProduct): boolean {
  const hasIngredients = !!p.ingredients?.trim();
  const hasNutrition =
    !!p.nutrition &&
    (!!p.nutrition.calories ||
      (p.nutrition.nutrients && p.nutrition.nutrients.length > 0) ||
      !!p.nutrition.servingSize);
  return hasIngredients || hasNutrition || !!p.nutritionData;
}

function logScrapedData(p: ScrapedProduct, url: string): void {
  const hasIng = !!p.ingredients?.trim();
  const hasNut =
    !!p.nutrition &&
    (!!p.nutrition.calories ||
      (p.nutrition.nutrients && p.nutrition.nutrients.length > 0) ||
      !!p.nutrition.servingSize);

  console.log(`  [DEBUG] name: ${p.name ?? "(null)"}`);
  console.log(`  [DEBUG] ingredients: ${hasIng ? `yes (${p.ingredients!.length} chars)` : "no"}`);
  console.log(`  [DEBUG] nutrition: ${p.nutritionData ? "image-api" : hasNut ? "yes" : "no"}`);
  console.log(`  [DEBUG] allergens: ${p.allergens ?? "(null)"}`);
  console.log(`  [DEBUG] upc12: ${p.upc12 ?? "(null)"}`);
  console.log(`  [DEBUG] image: ${p.imageUrl ?? "(null)"}`);
  console.log(`  [DEBUG] nutritionImage: ${p.nutritionImageUrl ?? "(null)"}`);
  console.log(`  [DEBUG] url: ${url}`);
}

function extractFromJsonLd(jsonText: string): Partial<ScrapedProduct> {
  const result: Partial<ScrapedProduct> = {};
  try {
    const data = JSON.parse(jsonText) as any;
    const item = Array.isArray(data) ? data.find((d) => d && d["@type"] === "Product") : data;
    if (!item || item["@type"] !== "Product") return result;

    result.name = decodeHtmlEntities(item.name) || null;
    if (item.image) {
      if (Array.isArray(item.image)) result.imageUrl = item.image[0];
      else result.imageUrl = item.image;
    }
    if (typeof item.brand === "string") result.brand = item.brand;
    if (item.brand && typeof item.brand.name === "string") result.brand = item.brand.name;

    if (typeof item.gtin12 === "string") result.upc12 = item.gtin12;
    if (typeof item.gtin13 === "string" && !result.upc12) {
      const m = item.gtin13.match(/(\d{12})/);
      result.upc12 = m?.[1] ?? item.gtin13;
    }
    if (typeof item.gtin === "string" && !result.upc12) {
      const m = item.gtin.match(/(\d{12})/);
      result.upc12 = m?.[1] ?? item.gtin;
    }
  } catch {
    // ignore
  }
  return result;
}

function pickLargestDynamicImage(dynamic: string | null): string | null {
  if (!dynamic) return null;
  try {
    const parsed = JSON.parse(dynamic) as Record<string, [number, number]>;
    const entries = Object.entries(parsed);
    if (entries.length === 0) return null;
    entries.sort((a, b) => {
      const aSize = (a[1]?.[0] || 0) * (a[1]?.[1] || 0);
      const bSize = (b[1]?.[0] || 0) * (b[1]?.[1] || 0);
      return bSize - aSize;
    });
    return entries[0][0] || null;
  } catch {
    return null;
  }
}

function normalizeAmazonImageUrl(url: string | null): string | null {
  if (!url) return null;
  return url.trim();
}

function extractMainImageUrl($: cheerio.CheerioAPI): string | null {
  const zoom = $("#ivLargeImage img").attr("src");
  if (zoom) return zoom;

  const landing = $("#landingImage");
  const dynamic = landing.attr("data-a-dynamic-image");
  const dynamicLargest = pickLargestDynamicImage(dynamic);
  if (dynamicLargest) return dynamicLargest;

  const hiRes = landing.attr("data-old-hires") || landing.attr("data-zoom-hires");
  if (hiRes) return hiRes;

  const img = $("#imgTagWrapperId img").attr("src");
  if (img) return img;
  const og = $('meta[property="og:image"]').attr("content");
  return og || null;
}

function extractCarouselImageUrls($: cheerio.CheerioAPI): string[] {
  const urls = new Set<string>();
  const addUrl = (u?: string | null) => {
    if (!u) return;
    const normalized = normalizeAmazonImageUrl(u.trim());
    if (normalized) urls.add(normalized);
  };

  addUrl($("#ivLargeImage img").attr("src"));

  $("img[data-a-dynamic-image]").each((_, el) => {
    const raw = $(el).attr("data-a-dynamic-image");
    if (!raw) return;
    const largest = pickLargestDynamicImage(raw);
    if (largest) addUrl(largest);
  });

  $("#altImages img, #imageBlockThumbs img, #imageBlock img").each((_, el) => {
    addUrl($(el).attr("data-old-hires"));
    addUrl($(el).attr("data-src"));
    addUrl($(el).attr("src"));
    addUrl($(el).attr("data-zoom-hires"));
  });

  const landing = $("#landingImage");
  const dynamic = landing.attr("data-a-dynamic-image");
  const largestLanding = pickLargestDynamicImage(dynamic);
  if (largestLanding) addUrl(largestLanding);
  addUrl(landing.attr("data-old-hires"));
  addUrl(landing.attr("data-zoom-hires"));

  return Array.from(urls);
}

function imageQualityScore(url: string): number {
  const slMatch = url.match(/_SL(\d+)_\./i);
  if (slMatch) return parseInt(slMatch[1], 10);
  const sizeMatch = url.match(/\\b(\\d{3,4})x(\\d{3,4})\\b/);
  if (sizeMatch) return parseInt(sizeMatch[1], 10) * parseInt(sizeMatch[2], 10);
  return 0;
}

function extractFromDetailBullets($: cheerio.CheerioAPI, labelRegex: RegExp): string | null {
  const lis = $("#detailBullets_feature_div li");
  let value: string | null = null;
  lis.each((_, el) => {
    const label = $(el).find("span.a-text-bold").first().text();
    if (labelRegex.test(normalizeWhitespace(label))) {
      const fullText = normalizeWhitespace($(el).text());
      value = normalizeWhitespace(fullText.replace(label, ""));
      return false;
    }
    return undefined;
  });
  return value;
}

function extractFromTables($: cheerio.CheerioAPI, labelRegex: RegExp): string | null {
  const tables = [
    "#productDetails_techSpec_section_1",
    "#productDetails_detailBullets_sections1",
    "#productDetails_techSpec_section_2",
    "#productDetails_detailBullets_sections2",
  ];
  for (const selector of tables) {
    const table = $(selector);
    if (!table.length) continue;
    const rows = table.find("tr");
    let found: string | null = null;
    rows.each((_, el) => {
      const th = normalizeWhitespace($(el).find("th").first().text());
      if (labelRegex.test(th)) {
        const td = normalizeWhitespace($(el).find("td").first().text());
        if (td) found = td;
        return false;
      }
      return undefined;
    });
    if (found) return found;
  }
  return null;
}

function extractFromAnyTable($: cheerio.CheerioAPI, labelRegex: RegExp): string | null {
  const tables = $("table");
  let found: string | null = null;
  tables.each((_, table) => {
    if (found) return;
    $(table)
      .find("tr")
      .each((_, row) => {
        const th = normalizeWhitespace($(row).find("th").first().text());
        if (!labelRegex.test(th)) return;
        const td = normalizeWhitespace($(row).find("td").first().text());
        if (td) {
          found = td;
          return false;
        }
        return undefined;
      });
  });
  return found;
}

function extractFromImportantInfo($: cheerio.CheerioAPI, labelRegex: RegExp): string | null {
  const section = $("#important-information");
  if (!section.length) return null;
  const headers = section.find("h4, h5, h6, b, strong, span");
  let found: string | null = null;
  headers.each((_, el) => {
    const label = normalizeWhitespace($(el).text());
    if (!labelRegex.test(label)) return undefined;
    const candidate = normalizeWhitespace($(el).parent().text().replace(label, ""));
    if (candidate) {
      found = candidate;
      return false;
    }
    const next = normalizeWhitespace($(el).next().text());
    if (next) {
      found = next;
      return false;
    }
    return undefined;
  });
  return found;
}

function extractUpcsFromText(text: string | null): string[] {
  if (!text) return [];
  const matches = text.match(/\b\d{12,13}\b/g);
  if (!matches) return [];
  const normalized = matches
    .map((m) => (m.length === 13 ? m.slice(1) : m))
    .filter((m) => m.length === 12);
  return Array.from(new Set(normalized));
}

function extractAllergens($: cheerio.CheerioAPI): string | null {
  const labelRegex = /allergen/i;
  const fromBullets = extractFromDetailBullets($, labelRegex);
  if (fromBullets) return fromBullets;
  const fromTable = extractFromTables($, labelRegex);
  if (fromTable) return fromTable;
  const fromAnyTable = extractFromAnyTable($, labelRegex);
  if (fromAnyTable) return fromAnyTable;
  const fromInfo = extractFromImportantInfo($, labelRegex);
  return fromInfo || null;
}

function extractNutritionFromText(text: string): Nutrition | null {
  const lower = text.toLowerCase();
  if (!/nutrition facts|calories|serving size/.test(lower)) return null;

  const servingSize = text.match(/Serving Size\s*:?\s*([^\n\r]+)/i)?.[1]?.trim() ?? null;
  const servingsPerContainer =
    text.match(/Servings? Per (?:Container|Package)\s*:?\s*([^\n\r]+)/i)?.[1]?.trim() ?? null;
  const calories = text.match(/Calories\s*:?\s*(\d+)/i)?.[1] ?? null;

  const nutrients: NutritionNutrient[] = [];
  const names = [
    "Total Fat",
    "Saturated Fat",
    "Trans Fat",
    "Cholesterol",
    "Sodium",
    "Total Carbohydrate",
    "Dietary Fiber",
    "Total Sugars",
    "Added Sugars",
    "Protein",
    "Vitamin D",
    "Calcium",
    "Iron",
    "Potassium",
  ];

  const lines = text
    .replace(/\u00a0/g, " ")
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);
  for (const line of lines) {
    for (const name of names) {
      const escaped = name.replace(/\s+/g, "\\s+");
      const re = new RegExp(
        `^(?:\\s*(\\d+%)\\s*)?${escaped}\\s*([0-9][0-9.,]*\\s*[a-zA-ZµmcgMG]+)?\\s*(\\d+%)?\\s*$`,
        "i"
      );
      const m = line.match(re);
      if (!m) continue;
      const percent = m[1] || m[3] || null;
      nutrients.push({
        name,
        amount: m[2] ? normalizeWhitespace(m[2]) : null,
        dailyValuePercent: percent ? percent.trim() : null,
      });
      break;
    }
  }

  if (!servingSize && !calories && nutrients.length === 0) return null;
  return {servingSize, servingsPerContainer, calories, nutrients};
}

function extractNutritionFromTable($: cheerio.CheerioAPI): Nutrition | null {
  const table = $("#nutritionFactsTable, #nutritionFacts_feature_div table, table[id*='nutritionFacts'], table[class*='nutritionFacts']").first();
  if (!table.length) return null;

  const nutrition: Nutrition = {
    servingSize: null,
    servingsPerContainer: null,
    calories: null,
    nutrients: [],
  };

  const fullText = normalizeWhitespace(table.text());
  nutrition.servingsPerContainer =
    fullText.match(/servings per container\\s*([\\d.]+[^A-Za-z0-9])?/i)?.[1]?.trim() ?? null;
  nutrition.servingSize =
    fullText.match(/serving size\\s*([^%]+?)\\s*(amount per serving|calories)/i)?.[1]?.trim() ?? null;
  nutrition.calories = fullText.match(/calories\\s*(\\d+)/i)?.[1] ?? null;

  table.find("tr").each((_, row) => {
    const cells = $(row).find("th, td");
    if (cells.length === 0) return;
    const rowText = normalizeWhitespace($(row).text());
    if (/calories/i.test(rowText)) return;

    let name = normalizeWhitespace($(cells[0]).text());
    if (!name) return;

    const amountMatch = rowText.match(/([0-9][0-9.,]*\\s*[a-zA-ZµmcgMG]+)?/i);
    const percentMatch = rowText.match(/(\\d+%)/);

    nutrition.nutrients.push({
      name,
      amount: amountMatch && amountMatch[1] ? normalizeWhitespace(amountMatch[1]) : null,
      dailyValuePercent: percentMatch ? percentMatch[1] : null,
    });
  });

  const hasAny =
    nutrition.servingSize || nutrition.servingsPerContainer || nutrition.calories || nutrition.nutrients.length > 0;
  return hasAny ? nutrition : null;
}

function extractNutritionFromNic($: cheerio.CheerioAPI): Nutrition | null {
  if (!$("#nic-nutrition-facts-serving-content").length) return null;

  const servingsPerContainer = normalizeWhitespace($("#nic-nutrition-facts-total-serving span").first().text());
  const servingSize = normalizeWhitespace(
    $("#nic-nutrition-facts-serving-size td.a-text-right span").first().text()
  );
  const calories = normalizeWhitespace(
    $("#nic-nutrition-facts-energy td.a-text-right span").first().text()
  );

  const nutrients: NutritionNutrient[] = [];
  $("#nic-nutrition-facts tr").each((_, row) => {
    const tds = $(row).find("td");
    if (tds.length < 2) return;
    const percent = normalizeWhitespace($(tds[0]).text()) || null;
    const name =
      normalizeWhitespace($(tds[1]).find("span").first().text()) ||
      normalizeWhitespace($(tds[1]).text());
    const amount =
      normalizeWhitespace(
        $(tds[1]).find("span[class*='nutrientAmountText']").first().text()
      ) || null;
    if (!name) return;
    nutrients.push({ name, amount, dailyValuePercent: percent });
  });

  const hasAny =
    servingsPerContainer ||
    servingSize ||
    calories ||
    (nutrients && nutrients.length > 0);
  if (!hasAny) return null;

  return {servingSize: servingSize || null,
    servingsPerContainer: servingsPerContainer || null,
    calories: calories || null,
    nutrients};
}

function extractNutritionFromApiJson(data: unknown): Nutrition | null {
  const queue: unknown[] = [data];
  let seen = 0;
  while (queue.length > 0 && seen < 5000) {
    const current = queue.shift();
    seen++;
    if (!current || typeof current !== "object") continue;
    if (Array.isArray(current)) {
      for (const item of current) queue.push(item);
      continue;
    }

    const obj = current as Record<string, unknown>;
    const hasServing = typeof obj.servingSize === "string" || typeof obj.serving_size === "string";
    const hasServingsPer =
      typeof obj.servingsPerContainer === "string" || typeof obj.servings_per_container === "string";
    const hasCalories =
      typeof obj.calories === "string" ||
      typeof obj.caloriesAmount === "string" ||
      typeof obj.caloriesPerServing === "string" ||
      typeof obj.caloriesPerServing === "number";
    const nutrientsArr =
      (Array.isArray(obj.nutrients) && obj.nutrients) ||
      (Array.isArray(obj.macronutrients) && obj.macronutrients) ||
      (Array.isArray(obj.vitaminsAndMinerals) && obj.vitaminsAndMinerals);

    if ((hasServing || hasServingsPer || hasCalories) && nutrientsArr) {
      const nutrients: NutritionNutrient[] = [];
      for (const item of nutrientsArr as any[]) {
        if (!item || typeof item !== "object") continue;
        const name = (item.name || item.nutrientName || item.label) as string | undefined;
        if (!name) continue;
        const amount =
          (typeof item.amount === "string" && item.amount) ||
          (typeof item.value === "string" && item.value) ||
          (typeof item.value === "number" ? String(item.value) : null);
        const unit = typeof item.unit === "string" ? item.unit : null;
        const percent =
          (typeof item.percent === "string" && item.percent) ||
          (typeof item.dailyValuePercent === "string" && item.dailyValuePercent) ||
          (typeof item.percentDailyValue === "string" && item.percentDailyValue) ||
          null;

        nutrients.push({
          name,
          amount: amount ? `${amount}${unit ?? ""}` : null,
          dailyValuePercent: percent,
        });
      }

      return {servingSize:
          (obj.servingSize as string) ||
          (obj.serving_size as string) ||
          null,
        servingsPerContainer:
          (obj.servingsPerContainer as string) ||
          (obj.servings_per_container as string) ||
          null,
        calories:
          (obj.calories as string) ||
          (obj.caloriesAmount as string) ||
          (obj.caloriesPerServing as string) ||
          (typeof obj.caloriesPerServing === "number" ? String(obj.caloriesPerServing) : null),
        nutrients};
    }

    for (const value of Object.values(obj)) {
      if (value && typeof value === "object") queue.push(value);
    }
  }
  return null;
}

function extractNutritionFromEmbeddedJson($: cheerio.CheerioAPI): Nutrition | null {
  const scripts = $("script[type='a-state'], script[type='application/json']");
  let found: Nutrition | null = null;
  scripts.each((_, el) => {
    if (found) return;
    const raw = $(el).contents().text();
    if (!raw || !/nutrition/i.test(raw)) return;
    try {
      const json = JSON.parse(raw);
      const parsed = extractNutritionFromApiJson(json);
      if (parsed) found = parsed;
    } catch {
      // ignore
    }
  });
  return found;
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

async function submitNutritionParseJob(imageUrl: string, mode?: string): Promise<string | null> {
  try {
    if (DEBUG_AMAZON) {
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
    if (DEBUG_AMAZON) {
      console.log("[DEBUG] submit nutrition parse response:");
      console.log(JSON.stringify(res?.data ?? null, null, 2));
    }
    return typeof jobId === "string" ? jobId : null;
  } catch (err) {
    if (DEBUG_AMAZON) console.log("[DEBUG] nutrition API error:", err);
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
      if (DEBUG_AMAZON) {
        console.log(`[DEBUG] nutrition parse status: ${status}`);
      }
      if (status === "completed") return res?.data?.result ?? null;
      if (status === "failed") return null;
    } catch (err) {
      if (DEBUG_AMAZON) console.log("[DEBUG] nutrition poll error:", err);
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
  if (DEBUG_AMAZON) {
    console.log("[DEBUG] nutrition parse result:");
    console.log(JSON.stringify(result ?? null, null, 2));
  }
  if (nutrition && typeof nutrition === "object") {
    const normalized = normalizeNutritionData(nutrition as ScraperNutritionData);
    const hasAny = Object.entries(normalized).some(
      ([key, value]) =>
        value !== undefined &&
        value !== null &&
        value !== "" &&
        !key.endsWith("_qualifier") &&
        key !== "serving_size_text" &&
        key !== "serving_size_unit_text" &&
        key !== "serving_size_unit_id"
    );
    return hasAny ? normalized : null;
  }
  return null;
}

async function fetchNutritionOcrText(imageUrl: string): Promise<string | null> {
  try {
    const jobId = await submitNutritionParseJob(imageUrl, "ocr_only");
    if (!jobId) return null;
    const result = await pollNutritionParseResult(jobId);
    const text = result?.ocr_text;
    if (DEBUG_AMAZON) {
      console.log("[DEBUG] nutrition api (ocr_only) result:");
      console.log(JSON.stringify(result ?? null, null, 2));
    }
    return typeof text === "string" ? text : null;
  } catch (err) {
    if (DEBUG_AMAZON) console.log("[DEBUG] nutrition api (ocr_only) error:", err);
  }
  return null;
}

async function detectNutritionImageFromCarousel(imageUrls: string[]): Promise<string | null> {
  if (DEBUG_AMAZON) {
    console.log(`[DEBUG] scanning ${imageUrls.length} carousel images for nutrition label`);
  }
  for (const url of imageUrls) {
    if (!url) continue;
    if (/loading|loadIndicators|\.gif($|\?)/i.test(url)) continue;
    await new Promise((r) => setTimeout(r, 200));
    const ocrText = await fetchNutritionOcrText(url);
    if (DEBUG_AMAZON) {
      const snippet = ocrText ? ocrText.replace(/\s+/g, " ").slice(0, 120) : "(no text)";
      console.log(`[DEBUG] carousel image: ${url}`);
      console.log(`[DEBUG] ocr snippet: ${snippet}`);
    }
    if (ocrText) {
      const lower = ocrText.toLowerCase();
      if (
        (lower.includes("nutrition") && lower.includes("facts")) ||
        lower.includes("supplement facts")
      ) {
        return url;
      }
    }
  }
  return null;
}

async function scrapeProductDetail(context: BrowserContext, productUrl: string, brandOverride?: string): Promise<ScrapedProduct | null> {
  const page = await context.newPage();
  try {
    let nutritionFromApi: Nutrition | null = null;
    let nutritionTextFromDom: string | null = null;
    let overlayImages: string[] = [];
    const debugResponses: Array<{ url: string; size?: number }> = [];
    page.on("response", async (res) => {
      if (nutritionFromApi) return;
      const ct = res.headers()["content-type"] || "";
      if (!ct.includes("application/json")) return;
      try {
        const text = await res.text();
        if (DEBUG_AMAZON) {
          debugResponses.push({ url: res.url(), size: text.length });
          if (/cholesterol|vitamin|calcium|iron|potassium|nutrition/i.test(text)) {
            try {
              fs.writeFileSync(
                `/tmp/amazon-nutrition-response-${Date.now()}.json`,
                text
              );
            } catch {
              // ignore
            }
          }
        }
        const json = JSON.parse(text);
        const found = extractNutritionFromApiJson(json);
        if (found) {
          nutritionFromApi = found;
          if (DEBUG_AMAZON) {
            try {
              fs.writeFileSync("/tmp/amazon-nutrition.json", JSON.stringify(json, null, 2));
            } catch {
              // ignore
            }
          }
        }
      } catch {
        // ignore
      }
    });

    await page.goto(buildScrapeDoUrl(productUrl), { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(500);
    await page.waitForLoadState("networkidle", { timeout: 2000 }).catch(() => {});

    // Early existence check before heavy parsing/overlay actions
    try {
      const earlyHtml = await page.content();
      const $early = cheerio.load(earlyHtml);
      let earlyJsonLd: Partial<ScrapedProduct> = {};
      $early('script[type="application/ld+json"]').each((_, el) => {
        const txt = $early(el).contents().text();
        if (!txt) return;
        const res = extractFromJsonLd(txt);
        if (res.name || res.upc12 || res.imageUrl || res.brand) earlyJsonLd = { ...earlyJsonLd, ...res };
      });

      const earlyName =
        earlyJsonLd.name ??
        decodeHtmlEntities($early("#productTitle").first().text()) ??
        decodeHtmlEntities($early("h1").first().text()) ??
        decodeHtmlEntities($early('meta[property="og:title"]').attr("content") || null) ??
        null;

      const earlyBrand =
        AMAZON_BRAND_OVERRIDE || brandOverride || earlyJsonLd.brand || extractBrandFromName(earlyName) || "Unknown";
      const earlyCleanedName = normalizeProductName(earlyName, AMAZON_BRAND_OVERRIDE || brandOverride || null);

      let earlyUpc12 = earlyJsonLd.upc12 ?? null;
      if (!earlyUpc12) {
        const upc =
          extractFromDetailBullets($early, /^upc/i) ||
          extractFromTables($early, /^upc/i) ||
          extractFromTables($early, /^gtin\-?12/i);
        if (upc) {
          const list = extractUpcsFromText(upc);
          if (list.length > 0) earlyUpc12 = list[0] ?? null;
        }
      }

      const exists = await checkProductExists({
        name: earlyCleanedName ?? earlyName,
        brand: earlyBrand,
        upc: earlyUpc12,
      });
      if (exists) {
        console.log(`[SKIP] ${earlyBrand} ${earlyCleanedName ?? earlyName ?? "(no name)"}: already exists`);
        return null;
      }
    } catch (e) {
      if (DEBUG_AMAZON) console.log("[DEBUG] early existence check failed:", e);
    }

    const maybeCloseImageZoom = async (): Promise<void> => {
      const closeBtn = page.locator("#ivClose, .ivClose, .ivCloseBtn, button[aria-label='Close'], button[aria-label='Close image']");
      if (await closeBtn.count()) {
        await closeBtn.first().click({ timeout: 1000 }).catch(() => {});
      } else {
        await page.keyboard.press("Escape").catch(() => {});
      }
      await page.waitForTimeout(200);
    };

    const maybeOpenImageZoom = async (): Promise<void> => {
      const candidates = [
        "#landingImage",
        "#imgTagWrapperId img",
        "#main-image-container img",
        "#imageBlock img",
      ];
      for (const sel of candidates) {
        const loc = page.locator(sel);
        if (!(await loc.count())) continue;
        await loc.first().scrollIntoViewIfNeeded().catch(() => {});
        await loc.first().click({ timeout: 1000 }).catch(() => {});
        await page.waitForSelector("#ivLargeImage img", { timeout: 2000 }).catch(() => {});
        break;
      }
    };

    const collectOverlayImages = async (): Promise<string[]> => {
      const images: string[] = [];
      const getLargeSrc = async (): Promise<string | null> => {
        for (let attempt = 0; attempt < 3; attempt++) {
          const src = await page.locator("#ivLargeImage img").first().getAttribute("src").catch(() => null);
          const normalized = src ? normalizeAmazonImageUrl(src) : null;
          if (normalized && !/loading|loadIndicators|\.gif($|\?)/i.test(normalized)) return normalized;
          await page.waitForTimeout(200);
        }
        return null;
      };

      const first = await getLargeSrc();
      if (first) images.push(first);

      const thumbs = page.locator("#ivThumbs img, #ivThumbs .ivThumb, #ivThumbs li");
      const count = await thumbs.count().catch(() => 0);
      for (let i = 0; i < count; i++) {
        const thumb = thumbs.nth(i);
        await thumb.scrollIntoViewIfNeeded().catch(() => {});
        await thumb.click({ timeout: 1000 }).catch(() => {});
        await page.waitForTimeout(200);
        const src = await getLargeSrc();
        if (src && !images.includes(src)) images.push(src);
      }
      return images;
    };

    const maybeExpandItemDetails = async (): Promise<void> => {
      const section = page.locator("#item_details, #important-information");
      if (await section.count()) {
        await section.first().scrollIntoViewIfNeeded().catch(() => {});
        await page.waitForTimeout(200);
      }
      const expander = page.locator("a.a-expander-header:has-text('Item details'), a[data-action='a-expander-toggle']:has-text('Item details')");
      if (await expander.count()) {
        const expanded = await expander.first().getAttribute("aria-expanded").catch(() => null);
        if (expanded === "false") {
          await expander.first().click({ timeout: 1000 }).catch(() => {});
        }
      }
      await page.waitForSelector("#item_details table", { timeout: 2000 }).catch(() => {});
      await page.waitForTimeout(300);
    };

    await maybeExpandItemDetails();
    await maybeCloseImageZoom();
    await maybeOpenImageZoom();
    overlayImages = await collectOverlayImages();

    const maybeExpandNutrition = async (): Promise<void> => {
      const section = page.locator("#nutritionalInfoAndIngredients_feature_div");
      if (await section.count()) {
        await section.first().scrollIntoViewIfNeeded().catch(() => {});
        await page.waitForTimeout(400);
      }
      const expander = page.locator("a[data-a-expander-name='nic-nutrition-facts-expander']");
      if (await expander.count()) {
        await expander.first().click({ timeout: 2000 }).catch(() => {});
      }
      await page.waitForTimeout(800);
      await page
        .waitForFunction(
          () =>
            /Cholesterol|Vitamin D|Calcium|Iron|Potassium/.test(
              document.body?.innerText || ""
            ),
          { timeout: 2000 }
        )
        .catch(() => {});
    };

    await maybeExpandNutrition();
    try {
      const sectionText = await page
        .locator("#nutritionalInfoAndIngredients_feature_div")
        .innerText({ timeout: 2000 });
      nutritionTextFromDom = sectionText;
      if (DEBUG_AMAZON) {
        try {
          fs.writeFileSync("/tmp/amazon-nutrition-section.txt", sectionText);
        } catch {
          // ignore
        }
      }
    } catch {
      // ignore
    }

    const allergensFromDom = await page
      .locator("#item_details tr", { hasText: "Allergen" })
      .locator("td")
      .first()
      .innerText()
      .catch(() => null);

    const html = await page.content();
    if (DEBUG_AMAZON) {
      try {
        fs.writeFileSync("/tmp/amazon-pdp.html", html);
        if (debugResponses.length > 0) {
          fs.writeFileSync(
            "/tmp/amazon-json-responses.txt",
            debugResponses.map((r) => `${r.url} (${r.size ?? 0})`).join("\n")
          );
        }
      } catch {
        // ignore
      }
    }

    const $ = cheerio.load(html);

    // JSON-LD
    let jsonLdResult: Partial<ScrapedProduct> = {};
    $('script[type="application/ld+json"]').each((_, el) => {
      const txt = $(el).contents().text();
      if (!txt) return;
      const res = extractFromJsonLd(txt);
      if (res.name || res.upc12 || res.imageUrl || res.brand) jsonLdResult = { ...jsonLdResult, ...res };
    });

    const name =
      jsonLdResult.name ??
      decodeHtmlEntities($("#productTitle").first().text()) ??
      decodeHtmlEntities($("h1").first().text()) ??
      decodeHtmlEntities($('meta[property="og:title"]').attr("content") || null) ??
      null;

    const brand = AMAZON_BRAND_OVERRIDE || brandOverride || jsonLdResult.brand || extractBrandFromName(name) || "Unknown";

    const cleanedName = normalizeProductName(name, AMAZON_BRAND_OVERRIDE || brandOverride || null);

    const overlayPrimaryImage = overlayImages.length > 0 ? overlayImages[0] : null;
    const imageUrl = overlayPrimaryImage ?? normalizeAmazonImageUrl(jsonLdResult.imageUrl ?? extractMainImageUrl($) ?? null);

    let upc12 = jsonLdResult.upc12 ?? null;
    let upcs: string[] = [];
    if (!upc12) {
      const upc =
        extractFromDetailBullets($, /^upc/i) ||
        extractFromTables($, /^upc/i) ||
        extractFromTables($, /^gtin\-?12/i);
      if (upc) {
        const list = extractUpcsFromText(upc);
        if (list.length > 0) {
          upcs = list;
          upc12 = list[0] ?? null;
        }
      }
    }
    const upcFromItemDetails = await page
      .locator("#item_details tr", { hasText: "UPC" })
      .locator("td")
      .first()
      .innerText()
      .catch(() => null);
    if (upcFromItemDetails) {
      const list = extractUpcsFromText(upcFromItemDetails);
      if (list.length > 0) {
        upcs = Array.from(new Set([...(upcs || []), ...list]));
        upc12 = upc12 || upcs[0] || null;
      }
    }
    if (upc12 && (!upcs || upcs.length === 0)) {
      upcs = [upc12];
    }

    const ingredients =
      normalizeWhitespace($("#nic-ingredients-content span").first().text()) ||
      extractFromDetailBullets($, /^ingredients?/i) ||
      extractFromTables($, /^ingredients?/i) ||
      extractFromImportantInfo($, /^ingredients?/i) ||
      null;
    const allergens =
      (allergensFromDom ? normalizeWhitespace(allergensFromDom) : null) ||
      extractAllergens($) ||
      extractFromDetailBullets($, /^allergen information/i) ||
      extractFromTables($, /^allergen information/i) ||
      extractFromImportantInfo($, /^allergen information/i) ||
      null;

    const carouselImages = overlayImages.length > 0 ? overlayImages : extractCarouselImageUrls($);
    const nutritionScanImages = [...carouselImages].sort(
      (a, b) => imageQualityScore(b) - imageQualityScore(a)
    );
    let nutritionImageUrl: string | null = null;
    let nutritionData: ScraperNutritionData | null = null;
    if (nutritionScanImages.length > 0) {
      nutritionImageUrl = await detectNutritionImageFromCarousel(nutritionScanImages);
      if (nutritionImageUrl) {
        nutritionData = await fetchNutritionFromImage(nutritionImageUrl);
      }
    }

    const nutritionFromNic = extractNutritionFromNic($);
    const nutritionFromEmbedded = extractNutritionFromEmbeddedJson($);
    const nutritionFromTable = extractNutritionFromTable($);
    const nutritionSectionText = normalizeWhitespace(
      $("#nutritionFacts_feature_div, #nutritional-information, #important-information").text()
    );
    const nutrition =
      nutritionFromNic ||
      nutritionFromApi ||
      nutritionFromEmbedded ||
      nutritionFromTable ||
      extractNutritionFromText(nutritionTextFromDom || nutritionSectionText || $("body").text());

    const nutritionFromDomData = transformNutritionToDb(nutrition);
    const mergedNutritionData = mergeNutritionData(nutritionData, nutritionFromDomData);

    const now = new Date().toISOString();

    return {brand,
      source: SOURCE,
      productUrl,
      name: cleanedName ?? name,
      ingredients: ingredients ? normalizeWhitespace(ingredients) : null,
      allergens: allergens ? normalizeWhitespace(allergens) : null,
      upc12,
      upcs: upcs && upcs.length > 0 ? upcs : undefined,
      nutrition,
      nutritionData: mergedNutritionData,
      nutritionImageUrl,
      imageUrl,
      scrapedAt: now,
      sourceCreatedAt: null,
      sourceLastUpdatedAt: null};
  } finally {
    await page.close().catch(() => null);
  }
}

function transformToOutput(p: ScrapedProduct): ScraperProductOutput {
  const now = new Date().toISOString();
  const serving =
    p.nutritionData?.serving_size_value != null
      ? { value: p.nutritionData.serving_size_value, unit: p.nutritionData.serving_size_unit_text || "serving" }
      : p.nutrition?.servingSize
        ? parseServingSize(p.nutrition.servingSize)
        : { value: null, unit: null };
  const servingText = p.nutritionData?.serving_size_text || p.nutrition?.servingSize || undefined;

  return {product_name: p.name || "",
    brand: p.brand,
    upc: p.upc12 || undefined,
    upcs: p.upcs && p.upcs.length > 0 ? p.upcs : undefined,
    ingredients_text: p.ingredients || "",
    allergen_statement: p.allergens || undefined,
    serving_size_value: serving.value ?? undefined,
    serving_size_unit: serving.unit ?? undefined,
    serving_size_text: servingText,
    source: SOURCE,
    source_id: p.productUrl,
    source_created_at: p.sourceCreatedAt || now,
    source_last_updated_at: p.sourceLastUpdatedAt || now,
    image_url: p.imageUrl || undefined,
    nutrition: p.nutritionData || transformNutritionToDb(p.nutrition) || undefined};
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

async function submitProduct(p: ScrapedProduct): Promise<boolean> {
  if (!p.name || !hasIngredientsOrNutrition(p)) {
    console.log(`Skipping ${p.productUrl}: missing name, ingredients, and nutrition`);
    return false;
  }
  if (!p.upc12) {
    console.log(`Skipping ${p.productUrl}: missing UPC`);
    return false;
  }
  try {
    const token = await getServiceToken();
    const body = transformToOutput(p);
    const { scraper_job_id: _, ...req } = body as any;
    if (DEBUG_AMAZON) {
      console.log("[DEBUG] submit body:");
      console.log(JSON.stringify(req, null, 2));
    }

    const res = await fetch(`${API_BASE_URL}/submit-product-for-review`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Service-Token": token },
      body: JSON.stringify(req),
    });

    if (res.ok) {
      console.log(`✅ Submitted "${p.name}"`);
      return true;
    }
    console.error(`❌ Submit failed: HTTP ${res.status}`);
    return false;
  } catch (e) {
    console.error(`❌ Submit failed:`, e);
    return false;
  }
}

async function uploadToS3(results: ScraperProductOutput[], runDateTime: string): Promise<void> {
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
        ...(error ? { ":error": error } : {}),
      },
    })
  );
}

function parseArgs() {
  const argv = process.argv.slice(2);
  let url: string | undefined;
  let searchUrl: string | undefined;
  let configPath: string | undefined;
  let limit: number | undefined;
  let local = false;
  let debug = false;
  let noHeadless = false;
  let headless = false;
  let offset = 0;
  let concurrency = 10;

  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--url" || argv[i] === "-u") {
      url = argv[i + 1];
      i++;
    } else if (argv[i] === "--search" || argv[i] === "-s") {
      searchUrl = argv[i + 1];
      i++;
    } else if (argv[i] === "--config" || argv[i] === "-c") {
      configPath = argv[i + 1];
      i++;
    } else if (argv[i] === "--limit" || argv[i] === "-l") {
      limit = parseInt(argv[i + 1], 10);
      if (isNaN(limit) || limit < 1) limit = undefined;
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
    } else if (argv[i] === "--no-headless") {
      noHeadless = true;
    } else if (argv[i] === "--headless") {
      headless = true;
    }
  }

  return { url, configPath, limit, local, searchUrl, debug, noHeadless, headless, offset, concurrency };
}

function normalizeAmazonUrl(href: string): string | null {
  if (!href) return null;
  if (href.startsWith("http")) return href;
  if (href.startsWith("//")) return `https:${href}`;
  if (href.startsWith("/")) return `https://www.amazon.com${href}`;
  return null;
}

function extractAsinFromUrl(url: string): string | null {
  const m = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/i);
  return m?.[1] ?? null;
}

async function scrapeAislePage(context: BrowserContext, aisleUrl: string, limit?: number): Promise<string[]> {
  const page = await context.newPage();
  try {
    await page.goto(aisleUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(1500);

    const seen = new Set<string>();
    let idleRounds = 0;
    let scrollCount = 0;
    let prevScrollHeight = 0;

    while (idleRounds < 3) {
      scrollCount++;
      const links = await page.$$eval("a[href*='/dp/'], a[href*='/gp/product/']", (els) =>
        els
          .map((el) => (el as HTMLAnchorElement).getAttribute("href") || "")
          .filter(Boolean)
      );

      let added = 0;
      for (const href of links) {
        const full = normalizeAmazonUrl(href);
        if (!full) continue;
        const asin = extractAsinFromUrl(full);
        if (!asin) continue;
        const canonical = `https://www.amazon.com/dp/${asin}`;
        if (!seen.has(canonical)) {
          seen.add(canonical);
          added++;
        }
        if (limit && seen.size >= limit) break;
      }

      console.log(`[DISCOVER] scroll ${scrollCount}: ${seen.size} urls collected`);

      if (limit && seen.size >= limit) break;

      const prevCount = seen.size;

      const showMore = page.getByRole("button", { name: /show more/i });
      if (await showMore.count()) {
        await showMore.first().click({ timeout: 2000 }).catch(() => {});
        await page.waitForTimeout(1500);
      } else {
        const currentHeight = await page.evaluate(() => document.body.scrollHeight).catch(() => 0);
        if (currentHeight > prevScrollHeight) {
          prevScrollHeight = currentHeight;
          await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
          await page.waitForTimeout(1200);
        } else {
          // Slow scroll to trigger lazy-load
          const steps = 4;
          for (let i = 0; i < steps; i++) {
            await page.evaluate(() => window.scrollBy(0, window.innerHeight * 0.95));
            await page.waitForTimeout(400);
          }
        }
      }

      // Re-scan after scroll to see if new items arrived
      const postLinks = await page.$$eval("a[href*='/dp/'], a[href*='/gp/product/']", (els) =>
        els
          .map((el) => (el as HTMLAnchorElement).getAttribute("href") || "")
          .filter(Boolean)
      );
      for (const href of postLinks) {
        const full = normalizeAmazonUrl(href);
        if (!full) continue;
        const asin = extractAsinFromUrl(full);
        if (!asin) continue;
        const canonical = `https://www.amazon.com/dp/${asin}`;
        if (!seen.has(canonical)) {
          seen.add(canonical);
        }
        if (limit && seen.size >= limit) break;
      }

      console.log(`[DISCOVER] scroll ${scrollCount} after load: ${seen.size} urls collected`);

      if (limit && seen.size >= limit) break;

      if (seen.size === prevCount && added === 0) {
        idleRounds++;
      } else {
        idleRounds = 0;
      }
    }

    return Array.from(seen);
  } finally {
    await page.close().catch(() => null);
  }
}

async function main(): Promise<void> {
  const { url, configPath, limit, local, searchUrl, debug, noHeadless, headless, offset, concurrency } = parseArgs();

  DEBUG_AMAZON = debug || DEBUG_AMAZON;
  if (noHeadless) AMAZON_HEADLESS = false;
  if (headless) AMAZON_HEADLESS = true;

  let productTargets: string[] = [];
  let searchTargets: string[] = [];
  let configBrand: string | undefined;

  if (url) {
    productTargets = [url];
  } else if (searchUrl) {
    searchTargets = [searchUrl];
  } else if (configPath) {
    const cfg = JSON.parse(fs.readFileSync(configPath, "utf8")) as AppConfig;
    if (Array.isArray(cfg.urls) && cfg.urls.length > 0) {
      productTargets = cfg.urls.filter((u) => typeof u === "string" && u.startsWith("https://www.amazon.com/"));
    }
    if (Array.isArray(cfg.searchUrls) && cfg.searchUrls.length > 0) {
      searchTargets = cfg.searchUrls.filter((u) => typeof u === "string" && u.startsWith("https://www.amazon.com/"));
    }
    if (typeof cfg.brand === "string" && cfg.brand.trim()) configBrand = cfg.brand.trim();
  } else {
    console.error("Usage: npx tsx scrape.ts --url <AMAZON_PRODUCT_URL>");
    console.error("   or: npx tsx scrape.ts --config ./config.json [--limit N] [--local]");
    process.exit(1);
  }

  if (offset > 0 && productTargets.length > 0) productTargets = productTargets.slice(offset);
  if (limit != null && productTargets.length > 0) productTargets = productTargets.slice(0, limit);

  if (productTargets.length === 0 && searchTargets.length === 0) {
    console.error("No valid Amazon product URLs to scrape.");
    process.exit(1);
  }

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
  }

  let browser: Browser;
  let context: BrowserContext;
  if (AMAZON_USER_DATA_DIR) {
    context = await chromium.launchPersistentContext(AMAZON_USER_DATA_DIR, {
      headless: AMAZON_HEADLESS,
      slowMo: AMAZON_SLOWMO,
      userAgent:
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      locale: "en-US",
      viewport: { width: 1280, height: 720 },
    });
    browser = context.browser() as Browser;
  } else {
    browser = await chromium.launch({ headless: AMAZON_HEADLESS, slowMo: AMAZON_SLOWMO });
    context = await browser.newContext({
      userAgent:
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      locale: "en-US",
      viewport: { width: 1280, height: 720 },
    });
  }

  const limiter = pLimit(concurrency);
  const outputs: ScraperProductOutput[] = [];
  let skipped = 0;

  try {
    let productUrls = productTargets;
    if (searchTargets.length > 0) {
      const discovered: string[] = [];
      for (const target of searchTargets) {
        const desiredCount = limit ? limit + offset : undefined;
        const remaining = desiredCount != null ? Math.max(desiredCount - discovered.length, 0) : undefined;
        const found = await scrapeAislePage(context, target, remaining && remaining > 0 ? remaining : undefined);
        console.log(`[DISCOVER] Collected ${found.length} product URLs from aisle page`);
        discovered.push(...found);
        if (limit && discovered.length >= limit + offset) break;
      }
      productUrls = discovered.slice(offset, limit ? offset + limit : undefined);
      if (productUrls.length === 0) {
        if (discovered.length > 0 && offset > 0) {
          console.error(`No valid Amazon product URLs found after applying offset ${offset}.`);
        } else {
          console.error("No valid Amazon product URLs found on aisle page.");
        }
        return;
      }
    }

    const completedBuffer: Array<{ product: ScrapedProduct; url: string }> = [];
    let flushChain = Promise.resolve();

    const processBatch = async (batch: Array<{ product: ScrapedProduct; url: string }>): Promise<void> => {
      for (const { product: p, url: u } of batch) {
        if (!hasIngredientsOrNutrition(p)) {
          skipped++;
          console.log(`[SKIP] ${p.name ?? "(no name)"}: no ingredients and no nutrition`);
          logScrapedData(p, u);
          continue;
        }
        if (!p.upc12) {
          skipped++;
          console.log(`[SKIP] ${p.name ?? "(no name)"}: missing UPC`);
          logScrapedData(p, u);
          continue;
        }

        outputs.push(transformToOutput(p));
        console.log(`[OK] ${p.name ?? "(no name)"} | ${u}`);

        await submitProduct(p);
      }
    };

    const enqueueFlush = (force = false): Promise<void> => {
      flushChain = flushChain.then(async () => {
        if (completedBuffer.length < 10 && !force) return;
        const batch = completedBuffer.splice(0, force ? completedBuffer.length : 10);
        if (batch.length > 0) {
          await processBatch(batch);
        }
      });
      return flushChain;
    };

    const tasks = productUrls.map((u) =>
      limiter(async () => {
        const p = await scrapeProductDetail(context, u, configBrand);
        if (!p) {
          skipped++;
          return;
        }
        completedBuffer.push({ product: p, url: u });
        await enqueueFlush(false);
      })
    );

    await Promise.all(tasks);
    await enqueueFlush(true);
  } catch (e) {
    console.error(e);
    if (!local) await updateJobStatus(jobId, "failed", String(e));
  } finally {
    await context.close().catch(() => null);
    await browser.close().catch(() => null);
  }

  const totalCount =
    searchTargets.length > 0
      ? limit
        ? Math.min(limit, outputs.length + skipped)
        : outputs.length + skipped
      : productTargets.length;
  console.log(
    `\nScraped ${outputs.length} valid products (skipped ${skipped} without ingredients/nutrition) out of ${totalCount} total`
  );

  if (!local && outputs.length > 0) {
    await uploadToS3(outputs, runDateTime);
    await updateJobStatus(jobId, "completed");
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
