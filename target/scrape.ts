import * as fs from "fs";
import { chromium, type BrowserContext, type Browser, type Page } from "playwright";
import * as cheerio from "cheerio";
import pLimit from "p-limit";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import { parseNutrientAmountWithQualifier } from "../nutrition-utils";

type BrandConfig = {
  brand: string;
  listingUrl: string;
};

type AppConfig = {
  urls?: string[];
  brands?: BrandConfig[];
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
  upc12: string | null;
  nutrition: Nutrition | null;
  imageUrl: string | null;
  scrapedAt: string;
  sourceCreatedAt: string | null;
  sourceLastUpdatedAt: string | null;
};

const SOURCE = "target.com";
const SCRAPER_NAME = process.env.JOB_NAME || "target";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://it7rdy3qbh.execute-api.us-west-2.amazonaws.com";
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
const REDSKY_API_KEY = process.env.TARGET_REDSKY_API_KEY || "9f36aeafbe60771e321a7cc95a78140772ab3e96";
const TARGET_STORE_ID = process.env.TARGET_STORE_ID || "305";

const s3Client = new S3Client({});
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);
const ssmClient = new SSMClient({});

let serviceTokenCache: string | null = null;

function normalizeWhitespace(s: string): string {
  return s.replace(/\s+/g, " ").trim();
}

function parseServingSize(servingSizeText: string | null): { value: number | null; unit: string | null } {
  if (!servingSizeText || typeof servingSizeText !== "string") {
    return { value: null, unit: null };
  }
  const cleaned = servingSizeText.trim().replace(/\([^)]*\)/g, "").trim();

  let match = cleaned.match(
    /^(\d+)\/(\d+)\s+(Tbsp|tbsp|TSP|tsp|cup|cups|oz|fl\s*oz|floz|ml|g|kg|lb|lbs|serving|servings|crackers?)\b/i
  );
  if (match) {
    const value = parseFloat(match[1]) / parseFloat(match[2]);
    let unit = match[3].trim();
    if (unit.toLowerCase() === "crackers") unit = "cracker";
    return { value, unit };
  }

  match = cleaned.match(
    /^(\d+(?:\.\d+)?)\s+(Tbsp|tbsp|TSP|tsp|cup|cups|oz|fl\s*oz|floz|ml|g|kg|lb|lbs|serving|servings|crackers?)\b/i
  );
  if (!match) match = cleaned.match(/^(\d+(?:\.\d+)?)(g|oz|ml)\b/i);
  if (match) {
    const value = parseFloat(match[1]);
    let unit = (match[2] || "g").trim();
    if (unit.toLowerCase() === "crackers") unit = "cracker";
    return { value, unit };
  }

  const numMatch = cleaned.match(/^(\d+(?:\.\d+)?)/);
  if (numMatch) return { value: parseFloat(numMatch[1]), unit: "serving" };

  return { value: null, unit: null };
}


const NUTRIENT_COLUMN_MAP: Record<string, string> = {
  "total fat": "total_fat_g",
  saturated: "saturated_fat_g",
  trans: "trans_fat_g",
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
};

function mapNutrientToColumn(name: string): string | null {
  const lower = name.toLowerCase().trim();
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
    if (col) {
      const parsed = parseNutrientAmountWithQualifier(n.amount);
      if (parsed !== null) {
        (result as unknown as Record<string, number>)[col] = parsed.value;
        if (parsed.qualifier) (result as unknown as Record<string, string>)[`${col}_qualifier`] = parsed.qualifier;
      }
    }
  }

  return result;
}

/** ---------- helpers for text slicing ---------- */
function sliceBetween(raw: string, startRe: RegExp, endRes: RegExp[]): string | null {
  const s = raw.search(startRe);
  if (s === -1) return null;

  const afterStart = raw.slice(s);
  let end = -1;
  for (const er of endRes) {
    const idx = afterStart.search(er);
    if (idx !== -1) end = end === -1 ? idx : Math.min(end, idx);
  }
  const block = end === -1 ? afterStart : afterStart.slice(0, end);
  return block.trim();
}

/** ---------- json walkers ---------- */
function findInObject(obj: unknown, keys: string[]): string | string[] | null {
  if (!obj || typeof obj !== "object") return null;
  const o = obj as Record<string, unknown>;

  for (const key of keys) {
    if (o[key] != null) {
      const v = o[key];
      if (typeof v === "string") return v;
      if (Array.isArray(v)) return v as string[];
    }
  }

  for (const v of Object.values(o)) {
    if (v && typeof v === "object") {
      const found = findInObject(v, keys);
      if (found) return found;
    }
  }
  return null;
}

function findNutritionInNextData(obj: unknown): Nutrition | null {
  if (!obj || typeof obj !== "object") return null;
  const o = obj as Record<string, unknown>;

  const servingSize = o.servingSize ?? o.serving_size ?? o.servingSizeText;
  const calories = o.calories ?? o.cal;
  const items = o.items ?? o.nutrients ?? o.nutritionFacts;

  if (
    (servingSize || calories || items) &&
    (typeof servingSize === "string" || typeof calories === "string" || Array.isArray(items))
  ) {
    const nutrients: NutritionNutrient[] = [];
    if (Array.isArray(items)) {
      for (const it of items) {
        const fields = (it && typeof it === "object" && (it as Record<string, unknown>).fields) ?? it;
        const f = typeof fields === "object" && fields ? (fields as Record<string, unknown>) : null;
        if (f?.label || f?.name) {
          nutrients.push({
            name: String(f?.label ?? f?.name ?? ""),
            amount: f?.amount != null ? String(f.amount) : null,
            dailyValuePercent: f?.dailyValue != null ? String(f.dailyValue) : null,
          });
        }
      }
    }
    return {
      servingSize: typeof servingSize === "string" ? servingSize : null,
      servingsPerContainer: typeof o.servingsPerContainer === "string" ? (o.servingsPerContainer as string) : null,
      calories: calories != null ? String(calories) : null,
      nutrients,
    };
  }

  for (const v of Object.values(o)) {
    if (v && typeof v === "object") {
      const found = findNutritionInNextData(v);
      if (found) return found;
    }
  }
  return null;
}

function findPreloadedQueries(obj: unknown): Record<string, unknown> | null {
  if (!obj || typeof obj !== "object") return null;
  const o = obj as Record<string, unknown>;
  if (o.__PRELOADED_QUERIES__ && typeof o.__PRELOADED_QUERIES__ === "object") {
    return o.__PRELOADED_QUERIES__ as Record<string, unknown>;
  }
  for (const v of Object.values(o)) {
    const found = findPreloadedQueries(v);
    if (found) return found;
  }
  return null;
}

const DEBUG_PDP = process.env.DEBUG_TARGET_PDP === "1";

type ExtractResult = {
  name?: string;
  ingredients?: string;
  nutrition?: Nutrition;
  imageUrl?: string;
  upc12?: string;
};

/**
 * Extract product data from Target's __PRELOADED_QUERIES__ / dehydratedState.
 */
function extractFromPreloadedQueries(html: string): ExtractResult {
  const result: ExtractResult = {};

  try {
    const $ = cheerio.load(html);
    const nextDataScript = $('script#__NEXT_DATA__').html();
    if (!nextDataScript) return result;

    const nextData = JSON.parse(nextDataScript) as Record<string, unknown>;
    const preloaded = findPreloadedQueries(nextData);
    const dehydrated =
      (nextData?.props as Record<string, unknown> | undefined)?.pageProps as Record<string, unknown> | undefined;
    const dehydratedQueries = (dehydrated?.dehydratedState as Record<string, unknown> | undefined)
      ?.queries as unknown;

    const queries: unknown[] = [];
    if (preloaded?.queries && Array.isArray(preloaded.queries)) queries.push(...preloaded.queries);
    if (Array.isArray(dehydratedQueries)) queries.push(...dehydratedQueries);

    for (const entry of queries) {
      let queryKey: unknown;
      let data: unknown;

      if (Array.isArray(entry) && entry.length >= 2) {
        queryKey = entry[0];
        const queryResponse = entry[1] as { data?: unknown } | undefined;
        data = queryResponse?.data;
      } else if (entry && typeof entry === "object") {
        const e = entry as Record<string, unknown>;
        queryKey = e.queryKey;
        const state = e.state as Record<string, unknown> | undefined;
        data = state?.data ?? e.data;
      }

      const key0 = Array.isArray(queryKey) ? String(queryKey[0]) : "";
      if (!key0 || !/pdp/i.test(key0)) continue;

      const fromRedsky = extractFromRedskyPdp(data);
      if (fromRedsky.name) result.name = result.name ?? fromRedsky.name;
      if (fromRedsky.imageUrl) result.imageUrl = result.imageUrl ?? fromRedsky.imageUrl;
      if (fromRedsky.upc12) result.upc12 = result.upc12 ?? fromRedsky.upc12;
      if (fromRedsky.ingredients) result.ingredients = result.ingredients ?? fromRedsky.ingredients;
      if (fromRedsky.nutrition) result.nutrition = result.nutrition ?? fromRedsky.nutrition;

      if (!result.ingredients) {
        const ing = findInObject(data, ["ingredients", "ingredient_information"]);
        if (typeof ing === "string") result.ingredients = normalizeWhitespace(ing);
      }
      if (!result.nutrition) {
        const nd = findNutritionInNextData(data);
        if (nd) result.nutrition = nd;
      }

      if (result.name && result.ingredients && result.nutrition) break;
    }
  } catch {
    // ignore
  }

  return result;
}

function extractFromRedskyPdp(data: unknown): {
  name?: string;
  ingredients?: string;
  nutrition?: Nutrition;
  imageUrl?: string;
  upc12?: string;
} {
  const result: {
    name?: string;
    ingredients?: string;
    nutrition?: Nutrition;
    imageUrl?: string;
    upc12?: string;
  } = {};

  if (!data || typeof data !== "object") return result;
  const root = data as Record<string, unknown>;
  const product =
    (root.product as Record<string, unknown>) ??
    ((root.data as Record<string, unknown> | undefined)?.product as Record<string, unknown>) ??
    ((root.data as Record<string, unknown> | undefined)?.data as Record<string, unknown> | undefined)
      ?.product ??
    findRedskyProductNode(root) ??
    null;
  const item = (product?.item as Record<string, unknown>) ?? null;
  const enrichment = (item?.enrichment as Record<string, unknown>) ?? null;
  if (!item || !enrichment) return result;

  const fillFromItem = (it: Record<string, unknown> | null | undefined): void => {
    if (!it) return;
    const productDesc = (it.product_description as Record<string, unknown>) ?? null;
    if (!result.name && productDesc?.title) result.name = productDesc.title as string;

    const enrich = (it.enrichment as Record<string, unknown>) ?? null;
    const imageInfo = (enrich?.image_info as Record<string, unknown>) ?? null;
    const primaryImage = (imageInfo?.primary_image as Record<string, string>) ?? null;
    if (!result.imageUrl && primaryImage?.url) {
      result.imageUrl = primaryImage.url.startsWith("//") ? `https:${primaryImage.url}` : primaryImage.url;
    }

    const nutritionFacts = (enrich?.nutrition_facts as Record<string, unknown>) ?? null;
    if (!result.ingredients && nutritionFacts?.ingredients && typeof nutritionFacts.ingredients === "string") {
      result.ingredients = normalizeWhitespace(nutritionFacts.ingredients);
    }

    if (!result.nutrition) {
      const lists =
        (nutritionFacts?.value_prepared_list as any[]) ??
        (nutritionFacts?.value_list as any[]) ??
        (nutritionFacts?.value_unprepared_list as any[]) ??
        (nutritionFacts?.nutrition_fact_list as any[]) ??
        null;

      if (Array.isArray(lists) && lists.length > 0) {
        const v: any = lists[0];
        const servingSize =
          v.serving_size && v.serving_size_unit_of_measurement
            ? `${v.serving_size} ${v.serving_size_unit_of_measurement}`
            : v.serving_size || v.servingSize || null;
        const servingsPer = v.servings_per_container || v.servingsPerContainer || null;

        let calories: string | null = null;
        const nutrients: NutritionNutrient[] = [];

        for (const n of v.nutrients || v.items || []) {
          const nm = (n?.name ?? n?.label ?? "").toString();
          const lower = nm.toLowerCase();

          const qty = n?.quantity ?? n?.amount ?? n?.value ?? null;
          const unit = n?.unit_of_measurement ?? n?.unit ?? "";
          const pct = n?.percentage ?? n?.dailyValue ?? n?.daily_value ?? null;

          if (lower === "calories") {
            if (qty != null) calories = String(qty);
            continue;
          }

          if (nm) {
            nutrients.push({
              name: nm,
              amount: qty != null ? `${qty}${unit || ""}` : null,
              dailyValuePercent: pct != null ? `${pct}%` : null,
            });
          }
        }

        result.nutrition = { servingSize, servingsPerContainer: servingsPer, calories, nutrients };
      }
    }

    const primaryBarcode = it.primary_barcode as string | undefined;
    if (!result.upc12 && primaryBarcode) {
      const m = primaryBarcode.match(/(\d{12})/);
      result.upc12 = m?.[1] ?? primaryBarcode;
    }
  };

  fillFromItem(item);

  const children = (product?.children as Array<Record<string, unknown>>) ?? [];
  for (const child of children) {
    const childItem = (child.item as Record<string, unknown>) ?? null;
    fillFromItem(childItem);
    if (result.ingredients && result.nutrition) break;
  }

  return result;
}

function findRedskyProductNode(obj: unknown): Record<string, unknown> | null {
  if (!obj || typeof obj !== "object") return null;
  const o = obj as Record<string, unknown>;

  if (o.item && typeof o.item === "object") {
    const item = o.item as Record<string, unknown>;
    const enrichment = item.enrichment as Record<string, unknown> | undefined;
    if (enrichment?.nutrition_facts || enrichment?.image_info || item.product_description) {
      return o;
    }
  }

  if (o.product && typeof o.product === "object") return o.product as Record<string, unknown>;

  for (const v of Object.values(o)) {
    if (v && typeof v === "object") {
      const found = findRedskyProductNode(v);
      if (found) return found;
    }
  }
  return null;
}

function findRedskyUrl(html: string): string | null {
  const match = html.match(/https:\/\/redsky\.target\.com\/redsky_aggregations\/v1\/web\/pdp\?[^"'<> ]+/i);
  if (!match) return null;
  return match[0].replace(/&amp;/g, "&");
}

function extractFromNextData(html: string): Partial<ScrapedProduct> {
  const result: Partial<ScrapedProduct> = {};
  try {
    const $ = cheerio.load(html);
    const script = $('script#__NEXT_DATA__').html();
    if (!script) return result;

    const data = JSON.parse(script) as Record<string, unknown>;
    const findString = (...keys: string[]) => {
      const v = findInObject(data, keys);
      return Array.isArray(v) ? v[0] : (v as string | null);
    };

    result.name = findString("title", "productTitle", "item", "name") || null;
    result.ingredients = findString("ingredients", "ingredient_information") || null;
    result.imageUrl = findString("image", "primary_image", "default_image") || null;

    if (result.imageUrl && !result.imageUrl.startsWith("http")) {
      result.imageUrl = result.imageUrl.startsWith("//")
        ? `https:${result.imageUrl}`
        : `https://target.scene7.com${result.imageUrl.startsWith("/") ? "" : "/"}${result.imageUrl}`;
    }

    const gtin = findString("gtin", "gtin12", "gtin13", "upc");
    if (gtin) {
      const m = gtin.match(/(\d{12})/);
      result.upc12 = m?.[1] ?? gtin;
    }
  } catch {
    // ignore
  }
  return result;
}

/**
 * Ingredients: pull from the Label info region if possible
 */
function parseIngredientsFromDom($: cheerio.CheerioAPI): string | null {
  const raw = $("body").text().replace(/\u00a0/g, " ");

  const labelBlock =
    sliceBetween(raw, /\bLabel info\b/i, [/\bSpecifications\b/i, /\bDescription\b/i, /\bAbout this item\b/i, /\bShipping\b/i]) ?? raw;

  const ingBlock = sliceBetween(
    labelBlock,
    /\bIngredients\b\s*:?\s*/i,
    [/\bAllergens?\b/i, /\bContains\b/i, /\bUPC\b/i, /\bNet wt\b/i, /\bServing Size\b/i, /\bSpecifications\b/i]
  );

  if (!ingBlock) return null;

  const cleaned = normalizeWhitespace(ingBlock.replace(/^\bIngredients\b\s*:?\s*/i, ""));
  if (cleaned.length < 20) return null;
  if (/whisk|griddle|waffle iron|do not overmix|pour about|bake until/i.test(cleaned)) return null;

  return cleaned;
}

/**
 * Nutrition: parse from Label info section using newline-preserving text
 */
function parseNutritionFromLabelInfoDom($: cheerio.CheerioAPI): Nutrition | null {
  const raw = $("body").text().replace(/\u00a0/g, " ");

  const labelBlock = sliceBetween(raw, /\bLabel info\b/i, [/\bIngredients\b/i, /\bSpecifications\b/i, /\bDescription\b/i, /\bAbout this item\b/i]);
  if (!labelBlock) return null;

  const servingSize =
    labelBlock.match(/\bServing Size\b\s*:?\s*([^\n\r]+)\b/i)?.[1]?.trim() ?? null;

  const servingsPerContainer =
    labelBlock.match(/\bServing(?:s)? Per Container\b\s*:?\s*([^\n\r]+)\b/i)?.[1]?.trim() ??
    labelBlock.match(/\bServings Per (?:Container|Package)\b\s*:?\s*([^\n\r]+)\b/i)?.[1]?.trim() ??
    null;

  const calories = labelBlock.match(/\bCalories\b\s*:?\s*(\d+)\b/i)?.[1] ?? null;

  const lines = labelBlock
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter((l) => l && !/% Daily Value/i.test(l));

  const nutrients: NutritionNutrient[] = [];
  const nutrientNames = [
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

  for (const line of lines) {
    for (const name of nutrientNames) {
      // e.g. "Sodium 380mg 17%" or "Sodium 380 mg 17%"
      const re = new RegExp(
        `^${name.replace(/\s+/g, "\\s+")}\\s+([0-9][0-9.,]*\\s*[a-zA-ZµmcgMG]+)?\\s*(\\d+%)?\\s*$`,
        "i"
      );
      const m = line.match(re);
      if (m) {
        nutrients.push({
          name,
          amount: m[1] ? normalizeWhitespace(m[1]) : null,
          dailyValuePercent: m[2] ? m[2].trim() : null,
        });
        break;
      }
    }
  }

  if (!servingSize && !calories && nutrients.length === 0) return null;
  return { servingSize, servingsPerContainer, calories, nutrients };
}

/**
 * UPC: specifically scrape from the Specifications section (your request)
 * Handles both "UPC: 191907944884" and "UPC 191907944884"
 */
function parseUpcFromSpecifications($: cheerio.CheerioAPI): string | null {
  const raw = $("body").text().replace(/\u00a0/g, " ");

  const specsBlock = sliceBetween(
    raw,
    /\bSpecifications\b/i,
    [/\bRelated\b/i, /\bShipping\b/i, /\bReturns\b/i, /\bDescription\b/i, /\bAbout this item\b/i]
  );

  const searchSpace = specsBlock ?? raw;
  const m = searchSpace.match(/\bUPC\b\s*:?\s*(\d{12})\b/i);
  return m?.[1] ?? null;
}

async function ensurePdpExpanded(page: Page): Promise<void> {
  await page.waitForTimeout(600);

  await page
    .getByRole("button", { name: /accept|agree|close|got it|continue/i })
    .first()
    .click({ timeout: 1500 })
    .catch(() => {});

  await page
    .getByRole("button", { name: /load all content at once/i })
    .first()
    .click({ timeout: 4000 })
    .catch(() => {});

  // Expand both Label info + Specifications
  await page.getByRole("button", { name: /label info/i }).first().click({ timeout: 2500 }).catch(() => {});
  await page.getByRole("button", { name: /specifications/i }).first().click({ timeout: 2500 }).catch(() => {});

  // Scroll to trigger lazy content
  for (let i = 0; i < 4; i++) {
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(300);
  }

  await page.waitForSelector("script#__NEXT_DATA__", { timeout: 5000 }).catch(() => {});
}

function extractBrandFromName(name: string | null): string | null {
  if (!name) return null;
  const parts = name.split(/\s+/);
  if (parts.length >= 2 && /^[A-Z][a-zA-Z0-9]+$/.test(parts[0])) return parts[0];
  return null;
}

function addImageParams(url: string | null): string | null {
  if (!url) return null;
  try {
    const u = new URL(url);
    u.searchParams.set("wid", "1200");
    u.searchParams.set("hei", "1200");
    u.searchParams.set("qlt", "80");
    return u.toString();
  } catch {
    return url;
  }
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

function stripBrandSuffix(name: string | null): string | null {
  if (!name) return null;
  const cleaned = name.trim();
  const re =
    /\s*[-–—]?\s*(Good\s*&\s*Gather|Favorite\s*Day)(?:™|TM)?\s*$/i;
  const next = cleaned.replace(re, "").trim();
  return next || cleaned;
}

function parseTcinFromUrl(url: string): string | null {
  const m = url.match(/\/A-(\d{8,})/);
  return m?.[1] ?? null;
}

function buildRedskyPdpClientUrl(tcin: string): string {
  const u = new URL("https://redsky.target.com/redsky_aggregations/v1/web/pdp_client_v1");
  u.searchParams.set("key", REDSKY_API_KEY);
  u.searchParams.set("tcin", tcin);
  u.searchParams.set("is_bot", "false");
  u.searchParams.set("store_id", TARGET_STORE_ID);
  u.searchParams.set("pricing_store_id", TARGET_STORE_ID);
  u.searchParams.set("has_pricing_store_id", "true");
  u.searchParams.set("include_obsolete", "true");
  u.searchParams.set("skip_personalized", "true");
  u.searchParams.set("skip_variation_hierarchy", "true");
  u.searchParams.set("channel", "WEB");
  u.searchParams.set("page", `/p/A-${tcin}`);
  return u.toString();
}

function stripWeightBrandSuffix(name: string | null): string | null {
  if (!name) return null;
  const clean = name.trim();

  // Only strip when the dash segment contains size/weight/ct signals
  // Example:
  // "Cage-Free ... (CA SEFS Compliant) - 36oz/18ct - Good & Gather™" -> keep left side
  const dashPattern =
    /\s[-–—]\s[^-–—]*(\b\d+(?:\.\d+)?\s?(?:oz|fl\s?oz|floz|lb|lbs|g|kg|ml|l|ct|count|pack|pk|size)\b|\b\d+\s?ct\b|\/\d+\s?ct)\b[^-–—]*/i;

  const match = clean.match(dashPattern);
  if (!match || match.index == null) return clean;

  const head = clean.slice(0, match.index).trim();
  return head || clean;
}

function normalizeProductName(name: string | null): string | null {
  if (!name) return null;
  const decoded = decodeHtmlEntities(name);
  const stripped = stripWeightBrandSuffix(decoded);
  return stripBrandSuffix(stripped);
}

function hasIngredientsOrNutrition(p: ScrapedProduct): boolean {
  const hasIngredients = !!p.ingredients?.trim();
  const hasNutrition =
    !!p.nutrition &&
    (!!p.nutrition.calories ||
      (p.nutrition.nutrients && p.nutrition.nutrients.length > 0) ||
      !!p.nutrition.servingSize);
  return hasIngredients || hasNutrition;
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
  console.log(`  [DEBUG] nutrition: ${hasNut ? "yes" : "no"}`);
  console.log(`  [DEBUG] upc12: ${p.upc12 ?? "(null)"}`);
  console.log(`  [DEBUG] url: ${url}`);
}

async function scrapeProductDetail(
  context: BrowserContext,
  productUrl: string,
  brandOverride?: string
): Promise<ScrapedProduct> {
  const page = await context.newPage();
  try {
    const tcin = parseTcinFromUrl(productUrl);
    let apiResult: ExtractResult | null = null;
    if (tcin) {
      const apiUrl = buildRedskyPdpClientUrl(tcin);
      if (DEBUG_PDP) console.log(`[DEBUG] api-first: fetching ${apiUrl}`);
      try {
        const resp = await page.request.get(apiUrl, { headers: { accept: "application/json" } });
        if (resp.ok()) {
          const data = await resp.json();
          apiResult = extractFromRedskyPdp(data);
          if (DEBUG_PDP) {
            console.log(
              `[DEBUG] api-first: extracted name=${apiResult?.name ? "yes" : "no"} ` +
                `ingredients=${apiResult?.ingredients ? "yes" : "no"} ` +
                `nutrition=${apiResult?.nutrition ? "yes" : "no"}`
            );
          }
        } else if (DEBUG_PDP) {
          console.log(`[DEBUG] redsky api status ${resp.status()} for ${apiUrl}`);
        }
      } catch {
        // ignore
      }
    }

    if (apiResult?.name && (apiResult.ingredients || apiResult.nutrition)) {
      if (DEBUG_PDP) console.log("[DEBUG] api-first: using redsky result, skipping DOM");
      const now = new Date().toISOString();
      return {
        brand: brandOverride || extractBrandFromName(apiResult.name) || "Unknown",
        source: SOURCE,
        productUrl,
        name: normalizeProductName(apiResult.name),
        ingredients: apiResult.ingredients ?? null,
        upc12: apiResult.upc12 ?? null,
        nutrition: apiResult.nutrition ?? null,
        imageUrl: addImageParams(apiResult.imageUrl ?? null),
        scrapedAt: now,
        sourceCreatedAt: null,
        sourceLastUpdatedAt: null,
      };
    }

    if (DEBUG_PDP) console.log("[DEBUG] api-first: falling back to DOM/response capture");

    const redskyPayloads: Array<{ url: string; data: unknown }> = [];
    let redskyDumped = false;

    page.on("response", async (resp) => {
      const url = resp.url();
      if (!/redsky\.target\.com\/redsky_aggregations\/v1\/web\/(pdp|product|pdp_)/i.test(url)) return;
      try {
        if (!resp.ok()) {
          if (DEBUG_PDP) console.log(`[DEBUG] redsky status ${resp.status()} for ${url}`);
          return;
        }
        const data = await resp.json();
        redskyPayloads.push({ url, data });
      } catch {
        // ignore
      }
    });

    await page.goto(productUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(800);
    await page.waitForLoadState("networkidle", { timeout: 5000 }).catch(() => {});

    await ensurePdpExpanded(page);

    // Grab name directly from rendered DOM first (helps avoid "digital-web")
    let nameText: string | null = null;
    nameText = await page.locator("h1").first().textContent().catch(() => null);
    nameText = nameText ? normalizeWhitespace(nameText) : null;

    const html = await page.content();
    if (DEBUG_PDP) {
      try {
        fs.writeFileSync("/tmp/target-pdp.html", html);
        console.log("[DEBUG] wrote /tmp/target-pdp.html");
      } catch {
        // ignore
      }
    }
    const $ = cheerio.load(html);

    const fromPreloaded = extractFromPreloadedQueries(html);
    const fromNext = extractFromNextData(html);
    let fromRedsky: ExtractResult = apiResult ?? {};

    const orderedPayloads = [...redskyPayloads].sort((a, b) => {
      const aScore =
        /pdp_client_v1/i.test(a.url) ? 3 : /pdp_server_v1/i.test(a.url) ? 2 : /pdp_/i.test(a.url) ? 1 : 0;
      const bScore =
        /pdp_client_v1/i.test(b.url) ? 3 : /pdp_server_v1/i.test(b.url) ? 2 : /pdp_/i.test(b.url) ? 1 : 0;
      return bScore - aScore;
    });

    for (const payload of orderedPayloads) {
      const candidate = extractFromRedskyPdp(payload.data);
      if (candidate.name && !fromRedsky.name) fromRedsky.name = candidate.name;
      if (candidate.imageUrl && !fromRedsky.imageUrl) fromRedsky.imageUrl = candidate.imageUrl;
      if (candidate.upc12 && !fromRedsky.upc12) fromRedsky.upc12 = candidate.upc12;
      if (candidate.ingredients && !fromRedsky.ingredients) fromRedsky.ingredients = candidate.ingredients;
      if (candidate.nutrition && !fromRedsky.nutrition) fromRedsky.nutrition = candidate.nutrition;
      if (fromRedsky.ingredients && fromRedsky.nutrition && fromRedsky.name) break;
    }

    if (!fromRedsky.ingredients || !fromRedsky.nutrition) {
      const redskyUrl = findRedskyUrl(html);
      if (redskyUrl) {
        if (DEBUG_PDP) console.log(`[DEBUG] redsky url from html: ${redskyUrl}`);
        try {
          const resp = await page.request.get(redskyUrl, {
            headers: { accept: "application/json" },
          });
          if (resp.ok()) {
            const data = await resp.json();
            const candidate = extractFromRedskyPdp(data);
            fromRedsky = { ...fromRedsky, ...candidate };
          } else if (DEBUG_PDP) {
            console.log(`[DEBUG] redsky fetch status ${resp.status()} for ${redskyUrl}`);
          }
        } catch {
          // ignore
        }
      } else if (DEBUG_PDP) {
        console.log("[DEBUG] redsky url not found in html");
      }
    }
    if (DEBUG_PDP && !redskyDumped) {
      redskyDumped = true;
      try {
        fs.writeFileSync("/tmp/target-redsky-all.json", JSON.stringify(redskyPayloads, null, 2));
        console.log("[DEBUG] wrote /tmp/target-redsky-all.json");
      } catch {
        // ignore
      }
    }

    let name =
      fromPreloaded.name ??
      fromRedsky.name ??
      nameText ??
      fromNext.name ??
      normalizeWhitespace($("h1").first().text()) ??
      $('meta[property="og:title"]').attr("content") ??
      null;
    name = normalizeProductName(name);

    const ingredients =
      fromPreloaded.ingredients ?? fromRedsky.ingredients ?? fromNext.ingredients ?? parseIngredientsFromDom($);

    const imageUrl =
      fromPreloaded.imageUrl ??
      fromRedsky.imageUrl ??
      fromNext.imageUrl ??
      $('meta[property="og:image"]').attr("content") ??
      null;
    const imageUrlWithParams = addImageParams(imageUrl);

    // ✅ UPC: prefer preloaded; otherwise scrape from SPECIFICATIONS section
    let upc12 = fromPreloaded.upc12 ?? fromRedsky.upc12 ?? fromNext.upc12 ?? null;
    if (!upc12) upc12 = parseUpcFromSpecifications($);

    let nutrition: Nutrition | null = fromPreloaded.nutrition ?? fromRedsky.nutrition ?? null;
    if (!nutrition) nutrition = parseNutritionFromLabelInfoDom($);
    if (!nutrition) {
      try {
        const nextData = JSON.parse($('script#__NEXT_DATA__').html() || "{}");
        const nd = findNutritionInNextData(nextData);
        if (nd) nutrition = nd;
      } catch {
        // ignore
      }
    }

    const now = new Date().toISOString();

    return {
      brand: brandOverride || extractBrandFromName(name) || "Unknown",
      source: SOURCE,
      productUrl,
      name,
      ingredients: ingredients ?? null,
      upc12,
      nutrition,
      imageUrl: imageUrlWithParams,
      scrapedAt: now,
      sourceCreatedAt: null,
      sourceLastUpdatedAt: null,
    };
  } finally {
    await page.close().catch(() => null);
  }
}

async function expandListingPage(page: Page): Promise<void> {
  const maxScrolls = 30;
  let lastHeight = 0;
  let stableCount = 0;

  for (let i = 0; i < maxScrolls; i++) {
    const productCount = await page.locator('a[href*="/p/"][href*="/-/A-"]').count().catch(() => 0);

    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);

    const loadMore = page.getByRole("button", { name: /load more|show more|view more/i }).first();
    if (await loadMore.isVisible({ timeout: 1000 }).catch(() => false)) {
      await loadMore.click({ timeout: 3000 }).catch(() => null);
      await page.waitForTimeout(400);
    }

    const newHeight = await page.evaluate(() => document.body.scrollHeight);
    const newCount = await page.locator('a[href*="/p/"][href*="/-/A-"]').count().catch(() => 0);

    if (newHeight === lastHeight && newCount === productCount) {
      stableCount++;
      if (stableCount >= 2) break;
    } else {
      stableCount = 0;
    }
    lastHeight = newHeight;
  }
}

function buildListingUrlWithNao(listingUrl: string, nao: number): string {
  const u = new URL(listingUrl);
  u.searchParams.set("Nao", String(nao));
  u.searchParams.set("moveTo", "product-list-grid");
  return u.toString();
}

async function paginateListingPages(
  page: Page,
  listingUrl: string,
  limit?: number
): Promise<string[]> {
  const out = new Set<string>();
  const pageSize = 24;
  const maxPages = 120;

  for (let pageIndex = 0; pageIndex < maxPages; pageIndex++) {
    if (limit != null && out.size >= limit) break;
    const nao = pageIndex * pageSize;
    const pageUrl = buildListingUrlWithNao(listingUrl, nao);
    if (DEBUG_PDP) console.log(`[DEBUG] listing page: ${pageUrl}`);

    await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(1200);
    await page.waitForSelector('a[href*="/p/"][href*="/-/A-"]', { timeout: 8000 }).catch(() => {});

    const html = await page.content();
    const urls = extractProductDetailUrls(listingUrl, html);
    const before = out.size;
    for (const u of urls) out.add(u);
    if (DEBUG_PDP) console.log(`[DEBUG] pageIndex ${pageIndex} found ${urls.length}, total ${out.size}`);

    if (urls.length === 0 || out.size === before) break;
  }

  const all = Array.from(out);
  return limit != null ? all.slice(0, limit) : all;
}

function extractProductDetailUrls(listingUrl: string, html: string): string[] {
  const $ = cheerio.load(html);
  const base = new URL(listingUrl);
  const out = new Set<string>();

  $('a[href*="/p/"][href*="/-/A-"]').each((_, el) => {
    const href = $(el).attr("href");
    if (!href) return;

    try {
      const abs = new URL(href, base).toString();
      const u = new URL(abs);

      if (u.hostname !== "www.target.com" && u.hostname !== "target.com") return;

      const match = u.pathname.match(/\/p\/([^/]+)\/-\/A-(\d{8,})/);
      if (!match) return;

      u.search = "";
      u.hash = "";
      out.add(u.toString());
    } catch {
      // skip invalid URLs
    }
  });

  return Array.from(out);
}

async function scrapeListing(
  browserContext: BrowserContext,
  brandCfg: BrandConfig,
  limit?: number
): Promise<string[]> {
  const page = await browserContext.newPage();
  try {
    await page.goto(brandCfg.listingUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(1200);
    return await paginateListingPages(page, brandCfg.listingUrl, limit);
  } finally {
    await page.close().catch(() => null);
  }
}

function transformToOutput(p: ScrapedProduct): ScraperProductOutput {
  const now = new Date().toISOString();
  const serving = p.nutrition?.servingSize
    ? parseServingSize(p.nutrition.servingSize)
    : { value: null, unit: null };

  return {
    product_name: p.name || "",
    brand: p.brand,
    upc: p.upc12 || undefined,
    ingredients_text: p.ingredients || "",
    serving_size_value: serving.value ?? undefined,
    serving_size_unit: serving.unit ?? undefined,
    serving_size_text: p.nutrition?.servingSize ?? undefined,
    source: SOURCE,
    source_id: p.productUrl,
    source_created_at: p.sourceCreatedAt || now,
    source_last_updated_at: p.sourceLastUpdatedAt || now,
    image_url: p.imageUrl || undefined,
    nutrition: transformNutritionToDb(p.nutrition) || undefined,
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

async function submitProduct(p: ScrapedProduct): Promise<boolean> {
  if (!p.name || !hasIngredientsOrNutrition(p)) {
    console.log(`Skipping ${p.productUrl}: missing name, ingredients, and nutrition (likely raw produce)`);
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

function parseArgs(): { url?: string; configPath?: string; limit?: number; local: boolean } {
  const argv = process.argv.slice(2);
  let url: string | undefined;
  let configPath: string | undefined;
  let limit: number | undefined;
  let local = false;

  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--url" || argv[i] === "-u") {
      url = argv[i + 1];
      i++;
    } else if (argv[i] === "--config" || argv[i] === "-c") {
      configPath = argv[i + 1];
      i++;
    } else if (argv[i] === "--limit" || argv[i] === "-l") {
      limit = parseInt(argv[i + 1], 10);
      if (isNaN(limit) || limit < 1) limit = undefined;
      i++;
    } else if (argv[i] === "--local") {
      local = true;
    }
  }

  return { url, configPath, limit, local };
}

async function main(): Promise<void> {
  const { url, configPath, limit, local } = parseArgs();

  type UrlWithBrand = { url: string; brand?: string };
  let targets: UrlWithBrand[] = [];

  if (url) {
    targets = [{ url }];
  } else if (configPath) {
    const cfg = JSON.parse(fs.readFileSync(configPath, "utf8")) as AppConfig;

    if (Array.isArray(cfg.urls) && cfg.urls.length > 0) {
      targets = cfg.urls
        .filter((u: string) => typeof u === "string" && u.startsWith("https://www.target.com/"))
        .map((u: string) => ({ url: u }));
    }
  } else {
    console.error("Usage: npx tsx scrape.ts --url <TARGET_PRODUCT_URL>");
    console.error("   or: npx tsx scrape.ts --config ./config.json [--limit N] [--local]");
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

  const browser: Browser = await chromium.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-software-rasterizer",
    ],
  });

  const context: BrowserContext = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    locale: "en-US",
    viewport: { width: 1280, height: 720 },
  });

  try {
    if (configPath) {
      const cfg = JSON.parse(fs.readFileSync(configPath, "utf8")) as AppConfig;
      if (Array.isArray(cfg.brands) && cfg.brands.length > 0) {
        const discovered: UrlWithBrand[] = [];
        for (const brandCfg of cfg.brands) {
          if (!brandCfg.listingUrl?.startsWith("https://www.target.com/")) continue;
          console.log(`\n[DISCOVER] ${brandCfg.brand}: ${brandCfg.listingUrl}`);
          const urls = await scrapeListing(context, brandCfg, limit);
          console.log(`[DISCOVER] ${brandCfg.brand}: ${urls.length} product URLs`);

          const brand =
            brandCfg.listingUrl?.toLowerCase().includes("good-gather") ? "Good & Gather" : brandCfg.brand;

          for (const u of urls) discovered.push({ url: u, brand });
        }
        targets = discovered.length > 0 ? discovered : targets;
      }
    }
  } catch (e) {
    console.error("Discovery failed:", e);
    if (!local) await updateJobStatus(jobId, "failed", String(e));
    await context.close().catch(() => null);
    await browser.close().catch(() => null);
    process.exit(1);
  }

  if (limit != null) targets = targets.slice(0, limit);

  if (targets.length === 0) {
    console.error("No valid Target product URLs to scrape.");
    await context.close().catch(() => null);
    await browser.close().catch(() => null);
    process.exit(1);
  }

  const limiter = pLimit(5);
  const outputs: ScraperProductOutput[] = [];
  let skipped = 0;

  try {
    const results = await Promise.all(
      targets.map(({ url: u, brand }) =>
        limiter(async () => {
          try {
            const p = await scrapeProductDetail(context, u, brand);
            return { product: p, url: u, timedOut: false };
          } catch (e: any) {
            if (e?.name === "TimeoutError") {
              return { product: null as any, url: u, timedOut: true };
            }
            throw e;
          }
        })
      )
    );

    for (const { product: p, url: u, timedOut } of results) {
      if (timedOut) {
        skipped++;
        console.log(`[SKIP] ${u}: timed out after 60s (likely non-product page)`);
        continue;
      }

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
  } catch (e) {
    console.error(e);
    if (!local) await updateJobStatus(jobId, "failed", String(e));
  } finally {
    await context.close().catch(() => null);
    await browser.close().catch(() => null);
  }

  console.log(
    `\nScraped ${outputs.length} valid products (skipped ${skipped} without ingredients/nutrition) out of ${targets.length} total`
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
