import * as fs from "fs";
import { chromium, type BrowserContext, type Browser, type Page } from "playwright";
import * as cheerio from "cheerio";
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

type BrandConfig = {
  brand: string;
  listingUrl: string;
};

type AppConfig = {
  urls?: string[];
  brands?: BrandConfig[];
  categoryUrls?: string[];
  skipListingUrls?: string[];
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
  allergenStatement: string | null;
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
const API_BASE_URL = process.env.API_BASE_URL || "https://api.mytummi.app";
const PRODUCTS_API_URL = `${API_BASE_URL}/products`;
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
const REDSKY_API_KEY = process.env.TARGET_REDSKY_API_KEY || "9f36aeafbe60771e321a7cc95a78140772ab3e96";
const TARGET_STORE_ID = process.env.TARGET_STORE_ID || "305";
const TARGET_CONTEXT_OPTIONS = {
  userAgent:
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  locale: "en-US",
  viewport: { width: 1280, height: 720 },
} as const;

const s3Client = new S3Client({});
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);
const ssmClient = new SSMClient({});

let serviceTokenCache: string | null = null;
let existingUpcSetPromise: Promise<Set<string>> | null = null;
let apiKeysCache: Record<string, any> | null = null;

function normalizeWhitespace(s: string): string {
  return s.replace(/\s+/g, " ").trim();
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
  "folate dfe": "folate_mcg",
  folate: "folate_mcg",
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
  if (/\bpotas\.?\b/.test(lower)) return "potassium_mg";
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
    if (DEBUG_PDP) {
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
    return {servingSize: typeof servingSize === "string" ? servingSize : null,
      servingsPerContainer: typeof o.servingsPerContainer === "string" ? (o.servingsPerContainer as string) : null,
      calories: calories != null ? String(calories) : null,
      nutrients};
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

let DEBUG_PDP = false;

type ExtractResult = {
  name?: string;
  ingredients?: string;
  allergenStatement?: string;
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
      if (!result.allergenStatement) {
        const allergens = findInObject(data, [
          "allergens",
          "allergen_information",
          "allergens_and_warnings",
          "allergenStatement",
        ]);
        if (typeof allergens === "string") result.allergenStatement = normalizeWhitespace(allergens);
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
  allergenStatement?: string;
  nutrition?: Nutrition;
  imageUrl?: string;
  upc12?: string;
} {
  const result: {
    name?: string;
    ingredients?: string;
    allergenStatement?: string;
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

  if (!result.allergenStatement) {
    const allergens = findInObject(root, [
      "allergens",
      "allergen_information",
      "allergens_and_warnings",
      "allergenStatement",
    ]);
    if (typeof allergens === "string") result.allergenStatement = normalizeWhitespace(allergens);
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
    result.allergenStatement =
      findString("allergens", "allergen_information", "allergens_and_warnings", "allergenStatement") || null;
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

function parseAllergenStatementFromDom($: cheerio.CheerioAPI): string | null {
  const direct = normalizeWhitespace(
    $("[data-test='productDetailTabs-nutritionFactsTab']")
      .find("h4")
      .filter((_, el) => /allergens?\s*&\s*warnings?/i.test(normalizeWhitespace($(el).text())))
      .first()
      .parent()
      .text()
      .replace(/Allergens?\s*&\s*Warnings?\s*:/i, "")
  );
  if (direct) return direct;

  const raw = $("body").text().replace(/\u00a0/g, " ");
  const block = sliceBetween(
    raw,
    /\bAllergens?\s*&\s*Warnings?\b\s*:?\s*/i,
    [/\bSpecifications\b/i, /\bDescription\b/i, /\bAbout this item\b/i, /\bShipping\b/i]
  );
  if (!block) return null;
  return normalizeWhitespace(block);
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
  return {servingSize, servingsPerContainer, calories, nutrients};
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

function parseBrandFromDom($: cheerio.CheerioAPI): string | null {
  const raw =
    $('a[data-test="shopAllBrandLink"] span').first().text().trim() ||
    $('a[data-test="shopAllBrandLink"]').first().text().trim() ||
    "";
  if (!raw) return null;
  const cleaned = raw.replace(/^shop\s+all\s+/i, "").replace(/\s+/g, " ").trim();
  return cleaned || null;
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

function normalizeProductName(name: string | null): string | null {
  return cleanProductName(name, {
    decodeHtml: true,
    stripTrailingDashSize: true,
    stripTrailingCount: true,
    stripTrailingWeight: true,
    stripBrandSuffixes: [
      /Good\s*&\s*Gather(?:™|TM|®)?/i,
      /Favorite\s*Day(?:™|TM|®)?/i,
      /Market\s*Pantry(?:™|TM|®)?/i,
    ],
  });
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
  const hasAllergens = !!p.allergenStatement?.trim();
  const hasNut =
    !!p.nutrition &&
    (!!p.nutrition.calories ||
      (p.nutrition.nutrients && p.nutrition.nutrients.length > 0) ||
      !!p.nutrition.servingSize);

  console.log(`  [DEBUG] name: ${p.name ?? "(null)"}`);
  console.log(`  [DEBUG] ingredients: ${hasIng ? `yes (${p.ingredients!.length} chars)` : "no"}`);
  console.log(`  [DEBUG] allergens: ${hasAllergens ? `yes (${p.allergenStatement!.length} chars)` : "no"}`);
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
      let allergenFromDom: string | null = null;
      let brandFromDom: string | null = null;
      if (!apiResult.allergenStatement || !brandOverride) {
        try {
          await page.goto(productUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
          await page.waitForTimeout(600);
          await page.waitForLoadState("networkidle", { timeout: 5000 }).catch(() => {});
          const html = await page.content();
          const $ = cheerio.load(html);
          allergenFromDom = parseAllergenStatementFromDom($);
          brandFromDom = parseBrandFromDom($);
          if (DEBUG_PDP) {
            console.log(`[DEBUG] api-first: allergens from dom: ${allergenFromDom ? "yes" : "no"}`);
            console.log(`[DEBUG] api-first: brand from dom: ${brandFromDom ?? "(null)"}`);
          }
        } catch {
          // ignore
        }
      }
      const now = new Date().toISOString();
      return {brand: brandOverride || brandFromDom || "Unknown",
        source: SOURCE,
        productUrl,
        name: normalizeProductName(apiResult.name),
        ingredients: apiResult.ingredients ?? null,
        allergenStatement: apiResult.allergenStatement ?? allergenFromDom ?? null,
        upc12: apiResult.upc12 ?? null,
        nutrition: apiResult.nutrition ?? null,
        imageUrl: addImageParams(apiResult.imageUrl ?? null),
        scrapedAt: now,
        sourceCreatedAt: null,
        sourceLastUpdatedAt: null};
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

    const gotoWithRetry = async (url: string): Promise<void> => {
      try {
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60_000 });
      } catch (e: any) {
        const msg = String(e?.message || e);
        if (/ERR_TIMED_OUT|Timeout|Navigation timeout/i.test(msg)) {
          await page.waitForTimeout(600);
          await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60_000 });
          return;
        }
        throw e;
      }
    };
    await gotoWithRetry(productUrl);
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
    const brandFromDom = parseBrandFromDom($);

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
    const allergenStatement =
      fromPreloaded.allergenStatement ??
      fromRedsky.allergenStatement ??
      fromNext.allergenStatement ??
      parseAllergenStatementFromDom($);

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

    return {brand: brandOverride || brandFromDom || "Unknown",
      source: SOURCE,
      productUrl,
      name,
      ingredients: ingredients ?? null,
      allergenStatement: allergenStatement ?? null,
      upc12,
      nutrition,
      imageUrl: imageUrlWithParams,
      scrapedAt: now,
      sourceCreatedAt: null,
      sourceLastUpdatedAt: null};
  } finally {
    await page.close().catch(() => null);
  }
}

async function expandListingPage(page: Page, listingUrl: string): Promise<string[]> {
  const collected = new Set<string>();
  const collectFromCurrentDom = async () => {
    const urls = await extractProductDetailUrlsFromPage(page, listingUrl);
    for (const url of urls) collected.add(url);
  };
  await page.evaluate(() => window.scrollTo(0, 0)).catch(() => null);
  await page.waitForTimeout(350);
  await collectFromCurrentDom();

  // Slow incremental scroll is needed for Target lazy rendering/virtualization.
  const maxPasses = 1;
  let stablePasses = 0;
  for (let pass = 0; pass < maxPasses; pass++) {
    const beforePass = collected.size;
    const metrics = await page.evaluate(() => ({
      viewport: window.innerHeight || 900,
      height: document.body.scrollHeight,
    }));
    const step = Math.max(480, Math.floor(metrics.viewport * 0.9));
    const maxSteps = 40;
    let y = 0;

    for (let s = 0; s < maxSteps; s++) {
      await page.evaluate((top) => window.scrollTo(0, top), y).catch(() => null);
      await page.waitForTimeout(180);
      await collectFromCurrentDom();
      if (y >= metrics.height - metrics.viewport - 8) break;
      y += step;
    }

    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight)).catch(() => null);
    await page.waitForTimeout(500);

    const loadMore = page.getByRole("button", { name: /load more|show more|view more/i }).first();
    if (await loadMore.isVisible({ timeout: 1200 }).catch(() => false)) {
      await loadMore.click({ timeout: 3000 }).catch(() => null);
      await page.waitForTimeout(550);
    }
    await collectFromCurrentDom();

    if (collected.size === beforePass) {
      stablePasses++;
      if (stablePasses >= 2) break;
    } else {
      stablePasses = 0;
    }
  }

  return Array.from(collected);
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
  let nao = 0;
  let pageSize = 24;
  const maxPages = 400;
  let stagnantPages = 0;
  let expectedTotal: number | null = null;
  const MAX_STAGNANT_PAGES = 2;

  for (let pageIndex = 0; pageIndex < maxPages; pageIndex++) {
    if (limit != null && out.size >= limit) break;
    const pageUrl = buildListingUrlWithNao(listingUrl, nao);
    console.log(`[DISCOVER] Listing Page: ${pageUrl}`);

    await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
    await page.waitForTimeout(1200);
    await page.waitForSelector('a[href*="/p/"][href*="/-/A-"]', { timeout: 7000 }).catch(() => {});
    const expandedUrls = await expandListingPage(page, listingUrl);

    const collectUrlsWithRetry = async (): Promise<{ pageText: string; urls: string[] }> => {
      let domUrls = await extractProductDetailUrlsFromPage(page, listingUrl);
      let urls = Array.from(new Set<string>([...expandedUrls, ...domUrls]));
      let pageText = normalizeWhitespace(await page.locator("body").innerText().catch(() => ""));
      if (urls.length > 0) return { pageText, urls };

      // Pages occasionally render late; scroll + short retry before treating as empty.
      for (let i = 0; i < 2; i++) {
        await page.mouse.wheel(0, 2500).catch(() => null);
        await page.waitForTimeout(450);
      }
      const expandedRetryUrls = await expandListingPage(page, listingUrl);
      await page.waitForLoadState("networkidle", { timeout: 8000 }).catch(() => {});
      await page.waitForTimeout(700);
      domUrls = await extractProductDetailUrlsFromPage(page, listingUrl);
      urls = Array.from(new Set<string>([...expandedRetryUrls, ...domUrls]));
      pageText = normalizeWhitespace(await page.locator("body").innerText().catch(() => ""));
      if (urls.length > 0) return { pageText, urls };

      // Some pages intermittently render empty; re-open the same Nao page once.
      await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
      await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
      await page.waitForTimeout(1200);
      const expandedReloadUrls = await expandListingPage(page, listingUrl);
      domUrls = await extractProductDetailUrlsFromPage(page, listingUrl);
      urls = Array.from(new Set<string>([...expandedReloadUrls, ...domUrls]));
      pageText = normalizeWhitespace(await page.locator("body").innerText().catch(() => ""));
      return { pageText, urls };
    };

    const { pageText, urls } = await collectUrlsWithRetry();
    const before = out.size;
    for (const u of urls) out.add(u);
    console.log(`[DISCOVER] Page ${pageIndex + 1}: found ${urls.length} product URLs (running total: ${out.size})`);

    if (expectedTotal == null) {
      const resultsMatch = pageText.match(/\b(\d{1,3}(?:,\d{3})*)\s+results\b/i);
      if (resultsMatch) {
        const total = parseInt(resultsMatch[1].replace(/,/g, ""), 10);
        if (!Number.isNaN(total) && total > 0) expectedTotal = total;
      }
    }
    const rangeMatch = pageText.match(/\b(\d{1,3}(?:,\d{3})*)\s*-\s*(\d{1,3}(?:,\d{3})*)\s+of\s+(\d{1,3}(?:,\d{3})*)\b/i);
    if (rangeMatch) {
      const start = parseInt(rangeMatch[1].replace(/,/g, ""), 10);
      const end = parseInt(rangeMatch[2].replace(/,/g, ""), 10);
      const total = parseInt(rangeMatch[3].replace(/,/g, ""), 10);
      const candidateSize = !Number.isNaN(start) && !Number.isNaN(end) ? end - start + 1 : NaN;
      // Only trust page size when the range start lines up with requested Nao.
      if (!Number.isNaN(candidateSize) && candidateSize > 0 && start === nao + 1) {
        pageSize = Math.min(96, Math.max(1, candidateSize));
      }
      if (!Number.isNaN(total) && total > 0) expectedTotal = total;
      if (!Number.isNaN(total) && end >= total) break;
    }

    if (urls.length === 0 || out.size === before) {
      stagnantPages++;
      if (stagnantPages >= MAX_STAGNANT_PAGES) break;
    } else {
      stagnantPages = 0;
    }

    if (expectedTotal != null && out.size >= expectedTotal) break;

    // Conservative advance prevents skipping product windows when range parsing is noisy.
    nao += Math.min(pageSize, 24);
  }

  const all = Array.from(out);
  if (DEBUG_PDP && expectedTotal != null && all.length < expectedTotal) {
    console.log(`[DEBUG] listing shortfall: extracted=${all.length}, expected≈${expectedTotal}`);
  }
  return limit != null ? all.slice(0, limit) : all;
}

function normalizeTargetPdpUrl(listingUrl: string, rawHref: string): string | null {
  try {
    const base = new URL(listingUrl);
    const abs = new URL(rawHref, base).toString();
    const u = new URL(abs);
    if (u.hostname !== "www.target.com" && u.hostname !== "target.com") return null;
    const match = u.pathname.match(/\/p\/(?:[^/]+\/)?-\/A-(\d{8,})/);
    if (!match) return null;
    u.search = "";
    u.hash = "";
    return u.toString();
  } catch {
    return null;
  }
}

async function extractProductDetailUrlsFromPage(page: Page, listingUrl: string): Promise<string[]> {
  const hrefs = await page
    .locator("a[href*='/p/'][href*='/-/A-']")
    .evaluateAll((els) =>
      Array.from(
        new Set(
          els
            .map((el) => (el as HTMLAnchorElement).getAttribute("href") || "")
            .filter((href) => !!href)
        )
      )
    )
    .catch(() => [] as string[]);

  const out = new Set<string>();
  for (const href of hrefs) {
    const normalized = normalizeTargetPdpUrl(listingUrl, href);
    if (normalized) out.add(normalized);
  }
  return Array.from(out);
}

async function scrapeListing(
  browser: Browser,
  brandCfg: BrandConfig,
  limit?: number
): Promise<string[]> {
  const listingContext = await browser.newContext(TARGET_CONTEXT_OPTIONS);
  const page = await listingContext.newPage();
  try {
    await page.goto(brandCfg.listingUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(1200);
    return await paginateListingPages(page, brandCfg.listingUrl, limit);
  } finally {
    await page.close().catch(() => null);
    await listingContext.close().catch(() => null);
  }
}

function normalizeTargetCategoryUrl(urlOrPath: string): string | null {
  if (!urlOrPath) return null;
  try {
    const u = new URL(urlOrPath, "https://www.target.com");
    if (u.hostname !== "www.target.com" && u.hostname !== "target.com") return null;
    return u.toString();
  } catch {
    return null;
  }
}

function normalizeTargetListingUrl(urlOrPath: string): string | null {
  if (!urlOrPath) return null;
  try {
    const u = new URL(urlOrPath, "https://www.target.com");
    if (u.hostname !== "www.target.com" && u.hostname !== "target.com") return null;
    if (!u.pathname.startsWith("/c/")) return null;
    u.search = "";
    u.hash = "";
    return u.toString();
  } catch {
    return null;
  }
}

async function discoverSubcategoryListingUrls(
  browserContext: BrowserContext,
  categoryUrl: string
): Promise<string[]> {
  const page = await browserContext.newPage();
  try {
    await page.goto(categoryUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
    await page.waitForTimeout(1200);

    // Expand all bubcat items when a "Show all N" button exists.
    for (let i = 0; i < 3; i++) {
      const showAllBtn = page
        .locator("button[data-test='loadMoreRecommendations'][aria-label*='Show all']")
        .first();
      const visible = await showAllBtn.isVisible({ timeout: 1000 }).catch(() => false);
      if (!visible) break;
      await showAllBtn.click({ timeout: 3000 }).catch(() => null);
      await page.waitForTimeout(800);
    }

    const hrefs = await page
      .locator("[data-test='@web/slingshot-components/bubcat'] a[href^='/c/']")
      .evaluateAll((els) =>
        Array.from(new Set(els.map((el) => (el as HTMLAnchorElement).getAttribute("href") || "").filter(Boolean)))
      )
      .catch(() => [] as string[]);

    const listingUrls = new Set<string>();
    for (const href of hrefs) {
      const normalized = normalizeTargetCategoryUrl(href);
      if (normalized) listingUrls.add(normalized);
    }

    return Array.from(listingUrls);
  } finally {
    await page.close().catch(() => null);
  }
}

function transformToOutput(p: ScrapedProduct): ScraperProductOutput {
  const now = new Date().toISOString();
  const serving = p.nutrition?.servingSize
    ? parseServingSize(p.nutrition.servingSize)
    : { value: null, unit: null };

  return {product_name: p.name || "",
    brand: p.brand,
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

async function getServiceToken(): Promise<string> {
  if (serviceTokenCache) return serviceTokenCache;
  const param = await getApiKeysParam();
  serviceTokenCache = param.InternalServiceToken;
  if (!serviceTokenCache) throw new Error("InternalServiceToken not found");
  return serviceTokenCache;
}

async function getApiKeysParam(): Promise<Record<string, any>> {
  if (apiKeysCache) return apiKeysCache;
  const cmd = new GetParameterCommand({ Name: API_KEYS_PARAMETER_NAME, WithDecryption: true });
  const res = await ssmClient.send(cmd);
  const param = JSON.parse(res.Parameter?.Value || "{}");
  apiKeysCache = param;
  return param;
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
    const res = await fetch(`${PRODUCTS_API_URL}/${productId}`, {
      method: "GET",
      headers: { "X-Service-Token": token },
    });
    return res.ok;
  } catch (e) {
    if (DEBUG_PDP) console.log("[DEBUG] product exists check failed:", e);
    return false;
  }
}

function normalizeUpc(upc: string | null | undefined): string | null {
  if (!upc) return null;
  const digits = upc.replace(/\D/g, "");
  if (!digits) return null;
  if (digits.length === 12) return digits;
  if (digits.length === 13 && digits.startsWith("0")) return digits.slice(1);
  if (digits.length === 14 && digits.startsWith("00")) return digits.slice(2);
  return digits;
}

async function loadExistingUpcs(): Promise<Set<string>> {
  if (existingUpcSetPromise) return existingUpcSetPromise;
  existingUpcSetPromise = (async () => {
    const set = new Set<string>();
    const { Client: PgClient } = await import("pg");
    const param = await getApiKeysParam();
    const conn = param.SupabaseDbUrl || process.env.SCRAPER_FAILURES_DB_URL || process.env.DATABASE_URL;
    if (!conn) throw new Error("SupabaseDbUrl not found");

    const client = new PgClient({ connectionString: conn });
    client.on("error", (err: any) => {
      if (DEBUG_PDP) console.log("[DEBUG] UPC DB client error:", err?.message || err);
    });

    try {
      await client.connect();
      const res = await client.query(`
        SELECT TRIM(upc) as upc
        FROM product_upcs
        WHERE upc IS NOT NULL
        UNION
        SELECT TRIM(upc) as upc
        FROM products
        WHERE upc IS NOT NULL
      `);
      for (const row of res.rows) {
        const upc = normalizeUpc((row as any)?.upc || null);
        if (upc) set.add(upc);
      }
    } finally {
      await client.end().catch(() => null);
    }
    if (DEBUG_PDP) console.log(`[DEBUG] loaded ${set.size} existing UPCs from SQL`);
    return set;
  })().catch((err) => {
    existingUpcSetPromise = null;
    throw err;
  });
  return existingUpcSetPromise;
}

async function checkProductExistsByUpc(upc: string | null): Promise<boolean> {
  const normalized = normalizeUpc(upc);
  if (!normalized) return false;
  const upcSet = await loadExistingUpcs();
  return upcSet.has(normalized);
}

async function submitProduct(p: ScrapedProduct): Promise<boolean> {
  const body = transformToOutput(p);
  if (!body.product_name || (!body.ingredients_text && !body.nutrition)) {
    console.log(`Skipping ${body.source_id}: missing name, ingredients, and nutrition (likely raw produce)`);
    return false;
  }
  if (!body.upc) {
    console.log(`Skipping ${body.source_id}: missing UPC`);
    return false;
  }
  try {
    const token = await getServiceToken();
    const { scraper_job_id: _, ...req } = body as any;
    const res = await fetch(`${API_BASE_URL}/submit-product-for-review`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Service-Token": token },
      body: JSON.stringify(req),
    });

    if (res.ok) {
      console.log(`✅ Submitted "${body.product_name}"`);
      return true;
    }
    console.error(`❌ Submit failed: HTTP ${res.status}`);
    return false;
  } catch (e) {
    console.error(`❌ Submit failed:`, e);
    return false;
  }
}

async function uploadToS3(filePath: string, runDateTime: string): Promise<void> {
  if (!SCRAPER_OUTPUTS_BUCKET) return;
  const key = `${SCRAPER_NAME}/${runDateTime}/products.json`;
  await s3Client.send(
    new PutObjectCommand({
      Bucket: SCRAPER_OUTPUTS_BUCKET,
      Key: key,
      Body: fs.createReadStream(filePath),
      ContentType: "application/json",
    })
  );
  console.log(`Uploaded to s3://${SCRAPER_OUTPUTS_BUCKET}/${key}`);
}

function createJsonArrayWriter(filePath: string) {
  const stream = fs.createWriteStream(filePath, { encoding: "utf8" });
  let writeChain: Promise<void> = Promise.resolve();
  let firstItem = true;
  let finalized = false;

  const writeChunk = (chunk: string): Promise<void> =>
    new Promise((resolve, reject) => {
      const onError = (err: Error) => reject(err);
      stream.once("error", onError);
      const done = () => {
        stream.removeListener("error", onError);
        resolve();
      };
      if (!stream.write(chunk, "utf8")) {
        stream.once("drain", done);
      } else {
        done();
      }
    });

  writeChain = writeChain.then(() => writeChunk("[\n"));

  return {
    append: (record: ScraperProductOutput): Promise<void> => {
      if (finalized) return Promise.reject(new Error("Cannot append after finalize"));
      const prefix = firstItem ? "" : ",\n";
      firstItem = false;
      writeChain = writeChain.then(() => writeChunk(prefix + JSON.stringify(record)));
      return writeChain;
    },
    finalize: async (): Promise<void> => {
      if (finalized) return;
      finalized = true;
      writeChain = writeChain.then(() => writeChunk("\n]\n"));
      await writeChain;
      await new Promise<void>((resolve, reject) => {
        stream.once("error", reject);
        stream.end(() => resolve());
      });
    },
  };
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
  let configPath: string | undefined;
  let limit: number | undefined;
  let offset = 0;
  let local = false;
  let debug = false;
  let noHeadless = false;
  let headless = false;

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
    } else if (argv[i] === "--offset" || argv[i] === "-o") {
      offset = parseInt(argv[i + 1], 10);
      if (isNaN(offset) || offset < 0) offset = 0;
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

  return { url, configPath, limit, offset, local, debug, noHeadless, headless };
}

async function main(): Promise<void> {
  const { url, configPath, limit, offset, local, debug, noHeadless, headless } = parseArgs();

  DEBUG_PDP = debug;

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
    console.error("   or: npx tsx scrape.ts --config ./config.json [--limit N] [--offset N] [--local]");
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

  let runHeadless = true;
  if (noHeadless) runHeadless = false;
  if (headless) runHeadless = true;

  const browser: Browser = await chromium.launch({
    headless: runHeadless,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-software-rasterizer",
    ],
  });

  const context: BrowserContext = await browser.newContext(TARGET_CONTEXT_OPTIONS);

  const outputFilePath = `/tmp/${SCRAPER_NAME}-${runDateTime}-${jobId}-products.json`;
  const outputWriter = createJsonArrayWriter(outputFilePath);
  let outputWriterFinalized = false;
  let outputCount = 0;
  let skipped = 0;
  let submitted = 0;
  let submitFailed = 0;
  const submitBatch: ScrapedProduct[] = [];
  const SUBMIT_BATCH_SIZE = 10;
  const MAX_PENDING_SUBMITS = 30;
  let plannedTotalTargets = 0;
  let seenDiscoveredTargets = 0;
  let selectedDiscoveredTargets = 0;

  const flushSubmitBatch = async (force: boolean) => {
    if (!force && submitBatch.length < SUBMIT_BATCH_SIZE) return;
    if (submitBatch.length === 0) return;
    const take = force ? submitBatch.length : Math.min(SUBMIT_BATCH_SIZE, submitBatch.length);
    const batch = submitBatch.splice(0, take);
    console.log(`\n➡️  Submitting batch (${batch.length} items)`);
    for (const p of batch) {
      const ok = await submitProduct(p);
      if (ok) submitted++;
      else submitFailed++;
    }
    batch.length = 0;
    if (typeof (globalThis as any).gc === "function") {
      (globalThis as any).gc();
    }
  };

  let flushChain: Promise<void> = Promise.resolve();
  const queueFlush = (force: boolean) => {
    flushChain = flushChain.then(() => flushSubmitBatch(force)).catch((err) => {
      console.error("[SUBMIT] batch flush failed:", err);
    });
    return flushChain;
  };

  const runScrapeQueue = async (batchTargets: UrlWithBrand[]) => {
    if (batchTargets.length === 0) return;
    plannedTotalTargets += batchTargets.length;

    const queue = [...batchTargets];
    const workerCount = Math.min(5, queue.length);
    const workers = Array.from({ length: workerCount }, async () => {
      while (queue.length) {
        const next = queue.shift();
        if (!next) break;
        const u = next.url;
        const brand = next.brand;
        console.log(`[SCRAPE] Product URL: ${u}`);
        let p: ScrapedProduct | null = null;
        try {
          p = await scrapeProductDetail(context, u, brand);
        } catch (e: any) {
          const msg = String(e?.message || e);
          if (e?.name === "TimeoutError" || /ERR_TIMED_OUT|Navigation timeout|ERR_ABORTED|ERR_CONNECTION_RESET/i.test(msg)) {
            skipped++;
            console.log(`[SKIP] ${u}: navigation failed (${msg.includes("ERR_") ? msg.match(/ERR_[A-Z_]+/)?.[0] ?? "timeout" : "timeout"})`);
            continue;
          }
          skipped++;
          console.log(`[SKIP] ${u}: unexpected scrape error`);
          if (DEBUG_PDP) console.error(e);
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

        const transformed = transformToOutput(p);
        const existsById = await checkProductExists({
          name: transformed.product_name || null,
          brand: transformed.brand || null,
          upc: transformed.upc || transformed.upcs?.[0] || null,
        });
        let existsByUpc = false;
        try {
          existsByUpc = await checkProductExistsByUpc(transformed.upc || transformed.upcs?.[0] || null);
        } catch (upcErr) {
          skipped++;
          console.log(
            `[SKIP] ${transformed.product_name || "(no name)"}: UPC verification failed`
          );
          if (DEBUG_PDP) console.log("[DEBUG] upc exists check failed:", upcErr);
          continue;
        }
        if (existsById || existsByUpc) {
          skipped++;
          console.log(
            `[SKIP] ${transformed.product_name || "(no name)"}: already exists` +
              (existsByUpc && !existsById ? " (matched by UPC)" : "")
          );
          continue;
        }
        outputCount++;
        await outputWriter.append(transformed);
        if (outputCount % 10 === 0) {
          console.log(`[PROGRESS] scraped ${outputCount} products`);
        }
        console.log(`[OK] ${p.name ?? "(no name)"} | ${u}`);
        submitBatch.push(p);
        if (submitBatch.length >= SUBMIT_BATCH_SIZE) {
          void queueFlush(false);
        }
        if (submitBatch.length >= MAX_PENDING_SUBMITS) {
          await queueFlush(false);
        }
      }
    });

    await Promise.all(workers);
  };

  const takeDiscovered = (items: UrlWithBrand[]): UrlWithBrand[] => {
    const selected: UrlWithBrand[] = [];
    for (const item of items) {
      if (seenDiscoveredTargets < offset) {
        seenDiscoveredTargets++;
        continue;
      }
      if (limit != null && selectedDiscoveredTargets >= limit) break;
      selected.push(item);
      seenDiscoveredTargets++;
      selectedDiscoveredTargets++;
    }
    return selected;
  };

  const reachedLimit = () => limit != null && selectedDiscoveredTargets >= limit;
  let incrementalDiscoveryMode = false;
  const processedListingUrls = new Set<string>();

  try {
    if (configPath) {
      const cfg = JSON.parse(fs.readFileSync(configPath, "utf8")) as AppConfig;
      const skipListingUrls = new Set<string>(
        (cfg.skipListingUrls || [])
          .map((u) => normalizeTargetListingUrl(u))
          .filter((u): u is string => !!u)
      );
      if (
        (Array.isArray(cfg.brands) && cfg.brands.length > 0) ||
        (Array.isArray(cfg.categoryUrls) && cfg.categoryUrls.length > 0)
      ) {
        incrementalDiscoveryMode = true;

        if (Array.isArray(cfg.categoryUrls) && cfg.categoryUrls.length > 0) {
          for (const categorySeed of cfg.categoryUrls) {
            if (reachedLimit()) break;
            const normalizedCategory = normalizeTargetCategoryUrl(categorySeed);
            if (!normalizedCategory) continue;

            console.log(`\n[DISCOVER] Category Seed: ${normalizedCategory}`);
            const listingUrls = await discoverSubcategoryListingUrls(context, normalizedCategory);
            console.log(`[DISCOVER] Category Seed: ${listingUrls.length} listing URLs`);

            for (const listingUrl of listingUrls) {
              if (reachedLimit()) break;
              const normalizedListing = normalizeTargetListingUrl(listingUrl) || listingUrl;
              if (skipListingUrls.has(normalizedListing)) {
                console.log(`[DISCOVER] Listing ${listingUrl}: skipped (config skipListingUrls)`);
                continue;
              }
              if (processedListingUrls.has(listingUrl)) {
                console.log(`[DISCOVER] Listing ${listingUrl}: skipped (already processed)`);
                continue;
              }
              processedListingUrls.add(listingUrl);
              const remaining = limit != null ? Math.max(limit - selectedDiscoveredTargets, 0) : undefined;
              const urls = await scrapeListing(browser, { brand: "", listingUrl }, remaining);
              console.log(`[DISCOVER] Listing ${listingUrl}: ${urls.length} product URLs`);
              const listingTargets = takeDiscovered(urls.map((u) => ({ url: u })));
              if (listingTargets.length > 0) {
                console.log(`[SCRAPE] Listing ${listingUrl}: ${listingTargets.length} product URLs`);
                await runScrapeQueue(listingTargets);
                await queueFlush(true);
                await flushChain;
                if (typeof (globalThis as any).gc === "function") {
                  (globalThis as any).gc();
                }
              }
            }
          }
        }

        if (Array.isArray(cfg.brands) && cfg.brands.length > 0 && !reachedLimit()) {
          for (const brandCfg of cfg.brands) {
            if (reachedLimit()) break;
            if (!brandCfg.listingUrl?.startsWith("https://www.target.com/")) continue;
            const normalizedBrandListing = normalizeTargetListingUrl(brandCfg.listingUrl) || brandCfg.listingUrl;
            if (skipListingUrls.has(normalizedBrandListing)) {
              console.log(`\n[DISCOVER] ${brandCfg.brand}: ${brandCfg.listingUrl} (skipped, config skipListingUrls)`);
              continue;
            }
            if (processedListingUrls.has(brandCfg.listingUrl)) {
              console.log(`\n[DISCOVER] ${brandCfg.brand}: ${brandCfg.listingUrl} (skipped, already processed)`);
              continue;
            }
            processedListingUrls.add(brandCfg.listingUrl);
            const remaining = limit != null ? Math.max(limit - selectedDiscoveredTargets, 0) : undefined;
            console.log(`\n[DISCOVER] ${brandCfg.brand}: ${brandCfg.listingUrl}`);
            const urls = await scrapeListing(browser, brandCfg, remaining);
            console.log(`[DISCOVER] ${brandCfg.brand}: ${urls.length} product URLs`);

            const brand =
              brandCfg.listingUrl?.toLowerCase().includes("good-gather") ? "Good & Gather" : brandCfg.brand;
            const brandTargets = takeDiscovered(urls.map((u) => ({ url: u, brand })));
            if (brandTargets.length > 0) {
              console.log(`[SCRAPE] ${brandCfg.brand}: ${brandTargets.length} product URLs`);
              await runScrapeQueue(brandTargets);
              await queueFlush(true);
              await flushChain;
              if (typeof (globalThis as any).gc === "function") {
                (globalThis as any).gc();
              }
            }
          }
        }
      }
    }
  } catch (e) {
    console.error("Discovery failed:", e);
    if (!local) await updateJobStatus(jobId, "failed", String(e));
    await context.close().catch(() => null);
    await browser.close().catch(() => null);
    process.exit(1);
  }

  if (!incrementalDiscoveryMode) {
    if (offset > 0) targets = targets.slice(offset);
    if (limit != null) targets = targets.slice(0, limit);

    if (targets.length === 0) {
      console.error("No valid Target product URLs to scrape.");
      await context.close().catch(() => null);
      await browser.close().catch(() => null);
      process.exit(1);
    }
  }

  try {
    if (!incrementalDiscoveryMode) {
      await runScrapeQueue(targets);
    }
    await queueFlush(true);
    await flushChain;
  } catch (e) {
    console.error(e);
    if (!local) await updateJobStatus(jobId, "failed", String(e));
  } finally {
    try {
      if (!outputWriterFinalized) {
        await outputWriter.finalize();
        outputWriterFinalized = true;
      }
    } catch (finalizeError) {
      console.error("[OUTPUT] failed to finalize output file:", finalizeError);
    }
    await context.close().catch(() => null);
    await browser.close().catch(() => null);
  }

  if (!outputWriterFinalized) {
    await outputWriter.finalize();
    outputWriterFinalized = true;
  }

  console.log(`\nScraped ${outputCount} valid products (skipped ${skipped} without ingredients/nutrition) out of ${plannedTotalTargets} total`);
  console.log(`📊 API: ${submitted} submitted, ${submitFailed} failed`);

  if (!local && outputCount > 0) {
    await uploadToS3(outputFilePath, runDateTime);
    await updateJobStatus(jobId, "completed");
  }

  try {
    fs.unlinkSync(outputFilePath);
  } catch {
    // ignore cleanup failure
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
