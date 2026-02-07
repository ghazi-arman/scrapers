import * as fs from "fs";
import axios from "axios";
import pLimit from "p-limit";
import { chromium, type Browser, type BrowserContext, type Page } from "playwright";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import { parseNutrientAmountWithQualifier } from "../nutrition-utils";

type AppConfig = {
  urls?: string[];
  bpns?: string[];
  searchUrls?: string[];
  searchUrl?: string;
  storeId?: string;
  banner?: string;
  bannerId?: string;
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

type ExtractResult = {
  name?: string;
  ingredients?: string;
  nutrition?: Nutrition;
  imageUrl?: string;
  upc12?: string;
};

const SOURCE = "vons.com";
const SCRAPER_NAME = process.env.JOB_NAME || "vons";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://it7rdy3qbh.execute-api.us-west-2.amazonaws.com";
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
const DEFAULT_STORE_ID = process.env.VONS_STORE_ID || "2053";
const DEFAULT_BANNER = process.env.VONS_BANNER || "vons";
const DEFAULT_BANNER_ID = process.env.VONS_BANNER_ID || "2";
const DEFAULT_ZIPCODE = process.env.VONS_ZIPCODE || "92110";
const DEBUG_VONS = process.env.DEBUG_VONS === "1";
const VONS_HEADLESS = process.env.VONS_HEADLESS !== "0";
const VONS_SLOWMO = process.env.VONS_SLOWMO ? parseInt(process.env.VONS_SLOWMO, 10) : undefined;
const VONS_USER_DATA_DIR = process.env.VONS_USER_DATA_DIR || null;

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

function normalizeProductName(name: string | null): string | null {
  if (!name) return null;
  let out = decodeHtmlEntities(name) ?? name;
  out = out.replace(/^\s*Signature\s+(Select|Cafe)\s+/i, "");
  // Remove trailing weight/size segments (e.g., "- 12 OZ", "12 OZ", "12oz/10ct")
  out = out.replace(
    /\s*[-–—]?\s*\b\d+(?:\.\d+)?\s?(?:oz|fl\s?oz|floz|lb|lbs|g|kg|ml|l|ct|count|pack|pk)\b(?:\s*\/\s*\d+\s?(?:ct|count))?\s*$/i,
    ""
  );
  return out.replace(/\s+/g, " ").trim();
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

function parseNutritionCandidate(obj: unknown): Nutrition | null {
  if (!obj || typeof obj !== "object") return null;
  const o = obj as Record<string, unknown>;

  const servingSize =
    (o.servingSize as string) ??
    (o.serving_size as string) ??
    (o.servingSizeText as string) ??
    null;
  const servingsPerContainer =
    (o.servingsPerContainer as string) ??
    (o.servings_per_container as string) ??
    null;
  const calories = (o.calories as string) ?? (o.calorie as string) ?? null;

  const items =
    (o.nutrients as unknown[]) ??
    (o.items as unknown[]) ??
    (o.nutritionFacts as unknown[]) ??
    (o.nutrition_fact_list as unknown[]) ??
    null;

  if (!servingSize && !calories && !Array.isArray(items)) return null;

  const nutrients: NutritionNutrient[] = [];
  if (Array.isArray(items)) {
    for (const it of items) {
      const f = (it && typeof it === "object" && (it as Record<string, unknown>)) || null;
      if (!f) continue;
      const nm = (f.name ?? f.label ?? f.nutrientName ?? "").toString();
      const qty = f.quantity ?? f.amount ?? f.value ?? null;
      const unit = f.unit_of_measurement ?? f.unit ?? "";
      const pct = f.percentage ?? f.dailyValue ?? f.daily_value ?? null;
      if (nm) {
        nutrients.push({
          name: nm,
          amount: qty != null ? `${qty}${unit || ""}` : null,
          dailyValuePercent: pct != null ? `${pct}%` : null,
        });
      }
    }
  }

  if (!servingSize && !calories && nutrients.length === 0) return null;
  return {
    servingSize: servingSize ? String(servingSize) : null,
    servingsPerContainer: servingsPerContainer ? String(servingsPerContainer) : null,
    calories: calories != null ? String(calories) : null,
    nutrients,
  };
}

function findNutritionInData(obj: unknown): Nutrition | null {
  if (!obj || typeof obj !== "object") return null;
  const candidate = parseNutritionCandidate(obj);
  if (candidate) return candidate;

  for (const v of Object.values(obj as Record<string, unknown>)) {
    if (v && typeof v === "object") {
      const found = findNutritionInData(v);
      if (found) return found;
    }
  }
  return null;
}

function extractFromPdpData(data: unknown): ExtractResult {
  const result: ExtractResult = {};
  const findString = (...keys: string[]) => {
    const v = findInObject(data, keys);
    return Array.isArray(v) ? v[0] : (v as string | null);
  };

  result.name =
    normalizeProductName(findString("name", "productName", "title", "displayName", "shortDescription")) || undefined;
  result.ingredients =
    normalizeWhitespace(
      decodeHtmlEntities(
        findString("ingredients", "ingredientStatement", "ingredient_information", "ingredientsText")
      ) || ""
    ) || undefined;

  result.imageUrl =
    findString("imageUrl", "primaryImageUrl", "primary_image_url", "image", "imageURL", "primaryImage") || undefined;

  const upc = findString("upc", "upc12", "gtin", "gtin12", "barcode", "primaryUpc");
  if (upc) {
    const m = upc.match(/(\d{12})/);
    result.upc12 = m?.[1] ?? upc;
  }

  const nutrition = findNutritionInData(data);
  if (nutrition) result.nutrition = nutrition;

  return result;
}

function buildPdpUrl(bpn: string, storeId: string, banner: string, bannerId: string): string {
  const u = new URL("https://www.vons.com/abs/pub/xapi/product/v2/pdpdata");
  u.searchParams.set("bpn", bpn);
  u.searchParams.set("banner", banner);
  u.searchParams.set("storeId", storeId);
  u.searchParams.set("bannerId", bannerId);
  u.searchParams.set("includeProductRating", "true");
  u.searchParams.set("realTimeReviewRating", "true");
  u.searchParams.set("guest", "true");
  u.searchParams.set("includeOffer", "true");
  u.searchParams.set("pgm", "abs");
  return u.toString();
}

function buildSearchApiUrl(baseUrl: string, start: number, rows: number): string {
  const u = new URL(baseUrl);
  u.searchParams.set("start", String(start));
  u.searchParams.set("rows", String(rows));
  return u.toString();
}

function getSearchContext(searchUrl: string): { storeId: string; zipcode: string } {
  try {
    const u = new URL(searchUrl);
    const storeId = u.searchParams.get("storeid") || u.searchParams.get("storeId") || DEFAULT_STORE_ID;
    const zipcode = u.searchParams.get("zipcode") || DEFAULT_ZIPCODE;
    return { storeId, zipcode };
  } catch {
    return { storeId: DEFAULT_STORE_ID, zipcode: DEFAULT_ZIPCODE };
  }
}

function buildSearchApiBase(searchUrl: string): string {
  const u = new URL(searchUrl);
  if (/\/abs\/pub\/xapi\/pgmsearch\/v1\/search\/products/i.test(u.pathname)) {
    return u.toString();
  }

  const q = u.searchParams.get("q") || "";
  const { storeId, zipcode } = getSearchContext(searchUrl);
  const api = new URL("https://www.vons.com/abs/pub/xapi/pgmsearch/v1/search/products");
  api.searchParams.set("url", "https://www.vons.com");
  api.searchParams.set("pageurl", "https://www.vons.com");
  api.searchParams.set("pagename", "search");
  api.searchParams.set("rows", "30");
  api.searchParams.set("start", "0");
  api.searchParams.set("search-type", "keyword");
  api.searchParams.set("storeid", storeId);
  api.searchParams.set("featured", "true");
  api.searchParams.set("q", q);
  api.searchParams.set("sort", "");
  api.searchParams.set("timezone", "America/Los_Angeles");
  api.searchParams.set("dvid", "web-4.1search");
  api.searchParams.set("channel", "instore");
  api.searchParams.set("wineshopstoreid", "5799");
  api.searchParams.set("zipcode", zipcode);
  api.searchParams.set("visitorId", "");
  api.searchParams.set("pgm", "intg-search,wineshop,merch-banner");
  api.searchParams.set("includeOffer", "true");
  api.searchParams.set("banner", DEFAULT_BANNER);
  return api.toString();
}

function parseBpnFromUrl(url: string): string | null {
  const m = url.match(/product-details\.(\d+)\.html/);
  return m?.[1] ?? null;
}

function extractBpnsFromSearchData(data: unknown): string[] {
  const out = new Set<string>();

  const walk = (obj: unknown): void => {
    if (!obj || typeof obj !== "object") return;
    const o = obj as Record<string, unknown>;

    if (typeof o.bpn === "string" && /\\d+/.test(o.bpn)) out.add(o.bpn);
    if (typeof o.productId === "string" && /\\d+/.test(o.productId)) out.add(o.productId);
    if (typeof o.product_id === "string" && /\\d+/.test(o.product_id)) out.add(o.product_id);

    if (typeof o.productUrl === "string") {
      const bpn = parseBpnFromUrl(o.productUrl);
      if (bpn) out.add(bpn);
    }

    for (const v of Object.values(o)) {
      if (v && typeof v === "object") walk(v);
    }
  };

  walk(data);
  return Array.from(out);
}

async function scrapeSearchListingViaBrowser(searchUrl: string, limit?: number): Promise<string[]> {
  const out = new Set<string>();
  const headless = process.env.VONS_HEADLESS !== "0";
  const slowMo = process.env.VONS_SLOWMO ? parseInt(process.env.VONS_SLOWMO, 10) : undefined;
  const userDataDir = process.env.VONS_USER_DATA_DIR || null;

  let browser: Browser | null = null;
  let context: BrowserContext | null = null;
  let page: Page | null = null;

  if (userDataDir) {
    context = await chromium.launchPersistentContext(userDataDir, {
      headless,
      slowMo,
      userAgent:
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    });
    page = await context.newPage();
  } else {
    browser = await chromium.launch({ headless, slowMo });
    context = await browser.newContext({
      userAgent:
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    });
    page = await context.newPage();
  }

  try {
    let dumped = false;
    const { storeId, zipcode } = getSearchContext(searchUrl);
    const seen = new Set<string>();
    page.on("request", (req) => {
      if (!DEBUG_VONS) return;
      const url = req.url();
      if (seen.has(url)) return;
      seen.add(url);
      if (/vons\.com\/abs\/pub\//i.test(url)) {
        try {
          fs.appendFileSync("/tmp/vons-requests.txt", `${url}\n`);
        } catch {
          // ignore
        }
      }
    });
    page.on("response", async (resp) => {
      const url = resp.url();
      if (!/\/abs\/pub\/xapi\/pgmsearch\/v1\/search\/products/i.test(url)) return;
      try {
        if (!resp.ok()) return;
        const data = await resp.json();
        const bpns = extractBpnsFromSearchData(data);
        for (const bpn of bpns) out.add(bpn);
        if (DEBUG_VONS) console.log(`[DEBUG] search api response bpns=${bpns.length}, total=${out.size}`);
        if (DEBUG_VONS && !dumped) {
          dumped = true;
          try {
            fs.writeFileSync("/tmp/vons-search.json", JSON.stringify(data, null, 2));
            console.log("[DEBUG] wrote /tmp/vons-search.json");
          } catch {
            // ignore
          }
        }
      } catch {
        // ignore
      }
    });

    await page!.request
      .get(`https://www.vons.com/abs/pub/xapi/preload/webpreload/storeflags/${storeId}?zipcode=${zipcode}`)
      .catch(() => null);
    await page!.request
      .get(`https://www.vons.com/abs/pub/xapi/storeresolver/storeaddress?storeid=${storeId}`)
      .catch(() => null);

    await page!.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page!.waitForLoadState("networkidle", { timeout: 8000 }).catch(() => {});
    await page!
      .waitForResponse((resp) => /\/abs\/pub\/xapi\/pgmsearch\/v1\/search\/products/i.test(resp.url()), {
        timeout: 20_000,
      })
      .catch(() => {});

    if (out.size === 0) {
      const base = new URL(buildSearchApiBase(searchUrl));
      const rows = parseInt(base.searchParams.get("rows") || "30", 10) || 30;
      let start = parseInt(base.searchParams.get("start") || "0", 10) || 0;

      for (let pageIndex = 0; pageIndex < 50; pageIndex++) {
        if (limit != null && out.size >= limit) break;
        const url = buildSearchApiUrl(base.toString(), start, rows);
        if (DEBUG_VONS) console.log(`[DEBUG] manual fetch search api: ${url}`);

        const data = await page!
          .evaluate(async (u) => {
            const controller = new AbortController();
            const timer = setTimeout(() => controller.abort(), 15000);
            try {
              const res = await fetch(u, {
                credentials: "include",
                headers: { accept: "application/json" },
                signal: controller.signal,
              });
              if (!res.ok) return { __error: `status:${res.status}` };
              return await res.json();
            } catch (e: any) {
              return { __error: e?.name || "fetch_error" };
            } finally {
              clearTimeout(timer);
            }
          }, url)
          .catch(() => ({ __error: "eval_error" }));

        if (data && typeof data === "object" && (data as any).__error) {
          if (DEBUG_VONS) console.log(`[DEBUG] manual fetch error: ${(data as any).__error}`);
          break;
        }
        if (!data) break;
        const bpns = extractBpnsFromSearchData(data);
        for (const bpn of bpns) out.add(bpn);
        if (DEBUG_VONS) console.log(`[DEBUG] manual fetch bpns=${bpns.length}, total=${out.size}`);
        if (bpns.length === 0) break;
        start += rows;
      }
    }

    for (let i = 0; i < 60; i++) {
      if (limit != null && out.size >= limit) break;

      const loadMore = page!.getByRole("button", { name: /load more/i }).first();
      const visible = await loadMore.isVisible({ timeout: 1500 }).catch(() => false);
      if (DEBUG_VONS) console.log(`[DEBUG] load more visible: ${visible}`);
      if (!visible) break;

      await loadMore.click({ timeout: 3000 }).catch(() => null);
      await page!.waitForTimeout(1500);
      await page!.waitForLoadState("networkidle", { timeout: 8000 }).catch(() => {});
    }

    const all = Array.from(out);
    return limit != null ? all.slice(0, limit) : all;
  } finally {
    await page?.close().catch(() => null);
    await context?.close().catch(() => null);
    await browser?.close().catch(() => null);
  }
}

function buildProductUrl(bpn: string): string {
  return `https://www.vons.com/shop/product-details.${bpn}.html`;
}

function extractBrandFromName(name: string | null): string | null {
  if (!name) return null;
  if (/^\s*Signature\s+Select\b/i.test(name)) return "Signature Select";
  if (/^\s*Signature\s+Cafe\b/i.test(name)) return "Signature Cafe";
  const parts = name.split(/\s+/);
  if (parts.length >= 2 && /^[A-Z][a-zA-Z0-9]+$/.test(parts[0])) return parts[0];
  return null;
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

async function fetchPdpData(bpn: string, storeId: string, banner: string, bannerId: string): Promise<unknown> {
  const url = buildPdpUrl(bpn, storeId, banner, bannerId);
  let lastErr: unknown = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await axios.get(url, {
        timeout: 60_000,
        headers: {
          "user-agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          accept: "application/json",
          "accept-language": "en-US,en;q=0.9",
          referer: "https://www.vons.com/shop/product-details.html",
          "cache-control": "no-cache",
        },
      });
      return res.data;
    } catch (e) {
      lastErr = e;
      const status = (e as any)?.response?.status;
      if (status === 403) break;
      if (DEBUG_VONS) console.log(`[DEBUG] pdp api retry ${attempt + 1}/3 failed for ${bpn}`);
      await new Promise((r) => setTimeout(r, 1000 * (attempt + 1)));
    }
  }
  // Fallback to user-initiated browser session if API is blocked
  const productUrl = buildProductUrl(bpn);
  const browserData = await fetchPdpDataViaBrowser(productUrl, storeId);
  if (browserData) return browserData;
  throw lastErr;
}

async function fetchPdpDataViaBrowser(productUrl: string, storeId: string): Promise<unknown | null> {
  let browser: Browser | null = null;
  let context: BrowserContext | null = null;
  let page: Page | null = null;

  try {
    if (VONS_USER_DATA_DIR) {
      context = await chromium.launchPersistentContext(VONS_USER_DATA_DIR, {
        headless: VONS_HEADLESS,
        slowMo: VONS_SLOWMO,
        userAgent:
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      });
      page = await context.newPage();
    } else {
      browser = await chromium.launch({ headless: VONS_HEADLESS, slowMo: VONS_SLOWMO });
      context = await browser.newContext({
        userAgent:
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      });
      page = await context.newPage();
    }

    let data: unknown | null = null;
    page.on("response", async (resp) => {
      const url = resp.url();
      if (!/\/abs\/pub\/xapi\/product\/v2\/pdpdata/i.test(url)) return;
      try {
        if (!resp.ok()) return;
        const json = await resp.json();
        data = json;
        if (DEBUG_VONS) console.log(`[DEBUG] captured pdpdata via browser for ${productUrl}`);
      } catch {
        // ignore
      }
    });

    await page.goto(productUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForLoadState("networkidle", { timeout: 8000 }).catch(() => {});
    await page
      .waitForResponse((resp) => /\/abs\/pub\/xapi\/product\/v2\/pdpdata/i.test(resp.url()), {
        timeout: 30_000,
      })
      .catch(() => {});

    return data;
  } finally {
    await page?.close().catch(() => null);
    await context?.close().catch(() => null);
    await browser?.close().catch(() => null);
  }
}

async function scrapeProductDetail(
  bpn: string,
  storeId: string,
  banner: string,
  bannerId: string,
  brandOverride?: string
): Promise<ScrapedProduct> {
  const data = await fetchPdpData(bpn, storeId, banner, bannerId);
  const extracted = extractFromPdpData(data);
  const now = new Date().toISOString();
  const productUrl = buildProductUrl(bpn);

  return {
    brand: brandOverride || extractBrandFromName(extracted.name ?? null) || "Unknown",
    source: SOURCE,
    productUrl,
    name: extracted.name ?? null,
    ingredients: extracted.ingredients ?? null,
    upc12: extracted.upc12 ?? null,
    nutrition: extracted.nutrition ?? null,
    imageUrl: extracted.imageUrl ?? null,
    scrapedAt: now,
    sourceCreatedAt: null,
    sourceLastUpdatedAt: null,
  };
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

function parseArgs(): {
  url?: string;
  bpn?: string;
  searchUrl?: string;
  configPath?: string;
  limit?: number;
  local: boolean;
} {
  const argv = process.argv.slice(2);
  let url: string | undefined;
  let bpn: string | undefined;
  let searchUrl: string | undefined;
  let configPath: string | undefined;
  let limit: number | undefined;
  let local = false;

  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--url" || argv[i] === "-u") {
      url = argv[i + 1];
      i++;
    } else if (argv[i] === "--bpn") {
      bpn = argv[i + 1];
      i++;
    } else if (argv[i] === "--search") {
      searchUrl = argv[i + 1];
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

  return { url, bpn, searchUrl, configPath, limit, local };
}

async function main(): Promise<void> {
  const { url, bpn, searchUrl, configPath, limit, local } = parseArgs();

  let storeId = DEFAULT_STORE_ID;
  let banner = DEFAULT_BANNER;
  let bannerId = DEFAULT_BANNER_ID;

  type Target = { bpn: string };
  let targets: Target[] = [];

  if (url) {
    const fromUrl = parseBpnFromUrl(url);
    if (fromUrl) targets = [{ bpn: fromUrl }];
  } else if (bpn) {
    targets = [{ bpn }];
  } else if (searchUrl) {
    const bpns = await scrapeSearchListingViaBrowser(searchUrl, limit);
    targets = bpns.map((v) => ({ bpn: v }));
  } else if (configPath) {
    const cfg = JSON.parse(fs.readFileSync(configPath, "utf8")) as AppConfig;
    if (cfg.storeId) storeId = cfg.storeId;
    if (cfg.banner) banner = cfg.banner;
    if (cfg.bannerId) bannerId = cfg.bannerId;

    const listingUrl = cfg.searchUrl || (Array.isArray(cfg.searchUrls) ? cfg.searchUrls[0] : undefined);
    if (listingUrl) {
      const bpns = await scrapeSearchListingViaBrowser(listingUrl, limit);
      targets = bpns.map((v) => ({ bpn: v }));
    }

    if (Array.isArray(cfg.bpns) && cfg.bpns.length > 0) {
      targets = cfg.bpns.filter((v) => typeof v === "string" && v.trim().length > 0).map((v) => ({ bpn: v }));
    } else if (Array.isArray(cfg.urls) && cfg.urls.length > 0) {
      targets = cfg.urls
        .filter((u) => typeof u === "string")
        .map((u) => parseBpnFromUrl(u))
        .filter((v): v is string => !!v)
        .map((v) => ({ bpn: v }));
    }
  } else {
    console.error("Usage: npx tsx scrape.ts --url <VONS_PRODUCT_URL>");
    console.error("   or: npx tsx scrape.ts --bpn <BPN>");
    console.error("   or: npx tsx scrape.ts --search <VONS_SEARCH_URL>");
    console.error("   or: npx tsx scrape.ts --config ./config.json [--limit N] [--local]");
    process.exit(1);
  }

  if (limit != null) targets = targets.slice(0, limit);

  if (targets.length === 0) {
    console.error("No valid Vons product targets to scrape.");
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

  const limiter = pLimit(10);
  const outputs: ScraperProductOutput[] = [];
  let skipped = 0;

  try {
    const results = await Promise.all(
      targets.map(({ bpn }) =>
        limiter(async () => {
          const p = await scrapeProductDetail(bpn, storeId, banner, bannerId);
          return { product: p, bpn };
        })
      )
    );

    for (const { product: p } of results) {
      if (!hasIngredientsOrNutrition(p)) {
        skipped++;
        console.log(`[SKIP] ${p.name ?? "(no name)"}: no ingredients and no nutrition`);
        logScrapedData(p, p.productUrl);
        continue;
      }
      if (!p.upc12) {
        skipped++;
        console.log(`[SKIP] ${p.name ?? "(no name)"}: missing UPC`);
        logScrapedData(p, p.productUrl);
        continue;
      }

      outputs.push(transformToOutput(p));
      console.log(`[OK] ${p.name ?? "(no name)"} | ${p.productUrl}`);

      await submitProduct(p);
    }
  } catch (e) {
    console.error(e);
    if (!local) await updateJobStatus(jobId, "failed", String(e));
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
