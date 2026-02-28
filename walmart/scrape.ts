import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";
import axios from "axios";
import * as cheerio from "cheerio";
import dotenv from "dotenv";
import { Client as PgClient } from "pg";
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

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

dotenv.config({ path: path.resolve(__dirname, ".env") });

const SOURCE = "walmart.com";
const SCRAPER_NAME = process.env.JOB_NAME || "walmart";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://api.mytummi.app";
const PRODUCTS_API_URL = `${API_BASE_URL}/products`;
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
let DEBUG_WM = false;
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
const SCRAPER_FAILURES_DB_URL = process.env.SCRAPER_FAILURES_DB_URL || "";
const MAX_SEARCH_PAGES = 25;

const s3Client = new S3Client({});
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);
const ssmClient = new SSMClient({});

let serviceTokenCache: string | null = null;
let apiKeysCache: Record<string, any> | null = null;
let failureDbClient: PgClient | null = null;

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
  sourceId: string;
  name: string | null;
  brand: string | null;
  ingredients: string | null;
  allergenStatement: string | null;
  upc12: string | null;
  upcs?: string[];
  nutrition: Nutrition | null;
  nutritionData: ScraperNutritionData | null;
  nutritionImageUrl: string | null;
  imageUrl: string | null;
  packSizeSkipped?: boolean;
  packSizeSkipReason?: string | null;
  scrapedAt: string;
  sourceCreatedAt: string | null;
  sourceLastUpdatedAt: string | null;
};

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
  if (lower.includes("folic acid")) return "folic_acid_mcg";
  if (lower.includes("folate")) return "folate_mcg";
  for (const [key, col] of Object.entries(NUTRIENT_COLUMN_MAP)) {
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

function extractProductIdFromUrl(url: string): string | null {
  try {
    const u = new URL(url);
    const m = u.pathname.match(/\/(\d+)(?:$|[/?])/);
    if (m) return m[1];
  } catch {
    // ignore
  }
  const m2 = url.match(/\/(\d+)(?:$|[/?])/);
  return m2 ? m2[1] : null;
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

function extractScriptJson(html: string, idOrKey: string): any | null {
  const idMatch = html.match(new RegExp(`<script[^>]*id=["']${idOrKey}["'][^>]*>([\\s\\S]*?)<\\/script>`, "i"));
  if (idMatch && idMatch[1]) {
    try {
      return JSON.parse(idMatch[1]);
    } catch {
      return null;
    }
  }

  const keyMatch = html.match(new RegExp(`${idOrKey}\\s*=\\s*([\\s\\S]*?);`, "i"));
  if (!keyMatch) return null;
  const raw = keyMatch[1];
  const start = raw.indexOf("{");
  const end = raw.lastIndexOf("}");
  if (start === -1 || end === -1) return null;
  const jsonText = raw.slice(start, end + 1);
  try {
    return JSON.parse(jsonText);
  } catch {
    return null;
  }
}

function extractNextData(html: string, $: cheerio.CheerioAPI): any | null {
  const scriptText = $("script#__NEXT_DATA__").first().text();
  if (scriptText) {
    try {
      return JSON.parse(scriptText);
    } catch {
      // fall through
    }
  }
  return extractScriptJson(html, "__NEXT_DATA__");
}

function extractReduxState(html: string, $: cheerio.CheerioAPI): any | null {
  const reduxText = $("script#__WML_REDUX_INITIAL_STATE__").first().text();
  if (reduxText) {
    try {
      return JSON.parse(reduxText);
    } catch {
      // fall through
    }
  }
  const preloadedText = $("script#__PRELOADED_STATE__").first().text();
  if (preloadedText) {
    try {
      return JSON.parse(preloadedText);
    } catch {
      // fall through
    }
  }
  return extractScriptJson(html, "__WML_REDUX_INITIAL_STATE__") ?? extractScriptJson(html, "__PRELOADED_STATE__");
}

function pickString(value: unknown): string | null {
  if (typeof value === "string") return value;
  return null;
}

function findProductInProductsMap(map: any, productId: string | null): any | null {
  if (!map) return null;
  if (productId && map[productId]) return map[productId];
  if (productId && map[String(productId)]) return map[String(productId)];
  const values = Array.isArray(map) ? map : Object.values(map);
  for (const v of values) {
    if (!v || typeof v !== "object") continue;
    const id = (v as any).id ?? (v as any).usItemId ?? (v as any).productId ?? (v as any).itemId;
    if (productId && id && String(id) === String(productId)) return v;
  }
  return null;
}

function extractProductFromData(nextData: any, redux: any, productId: string | null): any | null {
  const candidates: any[] = [];
  const next = nextData?.props?.pageProps;
  if (next?.initialData?.data?.product) candidates.push(next.initialData.data.product);
  if (next?.productData) candidates.push(next.productData);
  if (next?.data?.product) candidates.push(next.data.product);
  if (next?.product) candidates.push(next.product);
  if (next?.initialData?.data?.productData?.product) candidates.push(next.initialData.data.productData.product);

  const reduxProductMap = redux?.product?.products ?? redux?.product?.items ?? redux?.product?.product;
  const reduxProduct = findProductInProductsMap(reduxProductMap, productId);
  if (reduxProduct) candidates.push(reduxProduct);

  for (const c of candidates) {
    if (c && typeof c === "object") return c;
  }
  return null;
}

function extractNextDataRoot(nextData: any): any | null {
  const pageProps = nextData?.props?.pageProps;
  return (
    pageProps?.initialData?.data ||
    pageProps?.data ||
    pageProps?.initialData ||
    pageProps ||
    null
  );
}

function findNamedValue(node: any, targetName: string): string | null {
  if (!node || typeof node !== "object") return null;
  if (typeof node.name === "string" && node.name === targetName && typeof node.value === "string") {
    return node.value;
  }
  if (Array.isArray(node)) {
    for (const item of node) {
      const found = findNamedValue(item, targetName);
      if (found) return found;
    }
  } else {
    for (const value of Object.values(node)) {
      const found = findNamedValue(value, targetName);
      if (found) return found;
    }
  }
  return null;
}

function findFirstAllImagesUrl(node: any): string | null {
  if (!node || typeof node !== "object") return null;
  const allImages = (node as any).allImages;
  if (Array.isArray(allImages) && allImages.length > 0) {
    const first = allImages[0];
    if (first && typeof first.url === "string") return cleanText(first.url);
  }
  if (Array.isArray(node)) {
    for (const item of node) {
      const found = findFirstAllImagesUrl(item);
      if (found) return found;
    }
  } else {
    for (const value of Object.values(node)) {
      const found = findFirstAllImagesUrl(value);
      if (found) return found;
    }
  }
  return null;
}

function extractItemContext(nextData: any): any | null {
  const root = extractNextDataRoot(nextData);
  return root?.itemContext || root?.item || root?.product?.itemContext || null;
}

function extractIngredientsFromNextData(nextData: any): string | null {
  const root = extractNextDataRoot(nextData);
  const val =
    root?.ingredients?.ingredients?.value ||
    root?.ingredients?.value ||
    root?.ingredientsText ||
    root?.ingredientStatement ||
    root?.product?.ingredients?.ingredients?.value ||
    root?.product?.ingredients?.value ||
    root?.item?.ingredients?.ingredients?.value ||
    root?.item?.ingredients?.value ||
    null;
  const named = findNamedValue(root, "Ingredients");
  const picked = val || named || null;
  return cleanText(picked || null);
}

function extractAllergenFromNextData(nextData: any): string | null {
  const root = extractNextDataRoot(nextData);
  const val =
    root?.allergenStatement ||
    root?.allergens ||
    root?.allergenInformation ||
    root?.product?.allergenStatement ||
    root?.product?.allergens ||
    root?.item?.allergenStatement ||
    root?.item?.allergens ||
    null;
  const namedContains = findNamedValue(root, "Contains");
  const namedAllergens = findNamedValue(root, "Allergens");
  return cleanText(val || namedContains || namedAllergens || null);
}

function extractUpcFromNextData(nextData: any): string | null {
  const root = extractNextDataRoot(nextData);
  const ctx = extractItemContext(nextData);
  const val = root?.upc || root?.primaryUpc || ctx?.upc || null;
  if (!val) return null;
  const numeric = String(val).replace(/\D/g, "");
  if (numeric.length === 12) return numeric;
  if (numeric.length === 13) return numeric.slice(1);
  return null;
}

function extractNameBrandFromNextData(nextData: any): { name: string | null; brand: string | null } {
  const ctx = extractItemContext(nextData);
  const name = cleanText(ctx?.name || null);
  const brand = cleanText(ctx?.brand || null);
  return {name, brand};
}

function extractImageFromNextData(nextData: any): string | null {
  const root = extractNextDataRoot(nextData);
  const named = findNamedValue(root, "Nutrition facts label image");
  if (named) return cleanText(named);
  return null;
}

function stripQueryParams(url: string | null): string | null {
  if (!url) return null;
  try {
    const u = new URL(url);
    u.search = "";
    return u.toString();
  } catch {
    return url.split("?")[0];
  }
}

function extractAllImagesFromNextData(nextData: any): string[] {
  const root = extractNextDataRoot(nextData);
  const urls: string[] = [];

  const scan = (node: any): void => {
    if (!node || typeof node !== "object") return;
    const allImages = (node as any).allImages;
    if (Array.isArray(allImages)) {
      for (const img of allImages) {
        if (img && typeof img.url === "string") {
          urls.push(cleanText(img.url) || "");
        }
      }
    }
    if (Array.isArray(node)) {
      for (const item of node) scan(item);
    } else {
      for (const value of Object.values(node)) scan(value);
    }
  };

  scan(root);
  const seen = new Set<string>();
  const deduped = urls.filter((u) => u && !seen.has(u) && (seen.add(u), true));
  return deduped;
}

function extractProductImageFromNextData(nextData: any): string | null {
  const root = extractNextDataRoot(nextData);
  const allImages = extractAllImagesFromNextData(nextData);
  return allImages[0] || cleanText(root?.image?.url || root?.imageUrl || root?.thumbnailUrl || null);
}

function getPackSizeVariantList(nextData: any): any[] | null {
  const root = extractNextDataRoot(nextData);
  if (!root) return null;
  const scan = (node: any): any[] | null => {
    if (!node || typeof node !== "object") return null;
    const variants = (node as any).variantList;
    const type = (node as any).type;
    const name = (node as any).name;
    if (Array.isArray(variants) && (type === "DROPDOWN" || type === "dropdown")) {
      if (typeof name === "string" && name.toLowerCase().includes("pack size")) return variants;
    }
    if (Array.isArray(node)) {
      for (const item of node) {
        const found = scan(item);
        if (found) return found;
      }
    } else {
      for (const value of Object.values(node)) {
        const found = scan(value);
        if (found) return found;
      }
    }
    return null;
  };
  return scan(root);
}

function shouldSkipForPackSize(nextData: any): { skip: boolean; reason?: string } {
  const list = getPackSizeVariantList(nextData);
  if (!list || list.length === 0) return {skip: false};
  const selectedIndex = list.findIndex((v) => v && v.selected === true);
  if (selectedIndex <= 0) return {skip: false};
  return {skip: true, reason: "not smallest pack size variant selected"};
}

function normalizeProductName(name: string | null): string | null {
  return cleanProductName(name, {
    stripLeadingPack: true,
    keepBeforeComma: false,
    stripTrailingWeight: true,
    stripTrailingCount: true,
    stripTrailingDashSize: true,
    stripParenAtEnd: true,
  });
}

function extractIngredientsFromHtml($: cheerio.CheerioAPI): string | null {
  const blocks = [
    "[data-testid='ingredients']",
    "[data-automation-id='ingredients']",
    "#ingredients",
    "[aria-label*='Ingredients']",
  ];

  for (const sel of blocks) {
    const text = cleanText($(sel).text());
    if (text && /ingredient/i.test(text)) return text.replace(/^ingredients:?\s*/i, "");
  }

  const label = $("*:contains('Ingredients')").filter((_, el) => {
    const t = cleanText($(el).text()) || "";
    return /^ingredients\b/i.test(t);
  }).first();
  if (label.length) {
    const text = cleanText(label.text());
    if (text && /ingredient/i.test(text)) return text.replace(/^ingredients:?\s*/i, "");
  }

  return null;
}

function extractAllergenFromHtml($: cheerio.CheerioAPI): string | null {
  const blocks = [
    "[data-testid='allergens']",
    "[data-automation-id='allergens']",
    "#allergens",
    "[aria-label*='Allergen']",
    "[id*='allergen']",
  ];
  for (const sel of blocks) {
    const text = cleanText($(sel).text());
    if (text && /(contains|may contain|allergen)/i.test(text)) return text;
  }
  const candidates = $("p, li, div, span")
    .map((_, el) => cleanText($(el).text()))
    .get()
    .filter((t): t is string => !!t && /(?:contains|may contain|allergen)/i.test(t) && t.length <= 260);
  const direct = candidates.find((t) => /^(contains|may contain|allergen)/i.test(t));
  return cleanText(direct || candidates[0] || null);
}

function extractOgImage($: cheerio.CheerioAPI): string | null {
  const og = $('meta[property="og:image"]').attr("content");
  const tw = $('meta[name="twitter:image"]').attr("content");
  return cleanText(og || tw || null);
}

function toAmountString(value: any, unit?: any): string | null {
  if (value == null) return null;
  if (typeof value === "string") return cleanText(value);
  if (typeof value === "number") {
    const u = unit ? String(unit) : "";
    return `${value}${u}`;
  }
  return null;
}

function nutritionFromJsonLd(nutrition: any): Nutrition | null {
  if (!nutrition || typeof nutrition !== "object") return null;

  const nutrients: NutritionNutrient[] = [];
  const add = (name: string, amount: any) => {
    const amt = toAmountString(amount);
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

function normalizeNutrientName(n: string): string {
  return n.replace(/\s+/g, " ").trim();
}

function nutritionFromProductData(product: any): Nutrition | null {
  const raw =
    product?.nutritionFacts ??
    product?.nutrition ??
    product?.foodAndBeverage?.nutritionFacts ??
    product?.nutritionFacts?.nutritionFacts ??
    null;
  if (!raw) return null;

  const servingSize = cleanText(raw.servingSize || raw.servingSizeText || raw.serving_size || null);
  const servingsPerContainer = cleanText(raw.servingsPerContainer || raw.servings_per_container || null);
  const calories = cleanText(raw.calories || raw.caloriesPerServing || raw.calorie || null);

  const nutrients: NutritionNutrient[] = [];
  const list = raw.nutrients || raw.nutrientFacts || raw.nutrientData || raw.items || [];
  if (Array.isArray(list)) {
    for (const item of list) {
      if (!item || typeof item !== "object") continue;
      const name = normalizeNutrientName(String(item.name || item.nutrientName || item.label || item.displayName || ""));
      if (!name) continue;
      const amount = toAmountString(item.amount ?? item.value ?? item.amountPerServing ?? item.quantity, item.unit ?? item.uom ?? item.unitAbbreviation);
      const dv = toAmountString(item.dailyValuePercent ?? item.percentDailyValue ?? item.pctDV, "%");
      nutrients.push({ name, amount, dailyValuePercent: dv });
    }
  }

  if (!servingSize && !calories && nutrients.length === 0) return null;
  return {servingSize: servingSize || null, servingsPerContainer, calories: calories || null, nutrients};
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
    if (!col) continue;
    if (typeof parseNutrientAmountWithQualifier !== "function") {
      throw new Error("parseNutrientAmountWithQualifier import failed");
    }
    const parsed = parseNutrientAmountWithQualifier(n.amount);
    if (parsed !== null) {
      (result as unknown as Record<string, number>)[col] = parsed.value;
      if (parsed.qualifier) (result as unknown as Record<string, string>)[`${col}_qualifier`] = parsed.qualifier;
    }
  }

  return result;
}

function hasIngredientsOrNutrition(p: ScrapedProduct): boolean {
  return Boolean(p.ingredients && p.ingredients.trim()) || Boolean(p.nutrition) || Boolean(p.nutritionData);
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
    brand: p.brand || "",
    upc: p.upc12 || undefined,
    upcs: p.upcs && p.upcs.length ? p.upcs : undefined,
    ingredients_text: p.ingredients || "",
    allergen_statement: p.allergenStatement || undefined,
    serving_size_value: serving.value ?? undefined,
    serving_size_unit: serving.unit ?? undefined,
    serving_size_text: servingText,
    source: SOURCE,
    source_id: p.sourceId,
    source_created_at: p.sourceCreatedAt || now,
    source_last_updated_at: p.sourceLastUpdatedAt || now,
    image_url: p.imageUrl || undefined,
    nutrition: p.nutritionData || transformNutritionToDb(p.nutrition) || undefined};
}

function normalizeMergeText(value: string | null | undefined): string {
  if (!value) return "";
  return value.replace(/\s+/g, " ").trim().toLowerCase();
}

function nutritionSignature(nutrition: ScraperNutritionData | null | undefined): string {
  if (!nutrition) return "";
  const entries = Object.entries(nutrition)
    .filter(([, v]) => v !== undefined && v !== null && v !== "")
    .sort(([a], [b]) => a.localeCompare(b));
  return JSON.stringify(entries);
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

function mergeProducts(products: ScrapedProduct[]): ScrapedProduct[] {
  const merged = new Map<string, ScrapedProduct>();
  for (const product of products) {
    const key = [
      normalizeMergeText(product.brand),
      normalizeMergeText(product.name),
      normalizeMergeText(product.ingredients),
      nutritionSignature(product.nutritionData),
      normalizeMergeText(product.nutritionData?.serving_size_text || null),
    ].join("||");
    const existing = merged.get(key);
    if (!existing) {
      merged.set(key, { ...product, upcs: product.upc12 ? [product.upc12] : [] });
      continue;
    }
    const upcs = new Set([...(existing.upcs || []), ...(existing.upc12 ? [existing.upc12] : [])]);
    if (product.upc12) upcs.add(product.upc12);
    const sorted = Array.from(upcs).sort();
    existing.upcs = sorted;
    existing.upc12 = sorted[0] || existing.upc12 || null;
  }
  return Array.from(merged.values());
}

async function fetchHtml(url: string): Promise<string> {
  if (!SCRAPEDO_TOKEN) {
    throw new Error("SCRAPEDO_TOKEN is required to fetch Walmart pages via scrape.do");
  }
  const targetUrl = encodeURIComponent(url);
  const requestUrl = `http://api.scrape.do/?url=${targetUrl}&token=${SCRAPEDO_TOKEN}`;
  const isRetryable = (err: any): boolean => {
    const status = err?.response?.status;
    const code = err?.code;
    if ([408, 429, 500, 502, 503, 504].includes(status)) return true;
    if (["ECONNRESET", "ETIMEDOUT", "ECONNABORTED", "EAI_AGAIN"].includes(code)) return true;
    return false;
  };
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

  let res: any = null;
  let lastErr: any = null;
  const rateLimitBackoffsMs = [60_000, 180_000, 300_000];
  let rateLimitRetries = 0;
  const maxAttempts = 6;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      res = await axios.get(requestUrl, {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
          Accept: "text/html,application/xhtml+xml",
          "Accept-Language": "en-US,en;q=0.9",
        },
        timeout: 60_000,
      });
      lastErr = null;
      break;
    } catch (err) {
      lastErr = err;
      const status = (err as any)?.response?.status;
      if (!isRetryable(err) || attempt === maxAttempts) break;

      if (status === 429) {
        if (rateLimitRetries >= rateLimitBackoffsMs.length) break;
        const waitMs = rateLimitBackoffsMs[rateLimitRetries];
        rateLimitRetries++;
        console.warn(
          `[WARN] fetchHtml rate-limited (429) for ${url}; retrying in ${Math.round(waitMs / 60_000)} minute(s)`
        );
        await sleep(waitMs);
        continue;
      }

      const backoffMs = 600 * attempt;
      console.warn(
        `[WARN] fetchHtml retry ${attempt}/${maxAttempts} for ${url}: ${status || (err as any)?.code || (err as any)?.message || "error"}`
      );
      await sleep(backoffMs);
    }
  }
  if (lastErr) throw lastErr;
  if (!res) throw new Error(`Failed to fetch HTML for ${url}`);

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

function extractUpc(product: any, jsonLdProduct: any): string | null {
  const candidates = [
    pickString(product?.upc),
    pickString(product?.upc12),
    pickString(product?.primaryUpc),
    pickString(product?.gtin),
    pickString(product?.gtin12),
    pickString(product?.gtin13),
    pickString(jsonLdProduct?.gtin12),
    pickString(jsonLdProduct?.gtin13),
    pickString(jsonLdProduct?.sku),
  ].filter(Boolean) as string[];
  const numeric = candidates
    .map((c) => c.replace(/\D/g, ""))
    .find((c) => c.length === 12 || c.length === 13);
  if (!numeric) return null;
  return numeric.length === 13 ? numeric.slice(1) : numeric;
}

function extractBrand(product: any, jsonLdProduct: any): string | null {
  const fromLd = jsonLdProduct?.brand;
  if (typeof fromLd === "string") return cleanText(fromLd);
  if (fromLd && typeof fromLd === "object") return cleanText(fromLd.name || null);
  return cleanText(product?.brand || product?.brandName || product?.brandNameText || null);
}

function extractName(product: any, jsonLdProduct: any, $: cheerio.CheerioAPI, nextData: any): string | null {
  const fromNext = extractNameBrandFromNextData(nextData).name;
  const name = cleanText(
    fromNext ||
      product?.productName ||
      product?.name ||
      product?.title ||
      jsonLdProduct?.name ||
      $("h1").first().text() ||
      null
  );
  return name;
}

function extractIngredients(product: any, jsonLdProduct: any, $: cheerio.CheerioAPI, nextData: any): string | null {
  const candidate =
    extractIngredientsFromNextData(nextData) ||
    cleanText(product?.ingredients || product?.ingredientStatement || product?.ingredientsText || null) ||
    cleanText(jsonLdProduct?.ingredients || jsonLdProduct?.ingredientStatement || null) ||
    extractIngredientsFromHtml($);
  if (!candidate) return null;
  return candidate.replace(/^ingredients:?\s*/i, "");
}

function extractAllergenStatement(product: any, jsonLdProduct: any, $: cheerio.CheerioAPI, nextData: any): string | null {
  // Walmart PDPs we scrape do not provide a reliable allergen statement field.
  // Skip extraction to avoid incorrectly mapping other sections (e.g., key item features).
  void product;
  void jsonLdProduct;
  void $;
  void nextData;
  return null;
}

function extractNutrition(product: any, jsonLdProduct: any, $: cheerio.CheerioAPI): Nutrition | null {
  const fromProduct = nutritionFromProductData(product);
  if (fromProduct) return fromProduct;
  const ldNutrition = jsonLdProduct?.nutrition || jsonLdProduct?.nutritionInformation || null;
  const fromLd = nutritionFromJsonLd(ldNutrition);
  if (fromLd) return fromLd;

  const text = cleanText($("body").text() || "");
  if (text && /nutrition facts/i.test(text)) {
    if (DEBUG_WM) console.log("[DEBUG] nutrition text fallback triggered but no structured data parsed");
  }
  return null;
}

async function submitNutritionParseJob(imageUrl: string, mode?: string): Promise<string | null> {
  try {
    if (DEBUG_WM) {
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
    if (DEBUG_WM) {
      console.log("[DEBUG] submit nutrition parse response:");
      console.log(JSON.stringify(res?.data ?? null, null, 2));
    }
    return typeof jobId === "string" ? jobId : null;
  } catch (err) {
    if (DEBUG_WM) console.log("[DEBUG] nutrition API error:", err);
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
      if (DEBUG_WM) {
        console.log(`[DEBUG] nutrition parse status: ${status}`);
      }
      if (status === "completed") return res?.data?.result ?? null;
      if (status === "failed") return null;
    } catch (err) {
      if (DEBUG_WM) console.log("[DEBUG] nutrition poll error:", err);
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
  if (DEBUG_WM) {
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
    if (DEBUG_WM) {
      console.log("[DEBUG] nutrition api (ocr_only) result:");
      console.log(JSON.stringify(result ?? null, null, 2));
    }
    return typeof text === "string" ? text : null;
  } catch (err) {
    if (DEBUG_WM) console.log("[DEBUG] nutrition api (ocr_only) error:", err);
  }
  return null;
}

async function detectNutritionImageFromCarousel(imageUrls: string[]): Promise<string | null> {
  if (DEBUG_WM) {
    console.log(`[DEBUG] scanning ${imageUrls.length} carousel images for nutrition label`);
  }
  for (const url of imageUrls) {
    const stripped = stripQueryParams(url);
    if (!stripped) continue;
    const ocrText = await fetchNutritionOcrText(stripped);
    if (DEBUG_WM) {
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

async function scrapeProduct(url: string): Promise<ScrapedProduct | null> {
  const html = await fetchHtml(url);
  const $ = cheerio.load(html);

  const jsonLd = extractJsonLd($);
  const jsonLdProduct = jsonLd.find((o) => o && (o["@type"] === "Product" || o["@type"] === "ProductGroup")) || null;

  const nextData = extractNextData(html, $);
  const redux = extractReduxState(html, $);
  const productId = extractProductIdFromUrl(url);
  const product = extractProductFromData(nextData, redux, productId);

  const packCheck = shouldSkipForPackSize(nextData);
  if (packCheck.skip) {
    if (DEBUG_WM) console.log(`[DEBUG] skip pack size: ${packCheck.reason}`);
    return {
      productUrl: url,
      sourceId: url,
      name: null,
      brand: null,
      ingredients: null,
      allergenStatement: null,
      upc12: null,
      nutrition: null,
      nutritionData: null,
      nutritionImageUrl: null,
      imageUrl: null,
      packSizeSkipped: true,
      packSizeSkipReason: packCheck.reason || null,
      scrapedAt: new Date().toISOString(),
      sourceCreatedAt: null,
      sourceLastUpdatedAt: null,
    };
  }

  if (DEBUG_WM) {
    console.log(`[DEBUG] url=${url}`);
    console.log(`[DEBUG] productId=${productId || "(none)"}`);
    console.log(`[DEBUG] nextData=${nextData ? "yes" : "no"} redux=${redux ? "yes" : "no"} product=${product ? "yes" : "no"}`);
    if (!nextData && !redux) {
      try {
        const dumpPath = path.join("/tmp", `walmart-${productId || "page"}.html`);
        fs.writeFileSync(dumpPath, html);
        console.log(`[DEBUG] saved html to ${dumpPath}`);
      } catch (e) {
        console.log(`[DEBUG] failed to save html dump:`, e);
      }
    }
  }

  const name = normalizeProductName(extractName(product, jsonLdProduct, $, nextData));
  const nextBrand = extractNameBrandFromNextData(nextData).brand;
  const brand = cleanText(nextBrand || extractBrand(product, jsonLdProduct) || "Walmart") || "Walmart";
  const ingredients = extractIngredients(product, jsonLdProduct, $, nextData);
  const allergenStatement = extractAllergenStatement(product, jsonLdProduct, $, nextData);
  const nutrition = extractNutrition(product, jsonLdProduct, $);
  let nutritionImageUrl = stripQueryParams(cleanText(extractImageFromNextData(nextData)));
  const carouselImages = extractAllImagesFromNextData(nextData);
  if (!nutritionImageUrl && carouselImages.length > 0) {
    if (DEBUG_WM) console.log("[DEBUG] no nutrition image in specs; probing carousel images via OCR");
    nutritionImageUrl = await detectNutritionImageFromCarousel(carouselImages);
  }
  const imageUrl = cleanText(
    extractProductImageFromNextData(nextData) ||
      product?.imageUrl ||
      product?.image ||
      jsonLdProduct?.image ||
      extractOgImage($) ||
      null
  );
  const upc12 = extractUpcFromNextData(nextData) || extractUpc(product, jsonLdProduct);
  const nutritionData = nutritionImageUrl ? await fetchNutritionFromImage(nutritionImageUrl) : null;

  if (DEBUG_WM) {
    console.log("[DEBUG] parsed fields:");
    console.log(`[DEBUG] name=${name || "(none)"}`);
    console.log(`[DEBUG] brand=${brand || "(none)"}`);
    console.log(`[DEBUG] upc12=${upc12 || "(none)"}`);
    console.log(`[DEBUG] ingredients=${ingredients || "(none)"}`);
    console.log(`[DEBUG] allergens=${allergenStatement || "(none)"}`);
    console.log(`[DEBUG] imageUrl=${imageUrl || "(none)"}`);
    console.log(`[DEBUG] nutritionImageUrl=${nutritionImageUrl || "(none)"}`);
    console.log(`[DEBUG] nutrition=${nutritionData ? "image-api" : nutrition ? "yes" : "no"}`);
    if (!ingredients) {
      const root = extractNextDataRoot(nextData);
      const namedIngredients = findNamedValue(root, "Ingredients");
      if (namedIngredients) console.log(`[DEBUG] named Ingredients value=${namedIngredients.slice(0, 160)}${namedIngredients.length > 160 ? "…" : ""}`);
    }
    const root = extractNextDataRoot(nextData);
    const namedNutritionImage = findNamedValue(root, "Nutrition facts label image");
    if (namedNutritionImage) console.log(`[DEBUG] named Nutrition image=${namedNutritionImage}`);
  }

  const sourceCreatedAt = cleanText(product?.createdAt || product?.createdAtUtc || product?.created || null);
  const sourceLastUpdatedAt = cleanText(product?.updatedAt || product?.lastUpdated || product?.lastUpdateTime || null);

  const sourceId = url;

  return {
    productUrl: url,
    sourceId,
    name,
    brand,
    ingredients,
    allergenStatement,
    upc12,
    upcs: upc12 ? [upc12] : undefined,
    nutrition,
    nutritionData,
    nutritionImageUrl,
    imageUrl: typeof imageUrl === "string" ? imageUrl : null,
    scrapedAt: new Date().toISOString(),
    sourceCreatedAt: sourceCreatedAt || null,
    sourceLastUpdatedAt: sourceLastUpdatedAt || null,
  };
}

async function getServiceToken(): Promise<string> {
  if (serviceTokenCache) return serviceTokenCache;
  const param = await getApiKeysParam();
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
    if (DEBUG_WM) console.log("[DEBUG] product exists check failed:", err?.message || err);
    return false;
  }
}

async function getApiKeysParam(): Promise<Record<string, any>> {
  if (apiKeysCache) return apiKeysCache;
  const cmd = new GetParameterCommand({ Name: API_KEYS_PARAMETER_NAME, WithDecryption: true });
  const res = await ssmClient.send(cmd);
  const param = JSON.parse(res.Parameter?.Value || "{}");
  apiKeysCache = param;
  return param;
}

async function getFailureDbClient(): Promise<PgClient | null> {
  if (failureDbClient) return failureDbClient;
  try {
    let url = SCRAPER_FAILURES_DB_URL;
    if (!url) {
      const param = await getApiKeysParam();
      url = param.SupabaseDbUrl || "";
    }
    if (!url) return null;
    const client = new PgClient({ connectionString: url });
    await client.connect();
    failureDbClient = client;
    return client;
  } catch (err) {
    if (DEBUG_WM) console.log("[DEBUG] failed to init failure DB client:", err);
    return null;
  }
}

async function recordFailure(url: string, reason: string, failureType?: string): Promise<void> {
  try {
    const client = await getFailureDbClient();
    if (!client) return;
    await client.query(
      "INSERT INTO scraper_failures (id, scraper, url, reason, failure_type, occurred_at) VALUES ($1, $2, $3, $4, $5, NOW())",
      [uuidv4(), SCRAPER_NAME, url, reason, failureType || null]
    );
  } catch (err) {
    if (DEBUG_WM) console.log("[DEBUG] recordFailure error:", err);
  }
}

async function submitProductForReview(productOutput: ScraperProductOutput): Promise<boolean> {
  if (!productOutput.product_name || !productOutput.ingredients_text) return false;
  try {
    const token = await getServiceToken();
    const { scraper_job_id: _, ...body } = productOutput as any;
    if (DEBUG_WM) {
      console.log("[DEBUG] submit body:");
      console.log(JSON.stringify(body, null, 2));
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

function resolveSearchPageUrl(baseUrl: string, page: number): string {
  try {
    const u = new URL(baseUrl);
    if (page > 1) u.searchParams.set("page", String(page));
    else u.searchParams.delete("page");
    return u.toString();
  } catch {
    if (page > 1) return `${baseUrl}${baseUrl.includes("?") ? "&" : "?"}page=${page}`;
    return baseUrl;
  }
}

function extractSearchUrlsFromHtml(html: string): string[] {
  const $ = cheerio.load(html);
  const out = new Set<string>();

  const scripts = $('script[type="application/ld+json"]');
  scripts.each((_, el) => {
    const raw = $(el).text();
    if (!raw) return;
    try {
      const parsed = JSON.parse(raw);
      const list = parsed?.mainEntity?.itemListElement;
      if (Array.isArray(list)) {
        for (const item of list) {
          const url = item?.url;
          if (typeof url === "string" && url.includes("/ip/")) {
            out.add(url.split("?")[0]);
          }
        }
      }
    } catch {
      // ignore
    }
  });

  return Array.from(out);
}

async function discoverFromSearchUrl(searchUrl: string, limit?: number): Promise<string[]> {
  const urls: string[] = [];
  let page = 1;
  console.log(`\n[DISCOVER] Search Seed: ${searchUrl}`);

  while ((!limit || urls.length < limit) && page <= MAX_SEARCH_PAGES) {
    const pageUrl = resolveSearchPageUrl(searchUrl, page);
    console.log(`[DISCOVER] Search Page ${page}/${MAX_SEARCH_PAGES}: ${pageUrl}`);
    let html: string;
    try {
      html = await fetchHtml(pageUrl);
    } catch (err: any) {
      const status = err?.response?.status;
      const msg = status ? `HTTP ${status}` : err?.message || "unknown fetch error";
      console.warn(`[WARN] search discovery page failed (${msg}) for ${pageUrl}; stopping this search URL`);
      break;
    }
    const found = extractSearchUrlsFromHtml(html);
    if (found.length === 0) {
      console.log(`[DISCOVER] Search Page ${page}: found 0 URLs (stop)`);
      break;
    }
    urls.push(...found);
    console.log(`[DISCOVER] Search Page ${page}: found ${found.length} URLs, total ${urls.length}`);
    page++;
  }

  if (page > MAX_SEARCH_PAGES) {
    console.log(`[DISCOVER] Reached max page limit (${MAX_SEARCH_PAGES}) for ${searchUrl}`);
  }

  return urls;
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
  let local = false;
  let debug = false;
  let concurrency = 5;
  let offset = 0;

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

  return { url, configPath, limit, local, concurrency, offset, debug };
}

async function main(): Promise<void> {
  const { url, configPath, limit, local, concurrency, offset, debug } = parseArgs();

  DEBUG_WM = debug;

  if (local) {
    console.log("Running in local mode: skipping DynamoDB and S3; API submission still runs.");
  }
  if (limit != null) {
    console.log(`Limit: processing at most ${limit} products.`);
  }
  if (offset > 0) {
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
  let searchUrls: string[] = [];
  if (url) {
    urls = [url];
  } else {
    try {
      const raw = await fs.readFileSync(configPath, "utf-8");
      const cfg = JSON.parse(raw) as AppConfig;
      urls = (cfg.urls || []).filter((u) => typeof u === "string");
      searchUrls = (cfg.searchUrls || []).filter((u) => typeof u === "string");
    } catch {
      // ignore
    }
  }

  if (searchUrls.length === 0) {
    if (offset > 0 && urls.length > 0) urls = urls.slice(offset);
    if (limit != null) urls = urls.slice(0, limit);
    if (urls.length === 0) {
      console.error("No Walmart product URLs found. Provide --url or config.json with urls.");
      process.exit(1);
    }
    console.log(`Processing ${urls.length} product URLs`);
  } else {
    console.log(`Processing ${searchUrls.length} search seeds incrementally`);
  }

  const results: ScraperProductOutput[] = [];
  const submitBatch: ScraperProductOutput[] = [];
  const SUBMIT_BATCH_SIZE = 10;
  let scrapedCount = 0;
  let skippedCount = 0;
  let success = 0;
  let fail = 0;
  let plannedTotal = 0;
  let seenDiscovered = 0;
  let selectedDiscovered = 0;

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

  const processProductUrls = async (batchUrls: string[]) => {
    if (!batchUrls.length) return;
    plannedTotal += batchUrls.length;
    const queue = [...batchUrls];
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
          await recordFailure(u, "fetch or parse error", "fetch_error");
          skippedCount++;
          continue;
        }
        if (!p) {
          skippedCount++;
          continue;
        }
        if (p.packSizeSkipped) {
          const reason = p.packSizeSkipReason || "not smallest pack size variant selected";
          console.log(`[SKIP] ${p.productUrl}: ${reason}`);
          await recordFailure(p.productUrl, reason, "variant_not_smallest");
          skippedCount++;
          continue;
        }
        if (!p.nutritionImageUrl) {
          console.log(`Note: ${p.productUrl} has no nutrition image`);
        }
        if (p.nutritionImageUrl && !p.nutritionData) {
          console.log(`[SKIP] ${p.productUrl}: nutrition image parse failed`);
          await recordFailure(p.productUrl, "nutrition image parse failed", "nutrition_parse_failed");
          skippedCount++;
          continue;
        }
        const hasIngredients = Boolean(p.ingredients && p.ingredients.trim());
        if (!p.name || !hasIngredients) {
          const reasons: string[] = [];
          if (!p.name) reasons.push("missing name");
          if (!hasIngredients) reasons.push("missing ingredients");
          console.log(`[SKIP] ${p.productUrl}: ${reasons.join(", ")}`);
          await recordFailure(p.productUrl, reasons.join(", "), "missing_fields");
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
        console.log(`[PRODUCT] OK | ${output.product_name || "(no name)"} | ${p.productUrl}`);
        const out = { ...output, scraper_job_id: jobId };
        results.push(out);
        submitBatch.push(out);
        if (submitBatch.length >= SUBMIT_BATCH_SIZE) void queueFlush(false);
      }
    });
    await Promise.all(workers);
  };

  const selectDiscoveredUrls = (found: string[]): string[] => {
    const selected: string[] = [];
    for (const u of found) {
      if (seenDiscovered < offset) {
        seenDiscovered++;
        continue;
      }
      if (limit != null && selectedDiscovered >= limit) break;
      selected.push(u);
      selectedDiscovered++;
      seenDiscovered++;
    }
    return selected;
  };

  if (searchUrls.length > 0) {
    for (const s of searchUrls) {
      if (limit != null && selectedDiscovered >= limit) break;
      let found: string[] = [];
      try {
        found = await discoverFromSearchUrl(s);
      } catch (err: any) {
        console.warn(`[WARN] search URL failed: ${s} (${err?.message || "unknown error"})`);
      }
      const selected = selectDiscoveredUrls(found);
      console.log(`[DISCOVER] Search Seed Selected: ${selected.length} product URLs`);
      if (selected.length > 0) {
        await processProductUrls(selected);
        await queueFlush(true);
        await flushChain;
      }
    }
  } else {
    await processProductUrls(urls);
  }

  await queueFlush(true);
  await flushChain;

  if (!local) {
    await uploadToS3(results, jobId, runDateTime);
  }

  console.log(`\nScraped ${scrapedCount} valid products (skipped ${skippedCount}) out of ${plannedTotal} total`);
  console.log(`\n📊 API: ${success} submitted, ${fail} failed`);

  if (!local && SCRAPER_JOB_STATUS_TABLE_NAME) {
    if (results.length === 0) {
      await updateJobStatus(jobId, "error", "No products processed");
      process.exit(1);
    } else {
      await updateJobStatus(jobId, "complete");
    }
  }

  if (failureDbClient) {
    await failureDbClient.end().catch(() => null);
    failureDbClient = null;
  }

}

main().catch((err) => {
  console.error("Error:", err);
  if (failureDbClient) {
    void failureDbClient.end().catch(() => null);
    failureDbClient = null;
  }
  process.exit(1);
});
