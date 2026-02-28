const parseServingSizeFromText =
  (servingSizeUtils as any).parseServingSizeFromText ??
  (servingSizeUtils as any).default?.parseServingSizeFromText;
/**
 * Trader Joe's product processor
 *
 * Fetches Trader Joe's product pages, generates UPCs from SKU (add 0 before SKU, append 0-9),
 * transforms to ScraperProductOutput, and submits via submit-product-for-review API.
 *
 * Usage:
 *   npx tsx scrape.ts [--url <PRODUCT_URL>] [--config ./config.json] [--limit N] [--local]
 *
 * --url         Trader Joe's product URL to scrape
 * --config      Path to config with URLs (default: ./config.json)
 * --limit N      Process at most N products
 * --local        Skip DynamoDB job status and S3 upload; API submission still runs
 */
import axios from "axios";
import * as fs from "fs/promises";
import * as path from "path";
import { fileURLToPath } from "url";
import * as cheerio from "cheerio";
import { chromium } from "playwright";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import * as servingSizeUtils from "../serving-size-utils";
import * as nameUtils from "../name-utils";
import * as productIdUtils from "../product-id-utils";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const cleanProductName =
  (nameUtils as any).cleanProductName ?? (nameUtils as any).default?.cleanProductName;
const generateDeterministicProductId =
  (productIdUtils as any).generateDeterministicProductId ??
  (productIdUtils as any).default?.generateDeterministicProductId;

interface TraderJoesProduct {
  sku: string;
  item_title: string;
  source_url?: string;
  ingredients: Array<{ ingredient: string }>;
  allergen_statement?: string;
  nutrition?: Array<{
    serving_size?: string;
    [key: string]: unknown;
  }>;
  serving_size_value?: number;
  serving_size_unit?: string;
  nutrition_facts?: {
    serving_size_text?: string;
    servings_per_container?: string;
    calories?: number;
    nutrients: Array<{ name: string; amount: string | null; dailyValuePercent: string | null }>;
  };
  source_created_at?: string;
  source_last_updated_at?: string;
  primary_image_meta?: { url?: string };
  [key: string]: unknown;
}

type TraderJoesConfig = {
  urls?: string[];
  searchUrls?: string[];
};

// AWS clients
const s3Client = new S3Client({});
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);
const ssmClient = new SSMClient({});

const SCRAPER_NAME = process.env.JOB_NAME || "trader-joes";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://api.mytummi.app";
const PRODUCTS_API_URL = `${API_BASE_URL}/products`;
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
let DEBUG_TJ = false;
let TJ_HEADLESS = process.env.TJ_HEADLESS !== "0";
const TJ_USER_DATA_DIR = process.env.TJ_USER_DATA_DIR;

let serviceTokenCache: string | null = null;

async function getServiceToken(): Promise<string> {
  if (serviceTokenCache) return serviceTokenCache;
  const command = new GetParameterCommand({
    Name: API_KEYS_PARAMETER_NAME,
    WithDecryption: true,
  });
  const response = await ssmClient.send(command);
  if (!response.Parameter?.Value) {
    throw new Error(`Parameter "${API_KEYS_PARAMETER_NAME}" not found`);
  }
  const parameter = JSON.parse(response.Parameter.Value);
  serviceTokenCache = parameter.InternalServiceToken;
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
  } catch (e: any) {
    if (e?.response?.status === 404) return false;
    if (DEBUG_TJ) console.log("[DEBUG] product exists check failed:", e?.message || e);
    return false;
  }
}

/**
 * Generate UPCs from SKU: add 0 before SKU, then generate 10 UPCs with last digit 0-9.
 * Example: SKU 123456 -> 01234560, 01234561, ..., 01234569
 */
function generateUpcsFromSku(sku: string): string[] {
  const base = "0" + String(sku).trim();
  return Array.from({ length: 10 }, (_, i) => base + i);
}

function normalizeUnit(unit: string): string {
  if (!unit) return unit;
  const map: Record<string, string> = {
    g: "g", gram: "g", grams: "g", kg: "kg", oz: "oz", ounce: "oz", ounces: "oz",
    lb: "lb", pound: "lb", ml: "ml", l: "L", liter: "L", "fl oz": "fl oz",
    cup: "cup", tbsp: "tbsp", tsp: "tsp",
  };
  return map[unit.toLowerCase().trim()] ?? unit;
}

function parseServingSize(servingSizeText: string | null): { value: number | null; unit: string | null } {
  if (typeof parseServingSizeFromText !== "function") {
    throw new Error("parseServingSizeFromText import failed");
  }
  return parseServingSizeFromText(servingSizeText);
}


function combineIngredients(ingredients: Array<{ ingredient: string }>): string {
  if (!ingredients?.length) return "";
  return ingredients.map((i) => i.ingredient).join(", ");
}

function cleanIngredientsText(text: string): string {
  if (!text) return "";
  const withoutMayContain = text.replace(/\bmay contain\b[^.]*\.?/gi, " ");
  return withoutMayContain.replace(/\s+/g, " ").replace(/\s*\.$/, "").trim();
}

function extractSkuFromUrl(url: string): string | null {
  const last = url.split("/").filter(Boolean).pop() || "";
  const m = last.match(/-(\d{4,})$/);
  return m?.[1] ?? null;
}

function extractSkuFromHtml(html: string): string | null {
  const skuMatch = html.match(/"sku"\s*:\s*"(\d+)"/i);
  if (skuMatch) return skuMatch[1];
  const codeMatch = html.match(/"productCode"\s*:\s*"(\d+)"/i);
  if (codeMatch) return codeMatch[1];
  return null;
}

function pickProductImage(image: unknown): string | undefined {
  if (typeof image === "string") return image;
  if (Array.isArray(image)) {
    const urls = image.filter((u) => typeof u === "string") as string[];
    const productsUrl = urls.find((u) => u.includes("/products/"));
    if (productsUrl) return productsUrl;
    if (urls.length >= 2) return urls[1];
    return urls[0];
  }
  return undefined;
}

function extractJsonLd(html: string): Record<string, unknown>[] {
  const jsons: Record<string, unknown>[] = [];
  const $ = cheerio.load(html);
  $('script[type="application/ld+json"]').each((_, el) => {
    const txt = $(el).contents().text();
    if (!txt) return;
    try {
      const data = JSON.parse(txt);
      if (Array.isArray(data)) {
        for (const item of data) {
          if (item && typeof item === "object") jsons.push(item as Record<string, unknown>);
        }
      } else if (data && typeof data === "object") {
        jsons.push(data as Record<string, unknown>);
      }
    } catch {
      // ignore
    }
  });
  return jsons;
}

function extractSectionText($: cheerio.CheerioAPI, label: string): string | null {
  const header = $(`*:contains("${label}")`)
    .filter((_, el) => $(el).text().trim().toLowerCase() === label.toLowerCase())
    .first();
  if (!header.length) return null;
  const next = header.next();
  if (next.length) {
    const text = next.text().trim();
    if (text) return text;
  }
  const parentNext = header.parent().next();
  if (parentNext.length) {
    const text = parentNext.text().trim();
    if (text) return text;
  }
  return null;
}

function extractIngredientsFromSection($: cheerio.CheerioAPI): string | null {
  const listItems = $("li.IngredientsList_ingredientsList__item__1VrRy");
  if (listItems.length) {
    const parts = listItems
      .map((_, el) => $(el).text().trim())
      .get()
      .filter(Boolean);
    if (parts.length) return parts.join(", ");
  }

  const header = $(`*:contains("Ingredients")`)
    .filter((_, el) => $(el).text().trim().toLowerCase() === "ingredients")
    .first();
  if (!header.length) return null;

  const parts: string[] = [];
  const stopPattern = /(nutrition facts|may contain)/i;
  const stopTags = new Set(["h1", "h2", "h3", "h4", "h5", "h6"]);

  const siblings = header.parent().nextAll();
  siblings.each((_, el) => {
    const tag = (el as HTMLElement).tagName?.toLowerCase?.() || "";
    const text = $(el).text().trim();
    if (!text) return;
    if (stopTags.has(tag) || /nutrition facts/i.test(text)) return false;
    if (/may contain/i.test(text)) return false;

    const listItems = $(el).find("li");
    if (listItems.length) {
      listItems.each((__, li) => {
        const itemText = $(li).text().trim();
        if (itemText && !stopPattern.test(itemText)) parts.push(itemText);
      });
      return;
    }

    if (!stopPattern.test(text)) parts.push(text);
  });

  if (!parts.length) return null;
  return parts.join(", ");
}

function extractIngredientsAndAllergensFromSummary(
  $: cheerio.CheerioAPI
): { ingredients: string | null; allergens: string | null } {
  const container = $("[class*='IngredientsSummary_ingredientsSummary__']").first();
  if (!container.length) return { ingredients: null, allergens: null };

  const allergenParts = container
    .find("[class*='allergensList'] li, [class*='allergensListItem']")
    .map((_, el) => $(el).text().trim())
    .get()
    .filter(Boolean);

  const clone = container.clone();
  clone.find("[class*='allergensList']").remove();
  const ingredientListParts = clone
    .find("[class*='IngredientsList_ingredientsList'] li, [class*='IngredientsList_ingredientsList__item']")
    .map((_, el) => $(el).text().trim())
    .get()
    .filter(Boolean);

  const ingredientsText =
    (ingredientListParts.length > 0
      ? ingredientListParts.join(", ")
      : clone.text().replace(/\s+/g, " ").trim()) || null;

  const allergensText =
    allergenParts.length > 0
      ? allergenParts
          .map((part) => {
            const trimmed = part.trim();
            return /[.!?]$/.test(trimmed) ? trimmed : `${trimmed}.`;
          })
          .join(" ")
          .trim()
      : null;

  return { ingredients: ingredientsText, allergens: allergensText };
}

function extractNutritionFromText(text: string): TraderJoesProduct["nutrition_facts"] | null {
  if (!text) return null;
  const cleanedAll = text.replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
  if (!/Nutrition Facts/i.test(cleanedAll)) return null;
  const pickBlock = (label: string): string | null => {
    const idx = cleanedAll.toLowerCase().indexOf(label.toLowerCase());
    if (idx === -1) return null;
    return cleanedAll.slice(idx + label.length).trim();
  };

  let cleaned =
    pickBlock("As packaged") ||
    pickBlock("Per serving") ||
    (() => {
      const parts = cleanedAll.split(/Nutrition Facts/i);
      return parts.length > 1 ? parts[1] : cleanedAll;
    })();

  const servingSize =
    cleaned.match(/serving size\s*([^\n\r]+?)(?:calories per serving|amount|%dv|$)/i)?.[1]?.trim() ?? null;
  const servingsPerContainer =
    cleaned.match(/serves?\s*about\s*([^\n\r]+?)(?:serving size|calories per serving|amount|%dv|$)/i)?.[1]?.trim() ??
    null;
  const calories =
    cleaned.match(/calories per serving\s*(\d+)/i)?.[1] ??
    cleaned.match(/calories\s*(\d+)/i)?.[1] ??
    null;

  const nutrients: Array<{ name: string; amount: string | null; dailyValuePercent: string | null }> = [];
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
    "Includes",
    "Protein",
    "Vitamin D",
    "Calcium",
    "Iron",
    "Potassium",
  ];

  const lines = cleaned
    .split(/(?=Total Fat|Saturated Fat|Trans Fat|Cholesterol|Sodium|Total Carbohydrate|Dietary Fiber|Total Sugars|Added Sugars|Includes|Protein|Vitamin D|Calcium|Iron|Potassium)/i)
    .map((l) => l.trim())
    .filter(Boolean);

  for (const line of lines) {
    for (const name of names) {
      const escaped = name.replace(/\s+/g, "\\s+");
      const re = new RegExp(
        `^${escaped}\\s*([0-9][0-9.,]*\\s*[a-zA-ZµmcgMG]+)?\\s*(\\d+%)?`,
        "i"
      );
      const m = line.match(re);
      if (!m) continue;
      const label = name.toLowerCase() === "includes" ? "Added Sugars" : name;
      nutrients.push({
        name: label,
        amount: m[1] ? m[1].trim() : null,
        dailyValuePercent: m[2] ? m[2].trim() : null,
      });
      break;
    }
  }

  return {serving_size_text: servingSize ?? undefined,
    servings_per_container: servingsPerContainer ?? undefined,
    calories: calories ? parseInt(calories, 10) : undefined,
    nutrients};
}

function extractNutritionFromDom($: cheerio.CheerioAPI): TraderJoesProduct["nutrition_facts"] | null {
  const container = $(".NutritionFacts_nutritionFacts__1Nvz0");
  if (!container.length) return null;

  let activeBlock: cheerio.Cheerio<cheerio.Element> | null = null;
  container.find("div[style*='display: block']").each((_, el) => {
    const block = $(el);
    if (block.find(".Item_item__2z0x3").length) {
      activeBlock = block;
      return false;
    }
    return undefined;
  });
  if (!activeBlock) {
    const firstItem = container.find(".Item_item__2z0x3").first();
    if (firstItem.length) activeBlock = firstItem.parent();
  }
  if (!activeBlock) return null;

  const facts: TraderJoesProduct["nutrition_facts"] = { nutrients: [] };

  activeBlock.find(".Item_characteristics__item__2TgL-").each((_, el) => {
    const title = $(el).find(".Item_characteristics__title__7nfa8").text().trim().toLowerCase();
    const value = $(el).find(".Item_characteristics__text__dcfEC").text().trim();
    if (!title) return;
    if (title.includes("serves")) facts.servings_per_container = value || undefined;
    if (title === "serving size") facts.serving_size_text = value || undefined;
    if (title.includes("calories")) {
      const m = value.match(/(\d+)/);
      if (m) facts.calories = parseInt(m[1], 10);
    }
  });

  activeBlock.find(".Item_table__body__32J7y tr").each((_, el) => {
    const name = $(el).find("th").first().text().trim();
    const tds = $(el).find("td");
    if (!name || tds.length === 0) return;
    const amount = $(tds[0]).text().trim() || null;
    const dv = tds.length > 1 ? ($(tds[1]).text().trim() || null) : null;
    const label = name.toLowerCase() === "includes" ? "Added Sugars" : name;
    facts.nutrients.push({ name: label, amount, dailyValuePercent: dv });
  });

  return facts.nutrients.length ? facts : null;
}

const NUTRIENT_COLUMN_MAP: Record<string, string> = {
  "total fat": "total_fat_g",
  "saturated fat": "saturated_fat_g",
  "trans fat": "trans_fat_g",
  cholesterol: "cholesterol_mg",
  sodium: "sodium_mg",
  "total carbohydrate": "total_carbs_g",
  "dietary fiber": "fiber_g",
  "total sugars": "sugars_g",
  "added sugars": "added_sugars_g",
  protein: "protein_g",
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
  calcium: "calcium_mg",
  iron: "iron_mg",
  potassium: "potassium_mg",
  magnesium: "magnesium_mg",
  phosphorus: "phosphorus_mg",
  zinc: "zinc_mg",
};

function mapNutrientToColumn(name: string): string | null {
  const lower = name.toLowerCase().trim();
  for (const [key, col] of Object.entries(NUTRIENT_COLUMN_MAP)) {
    if (lower.includes(key)) return col;
  }
  return null;
}

async function fetchHtml(url: string): Promise<string> {
  const launchOpts = {
    headless: TJ_HEADLESS,
  };
  const context = TJ_USER_DATA_DIR
    ? await chromium.launchPersistentContext(TJ_USER_DATA_DIR, {
        ...launchOpts,
        userAgent:
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        locale: "en-US",
        viewport: { width: 1280, height: 720 },
        extraHTTPHeaders: {
          "accept-language": "en-US,en;q=0.9",
          "cache-control": "no-cache",
        },
      })
    : await chromium.launch({ ...launchOpts });

  const page = await context.newPage();
  await page.setExtraHTTPHeaders({
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "no-cache",
  });
  try {
    await page.goto("https://www.traderjoes.com/home", { waitUntil: "domcontentloaded", timeout: 60_000 }).catch(() => {});
    await page.waitForTimeout(1500);
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(2000);
    const perServingTab = page.locator(
      [
        "button:has-text('Per serving')",
        "[role='tab']:has-text('Per serving')",
        "button:has-text('As packaged')",
        "button.Nav_nav__1fRnP:has-text('As packaged')",
        "[role='tab']:has-text('As packaged')",
      ].join(", ")
    );
    if (await perServingTab.count()) {
      await perServingTab.first().click({ timeout: 2000 }).catch(() => null);
      await page.waitForTimeout(1200);
      if (DEBUG_TJ) {
        const active = await page
          .locator("button.Nav_active__m3-rZ")
          .first()
          .innerText()
          .catch(() => "");
        console.log(`[DEBUG] active nutrition tab: ${active || "(unknown)"}`);
      }
    }
    const html = await page.content();
    if (/Access Denied/i.test(html)) {
      if (DEBUG_TJ) console.log("[DEBUG] access denied in playwright content, retrying once...");
      await page.waitForTimeout(3000);
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60_000 });
      await page.waitForTimeout(2000);
      return await page.content();
    }
    return html;
  } finally {
    await page.close().catch(() => null);
    await context.close().catch(() => null);
  }
}

async function fetchProduct(url: string): Promise<TraderJoesProduct | null> {
  const html = await fetchHtml(url);
  if (DEBUG_TJ) {
    await fs.writeFile("/tmp/traderjoes-pdp.html", html).catch(() => null);
  }

  const $ = cheerio.load(html);
  const jsons = extractJsonLd(html);
  const productJson = jsons.find((j) => j["@type"] === "Product") || jsons[0];

  const sku =
    (productJson?.sku as string | undefined) ||
    extractSkuFromHtml(html) ||
    extractSkuFromUrl(url);
  if (DEBUG_TJ) console.log(`[DEBUG] sku: ${sku ?? "(null)"}`);
  if (!sku) return null;

  const name =
    (productJson?.name as string | undefined) ||
    $('meta[property="og:title"]').attr("content") ||
    $("h1").first().text().trim();
  const cleanedName = name
    ? typeof cleanProductName === "function"
      ? cleanProductName(name, {
          stripTrailingWeight: true,
          stripTrailingCount: true,
          stripTrailingDashSize: true,
          stripParenAtEnd: true,
          decodeHtml: true,
        })
      : name
    : null;
  if (DEBUG_TJ) console.log(`[DEBUG] name: ${cleanedName ?? "(null)"}`);
  if (name && /access denied/i.test(name)) {
    if (DEBUG_TJ) console.log(`[DEBUG] access denied title for ${url}`);
    return null;
  }

  const summaryParsed = extractIngredientsAndAllergensFromSummary($);
  let ingredientsSource = "none";
  let ingredientsText = summaryParsed.ingredients || "";
  let allergenStatement = summaryParsed.allergens || null;
  if (ingredientsText) {
    ingredientsSource = "ingredients-summary";
  } else {
    ingredientsText = extractIngredientsFromSection($) || "";
    if (ingredientsText) {
      ingredientsSource = "ingredients-section";
    } else {
      ingredientsText = extractSectionText($, "Ingredients") || "";
      if (ingredientsText) {
        ingredientsSource = "section-text";
      } else if (typeof productJson?.ingredients === "string") {
        ingredientsText = productJson.ingredients as string;
        ingredientsSource = "jsonld";
      }
    }
  }
  if (DEBUG_TJ) {
    console.log(`[DEBUG] ingredients length: ${ingredientsText.length}`);
    console.log(`[DEBUG] ingredients source: ${ingredientsSource}`);
    console.log(`[DEBUG] ingredients raw start`);
    console.log(ingredientsText);
    console.log(`[DEBUG] ingredients raw end`);
  }
  const ingredients = ingredientsText
    ? ingredientsText
        .split(/,\\s*/)
        .map((i) => i.trim())
        .filter(Boolean)
        .map((ingredient) => ({ ingredient }))
    : [];
  if (!allergenStatement) {
    allergenStatement =
      extractSectionText($, "May Contain") ||
      extractSectionText($, "Allergens") ||
      extractSectionText($, "Allergen Information") ||
      null;
  }

  const nutritionFacts = extractNutritionFromDom($) || extractNutritionFromText($("body").text());

  const img =
    pickProductImage(productJson?.image) ||
    $('img[src*="/products/"]').first().attr("src") ||
    $('img[srcoriginal*="/products/"]').first().attr("srcoriginal") ||
    $('meta[property="og:image"]').attr("content") ||
    undefined;

  const product: TraderJoesProduct = {
    sku,
    item_title: cleanedName || "",
    source_url: url,
    ingredients,
    allergen_statement: allergenStatement || undefined,
    nutrition: undefined,
    serving_size_value: undefined,
    serving_size_unit: undefined,
    nutrition_facts: nutritionFacts ?? undefined,
    source_created_at: undefined,
    source_last_updated_at: undefined,
    primary_image_meta: img ? { url: img.replace("https://www.traderjoes.com", "") } : undefined,
  };

  return product;
}

function transformToScraperOutput(product: TraderJoesProduct): ScraperProductOutput {
  const now = new Date().toISOString();
  const upcs = generateUpcsFromSku(product.sku);

  let servingSizeValue: number | undefined;
  let servingSizeUnit: string | undefined;
  let servingSizeText: string | undefined;
  if (product.serving_size_value !== undefined) servingSizeValue = product.serving_size_value;
  if (product.serving_size_unit) servingSizeUnit = product.serving_size_unit;
  if (product.nutrition_facts?.serving_size_text) servingSizeText = product.nutrition_facts.serving_size_text;
  if ((servingSizeValue === undefined || !servingSizeUnit) && product.nutrition_facts?.serving_size_text) {
    const parsed = parseServingSize(product.nutrition_facts.serving_size_text);
    if (parsed) {
      servingSizeValue = parsed.value;
      servingSizeUnit = parsed.unit;
    }
  }

  const nutrition: ScraperNutritionData | undefined = product.nutrition_facts
    ? (() => {
        const result: ScraperNutritionData = {
          serving_size_value: servingSizeValue ?? 1,
          serving_size_unit_text: servingSizeUnit ?? "serving",
          serving_size_text: servingSizeText ?? null,
        };
        if (product.nutrition_facts?.calories !== undefined) result.calories = product.nutrition_facts.calories;
        for (const n of product.nutrition_facts?.nutrients ?? []) {
          const col = mapNutrientToColumn(n.name);
          if (!col) continue;
          const amtMatch = n.amount?.match(/([\d.]+)/);
          if (!amtMatch) continue;
          (result as unknown as Record<string, number>)[col] = parseFloat(amtMatch[1]);
        }
        return result;
      })()
    : undefined;

  const output: ScraperProductOutput = {
    product_name: product.item_title || "",
    brand: "Trader Joe's",
    upcs,
    ingredients_text: combineIngredients(product.ingredients || []),
    allergen_statement: product.allergen_statement || undefined,
    source: SCRAPER_NAME,
    source_id: product.source_url || product.sku,
    source_created_at: product.source_created_at || now,
    source_last_updated_at: product.source_last_updated_at || now,
    nutrition,
  };
  if (servingSizeValue !== undefined && servingSizeUnit) {
    output.serving_size_value = servingSizeValue;
    output.serving_size_unit = servingSizeUnit;
    output.serving_size_text = servingSizeText;
  }
  const imgUrl = product.primary_image_meta?.url;
  if (imgUrl) output.image_url = `http://traderjoes.com${imgUrl}`;
  return output;
}

async function submitProductForReview(productOutput: ScraperProductOutput): Promise<boolean> {
  if (!productOutput.product_name || !productOutput.ingredients_text) return false;
  try {
    const token = await getServiceToken();
    const { scraper_job_id: _, ...body } = productOutput;
    if (DEBUG_TJ) {
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
  await s3Client.send(new PutObjectCommand({
    Bucket: SCRAPER_OUTPUTS_BUCKET,
    Key: key,
    Body: JSON.stringify(results, null, 2),
    ContentType: "application/json",
  }));
  console.log(`Uploaded to s3://${SCRAPER_OUTPUTS_BUCKET}/${key}`);
}

async function updateJobStatus(jobId: string, status: string, error: string | null = null): Promise<void> {
  if (!SCRAPER_JOB_STATUS_TABLE_NAME) return;
  const now = new Date().toISOString();
  await docClient.send(new UpdateCommand({
    TableName: SCRAPER_JOB_STATUS_TABLE_NAME,
    Key: { job_id: jobId },
    UpdateExpression: "SET #status = :status, updated_at = :updated_at" + (error ? ", error = :error" : ""),
    ExpressionAttributeNames: { "#status": "status" },
    ExpressionAttributeValues: {
      ":status": status,
      ":updated_at": now,
      ...(error && { ":error": error }),
    },
  }));
}

/** Parse CLI: --url, --search, --config, --limit N, --local */
function parseArgs() {
  const argv = process.argv.slice(2);
  const defaultConfig = path.resolve(__dirname, "./config.json");
  let configPath = defaultConfig;
  let url: string | undefined;
  let searchUrl: string | undefined;
  let limit: number | undefined;
  let offset = 0;
  let concurrency = 5;
  let local = false;
  let debug = false;
  let noHeadless = false;
  let headless = false;
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
    } else if (argv[i] === "--no-headless") {
      noHeadless = true;
    } else if (argv[i] === "--headless") {
      headless = true;
    }
  }
  return { url, searchUrl, configPath, limit, offset, concurrency, local, debug, noHeadless, headless };
}

function normalizeTraderJoesUrl(href: string): string | null {
  if (!href) return null;
  if (href.startsWith("http")) return href;
  if (href.startsWith("//")) return `https:${href}`;
  if (href.startsWith("/")) return `https://www.traderjoes.com${href}`;
  if (/^(home\/)?products\/pdp\//i.test(href)) return `https://www.traderjoes.com/${href.replace(/^\/+/, "")}`;
  return null;
}

function getCategoryPageFromUrl(categoryUrl: string): number {
  try {
    const u = new URL(categoryUrl);
    const filtersRaw = u.searchParams.get("filters");
    if (!filtersRaw) return 1;
    const parsed = JSON.parse(filtersRaw) as { page?: number };
    return Number.isFinite(parsed.page) && (parsed.page as number) > 0 ? (parsed.page as number) : 1;
  } catch {
    return 1;
  }
}

function buildCategoryPageUrl(categoryUrl: string, pageNum: number): string {
  const safePage = Math.max(1, Math.floor(pageNum));
  try {
    const u = new URL(categoryUrl);
    const filtersRaw = u.searchParams.get("filters");
    const filters: Record<string, unknown> = filtersRaw ? JSON.parse(filtersRaw) : {};
    filters.page = safePage;
    u.searchParams.set("filters", JSON.stringify(filters));
    return u.toString();
  } catch {
    return categoryUrl;
  }
}

function parsePageNumberFromLabel(label: string | null): number | null {
  if (!label) return null;
  const m = label.match(/\bpage\s+(\d+)\b/i);
  if (!m) return null;
  const n = parseInt(m[1], 10);
  return Number.isFinite(n) && n > 0 ? n : null;
}

async function scrapeCategoryPage(categoryUrl: string, limit?: number): Promise<string[]> {
  const context = TJ_USER_DATA_DIR
    ? await chromium.launchPersistentContext(TJ_USER_DATA_DIR, {
        headless: TJ_HEADLESS,
        userAgent:
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        locale: "en-US",
        viewport: { width: 1280, height: 720 },
      })
    : await chromium.launch({ headless: TJ_HEADLESS });

  const page = await context.newPage();
  const seen = new Set<string>();
  let pageCount = 0;
  let currentCategoryPage = getCategoryPageFromUrl(categoryUrl);

  try {
    await page.goto("https://www.traderjoes.com/home", { waitUntil: "domcontentloaded", timeout: 60_000 }).catch(() => {});
    await page.waitForTimeout(1500);
    await page.goto(categoryUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => {});
    await page.waitForTimeout(2000);
    const initialTitle = (await page.title().catch(() => "")).trim();
    if (/access denied/i.test(initialTitle)) {
      console.error(`[DISCOVER] Access denied for category page: ${categoryUrl}`);
      return [];
    }

    const dismiss = page.locator([
      "button:has-text('Accept')",
      "button:has-text('I Agree')",
      "button[aria-label*='close' i]",
      "button:has-text('Close')",
    ].join(", ")).first();
    if (await dismiss.count()) {
      await dismiss.click({ timeout: 3000 }).catch(() => null);
      await page.waitForTimeout(500);
    }

    const collectPageLinks = async (): Promise<string[]> => {
      const productCardHits = await page.$$eval("a[href*='/home/products/pdp/']", (els) => {
        const out: string[] = [];
        for (const el of els) {
          const href = (el as HTMLAnchorElement).getAttribute("href");
          if (href && /(\/home)?\/products\/pdp\//i.test(href)) out.push(href);
        }
        return out;
      });
      const attrHits = await page.$$eval("a, [data-href], [data-url]", (els) => {
        const out: string[] = [];
        for (const el of els) {
          const href = (el as HTMLAnchorElement).getAttribute("href");
          const dataHref = el.getAttribute("data-href");
          const dataUrl = el.getAttribute("data-url");
          for (const v of [href, dataHref, dataUrl]) {
            if (v && /(\/home)?\/products\/pdp\//i.test(v)) out.push(v);
          }
        }
        return out;
      });
      const nextDataText = await page.locator("script#__NEXT_DATA__").first().textContent().catch(() => null);
      const nextDataHits: string[] = [];
      if (nextDataText) {
        for (const m of nextDataText.matchAll(/"(?:\/home)?\/products\/pdp\/[^"?#]+"/gi)) {
          nextDataHits.push(m[0].slice(1, -1));
        }
        const decoded = nextDataText.replace(/\\u002f/gi, "/");
        for (const m of decoded.matchAll(/"(?:\/home)?\/products\/pdp\/[^"?#]+"/gi)) {
          nextDataHits.push(m[0].slice(1, -1));
        }
      }
      const html = await page.content();
      const regexHits = Array.from(
        html.matchAll(/["']((?:\/home)?\/products\/pdp\/[^"'?#]+)["']/gi),
        (m) => m[1]
      );
      const escapedRegexHits = Array.from(
        html.matchAll(/\\\/(?:home\\\/)?products\\\/pdp\\\/[^"'<\\\s?#]+/gi),
        (m) => m[0].replace(/\\\//g, "/")
      );
      const unicodeEscapedHits = Array.from(
        html.replace(/\\u002f/gi, "/").matchAll(/["']((?:\/home)?\/products\/pdp\/[^"'?#]+)["']/gi),
        (m) => m[1]
      );
      if (DEBUG_TJ && pageCount === 1) {
        await fs.writeFile("/tmp/traderjoes-category-page1.html", html).catch(() => null);
        console.log(
          `[DEBUG] link sources: productCards=${productCardHits.length} attr=${attrHits.length} nextData=${nextDataHits.length} regex=${regexHits.length} escaped=${escapedRegexHits.length} unicode=${unicodeEscapedHits.length}`
        );
      }
      return [...productCardHits, ...attrHits, ...nextDataHits, ...regexHits, ...escapedRegexHits, ...unicodeEscapedHits];
    };

    let stagnantRounds = 0;
    let lastSeenCount = 0;
    while (true) {
      pageCount++;
      let links = await collectPageLinks();
      if (!links.length) {
        await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => null);
        await page.waitForTimeout(1500);
        links = await collectPageLinks();
      }
      if (!links.length) {
        const title = (await page.title().catch(() => "")).trim();
        if (/access denied/i.test(title)) {
          console.error(`[DISCOVER] Access denied while scraping: ${categoryUrl}`);
          break;
        }
      }
      for (const href of links) {
        const full = normalizeTraderJoesUrl(href);
        if (!full) continue;
        seen.add(full.split("?")[0]);
        if (limit && seen.size >= limit) break;
      }
      console.log(`[DISCOVER] page ${pageCount}: ${seen.size} product urls collected`);
      if (limit && seen.size >= limit) break;
      if (seen.size === lastSeenCount) stagnantRounds++;
      else stagnantRounds = 0;
      lastSeenCount = seen.size;
      if (stagnantRounds >= 3) {
        console.log("[DISCOVER] stopping pagination: no new product URLs after multiple pages");
        break;
      }

      const pagination = page.locator(".Pagination_pagination__2zqib").first();
      if (await pagination.count()) {
        const nextButton = pagination.locator("button[aria-label^='Next page']").first();
        if (await nextButton.count()) {
          const isDisabled = await nextButton.isDisabled().catch(() => false);
          const ariaDisabled = await nextButton.getAttribute("aria-disabled").catch(() => null);
          if (isDisabled || ariaDisabled === "true") break;

          const nextLabel = await nextButton.getAttribute("aria-label").catch(() => null);
          const selectedLabel = await pagination
            .locator("[aria-current='page']")
            .first()
            .getAttribute("aria-label")
            .catch(() => null);
          const selectedPage = parsePageNumberFromLabel(selectedLabel);
          const nextPage = parsePageNumberFromLabel(nextLabel) ?? ((selectedPage ?? currentCategoryPage) + 1);
          const nextUrl = buildCategoryPageUrl(categoryUrl, nextPage);

          if (DEBUG_TJ) console.log(`[DEBUG] navigating category page by URL: ${nextUrl}`);
          await page.goto(nextUrl, { waitUntil: "domcontentloaded", timeout: 60_000 }).catch(() => null);
          await page.waitForLoadState("networkidle", { timeout: 10_000 }).catch(() => null);
          await page.waitForTimeout(1200);
          currentCategoryPage = nextPage;
          stagnantRounds = 0;
          continue;
        }
      }

      const nextHref = await page.$eval("a[rel='next']", (el) => (el as HTMLAnchorElement).href).catch(() => "");
      if (nextHref) {
        await page.goto(nextHref, { waitUntil: "domcontentloaded", timeout: 60_000 });
        await page.waitForTimeout(2000);
        stagnantRounds = 0;
        continue;
      }
      if (stagnantRounds >= 3) break;
      await page.waitForTimeout(1200);
    }
  } finally {
    await page.close().catch(() => null);
    await context.close().catch(() => null);
  }

  return Array.from(seen);
}

async function main(): Promise<void> {
  const { url, searchUrl, configPath, limit, offset, concurrency, local, debug, noHeadless, headless } = parseArgs();

  DEBUG_TJ = debug;
  if (noHeadless) TJ_HEADLESS = false;
  if (headless) TJ_HEADLESS = true;

  if (local) {
    console.log("Running in local mode: skipping DynamoDB and S3; API submission still runs.");
  }
  if (limit != null) {
    console.log(`Limit: processing at most ${limit} products.`);
  }
  if (offset > 0) {
    console.log(`Offset: skipping first ${offset} products.`);
  }
  const desiredCount = (limit ?? Number.POSITIVE_INFINITY) + (offset ?? 0);

  const jobId = uuidv4();
  const runDateTime = new Date().toISOString().replace(/[:.]/g, "-").slice(0, -5);

  if (!local && SCRAPER_JOB_STATUS_TABLE_NAME) {
    await docClient.send(new PutCommand({
      TableName: SCRAPER_JOB_STATUS_TABLE_NAME,
      Item: {
        job_id: jobId,
        scraper_name: SCRAPER_NAME,
        status: "active",
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      },
    }));
    console.log(`Created job ${jobId}`);
  }

  let urls: string[] = [];
  let searchUrls: string[] = [];
  if (url) {
    urls = [url];
  } else if (searchUrl) {
    searchUrls = [searchUrl];
  } else {
    try {
      const raw = await fs.readFile(configPath, "utf-8");
      const cfg = JSON.parse(raw) as TraderJoesConfig;
      urls = (cfg.urls || []).filter((u) => typeof u === "string");
      searchUrls = (cfg.searchUrls || []).filter((u) => typeof u === "string");
    } catch {
      // ignore
    }
  }

  if (searchUrls.length > 0) {
    const discovered: string[] = [];
    for (const su of searchUrls) {
      if (Number.isFinite(desiredCount) && discovered.length >= desiredCount) break;
      const remaining = Number.isFinite(desiredCount) ? Math.max(desiredCount - discovered.length, 0) : undefined;
      const found = await scrapeCategoryPage(su, remaining);
      discovered.push(...found);
      if (Number.isFinite(desiredCount) && discovered.length >= desiredCount) break;
    }
    urls = discovered;
  }

  if (offset > 0) urls = urls.slice(offset);
  if (limit != null) urls = urls.slice(0, limit);
  if (urls.length === 0) {
    console.error("No Trader Joe's product URLs found. Provide --url or config.json with urls/searchUrls.");
    process.exit(1);
  }
  console.log(`Processing ${urls.length} product URLs`);

  const results: ScraperProductOutput[] = [];
  const valid: TraderJoesProduct[] = [];
  const submitBatch: ScraperProductOutput[] = [];
  const SUBMIT_BATCH_SIZE = 10;
  let scrapedCount = 0;
  let submitted = 0;
  let submitFailed = 0;

  const flushSubmitBatch = async (force: boolean) => {
    if (!force && submitBatch.length < SUBMIT_BATCH_SIZE) return;
    if (!submitBatch.length) return;
    const take = force ? submitBatch.length : Math.min(SUBMIT_BATCH_SIZE, submitBatch.length);
    const batch = submitBatch.splice(0, take);
    console.log(`\n➡️  Submitting batch (${batch.length} items)`);
    for (const out of batch) {
      const { scraper_job_id: _, ...body } = out;
      const ok = await submitProductForReview(body);
      if (ok) submitted++;
      else submitFailed++;
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
  const workerCount = Math.max(1, Math.min(concurrency || 5, queue.length));
  const workers = Array.from({ length: workerCount }, async () => {
    while (queue.length) {
      const u = queue.shift();
      if (!u) break;
      let product: TraderJoesProduct | null = null;
      try {
        product = await fetchProduct(u);
      } catch (err) {
        console.error(`[PRODUCT] Failed | ${u}`, err);
        continue;
      }
      if (!product) {
        if (DEBUG_TJ) console.log(`[DEBUG] skipped ${u}: no product parsed`);
        continue;
      }
      if (!product.item_title || (product.ingredients?.length ?? 0) === 0) {
        if (DEBUG_TJ) {
          console.log(
            `[DEBUG] skipped ${u}: item_title=${product.item_title ? "yes" : "no"}, ingredients=${product.ingredients?.length ?? 0}`
          );
        }
        continue;
      }
      const output = transformToScraperOutput(product);
      const exists = await checkProductExists({
        name: output.product_name || null,
        brand: output.brand || null,
        upc: output.upc || output.upcs?.[0] || null,
      });
      if (exists) {
        console.log(`[SKIP] ${output.product_name || "(no name)"}: already exists`);
        continue;
      }
      valid.push(product);
      scrapedCount++;
      if (scrapedCount % 10 === 0) {
        console.log(`[PROGRESS] scraped ${scrapedCount} products`);
      }
      const outputWithJob = { ...output, scraper_job_id: jobId };
      results.push(outputWithJob);
      submitBatch.push(outputWithJob);
      if (submitBatch.length >= SUBMIT_BATCH_SIZE) {
        void queueFlush(false);
      }
    }
  });
  await Promise.all(workers);
  await queueFlush(true);
  await flushChain;

  if (!local) {
    await uploadToS3(results, jobId, runDateTime);
  }

  console.log(`\n📊 API: ${submitted} submitted, ${submitFailed} failed`);

  if (!local && SCRAPER_JOB_STATUS_TABLE_NAME) {
    if (valid.length === 0) {
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
