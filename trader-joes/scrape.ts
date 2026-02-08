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

const __dirname = path.dirname(fileURLToPath(import.meta.url));

interface TraderJoesProduct {
  sku: string;
  item_title: string;
  ingredients: Array<{ ingredient: string }>;
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
const API_BASE_URL = process.env.API_BASE_URL || "https://it7rdy3qbh.execute-api.us-west-2.amazonaws.com";
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
const DEBUG_TJ = process.env.DEBUG_TJ === "1";
const TJ_HEADLESS = process.env.TJ_HEADLESS !== "0";
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

function parseServingSize(servingSizeStr: string): { value: number; unit: string } | null {
  if (!servingSizeStr || typeof servingSizeStr !== "string") return null;
  const trimmed = servingSizeStr.trim();
  const fractionMatch = trimmed.match(/^(\d+)\s*\/\s*(\d+)\s*([a-zA-Z]+)/);
  if (fractionMatch) {
    const value = parseFloat(fractionMatch[1]) / parseFloat(fractionMatch[2]);
    if (!isNaN(value)) return { value, unit: normalizeUnit(fractionMatch[3]) };
  }
  const simpleMatch = trimmed.match(/^([\d.]+)\s+([a-zA-Z]+)/);
  if (simpleMatch) {
    const value = parseFloat(simpleMatch[1]);
    if (!isNaN(value)) return { value, unit: normalizeUnit(simpleMatch[2]) };
  }
  const parenMatch = trimmed.match(/\(([\d.]+)\s*([a-zA-Z]+)\)/);
  if (parenMatch) {
    const value = parseFloat(parenMatch[1]);
    if (!isNaN(value)) return { value, unit: normalizeUnit(parenMatch[2]) };
  }
  const compactMatch = trimmed.match(/^([\d.]+)([a-zA-Z]+)$/);
  if (compactMatch) {
    const value = parseFloat(compactMatch[1]);
    if (!isNaN(value)) return { value, unit: normalizeUnit(compactMatch[2]) };
  }
  return null;
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

  return {
    serving_size_text: servingSize ?? undefined,
    servings_per_container: servingsPerContainer ?? undefined,
    calories: calories ? parseInt(calories, 10) : undefined,
    nutrients,
  };
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
  try {
    const res = await axios.get(url, {
      headers: {
        "user-agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
      },
      timeout: 30_000,
    });
    if (DEBUG_TJ) console.log(`[DEBUG] fetch html via axios: ${url}`);
    return res.data as string;
  } catch (err: unknown) {
    const e = err as { response?: { status?: number } };
    if (e.response?.status !== 403) throw err;
    if (DEBUG_TJ) console.log(`[DEBUG] axios 403, falling back to playwright: ${url}`);
  }

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
  if (DEBUG_TJ) console.log(`[DEBUG] name: ${name ?? "(null)"}`);
  if (name && /access denied/i.test(name)) {
    if (DEBUG_TJ) console.log(`[DEBUG] access denied title for ${url}`);
    return null;
  }

  let ingredientsSource = "none";
  let ingredientsText = extractIngredientsFromSection($) || "";
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

  const nutritionFacts = extractNutritionFromDom($) || extractNutritionFromText($("body").text());

  const img =
    pickProductImage(productJson?.image) ||
    $('img[src*="/products/"]').first().attr("src") ||
    $('img[srcoriginal*="/products/"]').first().attr("srcoriginal") ||
    $('meta[property="og:image"]').attr("content") ||
    undefined;

  const product: TraderJoesProduct = {
    sku,
    item_title: name || "",
    ingredients,
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
    source: "trader_joes_api",
    source_id: product.sku,
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
function parseArgs(): { url?: string; searchUrl?: string; configPath: string; limit?: number; local: boolean } {
  const argv = process.argv.slice(2);
  const defaultConfig = path.resolve(__dirname, "./config.json");
  let configPath = defaultConfig;
  let url: string | undefined;
  let searchUrl: string | undefined;
  let limit: number | undefined;
  let local = false;
  for (let i = 0; i < argv.length; i++) {
    if ((argv[i] === "--url" || argv[i] === "-u") && argv[i + 1]) {
      url = argv[i + 1];
      i++;
    } else if ((argv[i] === "--search" || argv[i] === "-s") && argv[i + 1]) {
      searchUrl = argv[i + 1];
      i++;
    } else if (argv[i] === "--config" && argv[i + 1]) {
      configPath = path.resolve(argv[i + 1]);
      i++;
    } else if ((argv[i] === "--limit" || argv[i] === "-l") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n > 0) limit = n;
      i++;
    } else if (argv[i] === "--local") {
      local = true;
    }
  }
  return { url, searchUrl, configPath, limit, local };
}

function normalizeTraderJoesUrl(href: string): string | null {
  if (!href) return null;
  if (href.startsWith("http")) return href;
  if (href.startsWith("//")) return `https:${href}`;
  if (href.startsWith("/")) return `https://www.traderjoes.com${href}`;
  return null;
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

  try {
    await page.goto("https://www.traderjoes.com/home", { waitUntil: "domcontentloaded", timeout: 60_000 }).catch(() => {});
    await page.waitForTimeout(1500);
    await page.goto(categoryUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(2000);

    while (true) {
      pageCount++;
      const links = await page.$$eval("a[href*='/home/products/pdp/']", (els) =>
        els
          .map((el) => (el as HTMLAnchorElement).getAttribute("href") || "")
          .filter(Boolean)
      );
      for (const href of links) {
        const full = normalizeTraderJoesUrl(href);
        if (!full) continue;
        seen.add(full.split("?")[0]);
        if (limit && seen.size >= limit) break;
      }
      console.log(`[DISCOVER] page ${pageCount}: ${seen.size} product urls collected`);
      if (limit && seen.size >= limit) break;

      const nextLink = page.locator(
        [
          "a[rel='next']",
          "a[aria-label*='Next']",
          "button[aria-label*='Next']",
          "a:has-text('→')",
          "button:has-text('→')",
        ].join(", ")
      );
      if (await nextLink.count()) {
        const el = nextLink.first();
        const disabled = await el.getAttribute("aria-disabled");
        if (disabled === "true") break;
        if (DEBUG_TJ) console.log("[DEBUG] clicking next page control");
        await el.click({ timeout: 3000 }).catch(() => null);
        await page.waitForTimeout(2000);
        continue;
      }

      const nextHref = await page.$eval("a[rel='next']", (el) => (el as HTMLAnchorElement).href).catch(() => "");
      if (nextHref) {
        await page.goto(nextHref, { waitUntil: "domcontentloaded", timeout: 60_000 });
        await page.waitForTimeout(2000);
        continue;
      }
      break;
    }
  } finally {
    await page.close().catch(() => null);
    await context.close().catch(() => null);
  }

  return Array.from(seen);
}

async function main(): Promise<void> {
  const { url, searchUrl, configPath, limit, local } = parseArgs();

  if (local) {
    console.log("Running in local mode: skipping DynamoDB and S3; API submission still runs.");
  }
  if (limit != null) {
    console.log(`Limit: processing at most ${limit} products.`);
  }

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
      const found = await scrapeCategoryPage(su, limit);
      discovered.push(...found);
      if (limit && discovered.length >= limit) break;
    }
    urls = limit ? discovered.slice(0, limit) : discovered;
  }

  if (limit != null) urls = urls.slice(0, limit);
  if (urls.length === 0) {
    console.error("No Trader Joe's product URLs found. Provide --url or config.json with urls/searchUrls.");
    process.exit(1);
  }
  console.log(`Processing ${urls.length} product URLs`);

  const results: ScraperProductOutput[] = [];
  const valid: TraderJoesProduct[] = [];
  for (const u of urls) {
    const product = await fetchProduct(u);
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
    valid.push(product);
    const output = transformToScraperOutput(product);
    results.push({ ...output, scraper_job_id: jobId });
  }

  if (!local) {
    await uploadToS3(results, jobId, runDateTime);
  }

  console.log(`\n📤 Submitting ${results.length} products for review...`);
  let success = 0;
  let fail = 0;
  for (const r of results) {
    const { scraper_job_id: _, ...body } = r;
    const ok = await submitProductForReview(body);
    if (ok) success++;
    else fail++;
  }
  console.log(`\n📊 API: ${success} submitted, ${fail} failed`);

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
