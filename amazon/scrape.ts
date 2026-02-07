import * as fs from "fs";
import { chromium, type Browser, type BrowserContext } from "playwright";
import * as cheerio from "cheerio";
import pLimit from "p-limit";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import { parseNutrientAmountWithQualifier } from "../nutrition-utils";

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

// AMAZON_USER_DATA_DIR=/tmp/amazon-profile npx tsx scrape.ts --config config.json --local --limit 1

const SOURCE = "amazon.com";
const SCRAPER_NAME = process.env.JOB_NAME || "amazon";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://it7rdy3qbh.execute-api.us-west-2.amazonaws.com";
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
const DEBUG_AMAZON = process.env.DEBUG_AMAZON === "1";
const AMAZON_HEADLESS = process.env.AMAZON_HEADLESS !== "0";
const AMAZON_SLOWMO = process.env.AMAZON_SLOWMO ? parseInt(process.env.AMAZON_SLOWMO, 10) : undefined;
const AMAZON_USER_DATA_DIR = process.env.AMAZON_USER_DATA_DIR;

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
  if (!name) return null;
  let cleaned = decodeHtmlEntities(name) || name;
  if (brand) {
    const brandEscaped = brand.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    cleaned = cleaned.replace(new RegExp(`^${brandEscaped}\\s*,\\s*`, "i"), "");
  }
  cleaned = cleaned.replace(/\s*\|\s*.*$/, "");
  cleaned = cleaned.replace(/\s*-\s*[^-]*$/, (m) => {
    // Only drop the trailing segment if it looks like size/weight (contains digits and units)
    return /(\d+|\d+\.\d+)\s*(oz|ounce|ounces|fl oz|count|ct|pack|lb|lbs|g|kg)\b/i.test(m) ? "" : m;
  });
  cleaned = cleaned.replace(/\s*,\s*[^,]*$/, (m) => {
    return /(\d+|\d+\.\d+)\s*(oz|ounce|ounces|fl oz|count|ct|pack|lb|lbs|g|kg)\b/i.test(m) ? "" : m;
  });
  return normalizeWhitespace(cleaned);
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
  console.log(`  [DEBUG] image: ${p.imageUrl ?? "(null)"}`);
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

function extractMainImageUrl($: cheerio.CheerioAPI): string | null {
  const landing = $("#landingImage");
  const dynamic = landing.attr("data-a-dynamic-image");
  if (dynamic) {
    try {
      const parsed = JSON.parse(dynamic) as Record<string, unknown>;
      const urls = Object.keys(parsed);
      if (urls.length > 0) return urls[0];
    } catch {
      // ignore
    }
  }

  const img = $("#imgTagWrapperId img").attr("src");
  if (img) return img;
  const og = $('meta[property="og:image"]').attr("content");
  return og || null;
}

function normalizeAmazonImageUrl(url: string | null): string | null {
  if (!url) return null;
  return url.replace(/\._[^.]+_\./, "._SL1000_.");
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
  return { servingSize, servingsPerContainer, calories, nutrients };
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

  return {
    servingSize: servingSize || null,
    servingsPerContainer: servingsPerContainer || null,
    calories: calories || null,
    nutrients,
  };
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

      return {
        servingSize:
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
        nutrients,
      };
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

async function scrapeProductDetail(context: BrowserContext, productUrl: string, brandOverride?: string): Promise<ScrapedProduct> {
  const page = await context.newPage();
  try {
    let nutritionFromApi: Nutrition | null = null;
    let nutritionTextFromDom: string | null = null;
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

    await page.goto(productUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(1200);
    await page.waitForLoadState("networkidle", { timeout: 8000 }).catch(() => {});

    const maybeExpandNutrition = async (): Promise<void> => {
      const section = page.locator("#nutritionalInfoAndIngredients_feature_div");
      if (await section.count()) {
        await section.first().scrollIntoViewIfNeeded().catch(() => {});
        await page.waitForTimeout(600);
      }
      const expander = page.locator("a[data-a-expander-name='nic-nutrition-facts-expander']");
      if (await expander.count()) {
        await expander.first().click({ timeout: 2000 }).catch(() => {});
      }
      await page.waitForTimeout(2000);
      await page
        .waitForFunction(
          () =>
            /Cholesterol|Vitamin D|Calcium|Iron|Potassium/.test(
              document.body?.innerText || ""
            ),
          { timeout: 8000 }
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

    const brand = brandOverride || jsonLdResult.brand || extractBrandFromName(name) || "Unknown";

    const cleanedName = normalizeProductName(name, brand);

    const imageUrl = normalizeAmazonImageUrl(jsonLdResult.imageUrl ?? extractMainImageUrl($) ?? null);

    let upc12 = jsonLdResult.upc12 ?? null;
    if (!upc12) {
      const upc =
        extractFromDetailBullets($, /^upc/i) ||
        extractFromTables($, /^upc/i) ||
        extractFromTables($, /^gtin\-?12/i);
      if (upc) {
        const m = upc.match(/(\d{12})/);
        upc12 = m?.[1] ?? null;
      }
    }

    const ingredients =
      normalizeWhitespace($("#nic-ingredients-content span").first().text()) ||
      extractFromDetailBullets($, /^ingredients?/i) ||
      extractFromTables($, /^ingredients?/i) ||
      extractFromImportantInfo($, /^ingredients?/i) ||
      null;

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

    const now = new Date().toISOString();

    return {
      brand,
      source: SOURCE,
      productUrl,
      name: cleanedName ?? name,
      ingredients: ingredients ? normalizeWhitespace(ingredients) : null,
      upc12,
      nutrition,
      imageUrl,
      scrapedAt: now,
      sourceCreatedAt: null,
      sourceLastUpdatedAt: null,
    };
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

function parseArgs(): { url?: string; configPath?: string; limit?: number; local: boolean } {
  const argv = process.argv.slice(2);
  let url: string | undefined;
  let searchUrl: string | undefined;
  let configPath: string | undefined;
  let limit: number | undefined;
  let local = false;

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
    } else if (argv[i] === "--local") {
      local = true;
    }
  }

  return { url, configPath, limit, local, searchUrl };
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

    while (idleRounds < 8) {
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
        await page.waitForTimeout(2500);
      } else {
        // Slow scroll to trigger lazy-load
        const steps = 6;
        for (let i = 0; i < steps; i++) {
          await page.evaluate(() => window.scrollBy(0, window.innerHeight * 0.9));
          await page.waitForTimeout(600);
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
  const { url, configPath, limit, local, searchUrl } = parseArgs();

  let productTargets: string[] = [];
  let searchTargets: string[] = [];

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
  } else {
    console.error("Usage: npx tsx scrape.ts --url <AMAZON_PRODUCT_URL>");
    console.error("   or: npx tsx scrape.ts --config ./config.json [--limit N] [--local]");
    process.exit(1);
  }

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

  const limiter = pLimit(3);
  const outputs: ScraperProductOutput[] = [];
  let skipped = 0;

  try {
    let productUrls = productTargets;
    if (searchTargets.length > 0) {
      const discovered: string[] = [];
      for (const target of searchTargets) {
        const found = await scrapeAislePage(context, target, limit);
        console.log(`[DISCOVER] Collected ${found.length} product URLs from aisle page`);
        discovered.push(...found);
        if (limit && discovered.length >= limit) break;
      }
      productUrls = limit ? discovered.slice(0, limit) : discovered;
      if (productUrls.length === 0) {
        console.error("No valid Amazon product URLs found on aisle page.");
        return;
      }
    }

    const results = await Promise.all(
      productUrls.map((u) =>
        limiter(async () => {
          const p = await scrapeProductDetail(context, u);
          return { product: p, url: u };
        })
      )
    );

    for (const { product: p, url: u } of results) {
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
