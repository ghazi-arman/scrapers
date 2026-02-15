import axios from "axios";
import * as cheerio from "cheerio";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import { chromium } from "playwright";
import * as fs from "fs/promises";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import * as nutritionUtils from "../nutrition-utils";

const parseNutrientAmountWithQualifier =
  (nutritionUtils as any).parseNutrientAmountWithQualifier ??
  (nutritionUtils as any).default?.parseNutrientAmountWithQualifier;

const s3Client = new S3Client({});
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);
const ssmClient = new SSMClient({});

const SCRAPER_NAME = process.env.JOB_NAME || "nestle";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://it7rdy3qbh.execute-api.us-west-2.amazonaws.com";
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
let DEBUG_NESTLE = false;
let NESTLE_HEADLESS = process.env.NESTLE_HEADLESS !== "0";

let serviceTokenCache: string | null = null;

function cleanText(s: string | null | undefined): string {
  return (s ?? "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
}

function decodeHtmlEntities(s: string | null | undefined): string {
  if (!s) return "";
  return s
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .trim();
}

function stripWeightFromName(name: string): string {
  return name
    .replace(/\([^)]*\)$/g, "")
    .replace(/\bnet\s*wt\b.*$/i, "")
    .replace(
      /[, ]*\b\d+(?:\.\d+)?\s*(?:oz|ounce|ounces|fl oz|fluid ounce|g|kg|lb|lbs|ml|l|ct|count|pack|pk)\b.*$/i,
      ""
    )
    .replace(/\s+/g, " ")
    .trim();
}

function parseServingSize(servingSizeText: string | null): {
  value: number | null;
  unit: string | null;
} {
  if (!servingSizeText) return {value: null, unit: null};
  const cleaned = servingSizeText.replace(/\([^)]*\)/g, "").trim();
  const m1 = cleaned.match(/^(\d+\s*\/\s*\d+)\s*([a-zA-Z]+)/);
  if (m1) {
    const frac = m1[1].split("/").map((v) => parseFloat(v.trim()));
    if (frac.length === 2 && frac[1] !== 0) {
      return {value: frac[0] / frac[1], unit: m1[2].toLowerCase()};
    }
  }
  const m2 = cleaned.match(/^(\d+(?:\.\d+)?)\s*([a-zA-Z]+)/);
  if (m2) return {value: parseFloat(m2[1]), unit: m2[2].toLowerCase()};
  return {value: null, unit: null};
}

function parseAmountWithQualifier(raw: string): { value: number | null; qualifier?: string | null; unit?: string | null } {
  if (!raw) return {value: null};
  if (parseNutrientAmountWithQualifier) {
    const parsed = parseNutrientAmountWithQualifier(raw);
    const unitMatch = raw.match(/([a-zA-Zµ]+)\b/);
    return {value: parsed?.value ?? null, qualifier: parsed?.qualifier ?? null, unit: unitMatch?.[1] ?? null};
  }
  const m = raw.match(/([<>~])?\s*([\d.]+)\s*([a-zA-Z]+)?/);
  if (!m) return {value: null};
  return {value: parseFloat(m[2]), qualifier: m[1] ?? null, unit: m[3] ?? null};
}

function mapNutrient(label: string): { field?: keyof ScraperNutritionData; dvField?: keyof ScraperNutritionData } {
  const l = label.toLowerCase().trim();
  if (l.startsWith("total fat")) return {field: "total_fat_g"};
  if (l.startsWith("saturated fat")) return {field: "saturated_fat_g"};
  if (l.startsWith("trans fat")) return {field: "trans_fat_g"};
  if (l.startsWith("polyunsaturated fat")) return {field: "polyunsaturated_fat_g"};
  if (l.startsWith("monounsaturated fat") || l.startsWith("mononsaturated fat")) return {field: "monounsaturated_fat_g"};
  if (l.startsWith("cholesterol")) return {field: "cholesterol_mg"};
  if (l.startsWith("sodium")) return {field: "sodium_mg"};
  if (l.startsWith("total carbohydrate") || l.startsWith("total carbohydrates")) return {field: "total_carbs_g"};
  if (l.startsWith("dietary fiber")) return {field: "fiber_g"};
  if (l.startsWith("total sugars")) return {field: "sugars_g"};
  if (l.startsWith("added sugars") || l.startsWith("includes")) return {field: "added_sugars_g"};
  if (l.startsWith("protein")) return {field: "protein_g"};
  if (l.startsWith("vitamin d")) return {field: "vitamin_d_mcg", dvField: "vitamin_d_dv_pct"};
  if (l.startsWith("calcium")) return {field: "calcium_mg", dvField: "calcium_dv_pct"};
  if (l.startsWith("iron")) return {field: "iron_mg", dvField: "iron_dv_pct"};
  if (l.startsWith("potassium")) return {field: "potassium_mg", dvField: "potassium_dv_pct"};
  if (l.startsWith("vitamin a")) return {field: "vitamin_a_mcg", dvField: "vitamin_a_dv_pct"};
  if (l.startsWith("vitamin c")) return {field: "vitamin_c_mg", dvField: "vitamin_c_dv_pct"};
  return {};
}

function parseNutrition($: cheerio.CheerioAPI): ScraperNutritionData | null {
  const section =
    $(".--gdn-nutriction-facts-section-primary-table .nutritionalFacts").first().length > 0
      ? $(".--gdn-nutriction-facts-section-primary-table .nutritionalFacts").first()
      : $(".nutritionalFactsWrapper").first();
  if (!section.length) return null;

  const parseSection = (root: cheerio.Cheerio<cheerio.Element>): ScraperNutritionData | null => {
    const nutrition: ScraperNutritionData = {
      serving_size_value: 1,
      serving_size_unit_text: "serving",
      serving_size_text: null,
    };

    if (root.is(".nutritionalFactsWrapper")) {
      const servingSizeRow = root.find(".topSection .flexRow").filter((_, el) => {
        return /serving size/i.test(cleanText($(el).text()));
      });
      if (servingSizeRow.length) {
        const text = cleanText(servingSizeRow.last().text()).replace(/serving size/i, "").trim();
        nutrition.serving_size_text = text || null;
        const parsed = parseServingSize(text || null);
        if (parsed.value != null) nutrition.serving_size_value = parsed.value;
        if (parsed.unit) nutrition.serving_size_unit_text = parsed.unit;
      }
      if (DEBUG_NESTLE) {
        console.log(`[DEBUG] nutrition serving size text: ${nutrition.serving_size_text ?? "(null)"}`);
      }

      let caloriesText = cleanText(root.find(".topSection .boxed span").last().text());
      const perServingRow = root
        .find(".topSection .boxed div")
        .filter((_, el) => /amount per serving/i.test(cleanText($(el).text())))
        .first();
      if (perServingRow.length) {
        const candidate = cleanText(perServingRow.find("span").last().text());
        if (candidate) caloriesText = candidate;
      }
      if (caloriesText) {
        const n = caloriesText.match(/\d+/);
        if (n) nutrition.calories = Number(n[0]);
      }
      if (DEBUG_NESTLE) {
        console.log(`[DEBUG] nutrition calories text: ${caloriesText || "(null)"}`);
      }

      root.find("table.nutritionFactsTable tbody tr").each((_, row) => {
        const cells = $(row).find("td");
        if (cells.length < 2) return;
        const leftText = cleanText($(cells[0]).text());
        const amountText = cleanText($(cells[1]).text());
        const dvText = cleanText($(cells[2]).text());
        if (!leftText) return;

      const addedMatch = leftText.match(/incl\.?\s*(?:[<>~]?[\d.]+\s*(?:mcg|mg|g|iu|niu)?)?\s*added\s*sugars?/i);
      const hasAddedSugars = /added\s*sugars?/i.test(leftText);
      if (addedMatch || hasAddedSugars) {
        let parsed = parseAmountWithQualifier(amountText);
        if (parsed.value == null) {
          const amountMatch = leftText.match(/incl\.?\s*([<>~]?[\d.]+\s*(?:mcg|mg|g|iu|niu)?)\s*added\s*sugars?/i);
          if (amountMatch?.[1]) {
            parsed = parseAmountWithQualifier(amountMatch[1]);
          }
        }
        if (parsed.value != null) nutrition.added_sugars_g = parsed.value;
        if (parsed.qualifier) nutrition.added_sugars_g_qualifier = parsed.qualifier;
        if (DEBUG_NESTLE) {
          console.log(
            `[DEBUG] added sugars row: label="${leftText}" amount="${amountText}" dv="${dvText}" value="${nutrition.added_sugars_g ?? "(null)"}" qualifier="${nutrition.added_sugars_g_qualifier ?? "(null)"}"`
          );
        }
        const dv = dvText.match(/(\d+)%/);
        if (dv) nutrition.added_sugars_dv_pct = Number(dv[1]);
        return;
      }

        const { field, dvField } = mapNutrient(leftText);
        if (!field && !dvField) return;

        if (amountText) {
          const parsed = parseAmountWithQualifier(amountText);
          const unit = parsed.unit?.toLowerCase() ?? null;
          const amountLower = amountText.toLowerCase();
          const isVitaminAIU =
            field === "vitamin_a_mcg" && (unit === "iu" || unit === "niu" || amountLower.includes("iu"));
          if (parsed.value != null && !isVitaminAIU) (nutrition as any)[field as string] = parsed.value;
          if (parsed.qualifier && !isVitaminAIU) (nutrition as any)[`${String(field)}_qualifier`] = parsed.qualifier;
        }

        const dv = dvText.match(/(\d+)%/);
        if (dv && dvField) (nutrition as any)[dvField as string] = Number(dv[1]);
      });
    } else {
      const servingSizeRow = root.find(".meta .flexRow").filter((_, el) => {
        return /serving size/i.test(cleanText($(el).text()));
      });
      if (servingSizeRow.length) {
        const text = cleanText(servingSizeRow.last().text()).replace(/serving size/i, "").trim();
        nutrition.serving_size_text = text || null;
        const parsed = parseServingSize(text || null);
        if (parsed.value != null) nutrition.serving_size_value = parsed.value;
        if (parsed.unit) nutrition.serving_size_unit_text = parsed.unit;
      }

      const caloriesText = cleanText(root.find(".meta .flexRow strong").last().text());
      if (caloriesText) {
        const n = caloriesText.match(/\d+/);
        if (n) nutrition.calories = Number(n[0]);
      }

      root.find("table.nutritionFactsTable tbody tr").each((_, row) => {
        const cells = $(row).find("td");
        if (cells.length < 1) return;
        const leftText = cleanText($(cells[0]).text());
        const dvText = cleanText($(cells[1]).text());
        if (!leftText) return;

        if (/added\s*sugars?/i.test(leftText)) {
          const amountMatch = leftText.match(/incl\.?\s*([<>~]?[\d.]+\s*(?:mcg|mg|g|iu|niu)?)\s*added\s*sugars?/i);
          if (amountMatch?.[1]) {
            const parsed = parseAmountWithQualifier(amountMatch[1]);
            if (parsed.value != null) nutrition.added_sugars_g = parsed.value;
            if (parsed.qualifier) nutrition.added_sugars_g_qualifier = parsed.qualifier;
          }
          const dv = dvText.match(/(\d+)%/);
          if (dv) nutrition.added_sugars_dv_pct = Number(dv[1]);
          if (DEBUG_NESTLE) {
            console.log(
              `[DEBUG] added sugars row: label="${leftText}" dv="${dvText}" value="${nutrition.added_sugars_g ?? "(null)"}" qualifier="${nutrition.added_sugars_g_qualifier ?? "(null)"}"`
            );
          }
          return;
        }

        const m = leftText.match(/^(.+?)\s*([<>~]?[\d.]+\s*(?:mcg|mg|g|iu|niu)?)$/i);
        const label = cleanText(m?.[1] ?? leftText);
        const amountStr = cleanText(m?.[2] ?? "");
        const { field, dvField } = mapNutrient(label);
        if (!field && !dvField) return;

        const parsed = parseAmountWithQualifier(amountStr);
        const unit = parsed.unit?.toLowerCase() ?? null;
        const amountLower = amountStr.toLowerCase();
        const isVitaminAIU =
          field === "vitamin_a_mcg" && (unit === "iu" || unit === "niu" || amountLower.includes("iu"));
        if (parsed.value != null && !isVitaminAIU) (nutrition as any)[field as string] = parsed.value;
        if (parsed.qualifier && !isVitaminAIU) (nutrition as any)[`${String(field)}_qualifier`] = parsed.qualifier;

        const dv = dvText.match(/(\d+)%/);
        if (dv && dvField) (nutrition as any)[dvField as string] = Number(dv[1]);
      });
    }

    const hasAny = Object.keys(nutrition).some(
      (k) => k !== "serving_size_value" && k !== "serving_size_unit_text" && k !== "serving_size_text"
    );
    return hasAny ? nutrition : null;
  };

  const servingBlocks = section.find("[id^='serving-']").toArray();
  if (servingBlocks.length > 0) {
    let chosen: ScraperNutritionData | null = null;
    let chosenGrams: number | null = null;
    let chosenServingValue: number | null = null;
    let chosenUnit: string | null = null;

    for (const block of servingBlocks) {
      const parsed = parseSection($(block));
      if (!parsed) continue;
      const servingText = parsed.serving_size_text ?? "";
      const gramMatch = servingText.match(/(\d+(?:\.\d+)?)\s*g\b/i);
      const grams = gramMatch ? parseFloat(gramMatch[1]) : null;
      if (grams != null) {
        if (chosenGrams == null || grams < chosenGrams) {
          chosen = parsed;
          chosenGrams = grams;
          chosenServingValue = parsed.serving_size_value ?? null;
          chosenUnit = parsed.serving_size_unit_text ?? null;
        }
      } else if (chosenGrams == null) {
        const val = parsed.serving_size_value ?? null;
        const unit = parsed.serving_size_unit_text ?? null;
        if (chosen == null) {
          chosen = parsed;
          chosenServingValue = val;
          chosenUnit = unit;
        } else if (val != null && chosenServingValue != null && unit && chosenUnit && unit === chosenUnit && val < chosenServingValue) {
          chosen = parsed;
          chosenServingValue = val;
          chosenUnit = unit;
        }
      }
    }

    if (chosen) return chosen;
  }

  return parseSection(section);
}

function joinSubIngredients(items: string[], hasAndOr: boolean): string {
  if (items.length === 0) return "";
  if (items.length === 1) return items[0];
  if (hasAndOr) return `${items.slice(0, -1).join(", ")}, and/or ${items[items.length - 1]}`;
  return items.join(", ");
}

function parseIngredientList($: cheerio.CheerioAPI, ul: cheerio.Cheerio<cheerio.Element>): string[] {
  const items: string[] = [];
  ul.children("li").each((_, li) => {
    const el = $(li);
    const nested = el.children("ul.ingredients__list").first();
    const label = cleanText(
      el
        .clone()
        .children("ul.ingredients__list")
        .remove()
        .end()
        .find(".linked-list__text")
        .first()
        .text()
    );
    if (nested.length) {
      const subItems = parseIngredientList($, nested).filter(Boolean);
      let hasAndOr = false;
      const cleaned = subItems.map((s) => {
        if (/^and\/?or\s+/i.test(s)) {
          hasAndOr = true;
          return s.replace(/^and\/?or\s+/i, "").trim();
        }
        return s;
      });
      const joined = joinSubIngredients(cleaned, hasAndOr);
      if (label && joined) items.push(`${label} (${joined})`);
      else if (label) items.push(label);
      else if (joined) items.push(joined);
    } else if (label) {
      items.push(label);
    }
  });
  return items;
}

function extractIngredients($: cheerio.CheerioAPI): string | null {
  const body = $(".--gdn-nutriction-facts-section-secondary-table-ingredients-body").first();
  if (body.length) {
    const text = cleanText(body.text());
    return text || null;
  }
  const block = $(".--gdn-nutriction-facts-section-secondary-table-ingredients-body div, .--gdn-nutriction-facts-section-secondary-table-ingredients-body p").first();
  const text = cleanText(block.text());
  return text || null;
}

function isListingUrl(url: string): boolean {
  try {
    const u = new URL(url);
    if (u.hostname !== "www.goodnes.com") return false;
    const path = u.pathname.replace(/\/+$/, "");
    return /\/products($|\/all$)/i.test(path);
  } catch {
    return false;
  }
}

function extractListingProductLinks(listingUrl: string, html: string): string[] {
  const $ = cheerio.load(html);
  const links = new Set<string>();
  const candidates = $("a.gdn-pd-iii-view__card-link[href], a.gdn-pd-iii-view__card-title[href]");
  candidates.each((_, a) => {
    const href = $(a).attr("href");
    if (!href) return;
    let full: string;
    try {
      full = new URL(href, listingUrl).toString();
    } catch {
      return;
    }
    try {
      const u = new URL(full);
      if (u.hostname !== "www.goodnes.com") return;
      const path = u.pathname.replace(/\/+$/, "");
      if (!/\/products\/.+/i.test(path)) return;
      links.add(u.toString());
    } catch {
      return;
    }
  });
  return Array.from(links);
}

type UrlEntry = { url: string; brandOverride?: string | null };

async function discoverListingUrls(listingUrl: string, limit?: number, brandOverride?: string | null): Promise<UrlEntry[]> {
  const urls: UrlEntry[] = [];
  let page = 1;
  const seen = new Set<string>();
  while (true) {
    const pageUrl = page === 1 ? listingUrl : `${listingUrl}?page=${page}`;
    const html = await fetchRenderedHtml(pageUrl);
    const found = extractListingProductLinks(listingUrl, html);
    if (DEBUG_NESTLE) {
      console.log(`[DISCOVER] Listing page ${page}: ${found.length} links`);
    }
    let added = 0;
    for (const u of found) {
      if (seen.has(u)) continue;
      seen.add(u);
      urls.push({ url: u, brandOverride: brandOverride ?? null });
      added++;
      if (limit && urls.length >= limit) return urls;
    }
    if (added === 0) break;
    page += 1;
    if (page > 100) break;
  }
  return urls;
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

function extractFromJsonLd($: cheerio.CheerioAPI): { name?: string; brand?: string; image?: string } {
  const nodes = extractJsonLd($);
  for (const node of nodes) {
    if (node?.["@type"] === "Product") {
      const name = cleanText(node.name);
      let brand: string | undefined;
      if (typeof node.brand === "string") brand = cleanText(node.brand);
      else if (node.brand?.name) brand = cleanText(node.brand.name);
      let image: string | undefined;
      if (typeof node.image === "string") image = node.image;
      else if (Array.isArray(node.image)) image = node.image[0];
      return {name: name || undefined, brand: brand || undefined, image: image || undefined};
    }
  }
  return {};
}

function extractUpcFromEmbeddedJson(html: string): string | null {
  const anchored = html.match(/"gdn_product"\s*:\s*(\{.*?\})\s*,\s*"gdn_ip_detection"/s);
  if (anchored?.[1]) {
    try {
      const obj = JSON.parse(anchored[1]);
      const variants = Array.isArray(obj?.variants) ? obj.variants : [];
      const upc = variants[0]?.upc || obj?.product?.upc;
      return upc ? String(upc) : null;
    } catch {
      // fall through
    }
  }
  const generic = html.match(/"gdn_product"\s*:\s*(\{.*?\})/s);
  if (generic?.[1]) {
    try {
      const obj = JSON.parse(generic[1]);
      const variants = Array.isArray(obj?.variants) ? obj.variants : [];
      const upc = variants[0]?.upc || obj?.product?.upc;
      return upc ? String(upc) : null;
    } catch {
      // fall through
    }
  }
  const upcMatch = html.match(/"upc"\s*:\s*"(\d{11,14})"/);
  return upcMatch?.[1] ?? null;
}

async function fetchRenderedHtml(url: string): Promise<string> {
  const browser = await chromium.launch({ headless: NESTLE_HEADLESS });
  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
  });
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
    await page.waitForLoadState("networkidle", { timeout: 8000 }).catch(() => {});
    const html = await page.content();
    if (DEBUG_NESTLE) {
      await fs.writeFile("/tmp/nestle-rendered.html", html).catch(() => null);
    }
    return html;
  } finally {
    await page.close().catch(() => null);
    await browser.close().catch(() => null);
  }
}

async function fetchHtml(url: string): Promise<string> {
  try {
    const res = await axios.get(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        Accept: "text/html,application/xhtml+xml",
      },
      timeout: 30000,
    });
    return res.data;
  } catch (err: any) {
    const status = err?.response?.status;
    if (status === 403) {
      if (DEBUG_NESTLE) console.log(`[DEBUG] axios 403, falling back to playwright: ${url}`);
      return await fetchRenderedHtml(url);
    }
    throw err;
  }
}

async function getServiceToken(): Promise<string> {
  if (serviceTokenCache) return serviceTokenCache;
  const command = new GetParameterCommand({ Name: API_KEYS_PARAMETER_NAME, WithDecryption: true });
  const response = await ssmClient.send(command);
  if (!response.Parameter?.Value) throw new Error(`Parameter "${API_KEYS_PARAMETER_NAME}" not found`);
  const parameter = JSON.parse(response.Parameter.Value);
  serviceTokenCache = parameter.InternalServiceToken;
  if (!serviceTokenCache) throw new Error("InternalServiceToken not found");
  return serviceTokenCache;
}

async function submitProductForReview(productOutput: ScraperProductOutput): Promise<boolean> {
  if (!productOutput.product_name || !productOutput.ingredients_text || !productOutput.upc) return false;
  try {
    const token = await getServiceToken();
    const { scraper_job_id: _scraperJobId, ...body } = productOutput;
    const res = await axios.post(`${API_BASE_URL}/submit-product-for-review`, body, {
      headers: { "Content-Type": "application/json", "X-Service-Token": token },
    });
    if (res.status === 200) {
      console.log(`✅ Submitted "${productOutput.product_name}"${res.data?.data?.job_id ? ` (job_id: ${res.data.data.job_id})` : ""}`);
      return true;
    }
    console.error(`❌ Failed "${productOutput.product_name}": HTTP ${res.status}`);
    return false;
  } catch (err: any) {
    console.error(`❌ Failed "${productOutput.product_name}":`, err.response ? `${err.response.status} ${err.response.statusText}` : err.message);
    return false;
  }
}

async function uploadToS3(results: ScraperProductOutput[], jobId: string, runDateTime: string) {
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

async function updateJobStatus(jobId: string, status: string, error: string | null = null) {
  if (!SCRAPER_JOB_STATUS_TABLE_NAME) return;
  const now = new Date().toISOString();
  await docClient.send(
    new UpdateCommand({
      TableName: SCRAPER_JOB_STATUS_TABLE_NAME,
      Key: { job_id: jobId },
      UpdateExpression: "SET #status = :status, updated_at = :updated_at" + (error ? ", error = :error" : ""),
      ExpressionAttributeNames: { "#status": "status" },
      ExpressionAttributeValues: { ":status": status, ":updated_at": now, ...(error && { ":error": error }) },
    })
  );
}

function parseArgs() {
  const argv = process.argv.slice(2);
  const defaultConfig = new URL("./config.json", import.meta.url);
  let configPath = defaultConfig.pathname;
  let url: string | undefined;
  let limit: number | undefined;
  let offset: number | undefined;
  let local = false;
  let debug = false;
  let noHeadless = false;
  for (let i = 0; i < argv.length; i++) {
    if ((argv[i] === "--url" || argv[i] === "-u") && argv[i + 1]) {
      url = argv[i + 1];
      i++;
    } else if (argv[i] === "--config" && argv[i + 1]) {
      configPath = argv[i + 1];
      i++;
    } else if ((argv[i] === "--limit" || argv[i] === "-l") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n > 0) limit = n;
      i++;
    } else if ((argv[i] === "--offset" || argv[i] === "-o") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n >= 0) offset = n;
      i++;
    } else if (argv[i] === "--local") {
      local = true;
    } else if ((argv[i] === "--debug" || argv[i] === "-d")) {
      debug = true;
    } else if (argv[i] === "--no-headless") {
      noHeadless = true;
    }
  }
  return { url, configPath, limit, offset, local, debug, noHeadless };
}

async function fetchProduct(url: string, brandOverride?: string | null) {
  const html = await fetchHtml(url);
  const $ = cheerio.load(html);

  const jsonLd = extractFromJsonLd($);

  const brandRaw = cleanText($(".gdn-hero-pdp__brand, .gdn-hero-pdp__container-content-title, .gdn-hero-pdp__title").first().text());
  const brand = brandOverride || brandRaw || jsonLd.brand || cleanText($('meta[property="og:site_name"]').attr("content")) || "";

  let name =
    brandRaw ||
    cleanText($("h1").first().text()) ||
    jsonLd.name ||
    cleanText($('meta[property="og:title"]').attr("content")) ||
    "";
  if (brand && name.toLowerCase().startsWith(brand.toLowerCase())) {
    name = name.slice(brand.length).trim();
  }
  name = stripWeightFromName(name);

  const upc =
    cleanText($("[data-basketful-product-locator-upc]").attr("data-basketful-product-locator-upc")) ||
    cleanText($("[data-sc-upc]").attr("data-sc-upc")) ||
    extractUpcFromEmbeddedJson(html) ||
    null;

  const heroImg =
    $("img[src*=\"gdn_hero_pdp_product_image\"]").first().attr("src") ||
    $("source[srcset*=\"gdn_hero_pdp_product_image\"]").first().attr("srcset") ||
    null;
  const img =
    heroImg ||
    $("img[data-id='image__front']").first().attr("src") ||
    $("img[data-id='image__front']").first().attr("data-impression-content") ||
    jsonLd.image ||
    $('meta[property="og:image"]').attr("content") ||
    null;
  const imageUrl = img ? new URL(decodeHtmlEntities(img), url).toString() : null;

  const ingredientsText = extractIngredients($);
  const nutrition = parseNutrition($);

  if (DEBUG_NESTLE) {
    console.log(`[DEBUG] brand: ${brand || "(null)"}`);
    console.log(`[DEBUG] name: ${name || "(null)"}`);
    console.log(`[DEBUG] upc: ${upc || "(null)"}`);
    console.log(`[DEBUG] image: ${imageUrl || "(null)"}`);
    console.log(`[DEBUG] ingredients length: ${ingredientsText?.length ?? 0}`);
    if (ingredientsText) {
      console.log(`[DEBUG] ingredients snippet: ${ingredientsText.slice(0, 140)}`);
    }
    console.log(`[DEBUG] nutrition calories: ${nutrition?.calories ?? "(null)"}`);
  }

  return { url, name, brand, upc, imageUrl, ingredientsText, nutrition };
}

function transformToOutput(p: any, jobId: string): ScraperProductOutput {
  const now = new Date().toISOString();
  return {
    product_name: p.name || "",
    brand: p.brand || "",
    upc: p.upc || undefined,
    upcs: p.upc ? [p.upc] : undefined,
    ingredients_text: p.ingredientsText || "",
    source: "goodnes.com",
    source_id: p.url,
    source_created_at: now,
    source_last_updated_at: now,
    image_url: p.imageUrl || undefined,
    serving_size_text: p.nutrition?.serving_size_text ?? undefined,
    serving_size_value: p.nutrition?.serving_size_value ?? undefined,
    serving_size_unit: p.nutrition?.serving_size_unit_text ?? undefined,
    nutrition: p.nutrition ?? undefined,
    scraper_job_id: jobId,
  };
}

async function main() {
  const { url, configPath, limit, offset, local, debug, noHeadless } = parseArgs();

  DEBUG_NESTLE = debug;
  if (noHeadless) NESTLE_HEADLESS = false;
  if (local) {
    console.log("Running in local mode: skipping DynamoDB and S3; API submission still runs.");
  }
  if (limit != null) {
    console.log(`Limit: processing at most ${limit} products.`);
  }
  if (offset != null && offset > 0) {
    console.log(`Offset: skipping first ${offset} products.`);
  }

  const jobId = uuidv4();
  const runDateTime = new Date().toISOString().replace(/[:.]/g, "-").slice(0, -5);

  if (!local && SCRAPER_JOB_STATUS_TABLE_NAME) {
    await docClient.send(
      new PutCommand({
        TableName: SCRAPER_JOB_STATUS_TABLE_NAME,
        Item: { job_id: jobId, scraper_name: SCRAPER_NAME, status: "active", created_at: new Date().toISOString(), updated_at: new Date().toISOString() },
      })
    );
    console.log(`Created job ${jobId}`);
  }

  let urls: UrlEntry[] = [];
  if (url) {
    if (isListingUrl(url)) {
      const targetCount = limit != null ? limit + (offset ?? 0) : undefined;
      urls = await discoverListingUrls(url, targetCount, null);
    } else {
      urls = [{ url, brandOverride: null }];
    }
  } else {
    try {
      const raw = await import("fs/promises").then((m) => m.readFile(configPath, "utf-8"));
      const cfg = JSON.parse(raw);
      const searchEntries = (cfg.searchUrls || []).filter(
        (u: unknown) => typeof u === "string" || (u && typeof u === "object")
      );
      const rawUrls = (cfg.urls || []).filter((u: unknown) => typeof u === "string" || (u && typeof u === "object"));

      for (const u of searchEntries) {
        const entry = typeof u === "string" ? { url: u, brand: null } : u;
        const entryUrl = entry.url;
        const entryBrand = entry.brand || null;
        const targetCount = limit != null ? limit + (offset ?? 0) : undefined;
        const discovered = await discoverListingUrls(entryUrl, targetCount, entryBrand);
        urls.push(...discovered);
        if (limit && urls.length >= limit + (offset ?? 0)) break;
      }

      for (const u of rawUrls) {
        const entry = typeof u === "string" ? { url: u, brand: null } : u;
        urls.push({ url: entry.url, brandOverride: entry.brand || null });
        if (limit && urls.length >= limit + (offset ?? 0)) break;
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
    console.error("No Nestle URLs found. Provide --url or config.json with urls.");
    process.exit(1);
  }

  const results: ScraperProductOutput[] = [];
  for (const u of urls) {
    const product = await fetchProduct(u.url, u.brandOverride ?? null);
    if (!product || !product.name || !product.ingredientsText || !product.upc) continue;
    results.push(transformToOutput(product, jobId));
  }

  if (!local) {
    await uploadToS3(results, jobId, runDateTime);
  }

  console.log(`\n📤 Submitting ${results.length} products for review...`);
  let submitted = 0;
  let failed = 0;
  for (const product of results) {
    const ok = await submitProductForReview(product);
    if (ok) submitted++;
    else failed++;
  }
  console.log(`\n📊 API: ${submitted} submitted, ${failed} failed`);

  if (!local && SCRAPER_JOB_STATUS_TABLE_NAME) {
    await updateJobStatus(jobId, "completed");
  }
}

main().catch(async (err) => {
  console.error("Fatal error:", err);
  if (SCRAPER_JOB_STATUS_TABLE_NAME) {
    try {
      await updateJobStatus(uuidv4(), "failed", err?.message || "unknown error");
    } catch {
      // ignore
    }
  }
  process.exit(1);
});
