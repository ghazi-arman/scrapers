import axios from "axios";
import * as fs from "fs/promises";
import * as path from "path";
import { fileURLToPath } from "url";
import * as cheerio from "cheerio";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { v4 as uuidv4 } from "uuid";
import { chromium, type Browser } from "playwright";
import type { ScraperProductOutput, ScraperNutritionData } from "../shared-types";
import * as nameUtils from "../name-utils";
import * as servingSizeUtils from "../serving-size-utils";
import * as productIdUtils from "../product-id-utils";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const cleanProductName =
  (nameUtils as any).cleanProductName ?? (nameUtils as any).default?.cleanProductName;
const parseServingSizeFromText =
  (servingSizeUtils as any).parseServingSizeFromText ??
  (servingSizeUtils as any).default?.parseServingSizeFromText;
const generateDeterministicProductId =
  (productIdUtils as any).generateDeterministicProductId ??
  (productIdUtils as any).default?.generateDeterministicProductId;

type UrlEntry = string | { url: string; stripWeight?: boolean; reorderName?: boolean };

type AppConfig = {
  urls?: UrlEntry[];
  searchUrls?: UrlEntry[];
  reorderName?: boolean;
};

type ScrapedProduct = {
  productUrl: string;
  name: string | null;
  brand: string;
  upc12: string | null;
  upcs?: string[];
  ingredientsText: string | null;
  allergenStatement?: string | null;
  imageUrl: string | null;
  nutrition?: ScraperNutritionData | null;
  servingSizeText?: string | null;
  sourceCreatedAt: string | null;
  sourceLastUpdatedAt: string | null;
};

const SCRAPER_NAME = process.env.JOB_NAME || "smart-label-mondelez";
const SCRAPER_OUTPUTS_BUCKET = process.env.SCRAPER_OUTPUTS_BUCKET;
const SCRAPER_JOB_STATUS_TABLE_NAME = process.env.SCRAPER_JOB_STATUS_TABLE_NAME;
const API_BASE_URL = process.env.API_BASE_URL || "https://api.mytummi.app";
const PRODUCTS_API_URL = `${API_BASE_URL}/products`;
const API_KEYS_PARAMETER_NAME = process.env.API_KEYS_PARAMETER_NAME || "/tummi/api-keys";
let DEBUG_SMART_LABEL = false;
let SMART_LABEL_HEADLESS = process.env.SMART_LABEL_HEADLESS !== "0";

const s3Client = new S3Client({});
const dynamoDbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);
const ssmClient = new SSMClient({});

let serviceTokenCache: string | null = null;

type NutrientQualifier = "less_than" | "greater_than" | "approximately";
type ParsedNutrientAmount = { value: number; qualifier: NutrientQualifier | null };

function parseNutrientAmountWithQualifier(amount: string | null): ParsedNutrientAmount | null {
  if (!amount || typeof amount !== "string") return null;
  const cleaned = amount.trim();
  let qualifier: NutrientQualifier | null = null;
  let rest = cleaned;
  if (/^</.test(rest)) {
    qualifier = "less_than";
    rest = rest.replace(/^<\s*/, "");
  } else if (/^>/.test(rest)) {
    qualifier = "greater_than";
    rest = rest.replace(/^>\s*/, "");
  } else if (/^[≈~∼]/.test(rest)) {
    qualifier = "approximately";
    rest = rest.replace(/^[≈~∼]\s*/, "");
  }
  const match = rest.match(/^(\d+(?:\.\d+)?)/);
  if (!match) return null;
  const value = parseFloat(match[1]);
  if (Number.isNaN(value)) return null;
  return { value, qualifier };
}

async function getServiceToken(): Promise<string> {
  if (serviceTokenCache) return serviceTokenCache;
  const command = new GetParameterCommand({ Name: API_KEYS_PARAMETER_NAME, WithDecryption: true });
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
    if (DEBUG_SMART_LABEL) console.log("[DEBUG] product exists check failed:", e);
    return false;
  }
}

function normalizeWhitespace(text: string): string {
  return text.replace(/\s+/g, " ").trim();
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

function extractUpc(text: string): string | null {
  const upc12 = text.match(/\b(\d{12})\b/);
  if (upc12?.[1]) return upc12[1];
  const upc14 = text.match(/\b(\d{14})\b/);
  if (upc14?.[1]) return upc14[1].slice(-12);
  const upc13 = text.match(/\b(\d{13})\b/);
  if (upc13?.[1]) return upc13[1].slice(-12);
  return null;
}

function parseServingSize(servingSizeText: string | null): { value: number | null; unit: string | null } {
  if (typeof parseServingSizeFromText !== "function") {
    throw new Error("parseServingSizeFromText import failed");
  }
  return parseServingSizeFromText(servingSizeText);
}


const NUTRIENT_COLUMN_MAP: Record<string, string> = {
  "total fat": "total_fat_g",
  "saturated fat": "saturated_fat_g",
  "trans fat": "trans_fat_g",
  cholesterol: "cholesterol_mg",
  sodium: "sodium_mg",
  "total carbohydrate": "total_carbs_g",
  "dietary fiber": "fiber_g",
  sugars: "sugars_g",
  "total sugars": "sugars_g",
  "added sugars": "added_sugars_g",
  protein: "protein_g",
  "vitamin d": "vitamin_d_mcg",
  "vitamin c": "vitamin_c_mg",
  "vitamin a": "vitamin_a_mcg",
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
  magnesium: "magnesium_mg",
  phosphorus: "phosphorus_mg",
  potassium: "potassium_mg",
  zinc: "zinc_mg",
};

function mapNutrientToColumn(name: string): string | null {
  const lower = name.toLowerCase().trim();
  if (lower.includes("polyunsaturated")) return "polyunsaturated_fat_g";
  if (lower.includes("monounsaturated")) return "monounsaturated_fat_g";
  if (lower.includes("added sugars")) return "added_sugars_g";
  if (lower.includes("total sugars")) return "sugars_g";
  if (lower.includes("folic acid")) return "folic_acid_mcg";
  if (lower.includes("folate")) return "folate_mcg";
  for (const [key, col] of Object.entries(NUTRIENT_COLUMN_MAP)) {
    if (lower.includes(key)) return col;
  }
  return null;
}

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

function mapNutrientToDvColumn(name: string): string | null {
  const lower = name.toLowerCase().trim();
  if (lower.includes("folic acid")) return "folic_acid_dv_pct";
  if (lower.includes("folate")) return "folate_dv_pct";
  for (const [key, col] of Object.entries(NUTRIENT_DV_COLUMN_MAP)) {
    if (lower.includes(key)) return col;
  }
  return null;
}

function cleanAndOrText(text: string): { text: string; andOr: boolean } {
  const trimmed = text.trim();
  const andOrMatch = /^and\/?or\s+/i;
  if (andOrMatch.test(trimmed)) {
    return { text: trimmed.replace(andOrMatch, "").trim(), andOr: true };
  }
  return { text: trimmed, andOr: false };
}

function joinSubIngredients(items: string[], hasAndOr: boolean): string {
  if (items.length === 0) return "";
  if (items.length === 1) return items[0];
  if (hasAndOr) {
    if (items.length === 2) {
      return `${items[0]} and/or ${items[1]}`;
    }
    const head = items.slice(0, -1).join(", ");
    const last = items[items.length - 1];
    return `${head}, and/or ${last}`;
  }
  return items.join(", ");
}

function parseIngredientList($: cheerio.CheerioAPI, root: cheerio.Cheerio<cheerio.Element>): string[] {
  const items: string[] = [];
  root.children("li").each((_, li) => {
    const el = $(li);
    const childList = el.children("ul, ol").first();
    const textNodes = el
      .clone()
      .children("ul, ol")
      .remove()
      .end()
      .text()
      .trim();
    const baseText = normalizeWhitespace(textNodes);
    if (childList.length) {
      const subItemsRaw = parseIngredientList($, childList);
      let hasAndOr = false;
      const subItems = subItemsRaw
        .map((t) => {
          const cleaned = cleanAndOrText(t);
          if (cleaned.andOr) hasAndOr = true;
          return cleaned.text;
        })
        .filter(Boolean);
      const joined = joinSubIngredients(subItems, hasAndOr);
      if (baseText && joined) items.push(`${baseText} (${joined})`);
      else if (baseText) items.push(baseText);
      else if (joined) items.push(joined);
    } else if (baseText) {
      items.push(baseText);
    }
  });
  return items;
}

function extractIngredientsFromAriaTree($: cheerio.CheerioAPI): string | null {
  const nodes = $("[role='treeitem'][aria-level], [data-level]").toArray();
  if (nodes.length === 0) return null;

  type TreeNode = { text: string; children: TreeNode[] };
  const root: TreeNode = { text: "__root__", children: [] };
  const stack: { level: number; node: TreeNode }[] = [{ level: 0, node: root }];

  for (const el of nodes) {
    const $el = $(el);
    const rawText = normalizeWhitespace($el.text());
    if (!rawText) continue;
    const levelAttr = $el.attr("aria-level") ?? $el.attr("data-level");
    const level = Math.max(1, parseInt(String(levelAttr ?? "1"), 10) || 1);
    const node: TreeNode = { text: rawText, children: [] };
    while (stack.length && stack[stack.length - 1].level >= level) {
      stack.pop();
    }
    const parent = stack[stack.length - 1]?.node ?? root;
    parent.children.push(node);
    stack.push({ level, node });
  }

  const render = (node: TreeNode): string | null => {
    const name = normalizeWhitespace(node.text);
    if (!name) return null;
    if (!node.children.length) return name;
    const children = node.children
      .map((child) => render(child))
      .filter(Boolean) as string[];
    if (!children.length) return name;
    const joined = joinSubIngredients(children, false);
    return `${name} (${joined})`;
  };

  const rendered = root.children.map(render).filter(Boolean) as string[];
  return rendered.length ? rendered.join(", ") : null;
}

function extractIngredientsFromListGroup(
  $: cheerio.CheerioAPI,
  container: cheerio.Cheerio<cheerio.Element>
): string | null {
  const anchors = container.find("a").toArray();
  if (anchors.length === 0) return null;

  type TreeNode = { text: string; children: TreeNode[] };
  const root: TreeNode = { text: "__root__", children: [] };
  const stack: { level: number; node: TreeNode }[] = [{ level: 0, node: root }];

  for (const el of anchors) {
    const $el = $(el);
    const label =
      normalizeWhitespace($el.find(".list-title").first().text()) ||
      normalizeWhitespace($el.text());
    if (!label) continue;
    const level = $el.parentsUntil(container, "ul").length + 1;
    const node: TreeNode = { text: label, children: [] };
    while (stack.length && stack[stack.length - 1].level >= level) {
      stack.pop();
    }
    const parent = stack[stack.length - 1]?.node ?? root;
    parent.children.push(node);
    stack.push({ level, node });
  }

  const render = (node: TreeNode): string | null => {
    const name = normalizeWhitespace(node.text);
    if (!name) return null;
    if (!node.children.length) return name;
    const children = node.children.map((child) => render(child)).filter(Boolean) as string[];
    if (!children.length) return name;
    const joined = joinSubIngredients(children, false);
    return `${name} (${joined})`;
  };

  const rendered = root.children.map(render).filter(Boolean) as string[];
  return rendered.length ? rendered.join(", ") : null;
}

function extractIngredients($: cheerio.CheerioAPI): string | null {
  const smartLabelList = $("#ingredient-list");
  if (smartLabelList.length) {
    const container = smartLabelList.find("ul.list-group").first();
    if (container.length) {
      const treeText = extractIngredientsFromListGroup($, container);
      if (treeText) return treeText;
    }
  }

  const ingredientsSection = $("#ingredients");
  if (ingredientsSection.length) {
    const tree = extractIngredientsFromAriaTree(ingredientsSection);
    if (tree) return tree;
    const list = ingredientsSection.find("ul, ol").first();
    if (list.length) {
      const items = parseIngredientList($, list).filter(Boolean);
      return items.length ? items.join(", ") : null;
    }
  }

  const header = $("h1, h2, h3").filter((_, el) => $(el).text().trim().toLowerCase() === "ingredients").first();
  if (header.length) {
    const tree = extractIngredientsFromAriaTree(header.parent());
    if (tree) return tree;
    const list = header.parent().find("ul, ol").first();
    if (list.length) {
      const items = parseIngredientList($, list).filter(Boolean);
      return items.length ? items.join(", ") : null;
    }
  }

  return null;
}

function extractAllergenStatement($: cheerio.CheerioAPI): string | null {
  const list = $("#allergens-list");
  if (!list.length) return null;
  const contains = new Set<string>();
  const mayContain = new Set<string>();
  const shared = new Set<string>();
  list.find("li").each((_, el) => {
    const row = $(el);
    const name =
      normalizeWhitespace(row.find(".list-title h3").first().text()) ||
      normalizeWhitespace(row.find(".col-xs-8").first().text()) ||
      normalizeWhitespace(row.text().replace(/May Contain|Contains|Shared Facility/gi, ""));
    const badge =
      normalizeWhitespace(row.find(".contain-link span").first().text()) ||
      normalizeWhitespace(row.find(".badge").first().text());
    if (!name || !badge) return;
    if (/may\s*contain/i.test(badge)) mayContain.add(name.toLowerCase());
    else if (/contains/i.test(badge)) contains.add(name.toLowerCase());
    else if (/shared\s*facility/i.test(badge)) shared.add(name.toLowerCase());
  });
  const mayContainOnly = Array.from(mayContain).filter((item) => !contains.has(item));
  if (!contains.size && !mayContainOnly.length && !shared.size) return null;
  const parts: string[] = [];
  if (contains.size) parts.push(`Contains ${Array.from(contains).join(", ")}.`);
  if (mayContainOnly.length) parts.push(`May contain ${mayContainOnly.join(", ")}.`);
  if (shared.size) parts.push(`Made in a shared facility that may use ${Array.from(shared).join(", ")}.`);
  return parts.join(" ").trim();
}

function extractNutritionSection($: cheerio.CheerioAPI): cheerio.Cheerio<cheerio.Element> | null {
  const byId = $("#nutrition");
  if (byId.length) return byId.first();
  const byAnchor = $("a[name='nutrition']");
  if (byAnchor.length) return byAnchor.first().parent();
  const byHeader = $("h1, h2, h3").filter((_, el) => $(el).text().trim().toLowerCase() === "nutrition");
  if (byHeader.length) return byHeader.first().parent();
  return null;
}

function parseSmartLabelNutrition($: cheerio.CheerioAPI): { nutrition: ScraperNutritionData | null; servingSizeText: string | null } {
  const section = $(".nutrition-section").first();
  if (!section.length) return { nutrition: null, servingSizeText: null };

  const nutrition: ScraperNutritionData = {
    serving_size_value: 1,
    serving_size_unit_text: "serving",
    serving_size_text: null,
  };

  const servingsText =
    section.find(".nfp__header p").first().text().trim() ||
    section.find(".nfp__header").find("p").first().text().trim();
  const servingSizeText = section
    .find(".nfp__header .flex-row-container")
    .last()
    .text()
    .replace(/\s+/g, " ")
    .trim();

  if (servingSizeText) {
    nutrition.serving_size_text = servingSizeText.replace(/^Serving Size\s*/i, "").trim();
    const parsed = parseServingSize(nutrition.serving_size_text);
    nutrition.serving_size_value = parsed.value ?? 1;
    nutrition.serving_size_unit_text = parsed.unit ?? "serving";
  }

  const caloriesText =
    section.find(".top-fact-info .top-fact").first().find("span").first().text().trim() ||
    section.find(".nfp__values .calSize strong").last().text().trim();
  if (caloriesText) {
    const parsed = parseNutrientAmountWithQualifier(caloriesText);
    if (parsed) {
      nutrition.calories = parsed.value;
      if (parsed.qualifier) nutrition.calories_qualifier = parsed.qualifier;
    }
  }

  section.find(".nfp__row").each((_, row) => {
    let label = normalizeWhitespace($(row).find("span, strong").first().text()).toLowerCase();
    label = label.replace(/\[[^\]]*\]/g, "").replace(/\s+/g, " ").trim();
    const valueCandidates = $(row)
      .find(".nfp__values-right")
      .toArray()
      .map((el) => decodeHtmlEntities(normalizeWhitespace($(el).text())) || "")
      .filter(Boolean);
    const dvText = decodeHtmlEntities(normalizeWhitespace($(row).find(".nfp__values-dvp").first().text()));
    let valueText = valueCandidates.find((v) => /<|>|≈|~|\d/.test(v)) || "";
    const valueLooksLikeDv = /%$/.test(valueText.trim());
    if (!label) return;
    if (DEBUG_SMART_LABEL) {
      console.log(`[DEBUG] nutrition row: label="${label}" value="${valueText}"`);
    }
    const col = mapNutrientToColumn(label);
    if (col && valueText && !valueLooksLikeDv) {
      const parsed = parseNutrientAmountWithQualifier(valueText);
      if (parsed) {
        (nutrition as unknown as Record<string, number>)[col] = parsed.value;
        if (parsed.qualifier) (nutrition as unknown as Record<string, string>)[`${col}_qualifier`] = parsed.qualifier;
      }
    }
    const dvCandidate = dvText || (valueLooksLikeDv ? valueText : "");
    if (dvCandidate) {
      const dvCol = mapNutrientToDvColumn(label);
      if (dvCol) {
        const dvMatch = dvCandidate.match(/([\d.]+)/);
        if (dvMatch) {
          (nutrition as unknown as Record<string, number>)[dvCol] = parseFloat(dvMatch[1]);
        }
      }
    }
  });

  const hasNutrients = Object.keys(nutrition).some((k) =>
    [
      "calories",
      "protein_g",
      "total_carbs_g",
      "fiber_g",
      "sugars_g",
      "added_sugars_g",
      "total_fat_g",
      "saturated_fat_g",
      "trans_fat_g",
      "cholesterol_mg",
      "sodium_mg",
      "potassium_mg",
      "calcium_mg",
      "iron_mg",
      "vitamin_d_mcg",
      "folate_mcg",
      "folic_acid_mcg",
    ].includes(k)
  );
  return { nutrition: hasNutrients ? nutrition : null, servingSizeText: nutrition.serving_size_text ?? null };
}

function extractNutritionFromTables($: cheerio.CheerioAPI, root: cheerio.Cheerio<cheerio.Element>): ScraperNutritionData | null {
  const result: ScraperNutritionData = {
    serving_size_value: 1,
    serving_size_unit_text: "serving",
    serving_size_text: null,
  };

  let servingSizeText: string | null = null;
  let calories: number | null = null;
  let caloriesQualifier: 'less_than' | 'greater_than' | 'approximately' | null = null;

  root.find("tr").each((_, row) => {
    const cells = $(row).find("th, td");
    if (cells.length < 1) return;
    const label = normalizeWhitespace($(cells[0]).text()).toLowerCase();
    const val1 = cells.length > 1 ? normalizeWhitespace($(cells[1]).text()) : "";
    const val2 = cells.length > 2 ? normalizeWhitespace($(cells[2]).text()) : "";

    if (label.includes("serving size") && val1) servingSizeText = val1;
    if (label.startsWith("calories") && val1) {
    const parsed = parseNutrientAmountWithQualifier(val1);
      if (parsed) {
        calories = parsed.value;
        caloriesQualifier = parsed.qualifier;
      }
    }

    const col = mapNutrientToColumn(label);
    if (col && val1) {
    const parsed = parseNutrientAmountWithQualifier(val1);
      if (parsed) {
        (result as unknown as Record<string, number>)[col] = parsed.value;
        if (parsed.qualifier) (result as unknown as Record<string, string>)[`${col}_qualifier`] = parsed.qualifier;
      }
    }
    if (val2) {
      const dvCol = mapNutrientToDvColumn(label);
      if (dvCol) {
        const dvMatch = val2.match(/([\d.]+)/);
        if (dvMatch) {
          (result as unknown as Record<string, number>)[dvCol] = parseFloat(dvMatch[1]);
        }
      }
    }
    if (label.includes("added sugars") && val1) {
    const parsed = parseNutrientAmountWithQualifier(val1);
      if (parsed) {
        result.added_sugars_g = parsed.value;
        if (parsed.qualifier) result.added_sugars_g_qualifier = parsed.qualifier;
      }
    }
    if (label.includes("total sugars") && val1) {
    const parsed = parseNutrientAmountWithQualifier(val1);
      if (parsed) {
        result.sugars_g = parsed.value;
        if (parsed.qualifier) result.sugars_g_qualifier = parsed.qualifier;
      }
    }
    if (label.includes("protein") && val1) {
    const parsed = parseNutrientAmountWithQualifier(val1);
      if (parsed) {
        result.protein_g = parsed.value;
        if (parsed.qualifier) result.protein_g_qualifier = parsed.qualifier;
      }
    }
    if (label.includes("calcium") && val1) {
    const parsed = parseNutrientAmountWithQualifier(val1);
      if (parsed) {
        result.calcium_mg = parsed.value;
        if (parsed.qualifier) result.calcium_mg_qualifier = parsed.qualifier;
      }
    }
    if (label.includes("iron") && val1) {
    const parsed = parseNutrientAmountWithQualifier(val1);
      if (parsed) {
        result.iron_mg = parsed.value;
        if (parsed.qualifier) result.iron_mg_qualifier = parsed.qualifier;
      }
    }
    if (label.includes("potassium") && val1) {
    const parsed = parseNutrientAmountWithQualifier(val1);
      if (parsed) {
        result.potassium_mg = parsed.value;
        if (parsed.qualifier) result.potassium_mg_qualifier = parsed.qualifier;
      }
    }
  });

  if (servingSizeText) {
    result.serving_size_text = servingSizeText;
    const parsed = parseServingSize(servingSizeText);
    result.serving_size_value = parsed.value ?? 1;
    result.serving_size_unit_text = parsed.unit ?? "serving";
  }
  if (calories !== null) {
    result.calories = calories;
    if (caloriesQualifier) result.calories_qualifier = caloriesQualifier;
  }

  const hasNutrients = Object.keys(result).some((k) =>
    [
      "calories",
      "protein_g",
      "total_carbs_g",
      "fiber_g",
      "sugars_g",
      "added_sugars_g",
      "total_fat_g",
      "saturated_fat_g",
      "trans_fat_g",
      "cholesterol_mg",
      "sodium_mg",
      "potassium_mg",
      "calcium_mg",
      "iron_mg",
      "vitamin_d_mcg",
      "folate_mcg",
      "folic_acid_mcg",
    ].includes(k)
  );
  return hasNutrients ? result : null;
}

function extractNutritionFromText(text: string): { nutrition: ScraperNutritionData | null; servingSizeText: string | null } {
  const lower = text.toLowerCase();
  if (!/nutrition facts|serving size|calories/.test(lower)) return { nutrition: null, servingSizeText: null };

  const servingSize = text.match(/serving size\s*:?\\s*([^\n\r]+)/i)?.[1]?.trim() ?? null;
  const calories = text.match(/calories\s*:?\\s*(\d+)/i)?.[1] ?? null;

  const nutrition: ScraperNutritionData = {
    serving_size_value: 1,
    serving_size_unit_text: "serving",
    serving_size_text: servingSize,
  };
  if (servingSize) {
    const parsed = parseServingSize(servingSize);
    nutrition.serving_size_value = parsed.value ?? 1;
    nutrition.serving_size_unit_text = parsed.unit ?? "serving";
  }
  if (calories) nutrition.calories = parseInt(calories, 10);

  return { nutrition, servingSizeText: servingSize };
}

function extractSmartLabelGuid(html: string): string | null {
  const patterns = [
    /bioBadge\(\s*'([0-9a-f-]{36})'/i,
    /certiSus\(\s*'([0-9a-f-]{36})'/i,
    /getIngredient\(\s*'([0-9a-f-]{36})'/i,
  ];
  for (const re of patterns) {
    const match = html.match(re);
    if (match?.[1]) return match[1];
  }
  const any = html.match(/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i);
  return any?.[0] ?? null;
}

async function fetchNutritionHtmlGuess(url: string, html: string): Promise<string | null> {
  const guid = extractSmartLabelGuid(html);
  if (!guid) return null;
  const stripped = url.replace(/[#?].*$/, "");
  const lastSlash = stripped.lastIndexOf("/");
  const base = lastSlash >= 0 ? stripped.slice(0, lastSlash + 1) : stripped;
  const candidates = [
    `${base}${guid}-nutrition.html`,
    `${base}${guid}-nutritionfacts.html`,
    `${base}${guid}-nutrition-facts.html`,
    `${base}${guid}-nutritionfact.html`,
  ];
  for (const candidate of candidates) {
    try {
      const res = await axios.get(candidate, {
        headers: {
          "user-agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout: 30_000,
      });
      if (typeof res.data === "string" && /nutrition|calories|serving/i.test(res.data)) {
        return res.data as string;
      }
    } catch {
      // ignore candidate failures
    }
  }
  return null;
}

function extractHeaderName($: cheerio.CheerioAPI): string | null {
  const headerName =
    $(".product-header-name h1").first().text() ||
    $("[class*='Header__Title']").first().text() ||
    $(".Header__Title").first().text();
  if (headerName) return normalizeWhitespace(decodeHtmlEntities(headerName) || headerName);
  return null;
}

function extractName($: cheerio.CheerioAPI): string | null {
  const h1 = $("h1").first().text();
  if (h1) return normalizeWhitespace(decodeHtmlEntities(h1) || h1);
  const og = $("meta[property='og:title']").attr("content");
  if (og) return normalizeWhitespace(decodeHtmlEntities(og) || og);
  const title = $("title").text();
  if (title) return normalizeWhitespace(decodeHtmlEntities(title) || title);
  return null;
}

function extractHeaderBrand($: cheerio.CheerioAPI): string | null {
  const headerBrand =
    $(".product-subheader").first().text() ||
    $("[class*='Header__Brand']").first().text() ||
    $(".Header__Brand").first().text();
  if (headerBrand) return normalizeWhitespace(decodeHtmlEntities(headerBrand) || headerBrand);
  return null;
}

function isLikelyBrand(text: string | null): boolean {
  if (!text) return false;
  if (/,/.test(text)) return false;
  const words = text.trim().split(/\s+/).filter(Boolean);
  return words.length > 0 && words.length <= 4;
}

function extractBrand($: cheerio.CheerioAPI): string | null {
  const meta = $("meta[name='brand']").attr("content");
  if (meta) return normalizeWhitespace(meta);
  const ogSite = $("meta[property='og:site_name']").attr("content");
  if (ogSite) return normalizeWhitespace(ogSite);
  return null;
}

function deriveBrandAndName(name: string | null): { brand: string | null; name: string | null } {
  if (!name) return { brand: null, name: null };
  const parts = name.split(",").map((p) => p.trim()).filter(Boolean);
  if (parts.length <= 1) return { brand: null, name };
  const brand = parts[0];
  const rest = parts.slice(1);
  const reordered = rest.length >= 2 ? [rest[rest.length - 1], ...rest.slice(0, -1)] : rest;
  return { brand, name: reordered.join(", ") };
}

function stripWeightFromName(name: string | null): string | null {
  return cleanProductName(name, {
    stripTrailingWeight: true,
    stripTrailingCommaSize: true,
    stripParenAtEnd: true,
  });
}

function removeBrandPrefix(name: string | null, brand: string | null): string | null {
  if (!name || !brand) return name;
  const lowerName = name.toLowerCase();
  const lowerBrand = brand.toLowerCase();
  if (!lowerName.startsWith(lowerBrand)) return name;
  let trimmed = name.slice(brand.length);
  trimmed = trimmed.replace(/^[\s,]+/, "");
  return trimmed;
}

function normalizeIngredientsText(text: string | null): string {
  if (!text) return "";
  return text
    .toLowerCase()
    .replace(/[.,;:()]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isOnlyWaterIngredients(text: string | null): boolean {
  const normalized = normalizeIngredientsText(text);
  return normalized === "water" || normalized === "purified water";
}

function extractImage($: cheerio.CheerioAPI, baseOrigin: string): string | null {
  const original = $("#orignalImage, #originalImage").attr("src");
  if (original) {
    if (original.startsWith("http")) return original;
    if (original.startsWith("/")) return `${baseOrigin}${original}`;
    return original;
  }
  const og = $("meta[property='og:image']").attr("content");
  if (og) {
    if (og.startsWith("http")) return og;
    if (og.startsWith("/")) return `${baseOrigin}${og}`;
    return `${baseOrigin}/${og}`;
  }
  const img = $("img").first().attr("src");
  if (!img) return null;
  if (img.startsWith("http")) return img;
  if (img.startsWith("/")) return `${baseOrigin}${img}`;
  return img;
}

async function fetchRenderedHtml(browser: Browser, url: string): Promise<string> {
  const tryFetch = async (): Promise<string> => {
  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  });
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForSelector("#sections-container", { timeout: 8000 }).catch(() => {});
    await page.waitForFunction("typeof window.jQuery !== 'undefined'", { timeout: 8000 }).catch(() => {});
    await page.waitForTimeout(1000);
    if (url.includes("#nutrition")) {
      const tab = page.locator("#section-nutrition, .text-nutrition, [data-section='nutrition']");
      if (await tab.count()) {
        await tab.first().scrollIntoViewIfNeeded().catch(() => null);
        await tab.first().click({ timeout: 5000 }).catch(() => null);
        await page
          .evaluate(() => {
            const el = document.querySelector("#section-nutrition") as HTMLElement | null;
            if (el) {
              el.dispatchEvent(new MouseEvent("click", { bubbles: true }));
            }
            window.location.hash = "nutrition";
          })
          .catch(() => null);
      }
    } else if (url.includes("#ingredients")) {
      const tab = page.locator("#section-ingredients, .text-ingredients, [data-section='ingredients']");
      if (await tab.count()) {
        await tab.first().scrollIntoViewIfNeeded().catch(() => null);
        await tab.first().click({ timeout: 5000 }).catch(() => null);
      }
    } else if (url.includes("#allergens")) {
      const tab = page.locator("#section-allergens, .text-allergens, [data-section='allergens']");
      if (await tab.count()) {
        await tab.first().scrollIntoViewIfNeeded().catch(() => null);
        await tab.first().click({ timeout: 5000 }).catch(() => null);
      }
    }
    if (url.includes("#nutrition")) {
      await page
        .waitForResponse(
          (resp) => /nutrition|nutrient|nutritionfacts|nutrition-facts/i.test(resp.url()),
          { timeout: 8000 }
        )
        .catch(() => null);
    }
    await page.waitForLoadState("networkidle", { timeout: 8000 }).catch(() => {});
    if (url.includes("#nutrition")) {
      const start = Date.now();
      while (Date.now() - start < 12_000) {
        const hasNutritionText = await page
          .evaluate(() => {
            const text = document.body?.innerText || "";
            return /calories|serving size|nutrition facts/i.test(text);
          })
          .catch(() => false);
        if (hasNutritionText) break;
        await page.waitForTimeout(500);
      }
    }
    await page
      .waitForSelector(
        "#nutrition, #ingredients, #allergens, #allergens-list, [id*='nutrition'], [id*='ingredient'], [id*='allergen'], table",
        { timeout: 5000 }
      )
      .catch(() => {});
    const html = await page.content();
    return html;
  } finally {
    await page.close().catch(() => null);
  }
  };
  try {
    return await tryFetch();
  } catch (err) {
    if (DEBUG_SMART_LABEL) {
      console.error(`[DEBUG] render failed for ${url}, retrying once...`, err);
    }
    try {
      return await tryFetch();
    } catch (err2) {
      if (DEBUG_SMART_LABEL) {
        console.error(`[DEBUG] render failed for ${url} after retry:`, err2);
      }
      return "";
    }
  }
}


async function discoverProductUrls(
  browser: Browser,
  searchUrl: string,
  maxUrls?: number
): Promise<string[]> {
  if (maxUrls != null && maxUrls <= 0) return [];
  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  });
  try {
    const startUrl = new URL(searchUrl);
    const isSmartlabelSearch =
      /smartlabel\.org$/i.test(startUrl.host) && /\/product-search\//i.test(startUrl.pathname);
    if (isSmartlabelSearch) {
      const collected = new Set<string>();
      let pageNum = parseInt(startUrl.searchParams.get("pn") || "1", 10);
      if (!Number.isFinite(pageNum) || pageNum < 1) pageNum = 1;
      while (true) {
        startUrl.searchParams.set("pn", String(pageNum));
        await page.goto(startUrl.toString(), { waitUntil: "domcontentloaded", timeout: 60_000 });
        await page.waitForTimeout(800);
        const hrefs = await page.$$eval("a[href]", (links) =>
          links
            .map((a) => (a as HTMLAnchorElement).href)
            .filter((href) => /smartlabel\.mondelez\.info/i.test(href))
        );
        const beforeCount = collected.size;
        for (const href of hrefs) {
          if (!href) continue;
          collected.add(href.split("#")[0].replace(/\?.*$/, ""));
          if (maxUrls && collected.size >= maxUrls) {
            return Array.from(collected);
          }
        }
        if (DEBUG_SMART_LABEL) {
          const html = await page.content();
          await fs.writeFile(`/tmp/smart-label-catalog-${pageNum}.html`, html).catch(() => null);
        }
        if (hrefs.length === 0 || collected.size === beforeCount) break;
        pageNum += 1;
      }
      return Array.from(collected);
    }

    await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(1500);
    const collected = new Set<string>();
    let pageIndex = 0;
    while (true) {
      pageIndex += 1;
      const hrefs = await page.$$eval("a[href]", (links) =>
        links
          .map((a) => (a as HTMLAnchorElement).href)
          .filter((href) => /smartlabel\.mondelez\.info/i.test(href))
      );
      for (const href of hrefs) {
        if (!href) continue;
        collected.add(href.split("#")[0].replace(/\?.*$/, ""));
        if (maxUrls && collected.size >= maxUrls) {
          return Array.from(collected);
        }
      }
      if (DEBUG_SMART_LABEL) {
        const html = await page.content();
        await fs.writeFile(`/tmp/smart-label-catalog-${pageIndex}.html`, html).catch(() => null);
      }
      const firstHref = hrefs.find(Boolean) || "";
      const nextButton = page.locator("#pagination-next-page");
      const hasNext = (await nextButton.count()) > 0;
      if (!hasNext) break;
      const isDisabled = await nextButton.getAttribute("aria-disabled", { timeout: 3000 }).catch(() => "true");
      if (isDisabled === "true") break;
      await nextButton.click().catch(() => null);
      await page.waitForTimeout(400);
      await page
        .waitForFunction(
          (prev) => {
            const first = document.querySelector("a[href*='smartlabel.mondelez.info']") as HTMLAnchorElement | null;
            return !!first && first.href !== prev;
          },
          firstHref,
          { timeout: 3000 }
        )
        .catch(() => null);
      if (maxUrls && collected.size >= maxUrls) break;
    }
    return Array.from(collected);
  } finally {
    await page.close().catch(() => null);
  }
}

async function fetchProduct(
  browser: Browser,
  url: string,
  reorderName: boolean,
  stripWeight: boolean
): Promise<ScrapedProduct | null> {
  const fetchWithFallback = async (targetUrl: string): Promise<string> => {
    try {
      const res = await axios.get(targetUrl, {
        headers: {
          "user-agent":
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        timeout: 30_000,
      });
      return res.data as string;
    } catch (err: unknown) {
      const e = err as { response?: { status?: number } };
      if (e.response?.status === 404 && targetUrl.endsWith(".htm")) {
        const alt = targetUrl.replace(/\.htm$/i, ".html");
        const res = await axios.get(alt, {
          headers: {
            "user-agent":
              "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          },
          timeout: 30_000,
        });
        return res.data as string;
      }
      throw err;
    }
  };

  const html = await fetchWithFallback(url);
  if (DEBUG_SMART_LABEL) {
    await fs.writeFile("/tmp/smart-label.html", html).catch(() => null);
  }

  const $ = cheerio.load(html);
  const headerName = extractHeaderName($);
  const headerBrand = extractHeaderBrand($);
  const rawName =
    headerName ||
    extractName($);
  const useHeaderBrand = isLikelyBrand(headerBrand) && !!headerName;
  const derived = useHeaderBrand ? { brand: headerBrand, name: rawName } : deriveBrandAndName(rawName);
  const brand =
    ((useHeaderBrand ? headerBrand : derived.brand) ||
      extractBrand($) ||
      "Unknown"
    ).trim();
  const reordered = reorderName ? deriveBrandAndName(rawName) : { name: rawName };
  const baseName = headerName || (reordered.name ?? derived.name) || rawName;
  let name = baseName ? baseName.trim() : baseName;
  const preBrandName = name;
  
  if (name && brand) {
    const afterBrand = removeBrandPrefix(name, brand);
    // Defensive: only trim punctuation/whitespace if string ops exist (avoids rare truncation issues)
    name =
      (afterBrand as any)?.replace?.(/^[,\s]+|[,\s]+$/g, "")?.trim?.() ?? afterBrand;
  }
  
  const postBrandName = name;
  if (stripWeight) {
    name = stripWeightFromName(name);
  }
  const postWeightName = name;
  let ingredientsSource: string | null = null;
  let ingredientsText = extractIngredients($);
  if (ingredientsText) ingredientsSource = "smartlabel-dom";
  let allergenStatement = extractAllergenStatement($);
  const baseOrigin = new URL(url).origin;
  let imageUrl: string | null = extractImage($, baseOrigin);

  const upcFromText = extractUpc(html) || extractUpc($("body").text());
  const exists = await checkProductExists({ name, brand, upc: upcFromText });
  if (exists) {
    console.log(`[SKIP] ${brand} ${name ?? "(no name)"}: already exists`);
    return null;
  }
  const nutritionSection = extractNutritionSection($);
  let nutrition: ScraperNutritionData | null = null;
  let servingSizeText: string | null = null;
  if (nutritionSection) {
    nutrition = extractNutritionFromTables($, nutritionSection);
    if (nutrition?.serving_size_text) servingSizeText = nutrition.serving_size_text;
  }
  if (!nutrition) {
    const fallback = extractNutritionFromText($("body").text());
    nutrition = fallback.nutrition;
    servingSizeText = fallback.servingSizeText;
  }

  if (!nutrition) {
    const nutritionUrl = url.includes("#") ? url : `${url}#nutrition`;
    const renderedNutrition = await fetchRenderedHtml(browser, nutritionUrl);
    if (DEBUG_SMART_LABEL) {
      await fs.writeFile("/tmp/smart-label-rendered-nutrition.html", renderedNutrition).catch(() => null);
    }
    const $r = cheerio.load(renderedNutrition);
    if (!name) {
      const header = extractHeaderName($r) || extractName($r);
      if (header) name = stripWeightFromName(header);
    }
    if (!imageUrl) imageUrl = extractImage($r, baseOrigin);
    const parsedNutrition = parseSmartLabelNutrition($r);
    nutrition = parsedNutrition.nutrition;
    servingSizeText = parsedNutrition.servingSizeText ?? servingSizeText;
    if (!nutrition) {
      const section = extractNutritionSection($r);
      nutrition = section ? extractNutritionFromTables($r, section) : null;
      if (!nutrition) {
        const fallback = extractNutritionFromText($r("body").text());
        nutrition = fallback.nutrition;
        servingSizeText = fallback.servingSizeText;
      }
      if (nutrition?.serving_size_text) servingSizeText = nutrition.serving_size_text;
    }
  }

  if (!ingredientsText) {
    const ingredientsUrl = url.includes("#") ? url : `${url}#ingredients`;
    const renderedIngredients = await fetchRenderedHtml(browser, ingredientsUrl);
    if (DEBUG_SMART_LABEL) {
      await fs.writeFile("/tmp/smart-label-rendered-ingredients.html", renderedIngredients).catch(() => null);
    }
    const $r = cheerio.load(renderedIngredients);
    ingredientsText = extractIngredients($r);
    if (ingredientsText) ingredientsSource = "smartlabel-rendered";
    if (!allergenStatement) allergenStatement = extractAllergenStatement($r);
    if (!imageUrl) imageUrl = extractImage($r, baseOrigin);
  }
  if (!allergenStatement) {
    const allergensUrl = url.includes("#") ? url : `${url}#allergens`;
    const renderedAllergens = await fetchRenderedHtml(browser, allergensUrl);
    const $a = cheerio.load(renderedAllergens);
    allergenStatement = extractAllergenStatement($a);
  }
  if (DEBUG_SMART_LABEL) {
    console.log(`[DEBUG] ingredients source: ${ingredientsSource ?? "(null)"}`);
  }

  if (isOnlyWaterIngredients(ingredientsText)) {
    if (DEBUG_SMART_LABEL) {
      console.log(`[DEBUG] skipped ${url}: ingredients only water`);
    }
    return null;
  }

  if (!nutrition) {
    const nutritionHtml = await fetchNutritionHtmlGuess(url, html);
    if (nutritionHtml) {
      const $n = cheerio.load(nutritionHtml);
      const smartLabelNutrition = parseSmartLabelNutrition($n);
      nutrition = smartLabelNutrition.nutrition;
      servingSizeText = smartLabelNutrition.servingSizeText ?? servingSizeText;
      if (!nutrition) {
        const section = extractNutritionSection($n) || $n.root();
        nutrition = extractNutritionFromTables($n, section);
        if (!nutrition) {
          const fallback = extractNutritionFromText($n("body").text());
          nutrition = fallback.nutrition;
          servingSizeText = fallback.servingSizeText;
        }
        if (nutrition?.serving_size_text) servingSizeText = nutrition.serving_size_text;
      }
    }
  }

  if (DEBUG_SMART_LABEL) {
    console.log(`[DEBUG] headerName: ${headerName ?? "(null)"}`);
    console.log(`[DEBUG] rawName: ${rawName ?? "(null)"}`);
    console.log(`[DEBUG] baseName: ${baseName ?? "(null)"}`);
    console.log(`[DEBUG] name pre-brand: ${preBrandName ?? "(null)"}`);
    console.log(`[DEBUG] name post-brand: ${postBrandName ?? "(null)"}`);
    console.log(`[DEBUG] name post-weight: ${postWeightName ?? "(null)"}`);
    console.log(`[DEBUG] name: ${name ?? "(null)"}`);
    console.log(`[DEBUG] brand: ${brand ?? "(null)"}`);
    console.log(`[DEBUG] upc12: ${upcFromText ?? "(null)"}`);
    console.log(`[DEBUG] ingredients length: ${ingredientsText?.length ?? 0}`);
    console.log(`[DEBUG] allergens: ${allergenStatement ?? "(null)"}`);
    console.log(`[DEBUG] image: ${imageUrl ?? "(null)"}`);
  if (nutrition) {
      console.log(`[DEBUG] nutrition serving size: ${nutrition.serving_size_text ?? "(null)"}`);
      console.log(
        `[DEBUG] nutrition calories: ${nutrition.calories ?? "(null)"}`
      );
      console.log(
        `[DEBUG] nutrition sugars: ${nutrition.sugars_g ?? "(null)"}${nutrition.sugars_g_qualifier ? ` (${nutrition.sugars_g_qualifier})` : ""}`
      );
    } else {
      console.log(`[DEBUG] nutrition: (null)`);
    }
  }
  const status = ingredientsText && nutrition ? "OK" : "SKIP";
  console.log(`[PRODUCT] ${status} | ${name ?? "(no name)"} | ${url}`);

  return {
    productUrl: url,
    name,
    brand,
    upc12: upcFromText,
    ingredientsText,
    allergenStatement,
    imageUrl,
    nutrition,
    servingSizeText,
    sourceCreatedAt: null,
    sourceLastUpdatedAt: null,
  };
}

function transformToOutput(p: ScrapedProduct, jobId: string): ScraperProductOutput {
  const now = new Date().toISOString();
  return {product_name: p.name || "",
    brand: p.brand,
    upc: p.upc12 || undefined,
    upcs: p.upcs && p.upcs.length ? p.upcs : p.upc12 ? [p.upc12] : undefined,
    ingredients_text: p.ingredientsText || "",
    allergen_statement: p.allergenStatement || undefined,
    source: SCRAPER_NAME,
    source_id: p.productUrl,
    source_created_at: p.sourceCreatedAt || now,
    source_last_updated_at: p.sourceLastUpdatedAt || now,
    image_url: p.imageUrl || undefined,
    serving_size_text: p.servingSizeText || undefined,
    serving_size_value: p.nutrition?.serving_size_value ?? undefined,
    serving_size_unit: p.nutrition?.serving_size_unit_text ?? undefined,
    nutrition: p.nutrition ?? undefined,
    scraper_job_id: jobId};
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

function mergeProducts(products: ScrapedProduct[]): ScrapedProduct[] {
  const merged = new Map<string, ScrapedProduct>();
  for (const product of products) {
    const key = [
      normalizeMergeText(product.brand),
      normalizeMergeText(product.name),
      normalizeMergeText(product.ingredientsText),
      nutritionSignature(product.nutrition),
      normalizeMergeText(product.servingSizeText),
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

async function submitProductForReview(productOutput: ScraperProductOutput): Promise<boolean> {
  if (!productOutput.product_name || !productOutput.ingredients_text) return false;
  if (!productOutput.upc) return false;
  try {
    const token = await getServiceToken();
    const { scraper_job_id: _, ...body } = productOutput;
    if (DEBUG_SMART_LABEL) {
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
    console.error(
      `❌ Failed "${productOutput.product_name}":`,
      e.response ? `${e.response.status} ${e.response.statusText}` : e.message
    );
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
  let searchUrl: string | undefined;
  let limit: number | undefined;
  let offset: number | undefined;
  let concurrency = 5;
  let local = false;
  let debug = false;
  let noHeadless = false;
  let headless = false;
  let reorderName = false;

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
    } else if (argv[i] === "--local") {
      local = true;
    } else if ((argv[i] === "--debug" || argv[i] === "-d")) {
      debug = true;
    } else if ((argv[i] === "--concurrency" || argv[i] === "-n") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!Number.isNaN(n) && n > 0) concurrency = n;
      i++;
    } else if (argv[i] === "--no-headless") {
      noHeadless = true;
    } else if (argv[i] === "--headless") {
      headless = true;
    } else if (argv[i] === "--reorder-name") {
      reorderName = true;
    }
  }

  return { url, searchUrl, configPath, limit, offset, local, reorderName, debug, noHeadless, headless, concurrency };
}

async function main(): Promise<void> {
  const { url, searchUrl, configPath, limit, offset, local, reorderName, debug, noHeadless, headless, concurrency } = parseArgs();

  DEBUG_SMART_LABEL = debug;
  if (noHeadless) SMART_LABEL_HEADLESS = false;
  if (headless) SMART_LABEL_HEADLESS = true;

  if (local) {
    console.log("Running in local mode: skipping DynamoDB and S3; API submission still runs.");
  }
  if (offset != null && offset > 0) {
    console.log(`Offset: skipping first ${offset} products.`);
  }
  if (limit != null) {
    console.log(`Limit: processing at most ${limit} products.`);
  }
  const desiredCount = (limit ?? Number.POSITIVE_INFINITY) + (offset ?? 0);

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

  let urls: UrlEntry[] = [];
  let effectiveSearchUrl = searchUrl;
  let ranConfigSearch = false;
  let effectiveReorder = reorderName;
  const browser = await chromium.launch({ headless: SMART_LABEL_HEADLESS });
  if (url) {
    urls = [url];
  } else {
    try {
      const raw = await fs.readFile(configPath, "utf-8");
      const cfg = JSON.parse(raw) as AppConfig;
      if (DEBUG_SMART_LABEL) {
        console.log(`[DEBUG] loaded config: ${configPath}`);
        console.log(`[DEBUG] config urls: ${cfg.urls?.length ?? 0}, searchUrls: ${cfg.searchUrls?.length ?? 0}`);
      }
      urls = (cfg.urls || []).filter((u) => typeof u === "string" || typeof u === "object");
      const configReorder = cfg.reorderName === true;
      effectiveReorder = reorderName || configReorder;
      if (configReorder && !reorderName) {
        console.log("Reorder name: enabled via config");
      }
      const searchUrls = (cfg.searchUrls || []).filter((u) => typeof u === "string" || typeof u === "object");
      if (!effectiveSearchUrl && searchUrls.length) {
        const first = typeof searchUrls[0] === "string" ? { url: searchUrls[0], stripWeight: true } : searchUrls[0];
        effectiveSearchUrl = first?.url;
      }
      for (const searchUrl of searchUrls) {
        const entry =
          typeof searchUrl === "string"
            ? { url: searchUrl, stripWeight: true }
            : searchUrl;
        if (!entry?.url) continue;
        if (Number.isFinite(desiredCount) && urls.length >= desiredCount) break;
        const remaining = Number.isFinite(desiredCount) ? Math.max(desiredCount - urls.length, 0) : undefined;
        console.log(`[DISCOVER] SmartLabel catalog: ${entry.url}`);
        try {
          const discovered = await discoverProductUrls(browser, entry.url, remaining);
          console.log(`[DISCOVER] Found ${discovered.length} product URLs`);
          for (const d of discovered) {
            urls.push({
              url: d,
              stripWeight: entry.stripWeight !== false,
              reorderName: typeof entry.reorderName === "boolean" ? entry.reorderName : undefined,
            });
          }
        } catch (err) {
          console.error(`[DISCOVER] Failed ${entry.url}:`, err);
        }
      }
      ranConfigSearch = searchUrls.length > 0;
    } catch {
      // ignore
    }
  }
  if (!url && effectiveSearchUrl && !ranConfigSearch) {
    if (Number.isFinite(desiredCount) && urls.length < desiredCount) {
      const remaining = Number.isFinite(desiredCount) ? Math.max(desiredCount - urls.length, 0) : undefined;
      console.log(`[DISCOVER] SmartLabel catalog: ${effectiveSearchUrl}`);
      try {
        const discovered = await discoverProductUrls(browser, effectiveSearchUrl, remaining);
        console.log(`[DISCOVER] Found ${discovered.length} product URLs`);
        for (const d of discovered) urls.push({ url: d, stripWeight: true });
      } catch (err) {
        console.error(`[DISCOVER] Failed ${effectiveSearchUrl}:`, err);
      }
    }
  }

  const deduped = new Map<string, { url: string; stripWeight: boolean; reorderName?: boolean }>();
  for (const u of urls) {
    if (typeof u === "string") {
      if (!deduped.has(u)) deduped.set(u, { url: u, stripWeight: true, reorderName: undefined });
    } else if (u && typeof u === "object" && typeof u.url === "string") {
      if (!deduped.has(u.url)) {
        deduped.set(u.url, {
          url: u.url,
          stripWeight: u.stripWeight !== false,
          reorderName: typeof u.reorderName === "boolean" ? u.reorderName : undefined,
        });
      }
    }
  }
  let targets = Array.from(deduped.values());
  if (offset != null && offset > 0) targets = targets.slice(offset);
  if (limit != null) targets = targets.slice(0, limit);
  if (targets.length === 0) {
    console.error("No SmartLabel URLs found. Provide --url or config.json with urls.");
    process.exit(1);
  }

  const results: ScraperProductOutput[] = [];
  if (effectiveReorder) {
    console.log("Reorder name: enabled");
  }
  const valid: ScrapedProduct[] = [];
  let success = 0;
  let fail = 0;
  let submitQueue = Promise.resolve();
  let pendingBatch: ScraperProductOutput[] = [];
  const submittedKeys = new Set<string>();

  const flushBatch = (batch: ScraperProductOutput[]) => {
    submitQueue = submitQueue.then(async () => {
      console.log(`\n➡️  Submitting batch (${batch.length} items)`);
      const outcomes = await Promise.all(batch.map((r) => submitProductForReview(r)));
      for (const ok of outcomes) {
        if (ok) success++;
        else fail++;
      }
    });
  };

  const enqueueForSubmit = (output: ScraperProductOutput) => {
    pendingBatch.push(output);
    while (pendingBatch.length >= 10) {
      const batch = pendingBatch.splice(0, 10);
      flushBatch(batch);
    }
  };

  const queue = [...targets];
  const workerCount = Math.max(1, concurrency || 5);
  const workers = Array.from({ length: Math.min(workerCount, queue.length) }, async () => {
    while (queue.length) {
      const u = queue.shift();
      if (!u) break;
      const target = typeof u === "string" ? { url: u, stripWeight: true } : u;
      const reorder = typeof target.reorderName === "boolean" ? target.reorderName : effectiveReorder;
      let product: ScrapedProduct | null = null;
      try {
        product = await fetchProduct(browser, target.url, reorder, target.stripWeight);
      } catch (err) {
        console.error(`[SKIP] fetch failed: ${target.url}`, err);
        continue;
      }
      if (!product) continue;
      if (!product.name || !product.ingredientsText || !product.upc12) continue;
      valid.push(product);
      const mergeKey = [
        normalizeMergeText(product.brand),
        normalizeMergeText(product.name),
        normalizeMergeText(product.ingredientsText),
        nutritionSignature(product.nutrition),
        normalizeMergeText(product.servingSizeText),
      ].join("||");
      if (!submittedKeys.has(mergeKey)) {
        submittedKeys.add(mergeKey);
        enqueueForSubmit(transformToOutput(product, jobId));
      }
      if (!DEBUG_SMART_LABEL && valid.length % 5 === 0) {
        console.log(`[PROGRESS] scraped ${valid.length} products`);
      }
    }
  });
  await Promise.all(workers);
  const merged = mergeProducts(valid);
  if (merged.length !== valid.length) {
    console.log(`Merged ${valid.length} products into ${merged.length} unique records.`);
  }
  for (const product of merged) {
    results.push(transformToOutput(product, jobId));
  }

  if (!local) {
    await uploadToS3(results, jobId, runDateTime);
  }

  if (pendingBatch.length > 0) {
    const remaining = pendingBatch.splice(0, pendingBatch.length);
    flushBatch(remaining);
  }
  await submitQueue;
  console.log(`\n📊 API: ${success} submitted, ${fail} failed`);

  if (!local && SCRAPER_JOB_STATUS_TABLE_NAME) {
    if (valid.length === 0) {
      await updateJobStatus(jobId, "error", "No products processed");
      process.exit(1);
    } else {
      await updateJobStatus(jobId, "complete");
    }
  }

  await browser.close().catch(() => null);
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
