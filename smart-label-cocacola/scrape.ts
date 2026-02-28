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

type UrlEntry =
  | string
  | { url: string; stripWeight?: boolean; reorderName?: boolean; brand?: string; name?: string };

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

const SCRAPER_NAME = process.env.JOB_NAME || "smart-label-cocacola";
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
  const match = text.match(/\b(\d{12})\b/);
  return match?.[1] ?? null;
}

function isAllowedCocaColaUrl(url: string): boolean {
  return /^https?:\/\/(smartlabelpr\.)?smartlabel\.coca-colaproductfacts\.com\//i.test(url);
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

function parseCocaPrNutrition(
  $: cheerio.CheerioAPI
): { nutrition: ScraperNutritionData | null; servingSizeText: string | null } {
  const section = $(".nutrition-category").first();
  if (!section.length) return { nutrition: null, servingSizeText: null };

  const nutrition: ScraperNutritionData = {
    serving_size_value: 1,
    serving_size_unit_text: "serving",
    serving_size_text: null,
  };

  const servingSizeText = normalizeWhitespace(
    section
      .find(".serving-size, .serving-size-text, .serving-size-value, .serving-size-label")
      .first()
      .text()
  );
  if (servingSizeText) {
    nutrition.serving_size_text = servingSizeText;
    const parsed = parseServingSize(servingSizeText);
    nutrition.serving_size_value = parsed.value ?? 1;
    nutrition.serving_size_unit_text = parsed.unit ?? "serving";
  }

  const caloriesText = normalizeWhitespace(
    section.find(".nutrition-info_calories .calories-value").first().text()
  );
  if (caloriesText) {
    const parsed = parseNutrientAmountWithQualifier(caloriesText);
    if (parsed) {
      nutrition.calories = parsed.value;
      if (parsed.qualifier) nutrition.calories_qualifier = parsed.qualifier;
    }
  }

  const readAmount = (valueSel: string, unitSel: string): string => {
    const value = normalizeWhitespace(section.find(valueSel).first().text());
    if (!value) return "";
    const unit = normalizeWhitespace(section.find(unitSel).first().text());
    return unit ? `${value} ${unit}` : value;
  };

  const readPct = (pctSel: string): string => {
    return normalizeWhitespace(section.find(pctSel).first().text());
  };

  const assign = (label: string, valueSel: string, unitSel: string, pctSel?: string) => {
    const amountText = readAmount(valueSel, unitSel);
    if (amountText) {
      const col = mapNutrientToColumn(label);
      if (col) {
        const parsed = parseNutrientAmountWithQualifier(amountText);
        if (parsed) {
          if ((nutrition as any)[col] == null) {
            (nutrition as unknown as Record<string, number>)[col] = parsed.value;
            if (parsed.qualifier) {
              (nutrition as unknown as Record<string, string>)[`${col}_qualifier`] = parsed.qualifier;
            }
          }
        }
      }
    }
    if (pctSel) {
      const dvText = readPct(pctSel);
      if (dvText) {
        const dvCol = mapNutrientToDvColumn(label);
        if (dvCol) {
          const dvMatch = dvText.match(/([\d.]+)/);
          if (dvMatch) {
            if ((nutrition as any)[dvCol] == null) {
              (nutrition as unknown as Record<string, number>)[dvCol] = parseFloat(dvMatch[1]);
            }
          }
        }
        // If we have %DV but no amount, set amount to 0 to retain micronutrient presence
        if (!amountText) {
          const col = mapNutrientToColumn(label);
          if (col && (nutrition as any)[col] == null) {
            (nutrition as any)[col] = 0;
          }
        }
      }
    }
  };

  assign("Total Fat", ".fats-value", ".uom-fats-value", ".pct-fats-value");
  assign("Saturated Fat", ".sfat-value", ".uom-sfat-value", ".pct-sfat-value");
  assign("Trans Fat", ".tfat-value", ".uom-tfat-value", ".pct-tfat-value");
  assign("Cholesterol", ".chol-value", ".uom-chol-value", ".pct-chol-value");
  assign("Sodium", ".sodium-value", ".uom-sodium-value", ".pct-sodium-value");
  assign("Total Carbohydrate", ".carb-value", ".uom-carb-value", ".pct-carb-value");
  assign("Dietary Fiber", ".fibre-value", ".uom-fibre-value", ".pct-fibre-value");
  assign("Total Sugars", ".sugar-value", ".uom-sugar-value");
  assign("Added Sugars", ".add-sugar-value", ".uom-add-sugar-value", ".pct-add-sugar-value");
  assign("Protein", ".protein-value", ".uom-protein-value", ".pct-protein-value");
  assign("Vitamin D", ".vitaminD-value", ".uom-vitaminD-value", ".pct-vitaminD-value");
  assign("Vitamin C", ".vitaminC-value", ".uom-vitaminC-value", ".pct-vitaminC-value");
  assign("Vitamin B12", ".vitaminB12-value", ".uom-vitaminB12-value", ".pct-vitaminB12-value");
  assign("Vitamin B6", ".vitaminB6-value", ".uom-vitaminB6-value", ".pct-vitaminB6-value");
  assign("Calcium", ".calcium-value", ".uom-calcium-value", ".pct-calcium-value");
  assign("Iron", ".iron-value", ".uom-iron-value", ".pct-iron-value");
  assign("Potassium", ".potassium-value", ".uom-potassium-value", ".pct-potassium-value");

  // Generic row-based fallback (handles PR markup variants like Vitamin C rows)
  section.find("li.row").each((_, row) => {
    const $row = $(row);
    let label = normalizeWhitespace($row.find("span.col-xs-6").first().text());
    if (!label) {
      label = normalizeWhitespace($row.find("span.add-sugar").first().text());
    }
    if (!label) return;
    const lower = label.toLowerCase();
    if (lower.includes("amount/serving") || lower.includes("daily value") || lower === "calories") return;
    if (lower.startsWith("includes") && lower.includes("added sugars")) label = "Added Sugars";
    if (lower === "sugars") label = "Total Sugars";
    if (lower === "carbohydrates") label = "Total Carbohydrate";

    let amountText = normalizeWhitespace($row.find("span.col-xs-4").first().text());
    if (!amountText && /added sugars/i.test(label)) {
      const val = normalizeWhitespace($row.find(".add-sugar-value").first().text());
      const unit = normalizeWhitespace($row.find(".uom-add-sugar-value").first().text());
      if (val) amountText = unit ? `${val} ${unit}` : val;
    }
    const pctText =
      normalizeWhitespace($row.find("span.col-xs-2").first().text()) ||
      normalizeWhitespace($row.find("[class*='pct-']").first().text());

    if (amountText) {
      const col = mapNutrientToColumn(label);
      if (col && (nutrition as any)[col] == null) {
        const parsed = parseNutrientAmountWithQualifier(amountText);
        if (parsed) {
          (nutrition as any)[col] = parsed.value;
          if (parsed.qualifier) (nutrition as any)[`${col}_qualifier`] = parsed.qualifier;
        }
      }
    } else {
      const col = mapNutrientToColumn(label);
      if (col && pctText && (nutrition as any)[col] == null) {
        (nutrition as any)[col] = 0;
      }
    }
    if (pctText) {
      const dvCol = mapNutrientToDvColumn(label);
      if (dvCol && (nutrition as any)[dvCol] == null) {
        const dvMatch = pctText.match(/([\d.]+)/);
        if (dvMatch) (nutrition as any)[dvCol] = parseFloat(dvMatch[1]);
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
      "vitamin_c_mg",
    ].includes(k)
  );

  return { nutrition: hasNutrients ? nutrition : null, servingSizeText: nutrition.serving_size_text ?? null };
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

function extractIngredientsFromAriaTree(
  $: cheerio.CheerioAPI,
  root: cheerio.Cheerio<cheerio.Element>
): string | null {
  const nodes = root.find("[role='treeitem'][aria-level], [data-level]").toArray();
  if (nodes.length === 0) return null;

  type TreeNode = { text: string; children: TreeNode[] };
  const rootNode: TreeNode = { text: "__root__", children: [] };
  const stack: { level: number; node: TreeNode }[] = [{ level: 0, node: rootNode }];

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
    const parent = stack[stack.length - 1]?.node ?? rootNode;
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

  const rendered = rootNode.children.map(render).filter(Boolean) as string[];
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
  const cocaList = $("#ingredients-list a.header1");
  if (cocaList.length) {
    const items = cocaList
      .toArray()
      .map((el) => normalizeWhitespace($(el).text()))
      .filter(Boolean);
    if (items.length) return items.join(", ");
  }
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
    const tree = extractIngredientsFromAriaTree($, ingredientsSection);
    if (tree) return tree;
    const list = ingredientsSection.find("ul, ol").first();
    if (list.length) {
      const items = parseIngredientList($, list).filter(Boolean);
      return items.length ? items.join(", ") : null;
    }
  }

  const header = $("h1, h2, h3").filter((_, el) => $(el).text().trim().toLowerCase() === "ingredients").first();
  if (header.length) {
    const tree = extractIngredientsFromAriaTree($, header.parent());
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
      normalizeWhitespace(row.find(".col-xs-8").first().text()) ||
      normalizeWhitespace(row.text().replace(/May Contain|Contains|Shared Facility/gi, ""));
    const badge = normalizeWhitespace(row.find(".badge").first().text());
    if (!name || !badge) return;
    if (/may\s*contain/i.test(badge)) mayContain.add(name.toLowerCase());
    else if (/contains/i.test(badge)) contains.add(name.toLowerCase());
    else if (/shared\s*facility/i.test(badge)) shared.add(name.toLowerCase());
  });
  const mayContainOnly = Array.from(mayContain).filter((item) => !contains.has(item));
  if (!contains.size && !mayContainOnly.length && !shared.size) return null;
  const parts: string[] = [];
  if (contains.size) {
    parts.push(`Contains ${Array.from(contains).join(", ")}.`);
  }
  if (mayContainOnly.length) {
    parts.push(`May contain ${mayContainOnly.join(", ")}.`);
  }
  if (shared.size) {
    parts.push(`Made in a shared facility that may use ${Array.from(shared).join(", ")}.`);
  }
  return parts.join(" ").trim();
}

function parseCocaColaNutrition(
  $: cheerio.CheerioAPI,
  productSizeText: string | null
): { nutrition: ScraperNutritionData | null; servingSizeText: string | null } {
  let section = $("#nutrition .nutrition-section");
  if (!section.length) section = $(".nutrition-section").first();
  if (!section.length) return { nutrition: null, servingSizeText: null };
  const nutrition: ScraperNutritionData = {
    serving_size_value: 1,
    serving_size_unit_text: "serving",
    serving_size_text: null,
  };

  let servingSizeText = normalizeWhitespace(section.find(".servings-size-value").first().text());
  if (servingSizeText) {
    const hasUnit = /(oz|fl oz|fluid ounce|g|mg|ml|millilitre|milliliter|l|liter|litre)\b/i.test(
      servingSizeText
    );
    const productSize = productSizeText ? normalizeWhitespace(productSizeText) : "";
    if (!hasUnit && productSize) {
      servingSizeText = `${servingSizeText} (${productSize})`;
    }
    nutrition.serving_size_text = servingSizeText;
    const parsed = parseServingSize(servingSizeText);
    nutrition.serving_size_value = parsed.value ?? 1;
    nutrition.serving_size_unit_text = parsed.unit ?? "serving";
  }

  const getVal = (selector: string): string => normalizeWhitespace(section.find(selector).first().text());
  const setAmount = (label: string, valueText: string) => {
    if (!valueText) return;
    const col = mapNutrientToColumn(label);
    if (!col) return;
    const parsed = parseNutrientAmountWithQualifier(valueText);
    if (parsed) {
      (nutrition as unknown as Record<string, number>)[col] = parsed.value;
      if (parsed.qualifier) (nutrition as unknown as Record<string, string>)[`${col}_qualifier`] = parsed.qualifier;
    }
  };
  const setDv = (label: string, dvText: string) => {
    if (!dvText) return;
    const dvCol = mapNutrientToDvColumn(label);
    if (!dvCol) return;
    const dvMatch = dvText.match(/([\d.]+)/);
    if (dvMatch) (nutrition as unknown as Record<string, number>)[dvCol] = parseFloat(dvMatch[1]);
  };

  const calories = getVal(".calories-value");
  if (calories) {
    const parsed = parseNutrientAmountWithQualifier(calories);
    if (parsed) {
      nutrition.calories = parsed.value;
      if (parsed.qualifier) nutrition.calories_qualifier = parsed.qualifier;
    }
  }

  setAmount("total fat", `${getVal(".fats-value")} ${getVal(".uom-fats-value")}`.trim());
  setDv("total fat", getVal(".pct-fats-value"));
  setAmount("saturated fat", `${getVal(".sfat-value")} ${getVal(".uom-sfat-value")}`.trim());
  setDv("saturated fat", getVal(".pct-sfat-value"));
  setAmount("sodium", `${getVal(".sodium-value")} ${getVal(".uom-sodium-value")}`.trim());
  setDv("sodium", getVal(".pct-sodium-value"));
  setAmount("total carbohydrate", `${getVal(".carb-value")} ${getVal(".uom-carb-value")}`.trim());
  setDv("total carbohydrate", getVal(".pct-carb-value"));
  setAmount("dietary fiber", `${getVal(".fibre-value")} ${getVal(".uom-fibre-value")}`.trim());
  setDv("dietary fiber", getVal(".pct-fibre-value"));
  setAmount("total sugars", `${getVal(".sugar-value")} ${getVal(".uom-sugar-value")}`.trim());
  setAmount("added sugars", `${getVal(".add-sugar-value")} ${getVal(".uom-add-sugar-value")}`.trim());
  setDv("added sugars", getVal(".pct-add-sugar-value"));
  setAmount("protein", `${getVal(".protein-value")} ${getVal(".uom-protein-value")}`.trim());
  setDv("protein", getVal(".pct-protein-value"));
  setAmount("potassium", `${getVal(".K-value")} ${getVal(".uom-K-value")}`.trim());
  setDv("potassium", getVal(".pct-K-value"));

  if (DEBUG_SMART_LABEL) {
    console.log(`[DEBUG] coca nutrition serving size: ${nutrition.serving_size_text ?? "(null)"}`);
    console.log(`[DEBUG] coca nutrition calories: ${nutrition.calories ?? "(null)"}`);
    console.log(`[DEBUG] coca nutrition total fat: ${nutrition.total_fat_g ?? "(null)"}`);
    console.log(`[DEBUG] coca nutrition sodium: ${nutrition.sodium_mg ?? "(null)"}`);
    console.log(`[DEBUG] coca nutrition carbs: ${nutrition.total_carbs_g ?? "(null)"}`);
    console.log(`[DEBUG] coca nutrition sugars: ${nutrition.sugars_g ?? "(null)"}`);
    console.log(`[DEBUG] coca nutrition added sugars: ${nutrition.added_sugars_g ?? "(null)"}`);
    console.log(`[DEBUG] coca nutrition protein: ${nutrition.protein_g ?? "(null)"}`);
  }

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
    ].includes(k)
  );
  return { nutrition: hasNutrients ? nutrition : null, servingSizeText: nutrition.serving_size_text ?? null };
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
    const label = normalizeWhitespace($(row).find("span, strong").first().text()).toLowerCase();
    const valueCandidates = $(row)
      .find(".nfp__values-right")
      .toArray()
      .map((el) => decodeHtmlEntities(normalizeWhitespace($(el).text())) || "")
      .filter(Boolean);
    const dvText = decodeHtmlEntities(
      normalizeWhitespace($(row).find(".nfp__values-dvp").first().text())
    );
    let valueText = valueCandidates.find((v) => /<|>|≈|~|\d/.test(v)) || "";
    if (!valueText) {
      valueText = decodeHtmlEntities(normalizeWhitespace($(row).find("span").last().text())) || "";
    }
    if (!label || !valueText) return;
    if (DEBUG_SMART_LABEL) {
      console.log(`[DEBUG] nutrition row: label="${label}" value="${valueText}"`);
    }
    const col = mapNutrientToColumn(label);
    if (col) {
      const parsed = parseNutrientAmountWithQualifier(valueText);
      if (parsed) {
        (nutrition as unknown as Record<string, number>)[col] = parsed.value;
        if (parsed.qualifier) (nutrition as unknown as Record<string, string>)[`${col}_qualifier`] = parsed.qualifier;
      }
    }
    if (dvText) {
      const dvCol = mapNutrientToDvColumn(label);
      if (dvCol) {
        const dvMatch = dvText.match(/([\d.]+)/);
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

function normalizeCocaColaBrand(brand: string | null): string | null {
  if (!brand) return brand;
  const cleaned = brand.replace(/\bzero[\s-]*sugar\b/gi, "").replace(/\s+/g, " ").trim();
  return cleaned;
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
  const og = $("meta[property='og:image']").attr("content");
  if (og) {
    if (og.startsWith("http")) return og;
    if (og.startsWith("./")) return `${baseOrigin}/${og.slice(2)}`;
    if (og.startsWith("/")) return `${baseOrigin}${og}`;
    return `${baseOrigin}/${og}`;
  }
  const img = $("img").first().attr("src");
  if (!img) return null;
  if (img === "#") return null;
  if (img.startsWith("http")) return img;
  if (img.startsWith("./")) return `${baseOrigin}/${img.slice(2)}`;
  if (img.startsWith("/")) return `${baseOrigin}${img}`;
  return `${baseOrigin}/${img}`;
}

function normalizeImageUrl(value: string | null, baseOrigin: string): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (trimmed.startsWith("http")) return trimmed;
  if (trimmed.startsWith("./")) return `${baseOrigin}/${trimmed.slice(2)}`;
  if (trimmed.startsWith("/")) return `${baseOrigin}${trimmed}`;
  return `${baseOrigin}/${trimmed}`;
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
): Promise<UrlEntry[]> {
  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  });
  try {
    const startUrl = new URL(searchUrl);
    const isSmartlabelSearch =
      /smartlabel\.org$/i.test(startUrl.host) && /\/product-search\//i.test(startUrl.pathname);
    if (isSmartlabelSearch) {
      const collected: UrlEntry[] = [];
      const seen = new Set<string>();
      let pageNum = parseInt(startUrl.searchParams.get("pn") || "1", 10);
      if (!Number.isFinite(pageNum) || pageNum < 1) pageNum = 1;
      while (true) {
        startUrl.searchParams.set("pn", String(pageNum));
        await page.goto(startUrl.toString(), { waitUntil: "domcontentloaded", timeout: 60_000 });
        await page.waitForTimeout(800);
        const rows = await page.$$eval("table tr", (trs) =>
          trs
            .map((tr) => {
              const tds = tr.querySelectorAll("td");
              if (tds.length < 2) return null;
              const brand = (tds[0].textContent || "").trim();
              const link = tds[1].querySelector("a");
              const url = (link as HTMLAnchorElement | null)?.href?.trim() || "";
              return { url, brand };
            })
            .filter(Boolean)
        );
        const beforeCount = collected.length;
      for (const row of rows as Array<{ url: string; brand: string }>) {
        if (!row.url) continue;
        if (!isAllowedCocaColaUrl(row.url)) continue;
        if (seen.has(row.url)) continue;
        seen.add(row.url);
          collected.push({
            url: row.url.split("#")[0],
            brand: row.brand || undefined,
          });
          if (maxUrls && collected.length >= maxUrls) return collected;
        }
        if (DEBUG_SMART_LABEL) {
          const html = await page.content();
          await fs.writeFile(`/tmp/smart-label-catalog-${pageNum}.html`, html).catch(() => null);
        }
        if (rows.length === 0 || collected.length === beforeCount) break;
        pageNum += 1;
      }
      return collected;
    }

    await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: 60_000 });
    await page.waitForTimeout(1500);
    const collected = new Map<string, { url: string; brand?: string; name?: string }>();
    let pageIndex = 0;
    while (true) {
      pageIndex += 1;
      const tableRows = await page.$$eval("table tr", (trs) =>
        trs
          .map((tr) => {
            const tds = tr.querySelectorAll("td");
            if (tds.length < 2) return null;
            const brand = (tds[0].textContent || "").trim();
            const link = tds[1].querySelector("a");
            const url = (link as HTMLAnchorElement | null)?.href?.trim() || "";
            return { url, brand };
          })
          .filter(Boolean)
      );
      for (const row of tableRows as Array<{ url: string; brand: string }>) {
        if (!row.url) continue;
        if (!isAllowedCocaColaUrl(row.url)) continue;
        const normalizedUrl = row.url.split("#")[0];
        if (!collected.has(normalizedUrl)) {
          collected.set(normalizedUrl, {
            url: normalizedUrl,
            brand: row.brand || undefined,
          });
          if (maxUrls && collected.size >= maxUrls) {
            return Array.from(collected.values());
          }
        }
      }
      const hrefs = await page.$$eval("a[href]", (links) =>
        links
          .map((a) => (a as HTMLAnchorElement).href)
          .filter((href) => /smartlabel\.coca-colaproductfacts\.com/i.test(href))
      );
      for (const href of hrefs) {
        if (!href) continue;
        if (!isAllowedCocaColaUrl(href)) continue;
        const normalizedUrl = href.split("#")[0];
        if (!collected.has(normalizedUrl)) {
          collected.set(normalizedUrl, { url: normalizedUrl });
        }
        if (maxUrls && collected.size >= maxUrls) {
          return Array.from(collected.values());
        }
      }
      if (DEBUG_SMART_LABEL) {
        const html = await page.content();
        await fs.writeFile(`/tmp/smart-label-catalog-${pageIndex}.html`, html).catch(() => null);
      }
      const firstHref = hrefs.find(Boolean) || "";
      const nextButton = page.locator("#pagination-next-page");
      const isDisabled = await nextButton.getAttribute("aria-disabled");
      if (isDisabled === "true") break;
      await nextButton.click().catch(() => null);
      await page.waitForTimeout(400);
      await page
        .waitForFunction(
          (prev) => {
            const first = document.querySelector(
              "a[href*='smartlabel.coca-colaproductfacts.com']"
            ) as HTMLAnchorElement | null;
            return !!first && first.href !== prev;
          },
          firstHref,
          { timeout: 3000 }
        )
        .catch(() => null);
      if (maxUrls && collected.size >= maxUrls) break;
    }
    return Array.from(collected).map((u) => ({ url: u }));
  } finally {
    await page.close().catch(() => null);
  }
}

async function fetchProduct(
  browser: Browser,
  url: string,
  reorderName: boolean,
  stripWeight: boolean,
  overrides?: { brand?: string; name?: string }
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

  const urlObj = new URL(url);
  const html = await fetchWithFallback(url);
  if (DEBUG_SMART_LABEL) {
    await fs.writeFile("/tmp/smart-label.html", html).catch(() => null);
  }

  const $ = cheerio.load(html);
  const headerName = extractHeaderName($);
  const headerBrand = extractHeaderBrand($);
  const rawNameCandidate = headerName || extractName($);
  const notFound = /could not find the product/i.test(rawNameCandidate || "");
  const rawName = notFound ? null : rawNameCandidate;
  const useHeaderBrand = isLikelyBrand(headerBrand) && !!headerName;
  const derived = useHeaderBrand ? { brand: headerBrand, name: rawName } : deriveBrandAndName(rawName);
  let brand =
    ((useHeaderBrand ? headerBrand : derived.brand) ||
      extractBrand($) ||
      "Unknown"
    ).trim();
  const reordered = reorderName ? deriveBrandAndName(rawName) : { name: rawName };
  const baseName = headerName || (reordered.name ?? derived.name) || rawName;
  let name = baseName ? baseName.trim() : baseName;
  const preBrandName = name;

  const nameFromOverride = !!overrides?.name;
  const brandFromOverride = !!overrides?.brand;
  if (overrides?.brand) brand = overrides.brand;
  if (overrides?.name) name = overrides.name;
  brand = normalizeCocaColaBrand(brand);

  if (name && brand && !nameFromOverride) {
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

  if ((!name || !brand || brand === "Unknown") && !nameFromOverride && !brandFromOverride) {
    let queryName: string | null = null;
    for (const [key] of urlObj.searchParams) {
      if (key && key !== "upc") {
        queryName = key;
        break;
      }
    }
    if (queryName) {
      const decoded = decodeHtmlEntities(queryName) || queryName;
      let cleaned = decoded.replace(/[-_]+/g, " ").replace(/([a-z])([0-9])/gi, "$1 $2").trim();
      if (/coca\s*cola/i.test(cleaned) || /cocacola/i.test(cleaned)) {
        brand = "Coca-Cola";
        cleaned = cleaned.replace(/coca\s*cola/i, "").trim();
      }
      const fluidMatch = cleaned.match(/^(\d{2,3})\s*fluid\s*ounce/i);
      if (fluidMatch) {
        const digits = fluidMatch[1];
        if (digits.length === 3) {
          cleaned = cleaned.replace(digits, `${digits.slice(0, -1)}.${digits.slice(-1)}`);
        }
      }
      if (!name) name = cleaned;
    }
  }
  let ingredientsSource: string | null = null;
  let ingredientsText = extractIngredients($);
  if (ingredientsText) ingredientsSource = "smartlabel-dom";
  let allergenStatement = extractAllergenStatement($);
  const baseOrigin = new URL(url).origin;
  let imageUrl: string | null = extractImage($, baseOrigin);

  const upcFromQuery = urlObj.searchParams.get("upc");
  let upcFromText = upcFromQuery || extractUpc(html) || extractUpc($("body").text());

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
  const isCocaPr = /smartlabelpr\.coca-colaproductfacts\.com/i.test(urlObj.host);
  if (!nutrition || isCocaPr) {
    const prParsed = parseCocaPrNutrition($);
    if (prParsed.nutrition) {
      if (!nutrition) {
        nutrition = prParsed.nutrition;
      } else {
        for (const [key, value] of Object.entries(prParsed.nutrition)) {
          const existing = (nutrition as any)[key];
          if (existing === undefined || existing === null || existing === "") {
            (nutrition as any)[key] = value as any;
          }
        }
      }
    }
    servingSizeText = prParsed.servingSizeText ?? servingSizeText;
    if (DEBUG_SMART_LABEL && prParsed.nutrition) {
      const n = prParsed.nutrition as any;
      console.log(
        `[DEBUG] pr vitamin c: ${n.vitamin_c_mg ?? "(null)"} mg, dv ${n.vitamin_c_dv_pct ?? "(null)"}`
      );
    }
  }
  if (!nutrition) {
    const fallback = extractNutritionFromText($("body").text());
    nutrition = fallback.nutrition;
    servingSizeText = fallback.servingSizeText;
  }

  const isCocaCola = /coca-colaproductfacts\.com/i.test(urlObj.host);
  let nutritionUrl = url.includes("#") ? url : `${url}#nutrition`;
  let ingredientsUrl = url.includes("#") ? url : `${url}#ingredients`;
  if (nutritionUrl.includes("/nutrition/")) {
    ingredientsUrl = ingredientsUrl.replace("/nutrition/", "/ingredients/");
  }

  const renderedNutrition = (isCocaCola || isCocaPr || !nutrition || DEBUG_SMART_LABEL)
    ? await fetchRenderedHtml(browser, nutritionUrl)
    : null;
  if (renderedNutrition && DEBUG_SMART_LABEL) {
    await fs.writeFile("/tmp/smart-label-rendered-nutrition.html", renderedNutrition).catch(() => null);
  }
  if (renderedNutrition && DEBUG_SMART_LABEL && isCocaPr) {
    await fs
      .writeFile("/tmp/smart-labelpr-rendered-nutrition.html", renderedNutrition)
      .catch(() => null);
  }
  if (renderedNutrition) {
    const $r = cheerio.load(renderedNutrition);
    const cocaName = normalizeWhitespace($r("#product_name").text());
    let cocaBrand = "";
    const cocaBrandHtml = $r("#product_desc").html();
    if (cocaBrandHtml) {
      const cleanedHtml = cocaBrandHtml.replace(/<br\s*\/?>/gi, " ");
      cocaBrand = normalizeWhitespace(cheerio.load(`<div>${cleanedHtml}</div>`).text());
    } else {
      cocaBrand = normalizeWhitespace($r("#product_desc").text());
    }
    const cocaUpc = normalizeWhitespace($r("#product_upc").text());
    const cocaSize = normalizeWhitespace($r("#product_size").text());
    const cocaImage = $r(".product_image_src").attr("src") || "";
    if (!brandFromOverride && cocaBrand) brand = cocaBrand;
    if (!nameFromOverride && cocaName) name = cocaName;
    brand = normalizeCocaColaBrand(brand);
    if (cocaUpc) {
      const extracted = extractUpc(cocaUpc);
      if (extracted) upcFromText = extracted;
    }
    if (cocaImage) imageUrl = normalizeImageUrl(cocaImage, baseOrigin);
    if (!name) {
      const header = extractHeaderName($r) || extractName($r);
      if (header) name = stripWeightFromName(header);
    }
    if (!imageUrl) imageUrl = extractImage($r, baseOrigin);

    const cocaParsed = parseCocaColaNutrition($r, cocaSize || null);
    if (cocaParsed.nutrition) {
      nutrition = cocaParsed.nutrition;
      servingSizeText = cocaParsed.servingSizeText ?? servingSizeText;
    }
    if (!ingredientsText) {
      const extractedIngredients = extractIngredients($r);
      if (extractedIngredients) {
        ingredientsText = extractedIngredients;
        ingredientsSource = "smartlabel-rendered";
      }
    }
    if (!nutrition) {
      const parsedNutrition = parseSmartLabelNutrition($r);
      nutrition = parsedNutrition.nutrition;
      servingSizeText = parsedNutrition.servingSizeText ?? servingSizeText;
    }
    if (isCocaPr) {
      const prRendered = parseCocaPrNutrition($r);
      if (prRendered.nutrition) {
        if (!nutrition) {
          nutrition = prRendered.nutrition;
        } else {
          for (const [key, value] of Object.entries(prRendered.nutrition)) {
            const existing = (nutrition as any)[key];
            if (existing === undefined || existing === null || existing === "") {
              (nutrition as any)[key] = value as any;
            }
          }
        }
      }
      servingSizeText = prRendered.servingSizeText ?? servingSizeText;
      if (DEBUG_SMART_LABEL) {
        const n = prRendered.nutrition as any;
        console.log(
          `[DEBUG] pr(rendered) vitamin c: ${n?.vitamin_c_mg ?? "(null)"} mg, dv ${n?.vitamin_c_dv_pct ?? "(null)"}`
        );
      }
    }
    if (!allergenStatement) {
      allergenStatement = extractAllergenStatement($r);
    }
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

  if (!isCocaCola) {
    const renderedIngredients = (!ingredientsText || DEBUG_SMART_LABEL)
      ? await fetchRenderedHtml(browser, ingredientsUrl)
      : null;
    if (renderedIngredients && DEBUG_SMART_LABEL) {
      await fs.writeFile("/tmp/smart-label-rendered-ingredients.html", renderedIngredients).catch(() => null);
    }
    if (renderedIngredients && !ingredientsText) {
      const $r = cheerio.load(renderedIngredients);
      ingredientsText = extractIngredients($r);
      if (ingredientsText) ingredientsSource = "smartlabel-rendered";
      if (!imageUrl) imageUrl = extractImage($r, baseOrigin);
    }
  }

  if (!allergenStatement) {
    const allergensUrl = url.includes("#") ? url : `${url}#allergens`;
    const renderedAllergens = await fetchRenderedHtml(browser, allergensUrl);
    if (DEBUG_SMART_LABEL) {
      await fs.writeFile("/tmp/smart-label-rendered-allergens.html", renderedAllergens).catch(() => null);
    }
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

  imageUrl = normalizeImageUrl(imageUrl, baseOrigin);
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

function isHttpsUrl(value: string | null | undefined): boolean {
  if (!value) return false;
  return /^https?:\/\//i.test(value);
}

function mergeImageUrl(existing: string | null, candidate: string | null): string | null {
  if (isHttpsUrl(existing)) return existing;
  if (isHttpsUrl(candidate)) return candidate;
  return existing || candidate;
}

function nutritionSignature(nutrition: ScraperNutritionData | null | undefined): string {
  if (!nutrition) return "";
  const entries = Object.entries(nutrition)
    .filter(([, v]) => v !== undefined && v !== null && v !== "")
    .sort(([a], [b]) => a.localeCompare(b));
  return JSON.stringify(entries);
}

function buildMergeKey(product: ScrapedProduct): string {
  return [
    normalizeMergeText(product.brand),
    normalizeMergeText(product.name),
    normalizeMergeText(product.ingredientsText),
    nutritionSignature(product.nutrition),
    normalizeMergeText(product.servingSizeText),
  ].join("||");
}

function mergeProducts(products: ScrapedProduct[]): ScrapedProduct[] {
  const merged = new Map<string, ScrapedProduct>();
  for (const product of products) {
    const key = buildMergeKey(product);
    const existing = merged.get(key);
    if (!existing) {
      merged.set(key, { ...product, upcs: product.upc12 ? [product.upc12] : [] });
      continue;
    }
    existing.imageUrl = mergeImageUrl(existing.imageUrl, product.imageUrl);
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
  let local = false;
  let debug = false;
  let noHeadless = false;
  let headless = false;
  let reorderName = false;
  let concurrency = 5;

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
    } else if (argv[i] === "--config" || argv[i] === "-c") {
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
    } else if (argv[i] === "--no-headless") {
      noHeadless = true;
    } else if (argv[i] === "--headless") {
      headless = true;
    } else if (argv[i] === "--reorder-name") {
      reorderName = true;
    } else if ((argv[i] === "--concurrency" || argv[i] === "-n") && argv[i + 1]) {
      const n = parseInt(argv[i + 1], 10);
      if (!isNaN(n) && n > 0) concurrency = n;
      i++;
    }
  }

  return { url, searchUrl, configPath, limit, offset, local, reorderName, debug, noHeadless, headless, concurrency };
}

async function main(): Promise<void> {
  const { url, searchUrl, configPath, limit, offset, local, reorderName, debug, noHeadless, headless, concurrency } = parseArgs();
  let effectiveSearchUrl = searchUrl;

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
        const entry =
          typeof searchUrls[0] === "string"
            ? { url: searchUrls[0], stripWeight: true }
            : searchUrls[0];
        effectiveSearchUrl = entry?.url;
      }
      if (searchUrls.length) {
        for (const s of searchUrls) {
          if (Number.isFinite(desiredCount) && urls.length >= desiredCount) break;
          const entry =
            typeof s === "string"
              ? { url: s, stripWeight: true }
              : s;
          if (!entry?.url) continue;
          const remaining = Number.isFinite(desiredCount) ? Math.max(desiredCount - urls.length, 0) : undefined;
          console.log(`[DISCOVER] SmartLabel catalog: ${entry.url}`);
          try {
            const discovered = await discoverProductUrls(browser, entry.url, remaining);
            console.log(`[DISCOVER] Found ${discovered.length} product URLs`);
            for (const d of discovered) {
              const item = typeof d === "string" ? { url: d } : d;
              urls.push({
                url: item.url,
                stripWeight: entry.stripWeight !== false,
                reorderName: typeof entry.reorderName === "boolean" ? entry.reorderName : undefined,
                brand: (item as any).brand,
              });
            }
          } catch (err) {
            console.error(`[DISCOVER] Failed ${entry.url}:`, err);
          }
        }
      }
    } catch (err) {
      if (DEBUG_SMART_LABEL) {
        console.error(`[DEBUG] failed to read config: ${configPath}`, err);
      }
    }
  }

  if (effectiveSearchUrl) {
    try {
      const remaining = Number.isFinite(desiredCount) ? Math.max(desiredCount - urls.length, 0) : undefined;
      console.log(`[DISCOVER] SmartLabel catalog: ${effectiveSearchUrl}`);
      const discovered = await discoverProductUrls(browser, effectiveSearchUrl, remaining);
      console.log(`[DISCOVER] Found ${discovered.length} product URLs`);
      for (const d of discovered) {
        const item = typeof d === "string" ? { url: d } : d;
          urls.push({
            url: item.url,
            stripWeight: true,
            brand: (item as any).brand,
          });
        }
    } catch (err) {
      console.error(`[DISCOVER] Failed ${effectiveSearchUrl}:`, err);
    }
  }

  const deduped = new Map<
    string,
    { url: string; stripWeight: boolean; reorderName?: boolean; brand?: string }
  >();
  for (const u of urls) {
    if (typeof u === "string") {
      if (!deduped.has(u)) deduped.set(u, { url: u, stripWeight: true, reorderName: undefined });
    } else if (u && typeof u === "object" && typeof u.url === "string") {
      if (!deduped.has(u.url)) {
        deduped.set(u.url, {
          url: u.url,
          stripWeight: u.stripWeight !== false,
          reorderName: typeof u.reorderName === "boolean" ? u.reorderName : undefined,
          brand: u.brand,
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
  let success = 0;
  let fail = 0;

  if (effectiveReorder) {
    console.log("Reorder name: enabled");
  }
  const valid: ScrapedProduct[] = [];
  const workerCount = Math.max(1, concurrency || 5);
  const queue = [...targets];
  const workers = Array.from({ length: Math.min(workerCount, queue.length) }, async () => {
    while (queue.length) {
      const u = queue.shift();
      if (!u) break;
      const target = typeof u === "string" ? { url: u, stripWeight: true } : u;
      const reorder = typeof target.reorderName === "boolean" ? target.reorderName : effectiveReorder;
      const product = await fetchProduct(browser, target.url, reorder, target.stripWeight, {
        brand: (target as any).brand,
      });
      if (!product) continue;
      if (!product.name || !product.ingredientsText || !product.upc12) continue;
      valid.push(product);
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

  console.log(`\n📤 Submitting ${results.length} merged products for review...`);
  for (let i = 0; i < results.length; i += 10) {
    const batch = results.slice(i, i + 10);
    console.log(`\n➡️  Submitting batch (${batch.length} items)`);
    const outcomes = await Promise.all(batch.map((r) => submitProductForReview(r)));
    for (const ok of outcomes) {
      if (ok) success++;
      else fail++;
    }
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

  await browser.close().catch(() => null);
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
