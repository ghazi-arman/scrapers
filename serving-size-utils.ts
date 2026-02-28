export type ServingSize = { value: number | null; unit: string | null };

type UnitRule = { unit: string; regex: RegExp };

const UNIT_RULES: UnitRule[] = [
  { unit: "g", regex: /\b(\d+(?:\.\d+)?|\d+\s*\/\s*\d+)\s*(?:g|grams?|gram)\b/i },
  { unit: "fl oz", regex: /\b(\d+(?:\.\d+)?|\d+\s*\/\s*\d+)\s*(?:fl\s*oz|fluid\s*ounces?)\b/i },
  { unit: "oz", regex: /\b(\d+(?:\.\d+)?|\d+\s*\/\s*\d+)\s*(?:oz|ounces?)\b/i },
  { unit: "cup", regex: /\b(\d+(?:\.\d+)?|\d+\s*\/\s*\d+)\s*(?:cup|cups)\b/i },
  { unit: "ml", regex: /\b(\d+(?:\.\d+)?|\d+\s*\/\s*\d+)\s*(?:ml|milliliters?|millilitres?)\b/i },
];

function parseNumber(value: string): number | null {
  const cleaned = value.trim();
  const frac = cleaned.match(/^(\d+)\s*\/\s*(\d+)$/);
  if (frac) {
    const num = parseFloat(frac[1]);
    const den = parseFloat(frac[2]);
    if (!isNaN(num) && !isNaN(den) && den !== 0) return num / den;
  }
  const num = parseFloat(cleaned);
  return Number.isFinite(num) ? num : null;
}

export function parseServingSizeFromText(servingSizeText: string | null): ServingSize {
  if (!servingSizeText || typeof servingSizeText !== "string") {
    return { value: null, unit: null };
  }

  const cleaned = servingSizeText.replace(/\u00a0/g, " ").trim();

  for (const rule of UNIT_RULES) {
    const match = cleaned.match(rule.regex);
    if (match && match[1]) {
      const value = parseNumber(match[1]);
      if (value != null) return { value, unit: rule.unit };
    }
  }

  const fallback = cleaned.match(/(\d+(?:\.\d+)?|\d+\s*\/\s*\d+)\s*([a-zA-Z][a-zA-Z\s\-]*)/);
  if (fallback) {
    const value = parseNumber(fallback[1]);
    const unit = fallback[2]?.trim();
    if (value != null && unit) return { value, unit: unit.toLowerCase() };
    if (value != null) return { value, unit: "serving" };
  }

  return { value: null, unit: null };
}
