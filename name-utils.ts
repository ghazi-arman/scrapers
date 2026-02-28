export type CleanProductNameOptions = {
  brand?: string | null;
  stripBrandPrefix?: boolean;
  stripBrandSuffixes?: Array<string | RegExp>;
  stripPipe?: boolean;
  keepBeforeComma?: boolean;
  stripLeadingPack?: boolean;
  stripTrailingDashSize?: boolean;
  stripTrailingCommaSize?: boolean;
  stripTrailingCount?: boolean;
  stripTrailingWeight?: boolean;
  stripAfterWeight?: boolean;
  stripParenAtEnd?: boolean;
  decodeHtml?: boolean;
  preserveOriginal?: boolean;
};

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function decodeHtmlEntities(value: string): string {
  return value
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&#(\d+);/g, (_, num) => {
      const code = parseInt(num, 10);
      return Number.isFinite(code) ? String.fromCharCode(code) : _;
    });
}

function hasSizeUnit(text: string): boolean {
  return /(\d+(?:\.\d+)?\s*(oz|ounce|ounces|fl\s*oz|floz|lb|lbs|g|kg|mg|ml|l|ct|count|pack|pk|packs?))\b/i.test(
    text
  );
}

function hasHyphenatedDescriptor(text: string): boolean {
  return /\b\d+\s*[-–—]\s*[a-z]/i.test(text);
}

function isBareSizeSegment(text: string): boolean {
  const trimmed = text.trim();
  const unitPattern = /(oz|ounce|ounces|fl\s*oz|floz|lb|lbs|g|kg|mg|ml|l|ct|count|pack|pk|packs?)/i;
  if (!unitPattern.test(trimmed)) return false;
  // Remove common size patterns and see if anything meaningful remains
  const withoutSize = trimmed
    .replace(/\d+(?:\.\d+)?\s*(oz|ounce|ounces|fl\s*oz|floz|lb|lbs|g|kg|mg|ml|l)\b/gi, "")
    .replace(/\d+\s*(ct|count|pack|pk|packs?)\b/gi, "")
    .replace(/\d+\s*-\s*pack\b/gi, "")
    .replace(/[\/|(),.\-–—\s]/g, "");
  return withoutSize.length === 0;
}

export function cleanProductName(name: string | null, options: CleanProductNameOptions = {}): string | null {
  if (!name) return null;
  const original = name;
  const preserveOriginal = options.preserveOriginal !== false;
  let cleaned = options.decodeHtml ? decodeHtmlEntities(name) : name;
  cleaned = normalizeWhitespace(cleaned);
  // Remove inverted punctuation artifacts that occasionally appear in product names.
  cleaned = cleaned.replace(/[¿¡]+/g, "");
  cleaned = cleaned.replace(/^[\s™®]+/, "");

  // Keep brand text in product names. `stripBrandPrefix` is intentionally ignored.

  if (options.stripBrandSuffixes && options.stripBrandSuffixes.length > 0) {
    for (const suffix of options.stripBrandSuffixes) {
      if (typeof suffix === "string") {
        const esc = suffix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        cleaned = cleaned.replace(new RegExp(`\\s*[-–—]?\\s*${esc}(?:™|®|\\b)\\s*$`, "i"), "");
      } else {
        cleaned = cleaned.replace(suffix, "");
      }
    }
  }

  if (options.stripPipe) {
    cleaned = cleaned.replace(/\s*\|\s*.*$/, "");
  }

  if (options.stripLeadingPack) {
    cleaned = cleaned
      .replace(/^\(\s*\d+\s*-\s*pack\s*\)\s*/i, "")
      .replace(/^\(\s*\d+\s*(pack|pk|count|ct)\s*\)\s*/i, "")
      .replace(/^\d+\s*-\s*pack\s*/i, "")
      .replace(/^\d+\s*(pack|pk|count|ct)\s*/i, "");
  }

  if (options.keepBeforeComma && cleaned.includes(",")) {
    cleaned = cleaned.split(",")[0].trim();
  }

  if (options.stripTrailingDashSize) {
    // Normalize dangling trailing dashes first so "- 9oz -" can be treated as a size suffix.
    cleaned = cleaned.replace(/\s*[-–—]+\s*$/g, "");
    cleaned = cleaned.replace(/\s*[-–—]\s*([^–—-]*)$/i, (match, seg) => {
      if (hasHyphenatedDescriptor(match)) return match;
      return isBareSizeSegment(seg) ? "" : match;
    });
  }

  if (options.stripTrailingCommaSize) {
    cleaned = cleaned.replace(/\s*,\s*([^,]*)$/i, (match, seg) => {
      if (hasHyphenatedDescriptor(seg)) return match;
      return isBareSizeSegment(seg) ? "" : match;
    });
  }

  if (options.stripTrailingCount) {
    cleaned = cleaned.replace(/\s*,?\s*\d+(?:\.\d+)?\s*(ct|count|pack|pk|packs?)\b\.?/gi, "");
    cleaned = cleaned.replace(/\s*,?\s*\d+\s*-\s*pack\b\.?/gi, "");
    cleaned = cleaned.replace(
      /\s*(?:[,/]|[-–—])?\s*case\s+of\s+\d+(?:\s*(?:ct|count|pack|pk|packs?))?\s*[/.]*\s*$/gi,
      ""
    );
  }

  if (options.stripTrailingWeight) {
    const weightPattern = /\s*,?\s*\d+(?:\.\d+)?\s*(oz|ounce|ounces|fl\s*oz|floz|lb|lbs|g|kg|mg|ml|l)\b\.?/i;
    if (options.stripAfterWeight) {
      cleaned = cleaned.replace(new RegExp(`${weightPattern.source}.*$`, "i"), "");
    } else {
      cleaned = cleaned.replace(weightPattern, "");
    }
  }

  if (options.stripParenAtEnd) {
    cleaned = cleaned.replace(/\s*\([^)]*\)\s*$/g, "");
  }

  cleaned = normalizeWhitespace(cleaned).replace(/^[,\s\-–—]+|[,\s\-–—]+$/g, "").trim();
  if (!cleaned) return preserveOriginal ? normalizeWhitespace(original) : null;
  return cleaned;
}
