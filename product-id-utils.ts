import * as crypto from "crypto";

export function generateDeterministicProductId(
  name: string,
  brand?: string | null,
  upc?: string | null
): string {
  const input = `${name.toLowerCase().trim()}|${(brand || "").toLowerCase().trim()}|${(upc || "").trim()}`;
  const hash = crypto.createHash("md5").update(input).digest("hex");
  return `${hash.substring(0, 8)}-${hash.substring(8, 12)}-${hash.substring(12, 16)}-${hash.substring(16, 20)}-${hash.substring(20, 32)}`;
}
