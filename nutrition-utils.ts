/**
 * Parse nutrient amount string (e.g., "0g", "<1g", ">5mg", "~10g") into value and qualifier.
 * Qualifier: 'less_than' | 'greater_than' | 'approximately' | null (exact)
 */
export type NutrientQualifier = 'less_than' | 'greater_than' | 'approximately';

export interface ParsedNutrientAmount {
  value: number;
  qualifier: NutrientQualifier | null;
}

export function parseNutrientAmountWithQualifier(amount: string | null): ParsedNutrientAmount | null {
  if (!amount || typeof amount !== 'string') return null;

  const cleaned = amount.trim();
  let qualifier: NutrientQualifier | null = null;
  let rest = cleaned;

  if (/^</i.test(rest)) {
    qualifier = 'less_than';
    rest = rest.replace(/^<\s*/, '');
  } else if (/^>/i.test(rest)) {
    qualifier = 'greater_than';
    rest = rest.replace(/^>\s*/, '');
  } else if (/^[≈~∼∼]/.test(rest)) {
    qualifier = 'approximately';
    rest = rest.replace(/^[≈~∼∼]\s*/, '');
  }

  const match = rest.match(/^(\d+(?:\.\d+)?)/);
  if (match) {
    const value = parseFloat(match[1]);
    return isNaN(value) ? null : { value, qualifier };
  }
  return null;
}
