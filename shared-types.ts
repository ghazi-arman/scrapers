/**
 * Shared types for all scrapers
 * 
 * This file defines the standard format for scraped products that is used for:
 * - S3 uploads (with scraper_job_id)
 * - API submissions (without scraper_job_id)
 */

/** Qualifier for nutrient values: less_than (<), greater_than (>), approximately (~) */
export type NutrientQualifier = 'less_than' | 'greater_than' | 'approximately';

/**
 * Nutrition data matching the product_nutrition_facts table schema
 */
export interface ScraperNutritionData {
  serving_size_value: number;
  serving_size_unit_id?: string | null;
  serving_size_unit_text?: string | null;
  serving_size_text?: string | null;
  calories?: number | null;
  calories_qualifier?: NutrientQualifier | null;
  protein_g?: number | null;
  protein_g_qualifier?: NutrientQualifier | null;
  total_carbs_g?: number | null;
  total_carbs_g_qualifier?: NutrientQualifier | null;
  fiber_g?: number | null;
  fiber_g_qualifier?: NutrientQualifier | null;
  sugars_g?: number | null;
  sugars_g_qualifier?: NutrientQualifier | null;
  added_sugars_g?: number | null;
  added_sugars_g_qualifier?: NutrientQualifier | null;
  total_fat_g?: number | null;
  total_fat_g_qualifier?: NutrientQualifier | null;
  saturated_fat_g?: number | null;
  saturated_fat_g_qualifier?: NutrientQualifier | null;
  trans_fat_g?: number | null;
  trans_fat_g_qualifier?: NutrientQualifier | null;
  monounsaturated_fat_g?: number | null;
  monounsaturated_fat_g_qualifier?: NutrientQualifier | null;
  polyunsaturated_fat_g?: number | null;
  polyunsaturated_fat_g_qualifier?: NutrientQualifier | null;
  cholesterol_mg?: number | null;
  cholesterol_mg_qualifier?: NutrientQualifier | null;
  sodium_mg?: number | null;
  sodium_mg_qualifier?: NutrientQualifier | null;
  potassium_mg?: number | null;
  potassium_mg_qualifier?: NutrientQualifier | null;
  calcium_mg?: number | null;
  calcium_mg_qualifier?: NutrientQualifier | null;
  iron_mg?: number | null;
  iron_mg_qualifier?: NutrientQualifier | null;
  magnesium_mg?: number | null;
  magnesium_mg_qualifier?: NutrientQualifier | null;
  phosphorus_mg?: number | null;
  phosphorus_mg_qualifier?: NutrientQualifier | null;
  zinc_mg?: number | null;
  zinc_mg_qualifier?: NutrientQualifier | null;
  vitamin_a_mcg?: number | null;
  vitamin_a_mcg_qualifier?: NutrientQualifier | null;
  vitamin_c_mg?: number | null;
  vitamin_c_mg_qualifier?: NutrientQualifier | null;
  vitamin_d_mcg?: number | null;
  vitamin_d_mcg_qualifier?: NutrientQualifier | null;
  vitamin_e_mg?: number | null;
  vitamin_e_mg_qualifier?: NutrientQualifier | null;
  vitamin_k_mcg?: number | null;
  vitamin_k_mcg_qualifier?: NutrientQualifier | null;
  thiamin_mg?: number | null;
  thiamin_mg_qualifier?: NutrientQualifier | null;
  riboflavin_mg?: number | null;
  riboflavin_mg_qualifier?: NutrientQualifier | null;
  niacin_mg?: number | null;
  niacin_mg_qualifier?: NutrientQualifier | null;
  vitamin_b6_mg?: number | null;
  vitamin_b6_mg_qualifier?: NutrientQualifier | null;
  folate_mcg?: number | null;
  folate_mcg_qualifier?: NutrientQualifier | null;
  folic_acid_mcg?: number | null;
  folic_acid_mcg_qualifier?: NutrientQualifier | null;
  vitamin_b12_mcg?: number | null;
  vitamin_b12_mcg_qualifier?: NutrientQualifier | null;
  biotin_mcg?: number | null;
  biotin_mcg_qualifier?: NutrientQualifier | null;
  pantothenic_acid_mg?: number | null;
  pantothenic_acid_mg_qualifier?: NutrientQualifier | null;
}

/**
 * Standard scraper product output format
 * 
 * This is the format used for both S3 uploads and API submissions.
 * For S3 uploads, scraper_job_id is included.
 * For API submissions, scraper_job_id is omitted.
 */
export interface ScraperProductOutput {
  product_name: string;
  brand: string;
  upc?: string;
  /** List of UPCs; first is primary. Used when product has multiple UPCs (e.g. Trader Joe's SKU-based). */
  upcs?: string[];
  ingredients_text: string;
  serving_size_value?: number | null;
  serving_size_unit?: string | null;
  serving_size_text?: string | null;
  source: string;
  source_id: string;
  source_created_at: string;
  source_last_updated_at: string;
  image_url?: string;
  nutrition?: ScraperNutritionData;
  scraper_job_id?: string; // Only included in S3 uploads, not in API submissions
}
