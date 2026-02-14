{{ config(materialized='view') }}

WITH source AS (
    SELECT 
        product_id,
        sku,
        name,
        product_type,
        product_type_value,
        gender,
        category_name,
        category,
        attribute_set,
        attribute_set_id,
        material_design,
        CAST(price AS FLOAT64) AS price,
        CAST(min_price AS FLOAT64) AS min_price,
        CAST(max_price AS FLOAT64) AS max_price,
        min_price_format,
        max_price_format,
        CAST(gold_weight AS FLOAT64) AS gold_weight,
        CAST(fixed_silver_weight AS FLOAT64) AS fixed_silver_weight,
        CAST(none_metal_weight AS FLOAT64) AS none_metal_weight,
        CAST(platinum_palladium_info_in_alloy AS INT64) AS has_platinum_palladium,
        CAST(qty AS INT64) AS qty,
        crawled_url,
        store_code,
        type_id,
        collection_id,
        collection,
        visible_contents,
        CAST(show_popup_quantity_eternity AS INT64) = 1 AS is_eternity,
        CAST(bracelet_without_chain AS INT64) = 1 AS is_bracelet_without_chain
        
    FROM {{ source('test', 'products') }}
    WHERE product_id IS NOT NULL
),

cleaned AS (
    SELECT 
        *,
        REGEXP_EXTRACT(name, r'GLAMIRA\s+(\w+)') AS brand_line,
        REGEXP_EXTRACT(name, r'(\d+)\s*cm') AS size_cm,
        CASE 
            WHEN TRIM(category_name) = '' THEN 'Uncategorized'
            ELSE category_name
        END AS category_name_clean,
        CASE 
            WHEN price < 5000 THEN 'Budget'
            WHEN price < 20000 THEN 'Mid-Range'
            WHEN price < 50000 THEN 'Premium'
            ELSE 'Luxury'
        END AS price_tier
        
    FROM source
)

SELECT * FROM cleaned