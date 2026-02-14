{{ config(materialized='table') }}

WITH product_base AS (
    SELECT 
        CAST(product_id AS STRING) AS product_id,
        sku,
        name,
        product_type,
        product_type_value,
        gender,
        category_name_clean AS category_name,
        category,
        attribute_set,
        attribute_set_id,
        material_design,
        brand_line,
        size_cm,
        price,
        min_price,
        max_price,
        min_price_format,
        max_price_format,
        price_tier,
        gold_weight,
        fixed_silver_weight,
        none_metal_weight,
        has_platinum_palladium,
        qty,
        crawled_url,
        store_code,
        type_id,
        collection_id,
        collection,
        is_eternity,
        is_bracelet_without_chain,
        
        -- Deduplication: Take latest record
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY crawled_url DESC  -- Giả sử URL mới nhất = data mới nhất
        ) AS row_num
        
    FROM {{ ref('stg_products') }}
),

deduplicated AS (
    SELECT * EXCEPT(row_num)
    FROM product_base
    WHERE row_num = 1
)

SELECT 
    -- Surrogate key
    FARM_FINGERPRINT(product_id)  AS product_sk,
    
    -- Product identifiers
    CAST(product_id AS INT64) as product_id, 
    sku,
    
    -- Product attributes
    name,
    product_type,
    product_type_value,
    brand_line,
    size_cm,
    
    -- Classification
    gender,
    category_name,
    category,
    attribute_set,
    attribute_set_id,
    collection_id,
    collection,
    
    -- Material info
    material_design,
    gold_weight,
    fixed_silver_weight,
    none_metal_weight,
    CASE 
        WHEN gold_weight > 0 THEN TRUE 
        ELSE FALSE 
    END AS has_gold,
    CASE 
        WHEN fixed_silver_weight > 0 THEN TRUE 
        ELSE FALSE 
    END AS has_silver,
    has_platinum_palladium,
    
    -- Pricing
    price AS current_price,
    min_price,
    max_price,
    min_price_format,
    max_price_format,
    price_tier,
    max_price - min_price AS price_range,
    ROUND((max_price - min_price) / NULLIF(min_price, 0) * 100, 2) AS price_range_pct,
    
    -- Inventory
    qty AS stock_qty,
    CASE 
        WHEN qty > 0 THEN TRUE 
        ELSE FALSE 
    END AS is_in_stock,
    
    -- Product type flags
    is_eternity,
    is_bracelet_without_chain,
    
    -- Metadata
    crawled_url AS product_url,
    store_code,
    type_id,
    
    -- Timestamps
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
    
FROM deduplicated