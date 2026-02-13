{{ config(materialized='table') }}

SELECT 
    -- Surrogate key
    FARM_FINGERPRINT(CONCAT(
        CAST(o.order_id AS STRING),
        '|',
        CAST(o.product_id AS STRING),
        '|',
        CAST(COALESCE(o.option_id, '') AS STRING),
        '|',
        CAST(COALESCE(o.value_id, '') AS STRING)
    )) AS order_item_sk,
    
    -- Dimension keys
    c.customer_sk,
    d.date_sk AS order_date_sk,
    p.product_sk,
    s.store_sk,
    FARM_FINGERPRINT(CONCAT(
        COALESCE(o.ip_country_code, ''),
        '|',
        COALESCE(o.ip_city, '')
    )) AS location_sk,
    
    -- Degenerate dimensions
    o.order_id,
    o.product_id,
    o.option_id,
    o.value_id,
    
    -- Timestamps
    o.checkout_timestamp,
    o.checkout_date,
    
    -- Attributes
    o.currency,
    o.option_label,
    o.value_label,
    o.ip_country_code,
    o.ip_city,
    o.user_agent,
    o.resolution,
    
    -- Metrics
    1 AS quantity,
    CAST(o.price AS FLOAT64) AS unit_price,
    CAST(o.price AS FLOAT64) AS revenue,
    
    -- Flags
    o.user_id_db IS NOT NULL AS is_registered_user,
    o.email_address IS NOT NULL AND o.email_address != '' AS has_email,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_at

FROM {{ ref('stg_checkout') }} o

LEFT JOIN {{ ref('dim_customers') }} c
    ON COALESCE(o.user_id_db, o.device_id) = c.customer_key

LEFT JOIN {{ ref('dim_date') }} d
    ON o.checkout_date = d.date

LEFT JOIN {{ ref('dim_products') }} p
    ON o.product_id = p.product_id

LEFT JOIN {{ ref('dim_store') }} s
    ON o.store_id = s.store_id