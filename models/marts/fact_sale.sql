{{ config(
    materialized='incremental',
    unique_key='order_item_sk',
    partition_by={
        'field': 'checkout_date',
        'data_type': 'date',
        'granularity': 'day'
    }
) }}

WITH base_checkout AS (
    SELECT 
        order_id,
        product_id,
        option_id,
        value_id,
        COALESCE(user_id_db, device_id) AS customer_key,
        user_id_db,
        device_id,
        email_address,
        store_id,
        ip_country_code,
        ip_country_name,
        ip_city,
        ip_region_name,
        ip_continent,
        ip,
        TIMESTAMP_SECONDS(CAST(time_stamp AS INT64)) AS checkout_timestamp,
        DATE(TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) AS checkout_date,
        local_time,
        option_label,
        value_label,
        price,
        currency,
        user_agent,
        resolution,
        
        current_url,
        referrer_url,
        show_recommendation,
        api_version
        
    FROM {{ ref('stg_checkout') }}
    
    {% if is_incremental() %}
    WHERE DATE(TIMESTAMP_SECONDS(CAST(time_stamp AS INT64))) > (
        SELECT MAX(checkout_date) FROM {{ this }}
    )
    {% endif %}
),

exchange_rates AS (
    SELECT 
        currency_raw,
        country_short_code,
        exchange_rate_to_usd_amount
    FROM {{ ref('exchange_rate') }}
),

currency_mapping AS (
    SELECT 
        b.*,
        
        -- Map currency symbol to standard code
        CASE 
            WHEN b.currency = '€' THEN 'EUR'
            WHEN b.currency = '£' THEN 'GBP'
            WHEN b.currency = 'USD $' THEN 'USD'
            WHEN b.currency = '$' AND b.ip_country_code = 'HK' THEN 'HKD'
            WHEN b.currency = '$' AND b.ip_country_code = 'US' THEN 'USD'
            WHEN b.currency = '$' AND b.ip_country_code = 'CA' THEN 'CAD'
            WHEN b.currency = '$' AND b.ip_country_code = 'AU' THEN 'AUD'
            WHEN b.currency = '$' AND b.ip_country_code = 'MX' THEN 'MXN'
            WHEN b.currency = '$' AND b.ip_country_code = 'NZ' THEN 'NZD'
            WHEN b.currency = '$' AND b.ip_country_code = 'SG' THEN 'SGD'
            WHEN b.currency = 'kr' AND b.ip_country_code = 'NO' THEN 'NOK'
            WHEN b.currency = 'kr' AND b.ip_country_code = 'DK' THEN 'DKK'
            WHEN b.currency = 'kr' AND b.ip_country_code = 'SE' THEN 'SEK'
            WHEN b.currency = 'AU $' THEN 'AUD'
            WHEN b.currency = 'CAD $' THEN 'CAD'
            WHEN b.currency = 'CHF' THEN 'CHF'
            WHEN b.currency = 'HKD $' THEN 'HKD'
            WHEN b.currency = 'MXN $' THEN 'MXN'
            WHEN b.currency = 'NZD $' THEN 'NZD'
            WHEN b.currency = 'SGD $' THEN 'SGD'
            WHEN b.currency = 'R$' THEN 'BRL'
            WHEN b.currency = 'BOB Bs' THEN 'BOB'
            WHEN b.currency = 'CLP' THEN 'CLP'
            WHEN b.currency = 'COP $' THEN 'COP'
            WHEN b.currency = 'CRC ₡' THEN 'CRC'
            WHEN b.currency = 'DOP $' THEN 'DOP'
            WHEN b.currency = 'GTQ Q' THEN 'GTQ'
            WHEN b.currency = 'PEN S/.' THEN 'PEN'
            WHEN b.currency = 'UYU' THEN 'UYU'
            WHEN b.currency = 'Ft' THEN 'HUF'
            WHEN b.currency = 'Kč' THEN 'CZK'
            WHEN b.currency = 'Lei' THEN 'MDL'
            WHEN b.currency = 'kn' THEN 'HRK'
            WHEN b.currency = 'лв.' THEN 'BGN'
            WHEN b.currency = 'د.ك.‏' THEN 'KWD'
            WHEN b.currency = '₫' THEN 'VND'
            WHEN b.currency = '₱' THEN 'PHP'
            WHEN b.currency = '₲' THEN 'PYG'
            WHEN b.currency = '₹' THEN 'INR'
            WHEN b.currency = '₺' THEN 'TRY'
            WHEN b.currency = '￥' THEN 'JPY'
            WHEN b.currency = '' OR b.currency IS NULL THEN 
                CASE b.ip_country_code
                    WHEN 'AE' THEN 'AED'
                    WHEN 'US' THEN 'USD'
                    WHEN 'GB' THEN 'GBP'
                    ELSE 'USD'
                END
            ELSE 'USD'
        END AS currency_code,
        
        -- Get exchange rate
        COALESCE(er.exchange_rate_to_usd_amount, 1.0) AS exchange_rate
        
    FROM base_checkout b
    LEFT JOIN exchange_rates er
        ON b.currency = er.currency_raw
        AND (er.country_short_code = 'GLOBAL' OR er.country_short_code = b.ip_country_code)
),

enriched_facts AS (
    SELECT 
        cm.*,
        
        -- Calculate USD amounts
        cm.price * cm.exchange_rate AS revenue_usd,
        cm.price AS revenue_local,
        
        -- Join dimension keys
        c.customer_sk,
        d.date_sk AS order_date_sk,
        p.product_sk,
        s.store_sk,
        
        -- Location key (consistent với dim_location)
        FARM_FINGERPRINT(CONCAT(
            COALESCE(cm.ip_country_code, ''),
            '|',
            COALESCE(cm.ip_city, '')
        )) AS location_sk
        
    FROM currency_mapping cm
    
    LEFT JOIN {{ ref('dim_customers') }} c
        ON cm.customer_key = c.customer_key
    
    LEFT JOIN {{ ref('dim_date') }} d
        ON cm.checkout_date = d.date
    
    LEFT JOIN {{ ref('dim_products') }} p
        ON cm.product_id = p.product_id
    
    LEFT JOIN {{ ref('dim_store') }} s
        ON cm.store_id = s.store_id
)

SELECT 
    -- Surrogate key (Primary Key)
    FARM_FINGERPRINT(CONCAT(
        CAST(order_id AS STRING),
        '|',
        CAST(product_id AS STRING),
        '|',
        COALESCE(CAST(option_id AS STRING), ''),
        '|',
        COALESCE(CAST(value_id AS STRING), '')
    )) AS order_item_sk,
    
    -- Foreign Keys to Dimensions
    customer_sk,
    order_date_sk,
    product_sk,
    store_sk,
    location_sk,
    
    -- Degenerate Dimensions (IDs không có dimension table riêng)
    order_id,
    product_id,
    option_id,
    value_id,
    
    -- Time Attributes
    checkout_timestamp,
    checkout_date,
    EXTRACT(YEAR FROM checkout_date) AS checkout_year,
    EXTRACT(MONTH FROM checkout_date) AS checkout_month,
    EXTRACT(DAY FROM checkout_date) AS checkout_day,
    EXTRACT(DAYOFWEEK FROM checkout_date) AS checkout_day_of_week,
    EXTRACT(HOUR FROM checkout_timestamp) AS checkout_hour,
    local_time,
    
    -- Product Customization
    option_label,
    value_label,
    
    -- Currency Information
    currency AS currency_symbol,
    currency_code,
    exchange_rate,
    
    -- Location Attributes (denormalized for quick filtering)
    ip_country_code,
    ip_country_name,
    ip_city,
    ip_region_name,
    ip_continent,
    
    -- Device/Session Attributes
    user_agent,
    resolution,
    CASE 
        WHEN resolution LIKE '%x%' THEN CAST(SPLIT(resolution, 'x')[SAFE_OFFSET(0)] AS INT64)
        ELSE NULL 
    END AS screen_width,
    CASE 
        WHEN resolution LIKE '%x%' THEN CAST(SPLIT(resolution, 'x')[SAFE_OFFSET(1)] AS INT64)
        ELSE NULL 
    END AS screen_height,
    
    -- Device type detection
    CASE 
        WHEN LOWER(user_agent) LIKE '%mobile%' THEN 'Mobile'
        WHEN LOWER(user_agent) LIKE '%tablet%' OR LOWER(user_agent) LIKE '%ipad%' THEN 'Tablet'
        ELSE 'Desktop'
    END AS device_type,
    
    -- Browser detection
    CASE 
        WHEN LOWER(user_agent) LIKE '%chrome%' AND LOWER(user_agent) NOT LIKE '%edg%' THEN 'Chrome'
        WHEN LOWER(user_agent) LIKE '%firefox%' THEN 'Firefox'
        WHEN LOWER(user_agent) LIKE '%safari%' AND LOWER(user_agent) NOT LIKE '%chrome%' THEN 'Safari'
        WHEN LOWER(user_agent) LIKE '%edg%' THEN 'Edge'
        WHEN LOWER(user_agent) LIKE '%trident%' OR LOWER(user_agent) LIKE '%msie%' THEN 'IE'
        ELSE 'Other'
    END AS browser,
    
    -- OS detection
    CASE 
        WHEN LOWER(user_agent) LIKE '%windows%' THEN 'Windows'
        WHEN LOWER(user_agent) LIKE '%mac%' THEN 'Mac'
        WHEN LOWER(user_agent) LIKE '%android%' THEN 'Android'
        WHEN LOWER(user_agent) LIKE '%ios%' OR LOWER(user_agent) LIKE '%iphone%' THEN 'iOS'
        WHEN LOWER(user_agent) LIKE '%linux%' THEN 'Linux'
        ELSE 'Other'
    END AS operating_system,
    
    -- URLs
    current_url,
    referrer_url,
    
    -- Traffic source analysis
    CASE 
        WHEN referrer_url IS NULL OR referrer_url = '' THEN 'Direct'
        WHEN LOWER(referrer_url) LIKE '%google%' THEN 'Google'
        WHEN LOWER(referrer_url) LIKE '%facebook%' THEN 'Facebook'
        WHEN LOWER(referrer_url) LIKE '%instagram%' THEN 'Instagram'
        WHEN LOWER(referrer_url) LIKE '%youtube%' THEN 'YouTube'
        WHEN LOWER(referrer_url) LIKE '%bing%' THEN 'Bing'
        WHEN LOWER(referrer_url) LIKE '%yahoo%' THEN 'Yahoo'
        WHEN LOWER(referrer_url) LIKE '%glamira%' THEN 'Internal'
        ELSE 'Other'
    END AS traffic_source,
    
    -- Session attributes
    CASE 
        WHEN show_recommendation = 'true' THEN TRUE 
        ELSE FALSE 
    END AS has_recommendation,
    api_version,
    
    -- ===== FACTS (Metrics) =====
    
    -- Quantity (mỗi line item = 1 sản phẩm)
    1 AS quantity,
    
    -- Revenue in local currency
    revenue_local AS unit_price_local,
    revenue_local AS revenue_local,
    
    -- Revenue in USD (standardized)
    revenue_usd AS unit_price_usd,
    revenue_usd AS revenue_usd,
    
    -- Cost metrics (placeholder - cần data thực tế)
    NULL AS cost_usd,
    NULL AS gross_profit_usd,
    NULL AS gross_margin_pct,
    
    -- ===== FLAGS =====
    
    -- Customer type
    user_id_db IS NOT NULL AS is_registered_user,
    user_id_db IS NULL AS is_guest_user,
    email_address IS NOT NULL AND email_address != '' AS has_email,
    
    -- Product customization
    option_id IS NOT NULL AS has_customization,
    
    -- Transaction flags
    revenue_usd > 0 AS is_revenue_generating,
    
    -- ===== METADATA =====
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at,
    
    -- Data quality flags
    CASE 
        WHEN customer_sk IS NULL THEN 'Missing Customer'
        WHEN product_sk IS NULL THEN 'Missing Product'
        WHEN store_sk IS NULL THEN 'Missing Store'
        WHEN order_date_sk IS NULL THEN 'Missing Date'
        ELSE 'Valid'
    END AS data_quality_status

FROM enriched_facts