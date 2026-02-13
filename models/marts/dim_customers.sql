{{ config(materialized='table') }}

WITH customer_base AS (
    SELECT 
        COALESCE(user_id_db, device_id) AS customer_key,
        user_id_db,
        device_id,
        email_address,
        user_agent,
        resolution,
        ip,
        CAST(time_stamp AS INT64) AS event_timestamp,
        order_id,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(user_id_db, device_id) 
            ORDER BY CAST(time_stamp AS INT64) DESC
        ) AS row_num
    FROM {{ ref('stg_checkout') }}
),

deduplicated_customers AS (
    SELECT 
        customer_key,
        user_id_db,
        device_id,
        email_address,
        user_agent,
        resolution,
        ip,
        event_timestamp
    FROM customer_base
    WHERE row_num = 1
),

customer_summary AS (
    SELECT 
        c.customer_key,
        c.user_id_db,
        c.device_id,
        c.email_address,
        c.user_agent,
        c.resolution,
        c.ip,
        COUNT(DISTINCT s.order_id) AS total_orders,
        MIN(CAST(s.time_stamp AS INT64)) AS first_order_timestamp,
        MAX(CAST(s.time_stamp AS INT64)) AS last_order_timestamp,
        SUM(CAST(s.price AS FLOAT64)) AS lifetime_value
    FROM deduplicated_customers c
    LEFT JOIN {{ ref('stg_checkout') }} s
        ON c.customer_key = COALESCE(s.user_id_db, s.device_id)
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT 
    -- Surrogate key using FARM_FINGERPRINT
    FARM_FINGERPRINT(customer_key) AS customer_sk,
    
    customer_key,
    user_id_db AS customer_id,
    device_id,
    email_address,
    user_agent,
    resolution,
    ip,
    total_orders,
    lifetime_value,
    TIMESTAMP_SECONDS(first_order_timestamp) AS first_order_at,
    TIMESTAMP_SECONDS(last_order_timestamp) AS last_order_at,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM customer_summary