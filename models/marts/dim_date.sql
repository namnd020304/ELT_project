{{ config(materialized='table') }}

WITH date_range AS (
    SELECT date_day
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            '2020-01-01',  -- Start date
            '2030-12-31',  -- End date
            INTERVAL 1 DAY
        )
    ) AS date_day
)

SELECT
    -- Surrogate key
    FARM_FINGERPRINT(CAST(date_day AS STRING)) AS date_sk,
    
    -- Primary key: YYYYMMDD
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) AS date_key,
    
    -- Full date
    date_day AS date,
    
    -- Year components
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(WEEK FROM date_day) AS week,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DAYOFYEAR FROM date_day) AS day_of_year,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    
    -- Names
    FORMAT_DATE('%B', date_day) AS month_name,
    FORMAT_DATE('%b', date_day) AS month_name_short,
    FORMAT_DATE('%A', date_day) AS day_name,
    FORMAT_DATE('%a', date_day) AS day_name_short,
    
    -- Quarter/Year string
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM date_day) AS STRING), ' ', CAST(EXTRACT(YEAR FROM date_day) AS STRING)) AS quarter_year,
    CONCAT(FORMAT_DATE('%b', date_day), ' ', CAST(EXTRACT(YEAR FROM date_day) AS STRING)) AS month_year,
    
    -- Flags
    EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) AS is_weekend,
    date_day = DATE_TRUNC(date_day, MONTH) AS is_month_start,
    date_day = LAST_DAY(date_day, MONTH) AS is_month_end,
    
    -- Format strings
    FORMAT_DATE('%Y-%m-%d', date_day) AS date_formatted,
    
    CURRENT_TIMESTAMP() AS created_at

FROM date_range
ORDER BY date_key