{{ config(materialized='table') }}

WITH unique_locations AS (
    SELECT DISTINCT
        ip_country_code AS country_code,
        ip_country_name AS country,
        ip_city AS city,
        ip_region_name AS region,
        ip_continent AS continent
    FROM {{ ref('stg_checkout') }}
    WHERE ip_country_code IS NOT NULL
)

SELECT 
    FARM_FINGERPRINT(CONCAT(
        COALESCE(country_code, ''),
        '|',
        COALESCE(city, '')
    )) AS location_sk,
    
    country_code,
    country,
    city,
    region AS state_region,
    continent,
    
    CURRENT_TIMESTAMP() AS created_at
    
FROM unique_locations
ORDER BY country, city