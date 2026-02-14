{{ config(materialized='table') }}

WITH store_country_mapping AS (
    SELECT 
        store_id,
        ip_country_code,
        ip_country_name,
        ip_continent,
        COUNT(*) AS orders,
        ROW_NUMBER() OVER (
            PARTITION BY store_id 
            ORDER BY COUNT(*) DESC
        ) AS rank
    FROM {{ ref('stg_checkout') }}
    WHERE store_id IS NOT NULL
        AND ip_country_code IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

primary_country_per_store AS (
    SELECT 
        store_id,
        ip_country_code AS country_code,
        ip_country_name AS country,
        ip_continent AS region
    FROM store_country_mapping
    WHERE rank = 1
),

store_with_products AS (
    SELECT DISTINCT
        p.store_code,
        c.store_id
    FROM {{ ref('stg_products') }} p
    INNER JOIN {{ ref('stg_checkout') }} c
        ON p.product_id = c.product_id
    WHERE p.store_code IS NOT NULL
        AND c.store_id IS NOT NULL
),

store_enrichment AS (
    SELECT 
        COALESCE(s.store_id, sp.store_id) AS store_id,
        COALESCE(sp.store_code, CONCAT('gl', LOWER(s.country_code))) AS store_code,
        s.country_code,
        s.country,
        s.region,
        CASE s.country_code
            WHEN 'AD' THEN 'EUR'
            WHEN 'AT' THEN 'EUR'
            WHEN 'BE' THEN 'EUR'
            WHEN 'CY' THEN 'EUR'
            WHEN 'DE' THEN 'EUR'
            WHEN 'EE' THEN 'EUR'
            WHEN 'ES' THEN 'EUR'
            WHEN 'FI' THEN 'EUR'
            WHEN 'FR' THEN 'EUR'
            WHEN 'GR' THEN 'EUR'
            WHEN 'IE' THEN 'EUR'
            WHEN 'IT' THEN 'EUR'
            WHEN 'LT' THEN 'EUR'
            WHEN 'LU' THEN 'EUR'
            WHEN 'LV' THEN 'EUR'
            WHEN 'MC' THEN 'EUR'
            WHEN 'MT' THEN 'EUR'
            WHEN 'NL' THEN 'EUR'
            WHEN 'PT' THEN 'EUR'
            WHEN 'SI' THEN 'EUR'
            WHEN 'SK' THEN 'EUR'
            WHEN 'SM' THEN 'EUR'
            WHEN 'VA' THEN 'EUR'
            WHEN 'GB' THEN 'GBP'
            WHEN 'CH' THEN 'CHF'
            WHEN 'DK' THEN 'DKK'
            WHEN 'NO' THEN 'NOK'
            WHEN 'SE' THEN 'SEK'
            WHEN 'IS' THEN 'ISK'
            WHEN 'CZ' THEN 'CZK'
            WHEN 'HU' THEN 'HUF'
            WHEN 'PL' THEN 'PLN'
            WHEN 'RO' THEN 'RON'
            WHEN 'BG' THEN 'BGN'
            WHEN 'HR' THEN 'HRK'
            WHEN 'RU' THEN 'RUB'
            WHEN 'UA' THEN 'UAH'
            WHEN 'TR' THEN 'TRY'
            WHEN 'BY' THEN 'BYN'
            WHEN 'MD' THEN 'MDL'
            WHEN 'RS' THEN 'RSD'
            WHEN 'AL' THEN 'ALL'
            WHEN 'BA' THEN 'BAM'
            WHEN 'MK' THEN 'MKD'
            WHEN 'US' THEN 'USD'
            WHEN 'CA' THEN 'CAD'
            WHEN 'MX' THEN 'MXN'
            WHEN 'CR' THEN 'CRC'
            WHEN 'PA' THEN 'PAB'
            WHEN 'GT' THEN 'GTQ'
            WHEN 'HN' THEN 'HNL'
            WHEN 'NI' THEN 'NIO'
            WHEN 'SV' THEN 'USD'
            WHEN 'BZ' THEN 'BZD'
            WHEN 'DO' THEN 'DOP'
            WHEN 'JM' THEN 'JMD'
            WHEN 'TT' THEN 'TTD'
            WHEN 'BB' THEN 'BBD'
            WHEN 'BS' THEN 'BSD'
            WHEN 'KY' THEN 'KYD'
            WHEN 'BR' THEN 'BRL'
            WHEN 'AR' THEN 'ARS'
            WHEN 'CL' THEN 'CLP'
            WHEN 'CO' THEN 'COP'
            WHEN 'PE' THEN 'PEN'
            WHEN 'VE' THEN 'VES'
            WHEN 'EC' THEN 'USD'
            WHEN 'BO' THEN 'BOB'
            WHEN 'PY' THEN 'PYG'
            WHEN 'UY' THEN 'UYU'
            WHEN 'GY' THEN 'GYD'
            WHEN 'SR' THEN 'SRD'
            WHEN 'AE' THEN 'AED'
            WHEN 'SA' THEN 'SAR'
            WHEN 'QA' THEN 'QAR'
            WHEN 'KW' THEN 'KWD'
            WHEN 'BH' THEN 'BHD'
            WHEN 'OM' THEN 'OMR'
            WHEN 'JO' THEN 'JOD'
            WHEN 'LB' THEN 'LBP'
            WHEN 'SY' THEN 'SYP'
            WHEN 'IQ' THEN 'IQD'
            WHEN 'YE' THEN 'YER'
            WHEN 'IL' THEN 'ILS'
            WHEN 'PS' THEN 'ILS'
            WHEN 'JP' THEN 'JPY'
            WHEN 'CN' THEN 'CNY'
            WHEN 'KR' THEN 'KRW'
            WHEN 'TW' THEN 'TWD'
            WHEN 'HK' THEN 'HKD'
            WHEN 'MO' THEN 'MOP'
            WHEN 'SG' THEN 'SGD'
            WHEN 'MY' THEN 'MYR'
            WHEN 'TH' THEN 'THB'
            WHEN 'ID' THEN 'IDR'
            WHEN 'PH' THEN 'PHP'
            WHEN 'VN' THEN 'VND'
            WHEN 'BN' THEN 'BND'
            WHEN 'KH' THEN 'KHR'
            WHEN 'LA' THEN 'LAK'
            WHEN 'MM' THEN 'MMK'
            WHEN 'MN' THEN 'MNT'
            WHEN 'IN' THEN 'INR'
            WHEN 'PK' THEN 'PKR'
            WHEN 'BD' THEN 'BDT'
            WHEN 'LK' THEN 'LKR'
            WHEN 'NP' THEN 'NPR'
            WHEN 'BT' THEN 'BTN'
            WHEN 'MV' THEN 'MVR'
            WHEN 'AF' THEN 'AFN'
            WHEN 'KZ' THEN 'KZT'
            WHEN 'UZ' THEN 'UZS'
            WHEN 'TM' THEN 'TMT'
            WHEN 'KG' THEN 'KGS'
            WHEN 'TJ' THEN 'TJS'
            WHEN 'AZ' THEN 'AZN'
            WHEN 'AM' THEN 'AMD'
            WHEN 'GE' THEN 'GEL'
            WHEN 'AU' THEN 'AUD'
            WHEN 'NZ' THEN 'NZD'
            WHEN 'FJ' THEN 'FJD'
            WHEN 'PG' THEN 'PGK'
            WHEN 'WS' THEN 'WST'
            WHEN 'TO' THEN 'TOP'
            WHEN 'VU' THEN 'VUV'
            WHEN 'ZA' THEN 'ZAR'
            WHEN 'EG' THEN 'EGP'
            WHEN 'NG' THEN 'NGN'
            WHEN 'KE' THEN 'KES'
            WHEN 'GH' THEN 'GHS'
            WHEN 'MA' THEN 'MAD'
            WHEN 'TN' THEN 'TND'
            WHEN 'DZ' THEN 'DZD'
            WHEN 'ET' THEN 'ETB'
            WHEN 'TZ' THEN 'TZS'
            WHEN 'UG' THEN 'UGX'
            WHEN 'AO' THEN 'AOA'
            WHEN 'MZ' THEN 'MZN'
            WHEN 'ZM' THEN 'ZMW'
            WHEN 'ZW' THEN 'ZWL'
            WHEN 'BW' THEN 'BWP'
            WHEN 'NA' THEN 'NAD'
            WHEN 'MU' THEN 'MUR'
            WHEN 'SC' THEN 'SCR'
            WHEN 'RW' THEN 'RWF'
            
            ELSE 'USD'  -- Default to USD
        END AS currency_code,
        CASE s.country_code
            WHEN 'US' THEN 'US Dollar'
            WHEN 'GB' THEN 'British Pound'
            WHEN 'DE' THEN 'Euro'
            WHEN 'FR' THEN 'Euro'
            WHEN 'IT' THEN 'Euro'
            WHEN 'ES' THEN 'Euro'
            WHEN 'DK' THEN 'Danish Krone'
            WHEN 'SE' THEN 'Swedish Krona'
            WHEN 'NO' THEN 'Norwegian Krone'
            WHEN 'CH' THEN 'Swiss Franc'
            WHEN 'JP' THEN 'Japanese Yen'
            WHEN 'CN' THEN 'Chinese Yuan'
            WHEN 'AU' THEN 'Australian Dollar'
            WHEN 'CA' THEN 'Canadian Dollar'
            WHEN 'IN' THEN 'Indian Rupee'
            WHEN 'AE' THEN 'UAE Dirham'
            WHEN 'SA' THEN 'Saudi Riyal'
            WHEN 'BR' THEN 'Brazilian Real'
            WHEN 'MX' THEN 'Mexican Peso'
            WHEN 'ZA' THEN 'South African Rand'
            WHEN 'SG' THEN 'Singapore Dollar'
            WHEN 'HK' THEN 'Hong Kong Dollar'
            WHEN 'KR' THEN 'South Korean Won'
            WHEN 'TH' THEN 'Thai Baht'
            WHEN 'MY' THEN 'Malaysian Ringgit'
            WHEN 'ID' THEN 'Indonesian Rupiah'
            WHEN 'PH' THEN 'Philippine Peso'
            WHEN 'VN' THEN 'Vietnamese Dong'
            WHEN 'TR' THEN 'Turkish Lira'
            WHEN 'PL' THEN 'Polish Zloty'
            WHEN 'RU' THEN 'Russian Ruble'
            ELSE 'US Dollar'
        END AS currency_name,
        CASE s.country_code
            WHEN 'US' THEN 'https://www.glamira.com'
            WHEN 'GB' THEN 'https://www.glamira.co.uk'
            WHEN 'AE' THEN 'https://www.glamira.ae'
            ELSE CONCAT('https://www.glamira.', LOWER(s.country_code))
        END AS store_url,
        CASE s.country_code
            WHEN 'US' THEN 'English'
            WHEN 'GB' THEN 'English'
            WHEN 'CA' THEN 'English'
            WHEN 'AU' THEN 'English'
            WHEN 'NZ' THEN 'English'
            WHEN 'IE' THEN 'English'
            WHEN 'SG' THEN 'English'
            WHEN 'IN' THEN 'English'
            WHEN 'ZA' THEN 'English'
            WHEN 'DE' THEN 'German'
            WHEN 'AT' THEN 'German'
            WHEN 'CH' THEN 'German'
            WHEN 'FR' THEN 'French'
            WHEN 'BE' THEN 'French'
            WHEN 'LU' THEN 'French'
            WHEN 'MC' THEN 'French'
            WHEN 'ES' THEN 'Spanish'
            WHEN 'MX' THEN 'Spanish'
            WHEN 'AR' THEN 'Spanish'
            WHEN 'CO' THEN 'Spanish'
            WHEN 'CL' THEN 'Spanish'
            WHEN 'PE' THEN 'Spanish'
            WHEN 'IT' THEN 'Italian'
            WHEN 'PT' THEN 'Portuguese'
            WHEN 'BR' THEN 'Portuguese'
            WHEN 'NL' THEN 'Dutch'
            WHEN 'DK' THEN 'Danish'
            WHEN 'SE' THEN 'Swedish'
            WHEN 'NO' THEN 'Norwegian'
            WHEN 'FI' THEN 'Finnish'
            WHEN 'PL' THEN 'Polish'
            WHEN 'CZ' THEN 'Czech'
            WHEN 'GR' THEN 'Greek'
            WHEN 'TR' THEN 'Turkish'
            WHEN 'RU' THEN 'Russian'
            WHEN 'UA' THEN 'Ukrainian'
            WHEN 'JP' THEN 'Japanese'
            WHEN 'CN' THEN 'Chinese'
            WHEN 'KR' THEN 'Korean'
            WHEN 'TH' THEN 'Thai'
            WHEN 'VN' THEN 'Vietnamese'
            WHEN 'ID' THEN 'Indonesian'
            WHEN 'MY' THEN 'Malay'
            WHEN 'PH' THEN 'Filipino'
            WHEN 'AE' THEN 'Arabic'
            WHEN 'SA' THEN 'Arabic'
            WHEN 'EG' THEN 'Arabic'
            WHEN 'MA' THEN 'Arabic'
            WHEN 'DZ' THEN 'Arabic'
            WHEN 'TN' THEN 'Arabic'
            ELSE 'English'
        END AS language
        
    FROM primary_country_per_store s
    LEFT JOIN store_with_products sp
        ON s.store_id = sp.store_id
)

SELECT 
    FARM_FINGERPRINT(CONCAT(CAST(store_id AS STRING), store_code)) AS store_sk,
    store_id,
    store_code,
    country_code,
    country,
    region,
    currency_code,
    currency_name,
    language,
    store_url,
    TRUE AS is_active,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
    
FROM store_enrichment
WHERE store_id IS NOT NULL
ORDER BY store_id