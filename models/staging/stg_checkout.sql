WITH checkout as(
    SELECT time_stamp,
    ip, user_agent, resolution, user_id_db,
    device_id, api_version, store_id, local_time, show_recommendation, 
    current_url, referrer_url, email_address, order_id, c.product_id, 
    SAFE_DIVIDE(CAST(REGEXP_REPLACE(c.price, r'[^0-9]', '') AS FLOAT64), 100) as price ,c.currency, 
    o.option_id ,o.option_label, o.value_id, o.value_label
    FROM {{source('test', 'summarysummary')}}, unnest(cart_products) as c, unnest(c.option) as o
    where collection = 'checkout_success'
)

SELECT * FROM checkout