#  Glamira E-commerce Data Warehouse

> Dimensional data warehouse built with **dbt + BigQuery** for jewelry e-commerce analytics

---

##  Overview

Star schema data warehouse transforming Glamira's checkout events and product data into analytics-ready tables for BI and reporting across 50+ countries.

**Tech Stack:** dbt | BigQuery | Python

---

##  Architecture

```
         dim_date
             │
    dim_store│      dim_customer
         └───┼────┬────┘
             │    │
          FACT_SALE (central)
             │    │
         ┌───┼────┴────┐
    dim_products    dim_location
```

---

##  Data Models

### Staging (`models/staging/`)
- `stg_checkout` - Cleaned checkout events
- `stg_products` - Product catalog
- `stg_ip` - IP geolocation

### Dimensions (`models/marts/`)
- `dim_customers` - Customer master (1 row per customer)
- `dim_products` - Product catalog (1 row per product)
- `dim_date` - Date dimension (2020-2030)
- `dim_store` - Store/market master (1 row per store)
- `dim_location` - Geographic locations (country-city)

### Fact Table
- `fact_sale` - Sales transactions (1 row per order line item)
  - **Grain:** order_id + product_id + option_id + value_id
  - **Features:** Incremental loading, partitioned by date, clustered
  - **Metrics:** Revenue (USD + local), quantity, exchange rates
  - **Attributes:** Device type, browser, traffic source, location

---

##  Quick Start

```bash
pip install dbt-bigquery

dbt seed              # Load exchange rates
dbt run               # Build all models
dbt test              # Run quality tests
dbt docs generate     # Generate documentation
dbt docs serve        # View docs at localhost:8080
```

---

##  Key Tables

### `fact_sale` (Main table)
- Primary Key: `order_item_sk`
- Foreign Keys: `customer_sk`, `product_sk`, `store_sk`, `date_sk`, `location_sk`
- Metrics: `revenue_usd`, `revenue_local`, `quantity`, `exchange_rate`
- Attributes: `device_type`, `browser`, `traffic_source`, `country`, `city`
- Flags: `is_registered_user`, `has_customization`, `data_quality_status`

### `dim_customers`
- Primary Key: `customer_sk`
- Attributes: `customer_id`, `email`, `total_orders`, `lifetime_value`, `first/last_order_at`

### `dim_products`
- Primary Key: `product_sk`
- Attributes: `name`, `category`, `brand_line`, `price_tier`, `stock_qty`

---


