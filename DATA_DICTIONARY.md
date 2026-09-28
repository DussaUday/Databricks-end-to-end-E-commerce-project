# E-Commerce Data Dictionary

Complete schema reference for all tables across the Bronze, Silver, and Gold layers.

**Catalog:** `ecommerce`  
**Schemas:** `bronze`, `sliver`, `gold`

---

## Table of Contents

- [Bronze Layer (Raw Ingestion)](#bronze-layer-raw-ingestion)
  - [brz_brands](#brz_brands)
  - [brz_category](#brz_category)
  - [brz_customers](#brz_customers)
  - [brz_date](#brz_date)
  - [brz_products](#brz_products)
  - [brz_order_items](#brz_order_items)
- [Silver Layer (Cleansed)](#silver-layer-cleansed)
  - [slv_brands](#slv_brands)
  - [slv_category](#slv_category)
  - [slv_customers](#slv_customers)
  - [slv_date](#slv_date)
  - [slv_products](#slv_products)
  - [slv_order_items](#slv_order_items)
- [Gold Layer (Analytics-Ready)](#gold-layer-analytics-ready)
  - [gld_dim_products](#gld_dim_products)
  - [gld_dim_customers](#gld_dim_customers)
  - [gld_dim_date](#gld_dim_date)
  - [gld_fact_order_items](#gld_fact_order_items)
- [Entity Relationship Diagram](#entity-relationship-diagram)

---

## Bronze Layer (Raw Ingestion)

Source notebooks: `medallion_processing_dim/1_dim_notebook`, `medallion_processing_fact/1_fact_bronze`

All bronze tables include `_source_file` (original CSV path) and `ingested_at` (load timestamp) metadata columns.

### brz_brands

Source: `/Volumes/ecommerce/source_data/raw_data/brands/brands.csv`

| Column | Type | Description |
| --- | --- | --- |
| brand_Code | string | Brand code (e.g., ACME, NOVW, ZNTH) — uppercase |
| brand_Name | string | Brand display name (may have leading/trailing whitespace) |
| catagory_code | string | Category code (e.g., CE, APP, HNK) — uppercase; note: column name has typo "catagory" |
| _source_file | string | Source CSV file path |
| ingested_at | timestamp | Ingestion timestamp |

### brz_category

Source: `/Volumes/ecommerce/source_data/raw_data/category/category.csv`

| Column | Type | Description |
| --- | --- | --- |
| category_code | string | Category code (e.g., ce, app, hnk) — lowercase |
| category_name | string | Category display name (e.g., Electronics, Apparel) |
| _source_file | string | Source CSV file path |
| ingested_at | timestamp | Ingestion timestamp |

### brz_customers

Source: `/Volumes/ecommerce/source_data/raw_data/customers/customers.csv`

| Column | Type | Description |
| --- | --- | --- |
| customer_id | string | Customer ID (e.g., CUST000000000001) |
| phone | string | Phone number (may have `.0` suffix) |
| country_code | string | ISO country code (e.g., IN, AU, GB, US) |
| country | string | Country name (e.g., India, Australia) |
| state | string | State/region code (e.g., MH, VIC, ENG) |
| _source_file | string | Source CSV file path |
| ingested_at | timestamp | Ingestion timestamp |

### brz_date

Source: `/Volumes/ecommerce/source_data/raw_data/date/date.csv`

| Column | Type | Description |
| --- | --- | --- |
| date | string | Date in dd-MM-yyyy format |
| year | string | Year (e.g., "2025") |
| day_name | string | Day of week (e.g., "friday", "SATURDAY") — inconsistent casing |
| quarter | string | Quarter number (1-4) |
| week_of_year | string | Week number (some negative values) |
| _source_file | string | Source CSV file path |
| ingested_at | timestamp | Ingestion timestamp |

### brz_products

Source: `/Volumes/ecommerce/source_data/raw_data/products/products.csv`

| Column | Type | Description |
| --- | --- | --- |
| product_id | string | Product ID (numeric, e.g., "2000000000015") |
| sku | string | Stock keeping unit (e.g., STCR-HNK-00001) |
| category_code | string | Category code (lowercase, e.g., hnk, ce, app) |
| brand_code | string | Brand code (lowercase, e.g., stcr, novw, acme) |
| color | string | Product color |
| size | string | Product size (e.g., One-Size, S, M) |
| material | string | Material (may have typos: Coton, Ruber, Alumium) |
| weight_grams | string | Weight with "g" suffix (e.g., "305g") |
| length_cm | string | Length in cm (may use comma decimal separator) |
| width_cm | string | Width in cm |
| height_cm | string | Height in cm |
| rating_count | string | Rating count (may have negative values) |
| _source_file | string | Source CSV file path |
| ingested_at | timestamp | Ingestion timestamp |

### brz_order_items

Source: `/Volumes/ecommerce/source_data/raw_data/order_items/landing/order_items_*.csv`
Ingestion: Auto Loader (streaming, `availableNow` trigger)

| Column | Type | Description |
| --- | --- | --- |
| dt | string | Transaction date (yyyy-MM-dd) |
| order_ts | string | Order timestamp (yyyy-MM-dd HH:mm:ss) |
| customer_id | string | Customer ID (e.g., CUST000000241190) |
| order_id | string | Order ID (numeric) |
| item_seq | string | Item sequence within order (numeric) |
| product_id | string | Product ID (numeric) |
| quantity | string | Quantity (may be "Two" instead of 2) |
| unit_price_currency | string | Currency code (INR, USD, GBP, AED, SGD, CAD, AUD) |
| unit_price | string | Unit price (may have $ prefix) |
| discount_pct | string | Discount percentage (may have % suffix) |
| tax_amount | string | Tax amount |
| channel | string | Sales channel ("web" or "app") |
| coupon_code | string | Coupon code (e.g., FEST20, PRIME5, NEW10) or NULL |
| _source_file | string | Source CSV file path |
| ingested_at | timestamp | Ingestion timestamp |

---

## Silver Layer (Cleansed)

Source notebooks: `medallion_processing_dim/1_dim_sliver`, `medallion_processing_fact/2_fact_silver`

### slv_brands

| Column | Type | Description | Transformation |
| --- | --- | --- | --- |
| brand_code | string | Brand code (cleaned, uppercase) | Trimmed, regex cleaned (non-alphanumeric removed) |
| brand_name | string | Brand display name | Trimmed whitespace |
| catagory_code | string | Category code (anomalies fixed) | GROCERY→GRCY, BOOKS→BKS, TOYS→TOY |
| _source_file | string | Source CSV file path | — |
| ingested_at | timestamp | Ingestion timestamp | — |

### slv_category

| Column | Type | Description | Transformation |
| --- | --- | --- | --- |
| category_code | string | Category code (lowercase) | Trimmed |
| category_name | string | Category display name | Trimmed |
| _source_file | string | Source CSV file path | — |
| ingested_at | timestamp | Ingestion timestamp | — |

### slv_customers

| Column | Type | Description | Transformation |
| --- | --- | --- | --- |
| customer_id | string | Customer ID | Trimmed |
| phone | string | Phone number | Stripped `.0` suffix |
| country_code | string | ISO country code | — |
| country | string | Country name | — |
| state | string | State code | Cleaned |
| _source_file | string | Source CSV file path | — |
| ingested_at | timestamp | Ingestion timestamp | — |

### slv_date

| Column | Type | Description | Transformation |
| --- | --- | --- | --- |
| date | date | Date value | Parsed from dd-MM-yyyy string |
| year | integer | Year | Cast to int |
| day_name | string | Day of week | Normalized casing |
| quarter | integer | Quarter (1-4) | Cast to int |
| week_of_year | integer | Week number | Cast to int, negative values preserved |
| _source_file | string | Source CSV file path | — |
| ingested_at | timestamp | Ingestion timestamp | — |

### slv_products

| Column | Type | Description | Transformation |
| --- | --- | --- | --- |
| product_id | long | Product ID | Cast to long |
| sku | string | SKU | — |
| category_code | string | Category code (lowercase) | — |
| brand_code | string | Brand code (lowercase) | — |
| color | string | Product color | — |
| size | string | Product size | — |
| material | string | Material | Typos fixed: Coton→Cotton, Ruber→Rubber, Alumium→Aluminium |
| weight_grams | integer | Weight in grams | Stripped "g" suffix, cast to int |
| length_cm | float | Length in cm | Replaced comma decimal separator with dot, cast to float |
| width_cm | float | Width in cm | Cast to float |
| height_cm | float | Height in cm | Cast to float |
| rating_count | integer | Rating count | Negative values set to 0, cast to int |
| _source_file | string | Source CSV file path | — |
| ingested_at | timestamp | Ingestion timestamp | — |

### slv_order_items

| Column | Type | Description | Transformation |
| --- | --- | --- | --- |
| dt | date | Transaction date | Parsed from string |
| order_ts | timestamp | Order timestamp | Parsed from string (multiple format fallbacks) |
| customer_id | string | Customer ID | — |
| order_id | long | Order ID | Cast to long |
| item_seq | integer | Item sequence | Cast to int |
| product_id | long | Product ID | Cast to long |
| quantity | integer | Quantity | "Two"→2, cast to int |
| unit_price_currency | string | Currency code | — |
| unit_price | decimal(18,2) | Unit price | Stripped `$`, cast to decimal |
| discount_pct | decimal(5,2) | Discount percentage | Stripped `%`, cast to decimal |
| tax_amount | decimal(18,2) | Tax amount | Stripped non-numeric, cast to decimal |
| channel | string | Sales channel | Normalized: web→Website, app→Mobile |
| coupon_code | string | Coupon code | Lowercased, trimmed |
| _source_file | string | Source CSV file path | — |
| ingested_at | timestamp | Ingestion timestamp | — |
| processed_time | timestamp | Silver processing timestamp | Added during transformation |

Load method: **MERGE upsert** on (order_id, item_seq)

---

## Gold Layer (Analytics-Ready)

Source notebooks: `medallion_processing_dim/1_dim_gold`, `medallion_processing_fact/3_fact_gold`

### gld_dim_products

Enriched products table with brand and category names joined from silver layer.

| Column | Type | Description |
| --- | --- | --- |
| product_id | long | Product ID (primary key) |
| sku | string | Stock keeping unit |
| category_code | string | Category code (from products) |
| category_name | string | Category name (joined from category; 'Not Available' if no match) |
| brand_code | string | Brand code (from products) |
| brand_name | string | Brand name (joined from brands; 'Not Available' if no match) |
| color | string | Product color |
| size | string | Product size |
| material | string | Material (typo-corrected in silver) |
| weight_grams | integer | Weight in grams |
| length_cm | float | Length in cm |
| width_cm | float | Width in cm |
| height_cm | float | Height in cm |
| rating_count | integer | Rating count (non-negative) |
| _source_file | string | Source CSV file path |
| ingested_at | timestamp | Ingestion timestamp |

**Join Logic:**
```
WITH brands_categories AS (
  SELECT b.brand_name, b.brand_code, c.category_name, c.category_code
  FROM slv_brands b
  INNER JOIN slv_category c ON UPPER(b.catagory_code) = UPPER(c.category_code)
)
SELECT p.*, COALESCE(bc.brand_name, 'Not Available'), COALESCE(bc.category_name, 'Not Available')
FROM slv_products p
LEFT JOIN brands_categories bc ON UPPER(p.brand_code) = UPPER(bc.brand_code)
```

Row count: **50,000**

### gld_dim_customers

Customers enriched with geographic region mapping.

| Column | Type | Description |
| --- | --- | --- |
| country | string | Country name |
| state | string | State code |
| customer_id | string | Customer ID (primary key) |
| phone | string | Phone number |
| country_code | string | ISO country code |
| _source_file | string | Source CSV file path |
| ingested_at | timestamp | Ingestion timestamp |
| region | string | Mapped region (West, South, North, East, SouthEast, NorthEast, England, Wales, Scotland, Northern Ireland, NorthEast-USA, etc.) or 'Other' |

**Region Mapping:**
- India: MH/GJ/RJ→West, KA/TN/TS/AP/KL→South, UP/WB/DL→North
- Australia: VIC→SouthEast, WA→West, NSW→East, QLD→NorthEast
- United Kingdom: ENG→England, WLS→Wales, NIR→Northern Ireland, SCT→Scotland
- United States: MA→NorthEast-USA, etc.

### gld_dim_date

Date dimension with calendar attributes.

| Column | Type | Description |
| --- | --- | --- |
| date_id | integer | Date key in yyyyMMdd format (e.g., 20250801) |
| date | date | Date value (primary key) |
| year | integer | Year (e.g., 2025) |
| month_name | string | Month name (e.g., August, September) |
| day_name | string | Day of week (e.g., Monday, Saturday) |
| is_weekend | integer | 1 if Saturday/Sunday, else 0 |
| quarter | integer | Quarter (1-4) |
| week_of_year | integer | ISO week number |
| ingested_at | timestamp | Ingestion timestamp |
| _source_file | string | Source CSV file path |

Row count: **92** (Aug 1 - Oct 31, 2025)

### gld_fact_order_items

Fact table with computed financial metrics and INR currency conversion.

| Column | Type | Description |
| --- | --- | --- |
| date_id | integer | Date key in yyyyMMdd format (joins to gld_dim_date.date_id) |
| transaction_date | date | Transaction date |
| transaction_ts | timestamp | Order timestamp |
| transaction_id | long | Order ID (part of composite key with seq_no) |
| customer_id | string | Customer ID (joins to gld_dim_customers.customer_id) |
| seq_no | integer | Item sequence within order (part of composite key) |
| product_id | long | Product ID (joins to gld_dim_products.product_id) |
| channel | string | Sales channel (Website or Mobile) |
| coupon_code | string | Coupon code (lowercase) or NULL |
| coupon_flag | integer | 1 if coupon_code is not null, else 0 |
| unit_price_currency | string | Original currency code |
| quantity | integer | Quantity ordered |
| unit_price | decimal(18,2) | Unit price in original currency |
| gross_amount | decimal(29,2) | quantity × unit_price |
| discount_percent | decimal(5,2) | Discount percentage (0-100) |
| discount_amount | long | ceil(gross_amount × discount_pct / 100) |
| tax_amount | decimal(18,2) | Tax amount in original currency |
| net_amount | decimal(31,2) | gross_amount - discount_amount + tax_amount |
| net_amount_inr | long | ceil(net_amount × FX rate to INR) |

Load method: **MERGE upsert** on (transaction_id, seq_no)

Row count: **183,378**

**FX Rates Used:**

| Currency | Rate to INR |
| --- | --- |
| INR | 1.00 |
| AED | 24.18 |
| AUD | 57.55 |
| CAD | 62.93 |
| GBP | 117.98 |
| SGD | 68.18 |
| USD | 88.29 |

**Currency Distribution:**

| Currency | Row Count |
| --- | --- |
| INR | 100,505 |
| USD | 22,123 |
| GBP | 18,465 |
| AED | 12,818 |
| SGD | 10,789 |
| CAD | 9,342 |
| AUD | 9,336 |

---

## Entity Relationship Diagram

```
┌──────────────────────┐       ┌──────────────────────────┐
│   gld_dim_products   │       │    gld_dim_customers     │
│──────────────────────│       │──────────────────────────│
│ PK  product_id (long)│       │ PK  customer_id (string) │
│     sku              │       │     phone                │
│     category_code    │       │     country_code         │
│     category_name    │       │     country              │
│     brand_code       │       │     state                │
│     brand_name       │       │     region               │
│     color            │       └──────────────────────────┘
│     size             │                  ▲
│     material         │                  │
│     weight_grams     │                  │
│     length_cm        │                  │
│     width_cm         │                  │
│     height_cm        │                  │
│     rating_count     │                  │
└──────────┬───────────┘                  │
           │                              │
           │ product_id                   │ customer_id
           │                              │
┌──────────┴──────────────────────────────┴──────────┐
│              gld_fact_order_items                   │
│─────────────────────────────────────────────────────│
│ PK  transaction_id (long) + seq_no (integer)       │
│ FK  product_id  → gld_dim_products.product_id      │
│ FK  customer_id  → gld_dim_customers.customer_id   │
│ FK  date_id      → gld_dim_date.date_id            │
│     transaction_date, transaction_ts               │
│     channel, coupon_code, coupon_flag              │
│     unit_price_currency, quantity, unit_price      │
│     gross_amount, discount_percent, discount_amount│
│     tax_amount, net_amount, net_amount_inr         │
└─────────────────────────────────────────────────────┘
           │
           │ date_id
           ▼
┌──────────────────────┐
│     gld_dim_date     │
│──────────────────────│
│ PK  date_id (integer)│
│     date             │
│     year             │
│     month_name       │
│     day_name         │
│     is_weekend       │
│     quarter          │
│     week_of_year     │
└──────────────────────┘
```

### Join Keys Summary

| From (Fact) | To (Dimension) | Join Key |
| --- | --- | --- |
| gld_fact_order_items.product_id | gld_dim_products.product_id | product_id (long) |
| gld_fact_order_items.customer_id | gld_dim_customers.customer_id | customer_id (string) |
| gld_fact_order_items.date_id | gld_dim_date.date_id | date_id (integer, yyyyMMdd) |