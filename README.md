# E-Commerce Sales Analytics Platform

## Project Overview

This project implements a **Medallion Architecture** (Bronze → Silver → Gold) on Databricks for an e-commerce retail business. Raw CSV data is ingested from Unity Catalog volumes, progressively cleaned and enriched across three layers, and made available as analytics-ready gold tables for BI dashboards and ad-hoc analysis.

**Catalog:** `ecommerce`  
**Owner:** `udaydussa58@gmail.com`  
**Last Updated:** September 2026  

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        RAW DATA SOURCES                             │
│                  (UC Volume: /Volumes/ecommerce/)                   │
│  brands.csv │ category.csv │ customers.csv │ date.csv │ products.csv │
│              order_items/*.csv (incremental landing)                │
└──────────┬──────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw Ingestion)                     │
│              Schema: ecommerce.bronze                               │
│  All data ingested as-is with _source_file and ingested_at metadata  │
└──────────┬──────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Cleansing + Typing)                  │
│              Schema: ecommerce.sliver                               │
│  Type casting, trimming, anomaly fixes, deduplication, normalization│
└──────────┬──────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│               GOLD LAYER (Analytics-Ready Tables)                    │
│              Schema: ecommerce.gold                                 │
│  Joined dimensions, computed metrics, FX conversion, region mapping │
└──────────┬──────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────┐
│              DASHBOARD: E-commerce Sales & Customer Analytics        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
E-commerce/
├── 01_set_up                          # Initial setup notebook
├── README.md                          # This file
├── DATA_DICTIONARY.md                 # Full table schema reference
├── medallion_processing_dim/          # Dimension table processing
│   ├── 1_dim_notebook                 # Bronze: ingest dimension CSVs
│   ├── 1_dim_sliver                   # Silver: cleanse + type dimensions
│   └── 1_dim_gold                     # Gold: join dims, add computed columns
├── medallion_processing_fact/         # Fact table processing
│   ├── 1_fact_bronze                  # Bronze: Auto Loader streaming ingestion
│   ├── 2_fact_silver                  # Silver: cleanse, type, MERGE upsert
│   └── 3_fact_gold                    # Gold: compute metrics, FX conversion
└── E-commerce Sales & Customer       # AI/BI Dashboard
    Analytics (dashboard)
```

---

## Raw Data Sources

All raw data is stored in a Unity Catalog volume at `/Volumes/ecommerce/source_data/raw_data/`.

| Source | Path | Load Method |
| --- | --- | --- |
| Brands | `/Volumes/ecommerce/source_data/raw_data/brands/brands.csv` | Batch (spark.read) |
| Category | `/Volumes/ecommerce/source_data/raw_data/category/category.csv` | Batch (spark.read) |
| Customers | `/Volumes/ecommerce/source_data/raw_data/customers/customers.csv` | Batch (spark.read) |
| Date | `/Volumes/ecommerce/source_data/raw_data/date/date.csv` | Batch (spark.read) |
| Products | `/Volumes/ecommerce/source_data/raw_data/products/products.csv` | Batch (spark.read) |
| Order Items | `/Volumes/ecommerce/source_data/raw_data/order_items/landing/order_items_*.csv` | Streaming (Auto Loader) |

---

## Data Pipeline Flow

### Dimension Pipeline (medallion_processing_dim/)

**1_dim_notebook (Bronze)**
- Reads CSV files from UC volumes with explicit schemas
- Adds `_source_file` and `ingested_at` metadata columns
- Writes to `ecommerce.bronze.brz_*` tables (overwrite mode)

**1_dim_sliver (Silver)**
- **Brands:** Trim names, clean brand codes with regex, fix category code anomalies (GROCERY→GRCY, BOOKS→BKS, TOYS→TOY)
- **Category:** Trim codes and names
- **Customers:** Trim IDs, strip `.0` from phone numbers, clean state codes
- **Date:** Parse date string to date type, cast year/quarter/week to integers, normalize day names
- **Products:** Fix material typos (Coton→Cotton, Ruber→Rubber, Alumium→Aluminium), cast numeric fields, handle negative ratings
- **Order Items:** Parse date/timestamp, cast numerics, normalize channels (web→Website, app→Mobile), clean coupon codes, strip `$` and `%`
- Writes to `ecommerce.sliver.slv_*` tables (overwrite mode)

**1_dim_gold (Gold)**
- **gld_dim_products:** Products LEFT JOIN (brands INNER JOIN category) to enrich with brand_name and category_name. Uses `UPPER()` on join keys for case-insensitive matching. COALESCE fallback to 'Not Available'.
- **gld_dim_customers:** Customers joined with a hardcoded country→state→region mapping (India, Australia, UK, USA) to add a `region` column.
- **gld_dim_date:** Date dimension enriched with `date_id` (yyyyMMdd), `month_name`, and `is_weekend` flag.
- Writes to `ecommerce.gold.gld_dim_*` tables (overwrite mode)

### Fact Pipeline (medallion_processing_fact/)

**1_fact_bronze (Bronze)**
- Uses **Auto Loader** (`cloudFiles` format) for incremental CSV ingestion from the landing folder
- Trigger: `availableNow` (batch-style processing of new files only)
- Checkpoint location: `/Volumes/ecommerce/source_data/raw_data/_checkpoints/bronze_order_items`
- Schema location: `/Volumes/ecommerce/source_data/raw_data/_schemas/bronze_order_items`
- Writes to `ecommerce.bronze.brz_order_items` (append mode, streaming)

**2_fact_silver (Silver)**
- Drops duplicates on (order_id, item_seq)
- Type conversions: quantity ("Two"→2), strip `$` and `%`, cast to proper types
- Channel normalization: web→Website, app→Mobile
- Coupon code: lowercase + trim
- Adds `processed_time` timestamp
- Uses **MERGE** (upsert) into `ecommerce.sliver.slv_order_items` on (order_id, item_seq)

**3_fact_gold (Gold)**
- Computes derived financial metrics:
  - `gross_amount` = quantity × unit_price
  - `discount_amount` = ceil(gross_amount × discount_pct / 100)
  - `sale_amount` (net_amount) = gross_amount - discount_amount + tax_amount
- Applies **FX conversion** to INR using fixed rates:

  | Currency | INR Rate |
  | --- | --- |
  | INR | 1.00 |
  | AED | 24.18 |
  | AUD | 57.55 |
  | CAD | 62.93 |
  | GBP | 117.98 |
  | SGD | 68.18 |
  | USD | 88.29 |

  - `net_amount_inr` = ceil(sale_amount × inr_rate)
- Adds `date_id` (yyyyMMdd) and `coupon_flag` (1 if coupon_code is not null, else 0)
- Uses **MERGE** (upsert) into `ecommerce.gold.gld_fact_order_items` on (transaction_id, seq_no)

---

## Gold Tables (Analytics-Ready)

| Table | Type | Description | Row Count |
| --- | --- | --- | --- |
| `ecommerce.gold.gld_dim_products` | Dimension | Products with brand and category names | 50,000 |
| `ecommerce.gold.gld_dim_customers` | Dimension | Customers with mapped regions | ~250K |
| `ecommerce.gold.gld_dim_date` | Dimension | Date dimension with month/weekend flags | 92 |
| `ecommerce.gold.gld_fact_order_items` | Fact | Order line items with computed financials | 183,378 |

### Key Relationships

```
gld_fact_order_items.product_id  →  gld_dim_products.product_id
gld_fact_order_items.customer_id →  gld_dim_customers.customer_id
gld_fact_order_items.date_id     →  gld_dim_date.date_id
```

---

## Data Coverage

- **Date Range:** 2025-08-01 to 2025-10-31
- **Currencies:** INR, USD, GBP, AED, SGD, CAD, AUD
- **Channels:** Website, Mobile (app)
- **Countries:** India, Australia, United Kingdom, United States
- **Product Categories:** Electronics, Apparel, Home & Kitchen, Beauty & Personal Care, Books, Grocery, Toys, Sports & Outdoors
- **Brands:** 50 brands (AcmeTech, NovaWave, Zenith, ByteMax, EcoTone, SteelCraft, HomeNest, etc.)

---

## Known Fixes & Notes

1. **Brand Name Case-Sensitivity Fix (Sep 2026):** The `gld_dim_products` gold table had all brand names showing as 'Not Available' because join keys had case mismatches (products had lowercase brand codes like `acme`, brands had uppercase `ACME`; brands had uppercase category codes like `CE`, category table had lowercase `ce`). Fixed by wrapping join conditions with `UPPER()` in both the brands↔category INNER JOIN and the products↔brands_categories LEFT JOIN.

2. **Material Typos:** Raw product data contained misspellings (Coton, Ruber, Alumium) corrected in the Silver layer.

3. **Quantity Anomaly:** Some order items had quantity spelled as 'Two' instead of numeric 2, fixed in Silver layer.

4. **Category Code Anomalies:** Raw brand data had full-word category codes (GROCERY, BOOKS, TOYS) that didn't match the category dimension codes (GRCY, BKS, TOY), fixed via replacement mapping in Silver layer.

---

## How to Run the Pipeline

Run notebooks in this order:

1. **`01_set_up`** – Initial workspace/catalog setup
2. **`medallion_processing_dim/1_dim_notebook`** – Bronze dimension tables
3. **`medallion_processing_fact/1_fact_bronze`** – Bronze fact table (Auto Loader)
4. **`medallion_processing_dim/1_dim_sliver`** – Silver dimension + fact tables
5. **`medallion_processing_fact/2_fact_silver`** – Silver fact table (MERGE upsert)
6. **`medallion_processing_dim/1_dim_gold`** – Gold dimension tables
7. **`medallion_processing_fact/3_fact_gold`** – Gold fact table (MERGE upsert)

> **Note:** Steps 2-3 can run in parallel. Steps 4-5 can run in parallel. Step 6 requires 4 to be complete. Step 7 requires 5 to be complete.

---

## Tech Stack

- **Platform:** Databricks (Serverless compute)
- **Storage:** Unity Catalog managed Delta tables
- **Ingestion:** Auto Loader (cloudFiles) for streaming order items, batch spark.read for dimensions
- **Processing:** PySpark on Databricks
- **Upsert:** Delta Lake MERGE operations for incremental fact loading
- **BI:** AI/BI Lakeview Dashboard"# Databricks-end-to-end-ecommerce-project" 
