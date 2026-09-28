# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, FloatType, IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

cotalog_name= 'ecommerce'

brand_schema = StructType([
  StructField('brand_Code', StringType(), False),
  StructField('brand_Name', StringType(), True),
  StructField('catagory_code', StringType(), True),
])

# COMMAND ----------

raw_data_path ='/Volumes/ecommerce/source_data/raw_data/brands/*.csv'

# COMMAND ----------

df= spark.read.option("header", True).schema(brand_schema).csv(raw_data_path)
df=df.withColumn("_source_file", F.col("_metadata.file_path"))\
    .withColumn("ingested_at", F.current_timestamp())
display(df.limit(5))

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("mergeSchema", "true")\
    .saveAsTable(f"{cotalog_name}.bronze.brz_brands")

# COMMAND ----------

# DBTITLE 1,Category Bronze
# ── Bronze: category ──
category_schema = StructType([
    StructField('category_code', StringType(), False),
    StructField('category_name', StringType(), True),
])

category_path = '/Volumes/ecommerce/source_data/raw_data/category/*.csv'

df_category = spark.read.option("header", True).schema(category_schema).csv(category_path)
df_category = df_category.withColumn("_source_file", F.col("_metadata.file_path")) \
    .withColumn("ingested_at", F.current_timestamp())

display(df_category.limit(5))

df_category.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{cotalog_name}.bronze.brz_category")

# COMMAND ----------

# DBTITLE 1,Customers Bronze
# ── Bronze: customers ──
customers_schema = StructType([
    StructField('customer_id', StringType(), False),
    StructField('phone', StringType(), True),
    StructField('country_code', StringType(), True),
    StructField('country', StringType(), True),
    StructField('state', StringType(), True),
])

customers_path = '/Volumes/ecommerce/source_data/raw_data/customers/*.csv'

df_customers = spark.read.option("header", True).schema(customers_schema).csv(customers_path)
df_customers = df_customers.withColumn("_source_file", F.col("_metadata.file_path")) \
    .withColumn("ingested_at", F.current_timestamp())

display(df_customers.limit(5))

df_customers.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{cotalog_name}.bronze.brz_customers")

# COMMAND ----------

# DBTITLE 1,Date Bronze
# ── Bronze: date ──
date_schema = StructType([
    StructField('date', StringType(), False),
    StructField('year', StringType(), True),
    StructField('day_name', StringType(), True),
    StructField('quarter', StringType(), True),
    StructField('week_of_year', StringType(), True),
])

date_path = '/Volumes/ecommerce/source_data/raw_data/date/*.csv'

df_date = spark.read.option("header", True).schema(date_schema).csv(date_path)
df_date = df_date.withColumn("_source_file", F.col("_metadata.file_path")) \
    .withColumn("ingested_at", F.current_timestamp())

display(df_date.limit(5))

df_date.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{cotalog_name}.bronze.brz_date")

# COMMAND ----------

# DBTITLE 1,Order Items Bronze
# ── Bronze: order_items (CSVs land in landing/ subfolder) ──
order_items_schema = StructType([
    StructField('dt', StringType(), True),
    StructField('order_ts', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('order_id', StringType(), True),
    StructField('item_seq', StringType(), True),
    StructField('product_id', StringType(), True),
    StructField('quantity', StringType(), True),
    StructField('unit_price_currency', StringType(), True),
    StructField('unit_price', StringType(), True),
    StructField('discount_pct', StringType(), True),
    StructField('tax_amount', StringType(), True),
    StructField('channel', StringType(), True),
    StructField('coupon_code', StringType(), True),
])

order_items_path = '/Volumes/ecommerce/source_data/raw_data/order_items/landing/*.csv'

df_order_items = spark.read.option("header", True).schema(order_items_schema).csv(order_items_path)
df_order_items = df_order_items.withColumn("_source_file", F.col("_metadata.file_path")) \
    .withColumn("ingested_at", F.current_timestamp())

display(df_order_items.limit(5))

df_order_items.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{cotalog_name}.bronze.brz_order_items")

# COMMAND ----------

# DBTITLE 1,Products Bronze
# ── Bronze: products ──
products_schema = StructType([
    StructField('product_id', StringType(), False),
    StructField('sku', StringType(), True),
    StructField('category_code', StringType(), True),
    StructField('brand_code', StringType(), True),
    StructField('color', StringType(), True),
    StructField('size', StringType(), True),
    StructField('material', StringType(), True),
    StructField('weight_grams', StringType(), True),
    StructField('length_cm', StringType(), True),
    StructField('width_cm', StringType(), True),
    StructField('height_cm', StringType(), True),
    StructField('rating_count', StringType(), True),
])

products_path = '/Volumes/ecommerce/source_data/raw_data/products/*.csv'

df_products = spark.read.option("header", True).schema(products_schema).csv(products_path)
df_products = df_products.withColumn("_source_file", F.col("_metadata.file_path")) \
    .withColumn("ingested_at", F.current_timestamp())

display(df_products.limit(5))

df_products.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{cotalog_name}.bronze.brz_products")