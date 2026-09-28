# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

order_items_schema = StructType([
    StructField("dt",                 StringType(), True),
    StructField("order_ts",           StringType(), True),
    StructField("customer_id",        StringType(), True),
    StructField("order_id",           StringType(), True),
    StructField("item_seq",           StringType(), True),
    StructField("product_id",         StringType(), True),
    StructField("quantity",           StringType(), True),
    StructField("unit_price_currency",StringType(), True),
    StructField("unit_price",         StringType(), True),
    StructField("discount_pct",       StringType(), True),
    StructField("tax_amount",         StringType(), True),
    StructField("channel",            StringType(), True),
    StructField("coupon_code",        StringType(), True),
])

# COMMAND ----------

# Auto Loader: incrementally loads new CSV files from the landing folder
# Each job run processes only files that arrived since the last run
raw_data_path = "/Volumes/ecommerce/source_data/raw_data/order_items/landing/"
checkpoint_path = "/Volumes/ecommerce/source_data/raw_data/_checkpoints/bronze_order_items"
schema_location = "/Volumes/ecommerce/source_data/raw_data/_schemas/bronze_order_items"

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .option("header", "true")
    .option("delimiter", ",")
    .schema(order_items_schema)
    .load(raw_data_path)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("ingested_at", F.current_timestamp())
)

# COMMAND ----------

# Display schema (streaming DataFrame)
df.printSchema()

# COMMAND ----------

# Write to bronze table using Auto Loader with availableNow trigger
# Processes only new files in batch mode, then stops
streaming_query = (df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .outputMode("append")
    .toTable(f"{catalog_name}.bronze.brz_order_items")
)

streaming_query.awaitTermination()

print(f"Bronze load complete. New files processed incrementally.")
spark.sql(f"SELECT count(*) as total_rows FROM {catalog_name}.bronze.brz_order_items").show()