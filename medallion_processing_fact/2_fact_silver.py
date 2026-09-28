# Databricks notebook source
from pyspark.sql.types import StringType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'


# COMMAND ----------

df = spark.table(f'{catalog_name}.bronze.brz_order_items')
df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Transformation: Drop any duplicates
df = df.dropDuplicates(["order_id", "item_seq"])

# Transformation: Convert 'Two' → 2 and cast to Integer
df = df.withColumn(
    "quantity",
    F.when(F.col("quantity") == "Two", 2).otherwise(F.col("quantity")).cast("int")
)

# Transformation : Remove any '$' or other symbols from unit_price, keep only numeric
df = df.withColumn(
    "unit_price",
    F.regexp_replace("unit_price", "[$]", "").cast("double")
)

# Transformation : Remove '%' from discount_pct and cast to double
df = df.withColumn(
    "discount_pct",
    F.regexp_replace("discount_pct", "%", "").cast("double")
)

# Transformation : coupon code processing (convert to lower)
df = df.withColumn(
    "coupon_code", F.lower(F.trim(F.col("coupon_code")))
)

# Transformation : channel processing 
df = df.withColumn(
    "channel",
    F.when(F.col("channel") == "web", "Website")
    .when(F.col("channel") == "app", "Mobile")
    .otherwise(F.col("channel")),
)

# COMMAND ----------


# Transformation: datatype conversions
# 1) Convert dt (string → date)
df = df.withColumn(
    "dt",
    F.to_date("dt", "yyyy-MM-dd")     
)

# 2) Convert order_ts (string → timestamp)
df = df.withColumn(
    "order_ts",
    F.coalesce(
        F.to_timestamp("order_ts", "yyyy-MM-dd HH:mm:ss"),  # matches 2025-08-01 22:53:52
        F.to_timestamp("order_ts", "dd-MM-yyyy HH:mm")      # fallback for 01-08-2025 22:53
    )
)


# 3) Convert item_seq (string → integer)
df = df.withColumn(
    "item_seq",
    F.col("item_seq").cast("int")
)

# 4) Convert tax_amount (string → double, strip non-numeric characters)
df = df.withColumn(
    "tax_amount",
    F.regexp_replace("tax_amount", r"[^0-9.\-]", "").cast("double")
)


#Transformation : Add processed time 
df = df.withColumn(
    "processed_time", F.current_timestamp()
)

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# check the final datatypes
df.printSchema()

# COMMAND ----------

# Align column types with existing silver table schema
df = (df
    .withColumn("order_id", F.col("order_id").cast("long"))
    .withColumn("product_id", F.col("product_id").cast("long"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
    .withColumn("discount_pct", F.col("discount_pct").cast("decimal(5,2)"))
    .withColumn("tax_amount", F.col("tax_amount").cast("decimal(18,2)"))
)

# MERGE into silver table for incremental upsert (handles new + updated records)
from delta.tables import DeltaTable

silver_table = DeltaTable.forName(spark, f"{catalog_name}.sliver.slv_order_items")

(silver_table.alias("target")
    .merge(
        df.alias("source"),
        "target.order_id = source.order_id AND target.item_seq = source.item_seq"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

print("Silver MERGE complete.")
spark.sql(f"SELECT count(*) as total_rows FROM {catalog_name}.sliver.slv_order_items").show()

# COMMAND ----------

