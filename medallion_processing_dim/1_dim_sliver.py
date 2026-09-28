# Databricks notebook source
# DBTITLE 1,pre requirment
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, FloatType, IntegerType
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,volume assigning
catalog_name = 'ecommerce'

df_bronze= spark.table(f'{catalog_name}.bronze.brz_brands')

df_bronze.show()

# COMMAND ----------

# DBTITLE 1,brand df
df_sliver = df_bronze.withColumn('brand_Name', F.trim(F.col('brand_Name')))
display(df_sliver)

# COMMAND ----------

# DBTITLE 1,regexp of brand name and code
df_sliver = df_sliver.withColumn('brand_Code', F.regexp_replace(F.col("brand_Code"), r'[^A-Za-z0-9]', ''))

# COMMAND ----------

# DBTITLE 1,dispaly
display(df_sliver.limit(7))

# COMMAND ----------

# DBTITLE 1,distinct
display(df_sliver.select("catagory_code").distinct())

# COMMAND ----------

# DBTITLE 1,anomalies remove
anomalies = {
    "GROCERY" : "GRCY",
    "BOOKS" : "BKS",
    "TOYS" : "TOY"
}
df_sliver= df_sliver.replace(anomalies, subset="catagory_code")
display(df_sliver)

# COMMAND ----------

# DBTITLE 1,sliver: brand
df_sliver.write.format('delta').mode('overwrite').option('mergeSchema','true').saveAsTable(f'{catalog_name}.sliver.slv_brands')

# COMMAND ----------

# DBTITLE 1,Silver: Category
# ── Silver: category ──
df_bronze_cat = spark.table(f'{catalog_name}.bronze.brz_category')

df_sliver_cat = (df_bronze_cat
    .withColumn('category_code', F.trim(F.col('category_code')))
    .withColumn('category_name', F.trim(F.col('category_name')))
    .dropDuplicates(['category_code', 'category_name'])
)

display(df_sliver_cat)

df_sliver_cat.write.format('delta').mode('overwrite').option('mergeSchema', 'true') \
    .saveAsTable(f'{catalog_name}.sliver.slv_category')

# COMMAND ----------

# DBTITLE 1,Silver: Customers
# ── Silver: customers ──
df_bronze_cust = spark.table(f'{catalog_name}.bronze.brz_customers')

df_sliver_cust = (df_bronze_cust
    .withColumn('customer_id', F.trim(F.col('customer_id')))
    .withColumn('phone', F.regexp_replace(F.col('phone'), r'\.0$', ''))   # strip .0 suffix
    .withColumn('phone', F.trim(F.col('phone')))
    .withColumn('country_code', F.trim(F.col('country_code')))
    .withColumn('country', F.trim(F.col('country')))
    .withColumn('state', F.trim(F.col('state')))
    .filter(F.col('customer_id').isNotNull())                          # drop rows with null customer_id
)

display(df_sliver_cust.limit(5))

df_sliver_cust.write.format('delta').mode('overwrite').option('mergeSchema', 'true') \
    .saveAsTable(f'{catalog_name}.sliver.slv_customers')

# COMMAND ----------

# DBTITLE 1,Silver: Date
# ── Silver: date ──
df_bronze_date = spark.table(f'{catalog_name}.bronze.brz_date')

df_sliver_date = (df_bronze_date
    .withColumn('date', F.to_date(F.col('date'), 'dd-MM-yyyy'))
    .withColumn('year', F.col('year').cast(IntegerType()))
    .withColumn('day_name', F.initcap(F.lower(F.col('day_name'))))     # standardize casing
    .withColumn('quarter', F.col('quarter').cast(IntegerType()))
    .withColumn('week_of_year', F.abs(F.col('week_of_year').cast(IntegerType())))  # fix negative values
    .dropDuplicates(['date'])  # remove 3 duplicate dates from bronze source
)

display(df_sliver_date.limit(5))

df_sliver_date.write.format('delta').mode('overwrite').option('mergeSchema', 'true') \
    .saveAsTable(f'{catalog_name}.sliver.slv_date')

# COMMAND ----------

# DBTITLE 1,Silver: Order Items
# ── Silver: order_items ──
df_bronze_oi = spark.table(f'{catalog_name}.bronze.brz_order_items')

df_sliver_oi = (df_bronze_oi
    .withColumn('dt', F.to_date(F.col('dt'), 'yyyy-MM-dd'))
    .withColumn('order_ts', F.to_timestamp(F.col('order_ts'), 'yyyy-MM-dd HH:mm:ss'))
    .withColumn('order_id', F.col('order_id').cast('long'))
    .withColumn('item_seq', F.col('item_seq').cast('int'))
    .withColumn('product_id', F.col('product_id').cast('long'))
    .withColumn('quantity', F.regexp_replace(F.col('quantity'), 'Two', '2').cast('int'))
    .withColumn('unit_price', F.regexp_replace(F.col('unit_price'), '\\$', '').cast('decimal(18,2)'))
    .withColumn('discount_pct', F.regexp_replace(F.col('discount_pct'), '%', '').cast('decimal(5,2)'))
    .withColumn('tax_amount', F.col('tax_amount').cast('decimal(18,2)'))
)

display(df_sliver_oi.limit(5))

df_sliver_oi.write.format('delta').mode('overwrite').option('mergeSchema', 'true') \
    .saveAsTable(f'{catalog_name}.sliver.slv_order_items')

# COMMAND ----------

# DBTITLE 1,Silver: Products
# ── Silver: products ──
df_bronze_prod = spark.table(f'{catalog_name}.bronze.brz_products')

material_corrections = {
    'Coton': 'Cotton',
    'Ruber': 'Rubber',
    'Alumium': 'Aluminium'
}

df_sliver_prod = (df_bronze_prod
    .replace(material_corrections, subset='material')
    .withColumn('product_id', F.col('product_id').cast('long'))
    .withColumn('weight_grams', F.regexp_replace(F.col('weight_grams'), 'g', '').cast('int'))
    .withColumn('length_cm', F.regexp_replace(F.col('length_cm'), ',', '.').cast('float'))
    .withColumn('width_cm', F.col('width_cm').cast('float'))
    .withColumn('height_cm', F.col('height_cm').cast('float'))
    .withColumn('rating_count', F.when(F.col('rating_count').cast('int') < 0, F.lit(0)).otherwise(F.col('rating_count').cast('int')))
)

display(df_sliver_prod.limit(5))

df_sliver_prod.write.format('delta').mode('overwrite').option('mergeSchema', 'true') \
    .saveAsTable(f'{catalog_name}.sliver.slv_products')