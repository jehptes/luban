# Databricks notebook source
# MAGIC %md
# MAGIC Luban_ETH_Processing to Datalake

# COMMAND ----------

# Accessing FileStore
data_path = "/FileStore/ethereum_data/block_tracker.json"

# Read the JSON file into a DataFrame
df = spark.read.json(f"dbfs:{data_path}")
# display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW ethereum.eth_gas_processed
# MAGIC     AS 
# MAGIC     SELECT ts.*,  
# MAGIC            b.timestamp as block_timestamp,
# MAGIC            DATE(b.timestamp) as block_date,
# MAGIC            HOUR(b.timestamp) as block_hour,
# MAGIC            MINUTE(b.timestamp) as block_minute,
# MAGIC             CASE 
# MAGIC                 WHEN gas <= 20000 THEN 'Standard gas (≤20k)'
# MAGIC                 WHEN gas BETWEEN 20001 AND 100000 THEN 'Simple gas (20k-100k)'
# MAGIC                 WHEN gas BETWEEN 100001 AND 300000 THEN 'Complex gas (100k-300k)'
# MAGIC                 ELSE 'Very Complex gas (>300k)'
# MAGIC             END as gas_category_str,
# MAGIC
# MAGIC             CASE 
# MAGIC                 WHEN gas <= 20000 THEN '1'
# MAGIC                 WHEN gas BETWEEN 20001 AND 100000 THEN '2'
# MAGIC                 WHEN gas BETWEEN 100001 AND 300000 THEN '3'
# MAGIC                 ELSE '4'
# MAGIC             END as gas_category_int
# MAGIC
# MAGIC         FROM ethereum.transactions ts
# MAGIC         JOIN ethereum.blocks b ON ts.block_number = b.block_number;
# MAGIC

# COMMAND ----------

# Prepare data for datalake write
query = "SELECT * FROM ethereum.eth_gas_processed"

eth_transactions_processed_df = spark.sql(query)

# COMMAND ----------

# write to datalake 

adls_path= 'abfss://ethereum@lubandatalake.dfs.core.windows.net/'

eth_transactions_processed_df.write \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .format("delta") \
            .save(adls_path)