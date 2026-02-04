# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

customers_df = (spark.table("bronze")
                 .filter("topic = 'customers'")
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                 .select("v.*")
                 .filter(F.col("row_status").isin(["insert", "update"])))

display(customers_df
        .orderBy("customer_id", "row_time")
        )

# COMMAND ----------

from pyspark.sql.window import Window

window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())

ranked_df = (customers_df
            .withColumn("rank", F.rank()
            .over(window))
            .filter("rank == 1")
            .drop("rank")
            )
            
display(ranked_df .orderBy("customer_id", "row_time"))

# COMMAND ----------

# MAGIC %skip
# MAGIC # This will throw an exception because non-time-based window operations are not supported on streaming DataFrames.
# MAGIC ranked_df = (spark.readStream
# MAGIC                    .table("bronze")
# MAGIC                    .filter("topic = 'customers'")
# MAGIC                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
# MAGIC                    .select("v.*")
# MAGIC                    .filter(F.col("row_status").isin(["insert", "update"]))
# MAGIC                    .withColumn("rank", F.rank().over(window))
# MAGIC                    .filter("rank == 1")
# MAGIC                    .drop("rank")
# MAGIC              )
# MAGIC
# MAGIC (ranked_df.writeStream
# MAGIC             .option("checkpointLocation", f"{bookstore.checkpoint_path}/ranked")
# MAGIC             .trigger(availableNow=True)
# MAGIC             .format("console")
# MAGIC             .start()
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    
    (microBatchDF.filter(F.col("row_status").isin(["insert", "update"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank == 1")
                 .drop("rank")
                 .createOrReplaceTempView("ranked_updates")
    )
    
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
        ON c.customer_id=r.customer_id
            WHEN MATCHED AND c.row_time < r.row_time
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

df_country_lookup = spark.read.json(f"{bookstore.dataset_path}/country_lookup")
display(df_country_lookup)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver
# MAGIC (customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)

# COMMAND ----------

## toujourfs avec output method = "Append" (celle par défaut)
query = (spark.readStream
                  .table("bronze")
                  .filter("topic = 'customers'")
                  .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                  .select("v.*")
                  .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code") , "inner")
               .writeStream
                  .foreachBatch(batch_upsert)
                  .option("checkpointLocation", f"{bookstore.checkpoint_path}/customers_silver")
                  .trigger(availableNow=True)
                  .start()
          )

query.awaitTermination()

# COMMAND ----------

count = spark.table("customers_silver").count()
expected_count = spark.table("customers_silver").select("customer_id").distinct().count()

assert count == expected_count, "Unit test failed"
print("Unit test passed")
print(count)

# COMMAND ----------



# mieux.. check avec la table bronze
count_distinct_custo_in_bronze = (
    spark.table("bronze")
    .filter("topic = 'customers'")
    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
    .select("v.customer_id")
     .distinct()
    # .count()
)
# display(count_distinct_custo_in_bronze)

assert count_distinct_custo_in_bronze.count() == spark.table("customers_silver").count(), "Unit test failed"
print("Unit test passed")
