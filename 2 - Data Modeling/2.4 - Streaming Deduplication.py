# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

(spark.read
      .table("bronze")
      .filter("topic = 'orders'")
      .count()
)

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = """
order_id STRING, 
order_timestamp Timestamp, 
customer_id STRING, 
quantity BIGINT,
total BIGINT, 
books ARRAY <
      STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>
      >
"""

batch_total = (spark.read
                      .table("bronze")
                      .filter("topic = 'orders'")
                      .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                      .select("v.*")
                      .dropDuplicates(["order_id", "order_timestamp"])
                      .count()
                )

print(batch_total)

# COMMAND ----------

# supprime les duplicates(forcément dans le write stream !): remonte à "max(order_timestamp) (dans le ou les micros-batch) - moins 30 secondes. 
# Les "state informations" au delà sont supprimées
deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'orders'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("order_timestamp", "30 seconds")
                   .dropDuplicates(["order_id", "order_timestamp"]))

# COMMAND ----------

def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    
    sql_query = """
      MERGE INTO orders_silver a
      USING orders_microbatch b
      ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
      WHEN NOT MATCHED THEN INSERT *
    """
    microBatchDF.sparkSession.sql(sql_query)

spark.sql(
"""
CREATE TABLE IF NOT EXISTS orders_silver
(order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)
"""
)
## mode("Append") est le mode par défaut. C'est celui dont on a besoin ici. Si je ne dis pas de bêtise, le cache est égal : dernierti
query = (deduped_df.writeStream
                   .foreachBatch(upsert_data)
                   .option("checkpointLocation", f"{bookstore.checkpoint_path}/orders_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

streaming_total = spark.read.table("orders_silver").count()

print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")
