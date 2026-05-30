#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install kafka-python')


# In[2]:


# Import dan setup Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lit, to_timestamp,
    window, count, sum as spark_sum, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)

# Buat Spark Session dengan koneksi ke Kafka
spark = SparkSession.builder \
    .appName("TransactionStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session siap!")


# In[3]:


# Define Schema 
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", LongType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True)
])


# In[4]:


# Baca dari Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON dari Kafka
df_parsed = df_raw.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

print("Stream dari Kafka siap dibaca!")


# In[ ]:


# Cell 5 - Semua proses dalam satu foreachBatch
from pyspark.sql.functions import col, when, to_timestamp, current_timestamp, window, count
from datetime import datetime
valid_sources = ["mobile", "web", "pos"]

def process_batch(df, epoch_id):
    # Tambah kolom event_time
    df = df.withColumn(
        "event_time",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # Validasi
    df = df.withColumn(
        "error_reason",
        when(col("user_id").isNull(), "MISSING_USER_ID")
        .when(col("amount").isNull(), "MISSING_AMOUNT")
        .when(col("timestamp").isNull(), "MISSING_TIMESTAMP")
        .when(col("amount") < 1, "AMOUNT_TOO_SMALL")
        .when(col("amount") > 10000000, "AMOUNT_TOO_LARGE")
        .when(~col("source").isin(valid_sources), "INVALID_SOURCE")
        .when(col("event_time").isNull(), "INVALID_TIMESTAMP_FORMAT")
        .otherwise(None)
    ).withColumn(
        "is_valid",
        when(col("error_reason").isNull(), True).otherwise(False)
    )

    # Pisah valid dan invalid
    df_valid = df.filter(col("is_valid") == True)
    df_invalid = df.filter(col("is_valid") == False)

    # Kirim ke Kafka
    df_valid.selectExpr("to_json(struct(*)) AS value") \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "transactions_valid") \
        .save()

    df_invalid.selectExpr("to_json(struct(*)) AS value") \
        .write.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "transactions_dlq") \
        .save()

    # Output console dengan running_total
    valid_count = df_valid.count()
    print(f"=== Batch {epoch_id} ===")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Valid: {valid_count} | Invalid: {df_invalid.count()}")

    # Tumbling window per 1 menit
    df_valid.groupBy(
        window(col("event_time"), "1 minute")
    ).agg(
        count("*").alias("running_total")
    ).selectExpr(
        "current_timestamp() as timestamp",
        "running_total"
    ).show(truncate=False)

# Jalankan streaming
query = df_parsed.writeStream \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()


# In[ ]:




