from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# --- 1. SCHEMA DEFINITION ---
# Defining schema explicitly is faster and safer than schema inference.
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("is_fraud", BooleanType(), True)
])

# --- 2. SPARK SESSION WITH MEMORY CONSTRAINTS ---
# CRITICAL FOR CODESPACES: We limit driver memory to 1GB to prevent OOM crashes.

spark = SparkSession.builder \
    .appName("FintechStreamProcessor") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.memory", "1g") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 3. READ STREAM FROM KAFKA ---
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the binary Kafka value (which is JSON) into our structured columns
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), transaction_schema).alias("data")
).select("data.*")

# --- 4. FOREACHBATCH PROCESSOR ---
def process_batch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return
        
    print(f"Processing Batch ID: {batch_id} | Count: {count}")

    # --- HOT PATH: Cassandra (Real-time Fraud) ---
    # Why? Cassandra is optimized for fast, low-latency point queries. 
    # A fraud detection API will query this to check a specific transaction_id instantly.
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .options(table="transactions", keyspace="fintech") \
        .save()

    # --- COLD PATH: Delta Lake (Batch Analytics) ---
    # Why? Delta Lake provides ACID transactions, time travel, and is optimized for large-scale analytical scans.
    batch_df.write \
        .format("delta") \
        .mode("append") \
        .save("/workspaces/fintech-realtime-transaction-lakehouse/data/delta/transactions")
    
    print(f"Successfully wrote {count} records to Cassandra and Delta Lake.")
# --- 5. START THE STREAM ---
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "src/streaming/checkpoints/") \
    .start()

query.awaitTermination()
