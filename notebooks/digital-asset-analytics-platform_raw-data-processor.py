# Databricks notebook source
# MAGIC %md
# MAGIC # Raw Trade Data Aggregation
# MAGIC   
# MAGIC   **Environment:** Databricks Runtime
# MAGIC  
# MAGIC   **Purpose:** This notebook reads raw trade data from the S3 `raw/` directory, transforms it into 1-minute OHLCV candles, and writes the processed data back to the `processed/` directory in Parquet format.

# COMMAND ----------

# Define the path to your S3 bucket's raw data directory
s3_path = "s3://digital-asset-analytics-data-lake/raw/"

display(dbutils.fs.ls(s3_path))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, DoubleType, BooleanType
# Define the schema for the CSV file according to its documentation
trade_schema = StructType([
    StructField("trade_id", LongType(), True),
    StructField("price", DoubleType(), True),
    StructField("qty", DoubleType(), True),
    StructField("quote_qty", DoubleType(), True),
    StructField("time", LongType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
    StructField("is_best_match", BooleanType(), True)
])


# 1. Define the full path to CSV file in S3
file_path = "s3://digital-asset-analytics-data-lake/raw/BTCUSDT-trades-2025-05.csv"

# Read the CSV file into a Spark DataFrame

raw_trades_df = (spark.read
              .option("header", "false")
              .schema(trade_schema)
              .csv(file_path)
             )


# Chech the first few rows of the DataFrame to verify correctness
print("Successfully loaded data into a Spark DataFrame. Showing the first 10 rows:")
display(raw_trades_df.limit(10))

# Check the schema
print("DataFrame Schema:")
raw_trades_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
# 'time' column is in microseconds, convert to seconds and cast to timestamp
transformed_df = raw_trades_df.withColumn(
    "trade_time",
    (col("time") / 1000000).cast("timestamp")
)

print("Verification: Displaying original 'time' and new 'trade_time' columns.")
display(transformed_df.select("time", "trade_time").limit(10))

print("\nNew Schema with the 'trade_time' column:")
transformed_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import window, col, first, last, max, min, sum

print("Aggregating raw trades into 1-minute OHLCV candles")

# Group the data into 1-minute windows based on the 'trade_time' column. Then, for each window perform aggregations
candles_df = transformed_df.groupBy(
    window(col("trade_time"), "1 minute", "1 minute")
).agg(
    first("price").alias("open"),      # Get the first price in the window for the 'open'
    max("price").alias("high"),       # Get the maximum price in the window for the 'high'
    min("price").alias("low"),        # Get the minimum price in the window for the 'low'
    last("price").alias("close"),       # Get the last price in the window for the 'close'
    sum("qty").alias("volume")        # Sum up all quantities to get the total 'volume'
).orderBy(
    col("window").asc() # Order the results by time ascending
)

# Verify the results
print("Displaying the first 20 1-minute candles:")
display(candles_df.limit(20))

print("\nSchema of new candles_df:")
candles_df.printSchema()

# COMMAND ----------

# Define the output path in S3 bucket.

output_path = "s3://digital-asset-analytics-data-lake/processed/candles/"

print(f"Saving the candle data to: {output_path}")

# Write the DataFrame to S3 in Parquet format

(candles_df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Save complete")

# COMMAND ----------

# Verify the written files in S3
display(dbutils.fs.ls(output_path))

# COMMAND ----------

# --- Final Verification ---

print("Reading the processed Parquet data from S3")
processed_df = spark.read.parquet("s3://digital-asset-analytics-data-lake/processed/candles/")

row_count = processed_df.count()

print("Verification complete.")
print(f"The aggregated 'candles_df' has a total of {row_count:,} rows (1-minute candles).")
print("For a full 31-day month, expected number of rows is 44,640.")

# COMMAND ----------

