# Importing the Required Packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder.appName("RawToBase").getOrCreate()

# The Raw data read for all the Stocks Given
data = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load("s3://stock-analysis-praghadeesh/raw/stock_analysis-main/*.csv")
)

# The Symbol data read 
symbol_data = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load("s3://stock-analysis-praghadeesh/raw/stock_analysis-main/symbol_metadata.csv")
    .withColumnRenamed("Symbol", "stock_symbol")
)

# Filtering only the stocks data
stock_data = data.withColumn("input_file", input_file_name()).where(
    ~col("input_file").like("%symbol%")
)

# Cleaning the Stock Data to get the required Stock Symbol extracted from the file name
stock_data_cleaned = (
    stock_data.withColumn("timestamp", col("timestamp").cast(DateType()))
    .withColumn("open", col("open").cast(DoubleType()))
    .withColumn("high", col("high").cast(DoubleType()))
    .withColumn("low", col("low").cast(DoubleType()))
    .withColumn("close", col("close").cast(DoubleType()))
    .withColumn("volume", col("volume").cast(LongType()))
    .withColumn("stock_symbol_temp", split("input_file", "/")[5])
    .withColumn("stock_symbol", regexp_replace(col("stock_symbol_temp"), "\.csv$", ""))
    .drop("stock_symbol_temp", "input_file")
    .na.fill(0, subset=['open', 'high', 'low', 'close', 'volume']) # Filling the data as 0 if there are any nulls on the subset
)

# DQ Checks
# Data Type Check is redundant here as we have Casted the Data Types explicitly in Previous steps


# Stock Data and Symbol Data is written as Parquet

stock_data_cleaned.write.partitionBy("stock_symbol").format("parquet").mode("overwrite").save("s3://stock-analysis-praghadeesh/base/processed/stock_data")
symbol_data.write.format("parquet").mode("overwrite").save("s3://stock-analysis-praghadeesh/base/processed/symbol_data")
