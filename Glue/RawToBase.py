# Importing the Required Packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import great_expectations as ge

# Creating the SparkSession and setting the app name
spark = SparkSession.builder.appName("RawToBase").getOrCreate()

# The Raw data is read as it is and later filtered to have only the stock data
data = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load("s3://stock-analysis-praghadeesh/raw/stock_analysis-main/*.csv")
)

# The Symbol data is read directly with the full path
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
# Casting the Datatypes appropritely as all the columns are String
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
# Checking for Mandatory Columns in both the Dataframes
stock_data_ge = ge.dataset.SparkDFDataset(stock_data_cleaned)
symbol_data_ge = ge.dataset.SparkDFDataset(symbol_data)

# Stock Data Check
MANDATORY_COLUMNS = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'stock_symbol']
for column in MANDATORY_COLUMNS:
    assert stock_data_ge.expect_column_to_exist(column).success, f"Mandatory column {column} does not exist: FAILED"
    print(f"STOCK DATA : Column {column} exists : PASSED")

# Symbol Data Check
MANDATORY_COLUMNS = ['stock_symbol', 'Name', 'Country', 'Sector', 'Industry', 'Address']
for column in MANDATORY_COLUMNS:
    assert symbol_data_ge.expect_column_to_exist(column).success, f"Mandatory column {column} does not exist: FAILED"
    print(f"SYMBOL DATA : Column {column} exists : PASSED")
    
# Data Type Check and Nulls is redundant here as we have Casted the Data Types explicitly and filled the na as 0s (there weren't any nulls) in Previous steps


# Stock Data and Symbol Data is written as Parquet
# Stock Data is partitioned by stock_symbol, for faster querying and aggregations on the table at a later point of time
stock_data_cleaned.write.partitionBy("stock_symbol").format("parquet").mode("overwrite").save("s3://stock-analysis-praghadeesh/base/processed/stock_data")
symbol_data.write.format("parquet").mode("overwrite").save("s3://stock-analysis-praghadeesh/base/processed/symbol_data")
