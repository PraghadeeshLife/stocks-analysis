# Importing the Required Packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime

# Creating SparkSession and naming the app as BaseToReporting
spark = SparkSession.builder.appName("BaseToReporting").getOrCreate()

# Reading the processed data
stock_data_cleaned = spark.read.format("parquet").load("s3://stock-analysis-praghadeesh/base/processed/stock_data")
symbol_data = spark.read.format("parquet").load("s3://stock-analysis-praghadeesh/base/processed/symbol_data")

# Broadcasting the Smaller DF - Symbol Data so that the data is stored in all the worker nodes and reduces shuffle operation across nodes reducing Network I/O
joined_data = (
    stock_data_cleaned.join(broadcast(symbol_data), on="stock_symbol", how="left")
)

# Persisting the dataframe so that the same transformations doesn't need to be re executed everytime an operation is performed on the DF
joined_data.persist()

joined_data.count()

# Function to Calculate the All time summary
def all_time_summary_df():
      ats_df = joined_data.groupBy("Sector").agg(
                    avg("open").alias("AVG_OPEN_PRICE"),
                    avg("close").alias("AVG_CLOSE_PRICE"),
                    max("high").alias("MAX_HIGH_PRICE"),
                    min("low").alias("MIN_LOW_PRICE"),
                    avg("volume").alias("AVG_VOLUME"),
                )
      return ats_df
  
# Function to Calculate the summary for specified time
def specific_time_summary_df(start_date, end_date, sectors_interested):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    sts_df = joined_data.filter(
                (col("timestamp") > start_date)
                 & (col("timestamp") < end_date)
                & col("Sector").isin(sectors_interested)
                )
    
    sts_grouped = sts_df.groupBy("Sector").agg(
                              avg("open").alias("AVG_OPEN_PRICE"),
                              avg("close").alias("AVG_CLOSE_PRICE"),
                              max("high").alias("MAX_HIGH_PRICE"),
                              min("low").alias("MIN_LOW_PRICE"),
                              avg("volume").alias("AVG_VOLUME"),
                            )
    return sts_grouped
  
# Function to Calculate the detailed summary for specified time  
def specific_time_detailed_df(start_date, end_date, sectors_interested):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    std_df = joined_data.filter(
                        (col("timestamp") > start_date)
                        & (col("timestamp") < end_date)
                        & col("Sector").isin(sectors_interested)
                    )

    std_grouped = std_df.groupBy("stock_symbol", "Name", "Sector").agg(
                        avg("open").alias("AVG_OPEN_PRICE"),
                        avg("close").alias("AVG_CLOSE_PRICE"),
                        max("high").alias("MAX_HIGH_PRICE"),
                        min("low").alias("MIN_LOW_PRICE"),
                        avg("volume").alias("AVG_VOLUME"),
                    )
    return std_grouped


# All the Data is then written as parquet in the reporting directory
all_time_summary_df().write.format("parquet").mode("overwrite").save("s3://stock-analysis-praghadeesh/reporting/all_time_summary")

# Specific Time Summary DF
start_date = "2021-01-01"
end_date = "2021-05-26"
sectors_interested = ["FINANCE", "TECHNOLOGY"]
specific_time_summary_df(start_date, end_date, sectors_interested).write.format("parquet").mode("overwrite").save("s3://stock-analysis-praghadeesh/reporting/specific_time_summary")

# Specific Time Detailed DF
start_date = "2021-01-01"
end_date = "2021-05-26"
sectors_interested = ["TECHNOLOGY"]
specific_time_detailed_df(start_date, end_date, sectors_interested).write.format("parquet").mode("overwrite").save("s3://stock-analysis-praghadeesh/reporting/specific_time_detailed")
