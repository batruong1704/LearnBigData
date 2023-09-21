from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd

spark = SparkSession.builder.appName("FlightAnalysis").getOrCreate()

file_paths = [
    "C:/PySpark/LearnBigData/Data/Flight_012015.xlsx",
    "C:/PySpark/LearnBigData/Data/Flight_022015.xlsx",
    "C:/PySpark/LearnBigData/Data/Flight_032015.xlsx"
]

dfs = [pd.read_excel(file_path) for file_path in file_paths]
combined_df = pd.concat(dfs, ignore_index=True)

flight_df = spark.createDataFrame(combined_df)

top_airlines = flight_df \
    .filter((col("YEAR") == 2015) & (col("MONTH").isin([1, 2, 3]))) \
    .groupBy("AIRLINE") \
    .agg(count("*").alias("flight_count")) \
    .orderBy(desc("flight_count")) \
    .limit(3)

top_airlines.show()

query = top_airlines.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
