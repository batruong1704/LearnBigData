from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField, StructType)

from pyspark.sql.functions import col, count, desc
from pyspark import SparkConf

def main():
    spark = SparkSession.builder.appName("SparkStreamingFromDirectory").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType(
        [
            StructField("YEAR", IntegerType(), True),
            StructField("MONTH", IntegerType(), True),
            StructField("DAY", IntegerType(), True),
            StructField("DAY_OF_WEEK", IntegerType(), True),
            StructField("AIRLINE", StringType(), True),
            StructField("FLIGHT_NUMBER", IntegerType(), True),
            StructField("TAIL_NUMBER", StringType(), True),
            StructField("ORIGIN_AIRPORT", StringType(), True),
            StructField("DESTINATION_AIRPORT", StringType(), True),
            StructField("SCHEDULED_DEPARTURE", IntegerType(), True),
            StructField("DEPARTURE_TIME", IntegerType(), True),
            StructField("DEPARTURE_DELAY", IntegerType(), True),
            StructField("TAXI_OUT", IntegerType(), True),
            StructField("WHEELS_OFF", IntegerType(), True),
            StructField("SCHEDULED_TIME", IntegerType(), True),
            StructField("ELAPSED_TIME", IntegerType(), True),
            StructField("AIR_TIME", IntegerType(), True),
            StructField("DISTANCE", IntegerType(), True),
            StructField("WHEELS_ON", IntegerType(), True),
            StructField("TAXI_IN", IntegerType(), True),
            StructField("SCHEDULED_ARRIVAL", IntegerType(), True),
            StructField("ARRIVAL_TIME", IntegerType(), True),
            StructField("ARRIVAL_DELAY", IntegerType(), True), 
            StructField("DIVERTED", IntegerType(), True),
            StructField("CANCELLED", StringType(), True),
            StructField("CANCELLATION_REASON", IntegerType(), True),
            StructField("AIR_SYSTEM_DELAY", IntegerType(), True),
            StructField("SECURITY_DELAY", IntegerType(), True),
            StructField("AIRLINE_DELAY", IntegerType(), True),
            StructField("LATE_AIRCRAFT_DELAY", IntegerType(), True),
            StructField("WEATHER_DELAY", IntegerType(), True),
            StructField("AIR_SYSTEM_DELAY", IntegerType(), True),
            StructField("SECURITY_DELAY", IntegerType(), True),
        ]
    )

    df = (
        spark.readStream.schema(schema)
        .option("header", "true")
        .format("csv")
        .option("path", "C:/PySpark/LearnBigData/DataStreaming")
        .load()
    )

    df.printSchema()

    top_airlines = df \
        .filter((col("YEAR") == 2015) & (col("MONTH").isin([1, 2, 3]))) \
        .groupBy("AIRLINE") \
        .agg(count("*").alias("flight_count")) \
        .orderBy(desc("flight_count")) \
        .limit(3)

    query = top_airlines.writeStream.format("console").outputMode("complete").start()

    query.awaitTermination()    


if __name__ == "__main__":
    main()