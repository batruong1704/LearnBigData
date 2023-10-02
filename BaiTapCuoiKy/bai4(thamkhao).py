from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    DateType,
    TimestampType,
)
from pyspark.sql.functions import col


def main():
    spark = SparkSession.builder.appName("SparkStreamingFromDirectory").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType(
        [
            StructField("Div", StringType(), True),
            StructField("Time", TimestampType(), True),
            StructField("Date", StringType(), True),
            StructField("HomeTeam", StringType(), True),
            StructField("AwayTeam", StringType(), True),
            StructField("FTHG", IntegerType(), True),
            StructField("FTAG", IntegerType(), True),
            StructField("FTR", StringType(), True),
            StructField("HTHG", IntegerType(), True),
            StructField("HTAG", IntegerType(), True),
            StructField("HTR", StringType(), True),
            StructField("HS", IntegerType(), True),
            StructField("AS", IntegerType(), True),
            StructField("HST", IntegerType(), True),
            StructField("AST", IntegerType(), True),
            StructField("HF", IntegerType(), True),
            StructField("AF", IntegerType(), True),
            StructField("HC", IntegerType(), True),
            StructField("AC", IntegerType(), True),
            StructField("HY", IntegerType(), True),
            StructField("AY", IntegerType(), True),
            StructField("HR", IntegerType(), True),
            StructField("AR", IntegerType(), True),
        ]
    )

    df = (
        spark.readStream.schema(schema)
        .option("header", "true")
        .format("csv")
        .option("path", "C:/PySpark/LearnBigData/data_son")
        .load()
    )

    df.printSchema()

    real_madrid_df = df.filter(
        (col("HomeTeam") == "Real Madrid") | (col("AwayTeam") == "Real Madrid")
    )

    group_df = real_madrid_df.groupBy("Div", "Date").agg(
        {"HY": "sum", "AY": "sum", "HR": "sum", "AR": "sum"}
    )

    query = group_df.writeStream.format("console").outputMode("complete").start()

    query.awaitTermination()


if __name__ == "__main__":
    main()