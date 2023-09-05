# Cho biết số lượng vận động viên tham dự 5 kỳ thế vận hội gần nhất

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, dense_rank, count, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/LearnBigData/thuchanh_24.8/vdv_olympics.csv')
data = data.withColumn("Year", data["Year"].cast(IntegerType()))
data = data.withColumn("ID", data["ID"].cast(IntegerType()))


print("Câu 4: ")
loc_nam = data.groupBy("Year").agg(countDistinct("ID").alias("SoLuongVDV"))
sx = loc_nam.orderBy(col("Year").desc()).limit(5)
sx.show()
# sx.groupBy().sum("SoLuongVDV").show()

## Cách chuyển sang rdd
# total_sum = sx.select("SoLuongVDV").rdd.flatMap(lambda x: x).sum()
# print(total_sum)

## Cách sử dụng SQL
sx.createOrReplaceTempView("olympics_data")
result = spark.sql("SELECT sum(SoLuongVDV) as dem FROM olympics_data")
total_sum = result.collect()[0]["dem"]
print(f"=> Số lượng vận động viên là: {total_sum}\n")

## Đáp án: 