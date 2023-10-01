# Từ nào xuất hiện ít nhất trong phần Description?
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, explode, split, lower, col, count

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/exBigData/thuchanh_23.8/ex1.csv')

print("Câu 4:")
# words = data.select(explode(split(lower(col("Description")), r'\W+')).alias("word"))

# dem_tu = words.groupBy("word").count().sort("count")

# print("Các từ xuất hiện ít nhất trong Description:")
# for row in dem_tu.select("word").collect():
#     print(row[0])

split_df = data.select(explode(split(lower(col("Description")), r'\W+')).alias("word"))
word_counts = split_df.groupBy("word").agg(count("*").alias("count"))
min_count = word_counts.agg({"count":"min"}).collect()[0]["min(count)"]
min_words = word_counts.filter(col("count") == min_count).select("word").collect()
total_min_words = len(min_words)

print("- Từ có số lần xuất hiện ít nhất là: ")
for word in min_words:
  print("=>  " + word[0])

print("- Số lần xuất hiện của từ có số lần xuất hiện ít nhất là: ", min_count)
print("- Số Từ có số lần xuất hiện ít nhất là: ", total_min_words)