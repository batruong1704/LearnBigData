
print("Câu 7: Cho biết chiều cao tối thiểu trung bình và tối đa của mỗi quốc gia tham gia thế vận hội mùa đông và sắp xếp theo thứ tự giảm dần.")
# In ra các trường country, min_height, avg_hight, max_height

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, avg, min, max, round
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/LearnBigData/thuchanh_24.8/vdv_olympics.csv')
data = data.withColumn("Height", data["Height"].cast(IntegerType()))

nhom_quoc_gia = data.where(data["Season"] == "Winter").groupBy("NOC").agg(
    min("Height").alias("min_height"),
    round(avg("Height"), 2).alias("avg_height"),
    max("Height").alias("max_height")
).orderBy(desc("min_height"))
nhom_quoc_gia.show()