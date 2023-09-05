# Đưa ra thông tin về 7 quốc gia có số lượng vận đông viên đông nhất tham gia thế vận hội 2016

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, dense_rank, count
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/LearnBigData/thuchanh_24.8/vdv_olympics.csv')

print("Câu 3: ")
loc_nam_2016 = data.where(data["Year"] == 2016).groupBy("NOC").agg(count("*").alias("count"))
sap_xep = loc_nam_2016.sort(desc("count"))

sap_xep.show(7)