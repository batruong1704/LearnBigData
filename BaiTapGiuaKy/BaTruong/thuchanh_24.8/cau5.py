# Cho biết số lượng vận động viên nam tại mỗi kỳ thế vận hội trong thế kỷ 21

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, dense_rank, countDistinct
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/LearnBigData/thuchanh_24.8/vdv_olympics.csv')

# loc_tk21 = data.filter((col("Year") >= "2001") & (col("Year") <= "2100"))
loc_tk21 = data.where((data["Year"] >= 2001) & (data["Year"] <= 2100) )
# Nhóm dữ liệu theo năm và giới tính, sau đó tính tổng số lượng vận động viên nam
loc_nam = loc_tk21.filter(col("Sex") == "M").groupBy("Year").agg(countDistinct("ID").alias("SoLuong"))
# print(loc_nam.count(col("SoLuong")))
loc_nam.show()
