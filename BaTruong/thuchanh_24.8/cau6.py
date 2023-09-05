print(" => Đề bài: Cho biết số lượng vận động viên tại mỗi kỳ thế vận hội trong thập kỷ 1990 của nga. \n (in ra các trường country, season, number_of_athletes)")

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, dense_rank, countDistinct, count
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/LearnBigData/thuchanh_24.8/vdv_olympics.csv')

nga_1990 = data.filter((data["NOC"] == "RUS") & (data["Year"] >= 1990) & (data["Year"] <= 1999))

dem = nga_1990.withColumnRenamed("Team", "Country").groupBy("Country", "Season").agg(count("ID").alias("number_of_athletes"))

dem.show()

