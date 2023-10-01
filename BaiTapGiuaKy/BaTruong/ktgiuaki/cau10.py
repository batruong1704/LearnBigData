print('Câu 10: Liệt kê 10 vận động viên có giành nhiều huy chương vàng nhất thế kỷ 20')

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/PySpark/LearnBigData/BaTruong/ktgiuaki/vdv_olympics.csv')

loc_vdv = data.filter((data["Medal"] == "Gold") & (data["Year"] >= 1900) & (data["Year"] <= 1999))
nhom_dem_vdv = loc_vdv.groupBy("Name").count()

lay_ra_top_10 = nhom_dem_vdv.orderBy(col("count").desc()).limit(10)

lay_ra_top_10.show()
    