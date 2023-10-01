print('Câu 9: Liệt kê danh sách 10 quốc gia giành nhiều huy chương vàng nhất thế vận hội mùa hè 2012')

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/PySpark/LearnBigData/BaTruong/ktgiuaki/vdv_olympics.csv')

loc_quoc_gia = data.filter((data["Season"]=="Summer") & (data["Year"]=="2012") & (data["Medal"] == "Gold"))

nhom_dem_quoc_gia = loc_quoc_gia.groupBy("NOC").count()

lay_ra_top_10 = nhom_dem_quoc_gia.orderBy(col("count").desc()).limit(10)

lay_ra_top_10.show()
