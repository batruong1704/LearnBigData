print('Câu 8: Đưa ra tên các quốc gia không có thông tin quốc gia trong qg_noc')

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, avg, min, max, round
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("test").getOrCreate()

data = spark.read.format('csv').option('header', 'true').load('C:/PySpark/LearnBigData/BaTruong/ktgiuaki/qg_noc.csv')
data_vdv = spark.read.format('csv').option('header', 'true').load('C:/PySpark/LearnBigData/BaTruong/ktgiuaki/vdv_olympics.csv')

ketnoi = data_vdv.join(data, on=["NOC"], how="leftanti")
ketqua = ketnoi.select("Team", "NOC").distinct()
ketqua.show(ketqua.count())