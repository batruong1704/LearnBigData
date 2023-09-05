# Cho biết schema của bộ dữ liệu, hiển thị số bản ghi của bộ dữ liệu.
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, dense_rank, count
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/LearnBigData/thuchanh_24.8/qg_noc.csv')
data_vdv = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/LearnBigData/thuchanh_24.8/vdv_olympics.csv')
print("Câu 3.1: ")

# data1 = data.withColumn("Age", data["Age"].cast(IntegerType()))
# data1 = data.withColumn("Year", data["Year"].cast(IntegerType()))
# data.printSchema()

data_vdv = data_vdv.withColumn("Age", data_vdv["Age"].cast(IntegerType()))
data_vdv = data_vdv.withColumn("Year", data_vdv["Year"].cast(IntegerType()))
data_vdv.printSchema()
print(f"=> Số bản ghi: {data_vdv.count()}")