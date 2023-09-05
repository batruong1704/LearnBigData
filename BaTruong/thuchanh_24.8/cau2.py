# Đưa ra thông tin về các vận động viên Việt Nam tham gia Olympic trong thập kỷ 2000.
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, dense_rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/LearnBigData/thuchanh_24.8/vdv_olympics.csv')

print("Câu 2: ")
loc_vdv_tk2002 = data.filter(((data["Year"] < 2010) & (data["Year"] >= 2000)) & ((data["NOC"] == "VIE") | (data["NOC"] == "VNM")))

# loc_vdv_tk2002.show()
print(loc_vdv_tk2002.count())
