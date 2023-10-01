# Cho biết schema của bộ dữ liệu, hiển thị số bản ghi của bộ dữ liệu.
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, dense_rank, count, avg, min, max, round, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/LearnBigData/thuchanh_24.8/qg_noc.csv')
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/LearnBigData/thuchanh_24.8/vdv_olympics.csv')


print("C1: ")
data = data.withColumn("Age", data["Age"].cast(IntegerType()))
data = data.withColumn("Year", data["Year"].cast(IntegerType()))
data = data.withColumn("ID", data["ID"].cast(IntegerType()))
data = data.withColumn("Height", data["Height"].cast(IntegerType()))
data.printSchema()
print(f"=> So ban ghi: {data.count()}")
print("\n========================================================================================")


print("C2: ")
loc_vdv_tk2002 = data.filter(((data["Year"] < 2010) & (data["Year"] >= 2000)) & ((data["NOC"] == "VIE") | (data["NOC"] == "VNM")))
print(f"=> So luong VDV: {loc_vdv_tk2002.count()} " )
print("\n========================================================================================")


print("C3: ")
loc_nam_2016 = data.where(data["Year"] == 2016).groupBy("NOC").agg(count("*").alias("count"))
sap_xep = loc_nam_2016.sort(desc("count"))
sap_xep.show(7)
print("\n========================================================================================")


print("C4: ")
loc_nam = data.groupBy("Year").agg(countDistinct("ID").alias("SoLuongVDV"))
sx = loc_nam.orderBy(col("Year").desc()).limit(5)
sx.createOrReplaceTempView("olympics_data")
result = spark.sql("SELECT sum(SoLuongVDV) as dem FROM olympics_data")
total_sum = result.collect()[0]["dem"]
print(f"=> So luong VDV: {total_sum}\n")
print("\n========================================================================================")


print("C5: ")
loc_tk21 = data.where((data["Year"] >= 2001) & (data["Year"] <= 2100) )
loc_nam = loc_tk21.filter(col("Sex") == "M").groupBy("Year").agg(countDistinct("ID").alias("SoLuong"))
loc_nam.show()
print("\n========================================================================================")


print("C6: ")
nga_1990 = data.filter((data["NOC"] == "RUS") & (data["Year"] >= 1990) & (data["Year"] <= 1999))
dem = nga_1990.withColumnRenamed("Team", "Country").groupBy("Country", "Season").agg(count("ID").alias("number_of_athletes"))
dem.show()
print("\n========================================================================================")


print("C7: ")
nhom_quoc_gia = data.where(data["Season"] == "Winter").groupBy("NOC").agg(
    min("Height").alias("min_height"),
    round(avg("Height"), 2).alias("avg_height"),
    # avg("Height").alias("avg_hight"),
    max("Height").alias("max_height")
).orderBy(desc("min_height"))
nhom_quoc_gia.show()
print("\n========================================================================================")
