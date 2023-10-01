import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col, dense_rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/exBigData/thuchanh_23.8/ex1.csv')

print("Câu 3:")
# Nhóm dữ liệu theo cột "Country" và tính tổng số lượng đơn hàng cho mỗi nước
country_quantity = data.groupBy("Country").agg({"InvoiceNo": "sum"}).withColumnRenamed("sum(InvoiceNo)", "TotalInvoiceNo")

# Sắp xếp theo số lượng đơn hàng giảm dần
window_spec = Window.orderBy(desc("TotalInvoiceNo"))
country_quantity_ranked = country_quantity.withColumn("Rank", dense_rank().over(window_spec))

# Lấy dòng thứ 3 (vị trí 1-indexed) là nước có số lượng đơn hàng nhiều thứ 3
third_largest_country = country_quantity_ranked.filter(col("Rank") == 3).first()

country_name = third_largest_country["Country"]
InvoiceNo = third_largest_country["TotalInvoiceNo"]

print("=> Quốc gia đạt top 3: " + country_name)
print("=> Số lượng hoá đơn: " + str(InvoiceNo))


# Thêm cột rank_name dựa trên xếp hạng
country_quantity_ranked = country_quantity_ranked.withColumn("Rank_Name", col("Rank"))

# Hiển thị kết quả
country_quantity_ranked.show()
