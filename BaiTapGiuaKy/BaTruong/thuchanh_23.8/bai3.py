import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/exBigData/thuchanh_23.8/ex1.csv')

print("Câu 3:")
# Nhóm dữ liệu theo cột "Country" và tính tổng số lượng đơn hàng cho mỗi nước
country_quantity = data.groupBy("Country").agg({"InvoiceNo": "sum"})

# Sắp xếp theo số lượng đơn hàng giảm dần
country_quantity = country_quantity.sort(desc("sum(InvoiceNo)"))

# Lấy dòng thứ 2 (vị trí 1-indexed) là nước có số lượng đơn hàng nhiều thứ 3
third_largest_country = country_quantity.take(3)[2]

country_name = third_largest_country["Country"]
InvoiceNo = third_largest_country["sum(InvoiceNo)"]

print("Nước có số lượng đơn hàng nhiều thứ 3:", country_name)
print("Số lượng đơn hàng:", InvoiceNo)