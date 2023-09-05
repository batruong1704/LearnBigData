# Đưa ra tỉ lệ khách hàng không có thông tin
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col,split, explode, count, lower, isnull
spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header','true').load('C:/Users/84336/exBigData/thuchanh_23.8/ex1.csv')

print("Câu 2:")
# Tính tổng số dòng có giá trị cột "CustomerID" là rỗng hoặc thiếu
missing_customer_count = data.filter(data["CustomerID"].isNull() | (data["CustomerID"] == "")).count()

# Tính tổng số dòng trong tất cả các giao dịch
total_transactions = data.count()

# Tính tỉ lệ khách hàng không có thông tin
missing_customer_ratio = (missing_customer_count / total_transactions) * 100

print("Tỉ lệ khách hàng không có thông tin là:", missing_customer_ratio, "%")



