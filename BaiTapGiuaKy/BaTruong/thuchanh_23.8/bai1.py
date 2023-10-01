# Có tổng bao nhiêu giao dịch, sản phẩm và khách hàng khác nhau?
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, year, sum, lit, when, min, avg, max, col,split, explode, count, lower
spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header','true').load('C:/Users/84336/exBigData/thuchanh_23.8/ex1.csv')

print("Câu 1:")
total_transactions = data.select("InvoiceNo").distinct().count()
print("Tổng số giao dịch là:", total_transactions)
total_products = data.select("StockCode").distinct().count()
print("Tổng số sản phẩm là:", total_products)
total_customers = data.select("CustomerID").distinct().count()
print("Tổng số khách hàng là:", total_customers)