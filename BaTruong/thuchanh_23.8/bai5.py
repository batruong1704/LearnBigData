# Sản phẩm nào bán được số lượng (Quantity) lớn nhất ở United Kingdom?
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("test").getOrCreate()
data = spark.read.format('csv').option('header', 'true').load('C:/Users/84336/exBigData/thuchanh_23.8/ex1.csv')

print("Câu 5:")
loc_quoc_gia = data.filter(data["Country"] == "United Kingdom")
# Nhóm dữ liệu theo cột "Country" và tính tổng số lượng đơn hàng cho mỗi nước
loc_san_pham = loc_quoc_gia.groupBy("StockCode").agg({"Quantity": "sum"})
# Sắp xếp theo số lượng đơn hàng giảm dần
country_quantity = loc_san_pham.sort(desc("sum(Quantity)"))

largest_country = country_quantity.take(1)[0]
# country_quantity.show()
Quantity = largest_country["sum(Quantity)"]
Description = largest_country["StockCode"]

print("=> Mã Sản Phẩm:", Description)
print("=> Tổng: ", Quantity)