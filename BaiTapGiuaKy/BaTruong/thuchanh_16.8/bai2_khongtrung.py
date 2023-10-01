# Các phép biến đổi trong spark:
# - Map (2)
# - Filter 
# - Distinct (1)
# 1. In ra RDD mới không có các giá trị trùng nhau.
# 2. In ra RDD mới có các giá trị gấp 3 lần giá trị ban đầu.

from pyspark.sql import SparkSession

# Khởi tạo
spark = SparkSession.builder.appName("RDDvidu").getOrCreate()

# Tạo RDD từ danh sách
rdd_from_list = spark.sparkContext.parallelize([1, 2, 3, 4, 2])

# In nội dung của RDD
rdd_contents = rdd_from_list.collect()
print("Nội dung RDD: ", rdd_contents)

# Lọc các giá trị không trùng nhau
distinct_rdd = rdd_from_list.distinct()
distinct_contents = distinct_rdd.collect()
print("Các giá trị không trùng nhau: ", distinct_contents)

# Dừng SparkSession
spark.stop()

