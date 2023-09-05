# 2. In ra RDD mới có các giá trị gấp 3 lần giá trị ban đầu.

from pyspark.sql import SparkSession

# Khởi tạo
spark = SparkSession.builder.appName("RDDvidu").getOrCreate()

# Tạo RDD từ danh sách
rdd_from_list = spark.sparkContext.parallelize([1, 2, 3, 4, 2])

# In nội dung của RDD
rdd_contents = rdd_from_list.collect()
print("Nội dung RDD: ", rdd_contents)

# Tăng gấp ba lần giá trị bằng map
tripled_rdd = rdd_from_list.map(lambda x: x * 3)
tripled_contents = tripled_rdd.collect()
print("Các giá trị tăng gấp ba lần: ", tripled_contents)

# Dừng SparkSession
spark.stop()
