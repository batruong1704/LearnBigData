from pyspark.sql import SparkSession

# Khởi tạo
spark = SparkSession.builder.appName("RDDvidu").getOrCreate()

# Tạo RDD từ danh sách
rdd_from_list = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# In nội dung của RDD
rdd_contents = rdd_from_list.collect()
print("Nội dung RDD: ", rdd_contents)

even_rdd = rdd_from_list.filter(lambda x: x % 2 == 0)
even_rdd.foreach(lambda x:print(x))

# Dừng SparkSession
spark.stop()
