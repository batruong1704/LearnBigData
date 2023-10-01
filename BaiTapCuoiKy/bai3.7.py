import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,to_date,regexp_replace,col,max,year,desc,when,expr,abs, regexp_extract, format_number, asc
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("BigData-Nhom7").getOrCreate()
url = 'C:/PySpark/LearnBigData/Data/Pakistan Top 100 TikTokers.csv'
data = spark.read.format('csv').option('header', 'true').load(url)

data = data.withColumn("Videos", data["Videos"].cast("double"))
data = data.withColumn("Views", when(data["Views"].endswith('B'),(regexp_replace(col("Views"),"[^0-9.]",'').cast("double") *1000000000))
                        .otherwise(regexp_replace(col("Views"), "[^0-9.]", "").cast("double") *1000000))

data = data.orderBy(asc("Videos")).limit(3)

# Tính tỷ lệ Fan/View cho các TikToker này
data = data.withColumn("ratio", expr("Videos / Views").cast("double"))

# Hiển thị danh sách 3 TikToker có số lượng Fan thấp nhất và tỷ lệ Fan/View của họ
result = data.select("UserId", "Name", "Videos", "Views", "ratio").show(truncate=False)