import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,to_date,regexp_replace,col,max,year,desc,when,expr,abs, regexp_extract, format_number
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("BigData-Nhom7").getOrCreate()
url = 'C:/PySpark/LearnBigData/Data/Pakistan Top 100 TikTokers.csv'
data = spark.read.format('csv').option('header', 'true').load(url)

data = data.withColumn("Fans",(regexp_replace(col("Fans"),"[^0-9.]",'').cast("double") *1000000))
data = data.withColumn("Views", when(data["Views"].endswith('B'),(regexp_replace(col("Views"),"[^0-9.]",'').cast("double") *1000000000))
                        .otherwise(regexp_replace(col("Views"), "[^0-9.]", "").cast("double") *1000000))

data = data.withColumn("ratio", expr("Fans / Views").cast("double"))

result = data.orderBy(col("ratio")).limit(3)

result.show()