import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace,col,desc,when,expr
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("BigData-Nhom7").getOrCreate()
url = 'C:/PySpark/LearnBigData/Data/Pakistan Top 100 TikTokers.csv'
data = spark.read.format('csv').option('header', 'true').load(url)

data = data.withColumn("Fans",(regexp_replace(col("Fans"),"[^0-9.]",'').cast("double") *1000000))
data = data.withColumn("Views", 
                        when(data["Views"].endswith('B'),(regexp_replace(col("Views"),"[^0-9.]",'').cast("double") *1000000000))
                        .otherwise(regexp_replace(col("Views"), "[^0-9.]", "").cast("double") *1000000))
data = data.withColumn("Account",(regexp_replace(col("Account"),"[\\s+0-9./]",'')))

result = data.withColumn("Sum", expr("Fans * 10 + Views")).orderBy(desc(col("Sum"))).limit(3)
# result = result.withColumn("Fans", format_number(col("Fans"), 0))
# result = result.withColumn("Views", format_number(col("Views"), 0))
# result = result.withColumn("Sum", format_number(col("Sum"), 0))

result.select("UserId", "Name", "Fans", "Views", "Sum").show(truncate=False)