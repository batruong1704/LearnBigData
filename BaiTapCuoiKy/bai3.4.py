import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,to_date,regexp_replace,col,max,year,desc,when,expr,abs, regexp_extract
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("BigData-Nhom7").getOrCreate()
url = 'C:/PySpark/LearnBigData/Data/Pakistan Top 100 TikTokers.csv'
data = spark.read.format('csv').option('header', 'true').load(url)

data = data.withColumn("Fans",(regexp_replace(col("Fans"),"[^0-9.]",'').cast("double") *1000000))

data = data.withColumn("Account",(regexp_replace(col("Account"),"[\\s+0-9./]",'')))

data = data.filter(data["Account"] == "Pakistan").orderBy(data["Fans"].desc()).limit(3).show(truncate = False)