import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,to_date,regexp_replace,col,max,year,desc,when,expr,abs, regexp_extract
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("BigData-Nhom7").getOrCreate()
url = 'C:/PySpark/LearnBigData/Data/Pakistan Top 100 TikTokers.csv'
data = spark.read.format('csv').option('header', 'true').load(url)

split_col = split(data["Account"], " ")
data = data.withColumn("Date", split_col[1])
data = data.drop("Account")
data = data.withColumn("Date", to_date(data["Date"], "MM/dd/yyyy"))
data.select(data["Name"], year(data["Date"]).alias("Year")).show(data.count(), truncate=False)