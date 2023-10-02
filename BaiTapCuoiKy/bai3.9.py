import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("BigData-Nhom7").getOrCreate()
url = 'C:/PySpark/LearnBigData/Data/Pakistan Top 100 TikTokers.csv'
data = spark.read.format('csv').option('header', 'true').load(url)

hienthi=data.withColumn("Type", when(data["Videos"] < 1000, "Type1")
                        .when((data["Videos"] >= 1000) & (data["Videos"] < 2000), "Type2")
                        .otherwise("Type3"))
 
hienthi.show(hienthi.count(), truncate=False)