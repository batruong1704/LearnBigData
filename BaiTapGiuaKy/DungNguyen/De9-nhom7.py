import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,to_date,regexp_replace,col,max,year,desc,when,expr,abs
from pyspark.sql.types import DateType

spark = SparkSession.builder.appName("BigData-Nhom7").getOrCreate()
url = 'C:/PySpark/LearnBigData/Data/Pakistan Top 100 TikTokers.csv'
data = spark.read.format('csv').option('header', 'true').load(url)

data = data.withColumn(
                        "Views",
                        when(data.Views.endswith('B'),(regexp_replace(col("Views"),"[^0-9.]",'').cast("float") *1000000000))
                        .otherwise(regexp_replace(col("Views"), "[^0-9.]", "").cast("float") *1000000)
                    )
data = data.withColumn("Fans",(regexp_replace(col("Fans"),"[^0-9.]",'').cast("float") *1000000))
data = data.withColumn("Fans", data["Fans"].cast("int"))
data = data.withColumn("Views", data["Views"].cast("int"))
data = data.withColumn("Videos", data["Videos"].cast("int"))


print("--------------Câu 3.1-----------------")
#3.1 TikToker có thời gian tham gia sớm nhất
split_col = split(data["Account"], " ")
data = data.withColumn("Country", split_col[0])
data = data.withColumn("Date", split_col[1])
#xóa cột Account
data=data.drop("Account")
#định dạng cột Date có kiểu date
data = data.withColumn("Date", to_date(data["Date"], "MM/dd/yyyy"))
in4 = data.createOrReplaceTempView("ThongtinTikToker")
data.printSchema()
data.show()

result = spark.sql("""
                        SELECT Name, Date
                        FROM ThongtinTikToker
                        WHERE DATEDIFF(current_date(), Date) = (
                            SELECT MAX(DATEDIFF(current_date(), Date)) 
                            FROM ThongtinTikToker
                        )
                 """)
result.show()


print("--------------Câu 3.2-----------------")
#3.2 TikToker có số lượng Fan nhiều nhất

max_fans = data.groupBy("Country").agg(max("Fans").alias("Fan_max"))
data.join(max_fans,data.Fans==max_fans.Fan_max).select("UserId","Name","Fans").show(truncate=False)
print                   ("--------------Câu 3.3-----------------")
#3.3. Liệt kê các năm gia nhập của các Tiktoker?
data.select("UserId","Name", year("Date").alias("Năm gia nhập")).show(data.count(),truncate=False)


print("--------------Câu 3.4-----------------")
#3.4. Liệt kê danh sách 3 Tiktoker hàng đầu của Pakistan theo số lượng Fan?
three_Tiktoker = data.orderBy(desc("Fans")).limit(3)
three_Tiktoker.select("UserId","Name","Fans").show(truncate=False)


print("--------------Câu 3.5-----------------")
#3.5. Liệt kê danh sách 3 Tiktoker hàng đầu của Pakistan theo Tổng số lượng Fan và số lượng View? Trong đó: số lượng Fan nhân hệ số 10
result=data.withColumn("Tổng Fansx10 +View", expr("Fans*10+Views")).orderBy(desc(col("Tổng Fansx10 +View"))).limit(3)
result.select("UserId","Name","Tổng Fansx10 +View").show(truncate=False)

print("--------------Câu 3.6-----------------")
#3.6. Liệt kê tỷ lệ giữa số lượng Fan và số lượng View của 3 Tiktoker có số lượng Fan thấp nhất?
result = data.orderBy(abs(col("Fans"))).limit(3).withColumn("Tỉ lệ Fans/View", expr("Fans / Views").cast("float"))
result.select("UserId","Name","Tỉ lệ Fans/View").show(truncate=False)


print("--------------Câu 3.7-----------------")
#3.7. Liệt kê tỷ lệ giữa số lượng Video và số lượng View của 3 Tiktoker có số lượng View cao nhất?
result = data.orderBy(col("Views").desc()).limit(3).withColumn("Tỉ lệ Videos/Views", expr("Videos/Views").cast("float"))
result.select("UserId","Name","Tỉ lệ Videos/Views").show(data.count(),truncate=False)


#3.8. Tính số lượng View trung bình của tất cả các Tiktoker?
print("--------------Câu 3.8-----------------")
result=data.withColumn("SL View trung bình", expr("Views/Videos").cast("int"))
result.select("UserId","Name","SL View trung bình").show(data.count(),truncate=False)

print("--------------Câu 3.9-----------------")
#3.9. Tạo một cột mới nếu số lượng Video < 1000 điền là “Type1”, 1000 <= Số Video < 2000 điền là “Type2”, còn lại điền là “Type3”
hienthi=data.withColumn(
                    "Type", when(data["Videos"] < 1000, "Type1")
                    .when((data["Videos"] >= 1000) & (data["Videos"] < 2000), "Type2")
                    .otherwise("Type3")
                    )

hienthi.select("UserId","Name","Videos","Type").show(hienthi.count(),truncate=False)
#3.10. Tạo một cột mới điền thông tin tỷ lệ giữa số lượng View và Fan, Video theo công thức Tỷ lệ = Số lượng View/(Số lượng Fan * Số lượng Video)
print("--------------Câu 3.10-----------------")
hienthi=data.withColumn("Tỉ lệ VFV", expr("(Views / Fans) * (1 / Videos)").cast("float"))
hienthi.select("UserId","Name","Tỉ lệ VFV").show(data.count(),truncate=False)


