# A. Setup
#### Environment Variables:
- HADOOP_HOME- C:\hadoop
- JAVA_HOME- C:\java\jdk
- SPARK_HOME- C:\spark\spark-3.3.1-bin-hadoop2
- PYTHONPATH- %SPARK_HOME%\python;%SPARK_HOME%\python\lib\py4j-0.10.9-src;%PYTHONPATH%
- PYSPARK_HOME- C:\Users\84336\AppData\Local\Programs\Python\Python38\python.exe
#### Required Paths:
- %SPARK_HOME%\bin
- %HADOOP_HOME%\bin
- %JAVA_HOME%\bin
#### Check:
- Mở cmd, sau đó gõ:
   ```cmd
   spark-shell
   ```
![image](https://github.com/batruong1704/LearnBigData/assets/142201301/54620c30-a071-468a-b170-2b81500fec6c)
Thoát bằng cách gõ Ctrl + C
- Mở Web UI:
   ```cmd
   pyspark
   ```
![image](https://github.com/batruong1704/LearnBigData/assets/142201301/db95ab18-b667-4424-ae3d-1bc61fb96cb6)
Tại đây sẽ hiển thị web UI của spark, Ctrl rồi click vào link để theo dõi.
- Cài panda:
  ```
  python get-pip.py
  pip install pandas
  pip install openpyxl
  ```
  xong rùi á ❤️❤️❤️
  
# B. Learn BigData
1. **Khởi tạo SparkSession**:
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("MyApp").getOrCreate()
   ```

2. **Đọc dữ liệu từ tệp CSV**:
   ```python
   data = spark.read.format("csv").option("header", "true").load("data.csv")
   ```

3. **Hiển thị cấu trúc của DataFrame**:
   ```python
   data.printSchema()
   ```

4. **Xem một số dòng đầu của DataFrame**:
   ```python
   data.show()
   ```

5. **Lọc dữ liệu**:
   ```python
   filtered_data = data.filter(data["Age"] > 20)
   ```

6. **Nhóm và tổng hợp dữ liệu**:
   ```python
   grouped_data = data.groupBy("Country").agg({"Medal": "count"})
   ```

7. **Sắp xếp dữ liệu**:
   ```python
   sorted_data = data.orderBy("Age", ascending=False)
   ```

8. **Thêm cột mới**:
   ```python
   from pyspark.sql.functions import col
   new_data = data.withColumn("DoubleAge", col("Age") * 2)
   ```

9. **Xóa cột**:
   ```python
   cleaned_data = data.drop("Weight")
   ```

10. **Ghi dữ liệu ra tệp CSV**:
    ```python
    cleaned_data.write.format("csv").option("header", "true").save("cleaned_data.csv")
    ```

11. **Đóng SparkSession**:
    ```python
    spark.stop()
    ```

12. **Lệnh SQL** (chuyển DataFrame sang view SQL và thực hiện truy vấn SQL):
    ```python
    data.createOrReplaceTempView("olympics_data")
    result = spark.sql("SELECT * FROM olympics_data WHERE Age > 25")
    ```

13. **Điều kiện phức tạp (AND, OR, NOT)**:
    ```python
    from pyspark.sql.functions import col
    complex_condition = (col("Age") > 25) & (col("Medal") == "Gold")
    filtered_data = data.filter(complex_condition)
    ```

14. **Tính tổng, trung bình, tối thiểu, tối đa**:
    ```python
    from pyspark.sql.functions import sum, avg, min, max
    total_weight = data.select(sum("Weight")).first()[0]
    average_age = data.select(avg("Age")).first()[0]
    ```

15. **Làm việc với cửa sổ (Window)**:
    ```python
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    window_spec = Window.partitionBy("Country").orderBy(col("Medal").desc())
    ranked_data = data.withColumn("Rank", row_number().over(window_spec))
    ```

16. **Tham chiếu cột bằng biến**:
    ```python
    medal_col = col("Medal")
    filtered_data = data.filter(medal_col == "Gold")
    ```

17. **Chọn các cột nào đó**:
    ```python
    selected_columns = data.select("Name", "Age", "Medal")
    ```

18. **Thực hiện các thao tác dựa trên ngày tháng**:
    ```python
    from pyspark.sql.functions import year, month, dayofmonth
    data_with_year = data.withColumn("Year", year(col("Date")))
    data_with_month = data.withColumn("Month", month(col("Date")))
    ```

19. **Đọc dữ liệu từ RDD**:
    ```python
    rdd = spark.sparkContext.parallelize([(1, "Alice"), (2, "Bob")])
    rdd_data = spark.createDataFrame(rdd, ["ID", "Name"])
    ```


| STT | Lệnh          | Mô tả                                                                                   | Ví dụ                                        |
|-----|---------------|-----------------------------------------------------------------------------------------|----------------------------------------------|
| 1   | `read`        | Đọc dữ liệu từ tệp CSV và tạo DataFrame                                               | `data = spark.read.format("csv").option("header", "true").load("data.csv")` |
| 2   | `show`        | Hiển thị một số dòng đầu của DataFrame                                                 | `data.show()`                                |
| 3   | `filter`      | Lọc dữ liệu dựa trên điều kiện                                                           | `data.filter(col("Age") > 25)`               |
| 4   | `select`      | Chọn các cột dữ liệu để hiển thị                                                         | `data.select("Name", "Age")`                |
| 5   | `distinct`    | Loại bỏ các dòng dữ liệu trùng lặp                                                       | `data.select("Country").distinct()`         |
| 6   | `groupBy`     | Nhóm dữ liệu dựa trên một hoặc nhiều cột và áp dụng hàm tổng hợp (vd: `sum`, `count`)   | `data.groupBy("Country").agg(sum("Age"))`   |
| 7   | `orderBy`     | Sắp xếp dữ liệu dựa trên một hoặc nhiều cột                                               | `data.orderBy(col("Age").asc())`            |
| 8   | `withColumn`  | Thêm cột mới vào DataFrame                                                              | `data.withColumn("Height_m", col("Height") / 100)` |
| 9   | `alias`       | Đặt tên mới cho cột                                                                     | `data.select(col("Age").alias("YearsOld"))` |
| 10  | `count`       | Đếm số dòng dữ liệu                                                                      | `data.count()`                              |
| 11  | `collect`     | Thu thập dữ liệu vào danh sách (không thích hợp cho dữ liệu lớn)                          | `data.select("Country").collect()`          |
| 12  | `map`         | Áp dụng một hàm lên từng dòng dữ liệu và trả về RDD                                      | `data.rdd.map(lambda row: row.Name)`        |
| 13  | `agg`         | Tính toán tổng hợp trên cột dữ liệu                                                      | `data.agg({"Age": "avg", "Height": "max"})` |
| 14  | `lambda`      | Hàm vô danh dùng để xử lý dữ liệu một cách linh hoạt                                   | `data.filter(lambda row: row.Age > 25)`     |
| 15  | `dropDuplicates` | Loại bỏ các dòng trùng lặp dựa trên cột(s) chỉ định                                     | `data.dropDuplicates(["Name", "Age"])`     |
| 16  | `join`        | Kết hợp hai DataFrame dựa trên cột(s) chỉ định                                           | `joined_data = data.join(other_data, on="ID", how="inner")` |
| 17  | `where`       | Lọc dữ liệu dựa trên điều kiện                                                           | `data.where((col("Age") > 25) & (col("Sex") == "M"))` |
| 18  | `sort`        | Sắp xếp dữ liệu dựa trên cột(s) chỉ định                                                  | `data.sort(col("Age").asc())`              |
