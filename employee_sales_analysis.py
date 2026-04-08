from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder \
    .appName("Employee Sales Analysis") \
    .master("local[*]") \
    .getOrCreate()

emp_df = spark.read.option("header", True).csv("data/employees.csv")
sales_df = spark.read.option("header", True).csv("data/sales.csv")

sales_df = sales_df.withColumn("Amount", col("Amount").cast("int"))

joined_df = emp_df.join(sales_df, on="EmpID", how="inner")

result_df = joined_df.groupBy("EmpID", "Name", "Department") \
    .agg(sum("Amount").alias("TotalSales"))

result_df.show()

result_df.write.mode("overwrite").option("header", True).csv("output/employee_sales")

print("Output saved to output/employee_sales")

spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder \
    .appName("Employee Sales Analysis") \
    .master("local[*]") \
    .getOrCreate()

emp_df = spark.read.option("header", True).csv("data/employees.csv")
sales_df = spark.read.option("header", True).csv("data/sales.csv")

sales_df = sales_df.withColumn("Amount", col("Amount").cast("int"))

joined_df = emp_df.join(sales_df, on="EmpID", how="inner")

result_df = joined_df.groupBy("EmpID", "Name", "Department") \
    .agg(sum("Amount").alias("TotalSales"))

result_df.show()

result_df.write.mode("overwrite").option("header", True).csv("output/employee_sales")

print("Output saved to output/employee_sales")

spark.stop()