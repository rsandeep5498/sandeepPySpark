from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Employee Data Analysis V2").master("local[*]").getOrCreate()

df = spark.read.csv("data/employees.csv", header=True, inferSchema=True)

print("All Employee Data")
df.show()

print("Employees with salary greater than 60000")
df.filter(col("Salary") > 60000).show()

print("Only Name and Department")
df.select("Name", "Department").show()

print("Employee count by department")
df.groupBy("Department").count().show()

print("Adding Bonus Column")
df_bonus = df.withColumn("Bonus", col("Salary") * 0.10)
df_bonus.show()

print("Top 2 Highest Paid Employees")
df.orderBy(col("Salary").desc()).show(3)

spark.stop()