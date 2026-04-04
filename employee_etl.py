from pyspark.sql.functions import col

df = spark.read.option("header", True).csv("/Volumes/workspace/default/myfiles/employees.csv")

df = df.withColumn("Salary", col("Salary").cast("int"))

df_filtered = df.filter(col("Salary") > 60000)

df_final = df_filtered.withColumn("Bonus", col("Salary") * 0.10)

df_final.write.mode("overwrite").saveAsTable("high_salary_employees")