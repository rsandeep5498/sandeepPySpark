from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

spark = (
    SparkSession.builder
    .appName("Sales Data Analysis")
    .master("local[*]")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)

print("All Sales Data")
df.show()

print("Sales greater than 500")
df.filter(col("Sales") > 500).show()

print("Total sales by region")
df.groupBy("Region").agg(sum("Sales").alias("Total_Sales")).show()

print("Average sales by product")
df.groupBy("Product").agg(avg("Sales").alias("Average_Sales")).show()

print("Top 3 sales records")
df.orderBy(col("Sales").desc()).show(3)

spark.stop()