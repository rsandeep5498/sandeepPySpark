import os
import urllib.request
from pyspark.sql import SparkSession

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

url = "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt"
path = os.path.join(data_dir, "test.txt")
urllib.request.urlretrieve(url, path)

spark = (
    SparkSession.builder
    .appName("pyspark")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .getOrCreate()
)

df = spark.read.text("data/test.txt")
df.show()

spark.stop()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()

data = [("Alice", 25), ("Bob", 30)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("employee_practice").master("local[*]").getOrCreate()

data = [
    ("Alice", "HR", 50000),
    ("Bob", "IT", 75000),
    ("Charlie", "IT", 62000),
    ("David", "Finance", 58000),
    ("Eva", "HR", 67000),
    ("Frank", "Finance", 72000)
]

df = spark.createDataFrame(data, ["Name", "Department", "Salary"])

print("All Data")
df.show()

print("Employees with salary greater than 60000")
df.filter(col("Salary") > 60000).show()

print("Only Name and Department")
df.select("Name", "Department").show()

print("Count of employees in each department")
df.groupBy("Department").count().show()

print("Sorted by salary descending")
df.orderBy(col("Salary").desc()).show()

spark.stop()