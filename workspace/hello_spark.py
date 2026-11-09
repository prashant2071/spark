from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("HelloSpark").getOrCreate()

data = [
    ("Alice", "Engineering", 120000),
    ("Bob", "Engineering", 95000),
    ("Cara", "Marketing", 70000),
    ("Dan", "Marketing", 82000),
    ("Eve", "Sales", 60000),
]
df = spark.createDataFrame(data, ["name", "dept", "salary"])

print("All rows:")
df.show()

print("Average salary by department:")
df.groupBy("dept").agg(F.avg("salary").alias("avg_salary")).orderBy("dept").show()

spark.stop()
