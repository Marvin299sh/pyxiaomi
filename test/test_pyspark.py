

from pyspark.sql import SparkSession

spark = SparkSession.builder \
.appName("PySpark Tutorial") \
.config("spark.memory.offHeap.enabled", "true") \
.config("spark.memory.offHeap.size", "10g") \
.getOrCreate()

df = spark.read.csv('datacamp_ecommerce.csv', header=True, escape="\"")
df.show(5, 0)

