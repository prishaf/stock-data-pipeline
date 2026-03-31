from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("stock").getOrCreate()

df = spark.read.option("header", "true").csv("s3://stock-elt-bucket-pri-2026/raw/")

df = df.withColumn("price", col("price").cast("double")) \
       .withColumn("volume", col("volume").cast("long"))

df.write.mode("overwrite").parquet("s3://stock-elt-bucket-pri-2026/processed/")