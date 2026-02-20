from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, to_timestamp, to_date

spark = SparkSession.builder.appName("LogProcessor").getOrCreate()
regex = """^(\S+)\s+(\S+)\s+\[([^\]]+)\]\s+"([A-Z]+)\s+(\S+)\s+HTTP/([0-9.]+)"\s+([0-9]{3})\s+([0-9]+)\s+"([^"]+)"\s+"([^"]+)"\s+"([^"]+)"$"""
df = spark.read.text("data/raw/access.log")

parsed_df = df.select(
    regexp_extract(col("value"), regex, 1).alias("host"),
    regexp_extract(col("value"), regex, 2).alias("userid"),
    regexp_extract(col("value"), regex, 3).alias("datetime_str"),
    regexp_extract(col("value"), regex, 4).alias("method"),
    regexp_extract(col("value"), regex, 5).alias("request"),
    regexp_extract(col("value"), regex, 6).alias("http_version"),
    regexp_extract(col("value"), regex, 7).cast("integer").alias("status"),
    regexp_extract(col("value"), regex, 8).cast("integer").alias("size"),
    regexp_extract(col("value"), regex, 9).alias("other_data"),
    regexp_extract(col("value"), regex, 10).alias("referer"),
    regexp_extract(col("value"), regex, 11).alias("user_agent")
  )
cleaned_df = parsed_df.filter(col("host") != "")
final_df = cleaned_df.withColumn(
    "timestamp", to_timestamp(col("datetime_str"), "dd/MMM/yyyy:HH:mm:ss Z")
).withColumn(
    "date", to_date(col("timestamp"))
).drop("datetime_str")
final_df.printSchema()
final_df.select("host", "timestamp", "method", "status", "size").show(5, truncate=False)

output_dir = "data/processed/logs_parquet"
print(f"Writing data to {output_dir}")
final_df.write.mode("overwrite").partitionBy("date", "status").parquet(output_dir)
print("Processing Complete!")
