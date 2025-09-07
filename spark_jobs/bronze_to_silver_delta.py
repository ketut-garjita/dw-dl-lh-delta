from pyspark.sql import SparkSession, functions as F
spark = (SparkSession.builder.appName("bronze_to_silver")
         .config("spark.hadoop.fs.s3a.endpoint","http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key","admin")
         .config("spark.hadoop.fs.s3a.secret.key","password123")
         .config("spark.hadoop.fs.s3a.path.style.access","true")
         .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

bronze = "s3a://bronze/rides/"
silver = "s3a://silver/rides_delta/"

raw = (spark.read.json(bronze)
       .withColumn("pickup_ts", F.to_timestamp("pickup_ts"))
       .withColumn("dropoff_ts", F.to_timestamp("dropoff_ts"))
       .withColumn("fare", F.col("fare").cast("double"))
       .withColumn("tip", F.col("tip").cast("double"))
       .withColumn("pickup_date", F.to_date("pickup_ts")))

(raw.write.format("delta")
    .mode("append")
    .partitionBy("pickup_date")
    .save(silver))

print('Wrote to', silver)
