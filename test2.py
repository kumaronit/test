from pyspark.sql import SparkSession

from pyspark.sql.types import  *

spark = SparkSession.builder.appName("yourApp") \
		.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
		.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
		.getOrCreate()


spark.sparkContext._jsc\
          .hadoopConfiguration().set("fs.s3a.access.key", "nECGXYrQDtWQKRU2")
spark.sparkContext._jsc\
          .hadoopConfiguration().set("fs.s3a.secret.key", "nlyfAOILXoVfVwdzQiVUsXtsbJNGVnDk")
spark.sparkContext._jsc\
          .hadoopConfiguration().set("fs.s3a.endpoint", "http://192.168.1.8:9000")
spark.sparkContext._jsc\
          .hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc\
          .hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.sparkContext._jsc\
          .hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")
spark.sparkContext._jsc\
         .hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")


df=spark.read.format('parquet').option("inferScema","true").load("s3a://testdataoutput/data_2")

df2=df.select("from_json(value).edit_history_tweet_ids","from_json(value).id","from_json(value).text")
df2.write.format("delta").save("s3a://testdataoutput/data_14")