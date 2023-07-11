from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext

KAFKA_TOPIC_NAME = "test"
KAFKA_BOOTSTRAP_SERVER = "localhost:9093"

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

sampleDataframe = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC_NAME)  \
        .option("startingOffsets", "latest") \
        .load() \

#sampleDataframe.printSchema()
personStringDF = sampleDataframe.selectExpr("CAST(value AS STRING)")

#schema = StructType([StructField("fruit",StringType()),StructField("size",StringType()),StructField("color",StringType())])
schema=StructType() \
        .add("edit_history_tweet_ids", StringType(),True) \
        .add("id",StringType(), True) \
        .add("text",StringType(),True) \
        .add("invoiceTime",TimestampType(),True)

personDF = personStringDF.select(from_json(col("value"),schema)).alias('data').select("data.*")
#newdf=personDF.select("id")

#personDF.writeStream.format("console").trigger(processingTime="5 seconds").outputMode("append").start().awaitTermination()
#personDF.writeStream.format("csv").option("checkpointLocation", "s3a//testdataoutput/chk").option("path", "s3a://testdataoutput/data__1").start()

df2=personDF.select("from_json(value).edit_history_tweet_ids","from_json(value).id","from_json(value).text","from_json(value).invoiceTime")
#df2.writeStream.format("delta").outputMode("append").option("checkpointLocation", "s3a//testdataoutput/chk").trigger(processingTime='5 seconds').start("s3a://testdataoutput/data_8") \.awaitTermination()

df2.writeStream.format("console").trigger(processingTime="5 seconds").outputMode("append").start().awaitTermination()

