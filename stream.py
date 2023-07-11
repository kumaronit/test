from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from urllib.request import urlopen
import delta

spark=SparkSession.builder\
        .enableHiveSupport() \
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


sch =StructType([StructField('id', IntegerType(), True),
                     StructField('name', StringType(), True),
                     StructField('age', IntegerType(), True),
                     StructField('profession', StringType(), True),
                     StructField('city', StringType(), True),
                     StructField('salary', DoubleType(), True)])



#df = spark.readStream.format("csv").schema(sch).option("header", True).load(r"C:\Users\ronkumar\Documents\data\*")

df=spark.readStream.format("socket").option("host","localhost").option("port","9090").load()


print(df.isStreaming)

#df1=df.withWatermark("timestamp", "10 minutes").groupBy(window(df.timestamp, "10 minutes", "5 minutes"),df.id).count()
print("yes")
#df1.writeStream.format("console").outputMode("complete").start().awaitTermination()
#df.writeStream.format("csv").option("checkpointLocation", "C:/Users/ronkumar/Documents/").option("path", "C:/Users/ronkumar/Documents/data/op/").start()

df.writeStream.format("console").trigger(processingTime="5 seconds").outputMode("append").start().awaitTermination()

df.writeStream.format("delta").outputMode("append").option("checkpointLocation", "C:/Users/ronkumar/Documents/chk").start("s3a://testdataoutput/demo")


