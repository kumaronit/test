from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
import delta
from pyspark.sql.functions import *



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
          .hadoopConfiguration().set("fs.s3a.endpoint", "http://192.168.1.8:9090")
spark.sparkContext._jsc\
          .hadoopConfiguration().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc\
          .hadoopConfiguration().set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.sparkContext._jsc\
          .hadoopConfiguration().set("fs.s3a.multipart.size", "104857600")
spark.sparkContext._jsc\
         .hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")

ratings = spark.read\
               .format("csv")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .load("s3a://testdataoutput/testoutput/")
#print("dd")
#ratings1=ratings.filter("name=='ronit'")
ratings.show()
#ratings2=ratings1.withColumn("salhike",expr("sal"))

#ratings2.write.format("delta").mode("append").save("s3a://testdataoutput/demo4")

#ratings1.show()