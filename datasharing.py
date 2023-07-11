from pyspark.sql import SparkSession






spark=SparkSession.builder\
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


profile_file ="s3a://testdataoutput/demo4"
#client = delta_sharing.SharingClient(profile_file)

ratings = spark.read\
               .format("delta")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .load("s3a://testdataoutput/demo5/")

'''
ratings = spark.read\
               .format("csv")\
               .option("header", "true")\
               .option("inferSchema", "true")\
               .load("s3a://testdataoutput/testoutput/")


ratings.write.format("delta").mode("append").save("s3a://testdataoutput/demo5")
'''
ratings.show()