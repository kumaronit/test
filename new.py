from pyspark.sql import SparkSession
from datetime import date,datetime
from pyspark.sql.functions import *
spark=SparkSession.builder.enableHiveSupport().getOrCreate()
#df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(r"C:\Users\ronkumar\Documents\test.csv")


'''
df.createGlobalTempView("ronit")

#spark.sql("select * from global_temp.ronit").show()
spark.sql("create database if not exists ronit_db")
spark.sql("create table if not exists ronit_db.ronit_csv1(name string,id string)")
spark.sql("insert into ronit_db.ronit_csv1 select * from global_temp.ronit")
'''

#df.select("a","c").distinct().show()

#spark.sql("select * from ronit_db.ronit_csv1").show()
#df1=df.withColumnRenamed("a","name")\
 #     .withColumnRenamed("b","id")

#df1=df.withColumn("c",to_date("c","dd-MM-yyyy"))


#df1.select("a",year("c"),expr("b as babu"))\
 #  .show()

#df1.printSchema()

#data_list = [1,2]

#print(data_list)
#spark.createDataFrame(data_list)

sc=spark.sparkContext

data=sc.textFile("ronit.text")

#text_file = sc.textFile("firstprogram.txt")
counts = data.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: (word, 1)) \
                             .reduceByKey(lambda x, y: x + y)
