from pyspark.sql import SparkSession
from pyspark.sql import Row
spark=SparkSession.builder.getOrCreate()

data=[Row(name="ronit",age=20),
      Row(name="swar",age=19)]

df=spark.createDataFrame(data)
df.show()
