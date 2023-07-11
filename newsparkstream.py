from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("NiFiReceiver").config('spark.jars.packages','org.apache.nifi:nifi-spark-receiver:0.0.2-incubating').getOrCreate()
spark.sparkContext.setLogLevel("WARN")
port_name = "streaming"