from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkTest").config("spark.driver.memory", "1g").getOrCreate()


df = spark.read.json("json").option("multiline","true").option("inferSchema","true").option("header","true").load("C:\\Users\\sheel\\PycharmProjects\\pyspark\\Data\\qualifying.json")

print(df.show())



#
# df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("C:\\Users\\sheel\\PycharmProjects\\pyspark\\Data\\results.csv")
#
# # df = df.withColumn("Ingestion_date", current_timestamp())
#
# df = df.withColumnRenamed("resultid", "result_id").withColumnRenamed("raceid","race_id").withColumnRenamed("driverid","driver_id").withColumnRenamed("constructorid","constructor_id").withColumnRenamed("positionText","position_text").withColumnRenamed("positionOrder","position_order").withColumnRenamed("fastestlap","fastest_lap").withColumnRenamed("fastestlaptime","fastest_lap_time").withColumnRenamed("fastestlapspeed","fastest_lap_speed")
#
# df = df.withColumn("Ingestion_date", current_timestamp())
#
# print(df.show())
# df.write.mode("overwrite").partitionBy("race_id").parquet("C:\\Users\\sheel\\PycharmProjects\\pyspark\\DataWrite\\json_handle")
#
# # dftest= spark.read.format("parquet").option("inferSchema","true").option("header","true").load("C:\\Users\\sheel\\PycharmProjects\\pyspark\\DataWrite\\json_handle\\race_id=1\\part-00000-18cc809c-2ddb-46d8-a8ff-4511ddc8a2ac.c000.snappy.parquet")
# # print(dftest.show())


