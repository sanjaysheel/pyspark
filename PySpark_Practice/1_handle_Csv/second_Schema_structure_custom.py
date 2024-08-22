# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
# from pyspark.sql.functions import current_timestamp, lit
# from pyspark.sql.functions import col
#
#
# spark = SparkSession.builder.appName("SparkTest").config("spark.driver.memory", "1g").getOrCreate()
#
# schema = StructType([
#     StructField("circuitId", IntegerType(), True),
#     StructField("circuitRef", StringType(), True),
#     StructField("name", StringType(), True),
#     StructField("location", StringType(), True),
#     StructField("country", StringType(), True),
#     StructField("lat", DoubleType(), True),
#     StructField("lng", DoubleType(), True),
#     StructField("alt", IntegerType(), True),
#     StructField("url", StringType(), True)]
# )
# df = spark.read.format("csv").schema(schema=schema).option("header","true").load("C:\\Users\\sheel\\PycharmProjects\\pyspark\\Data\\circuits.csv")

# print(df.show(2))

#Selecting Columns
# print(df.select((["circuitId","circuitRef", "name", "location","country"])).show())

# print(df.select("circuitId","circuitRef", "name", "location","country").show())

# print(df.select(df.circuitId,df.circuitRef, df.name, df.location, df.country).show())

# print(df.select(df.circuitId,df.circuitRef, df.name, df.location, "country").show())

# print(df.select(df["circuitId"],df["circuitRef"], df.name, df.location, "country").show())

# selected_df = df.select(col("circuitId"), col("circuitRef"), df["name"], "location", df.country)

#using col fucntion as can alias of the columns name
# selected_df = df.select(col("circuitId"), col("circuitRef").alias("CircuitRef"), df["name"].alias("Name"), "location", df.country.alias("Country"))
# print(selected_df.show())




#  WithColumnRenamed Return A new DataFrame by renaming the existing columns,
# print(df.show(2))
# df1 = df.withColumnRenamed("name", "Name").withColumnRenamed("lat","latitude").withColumnRenamed("url", "URL").withColumnRenamed("country","Country_Name").withColumnRenamed("circuitId","Circuit_Id").withColumnRenamed("circuitRef","Circuit_Ref")
# print(df1.show(2))
#

# df1 = df.withColumn("TimeStamp", current_timestamp()).withColumn("env", lit("productions"))
# print(df1.show(2, truncate=False))

# df1.write.mode("overwrite").csv("C:\\Users\\sheel\\PycharmProjects\\pyspark\\DataWrite\\circuits_1.csv")
# df1.write.mode("overwrite").csv("C:\\Users\\sheel\\PycharmProjects\\pyspark\\DataWrite\\circuits_1.csv")

#Assignment
# File Ingestion - Races (Assignment)
# cav --> Read Data --> Transform Data --> Write Data --> parquet




from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("SparkTest").config("spark.driver.memory", "1g").getOrCreate()

schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("year", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)]
)
df = spark.read.format("csv").option("inferSchema","true").option("header","true").load("C:\\Users\\sheel\\PycharmProjects\\pyspark\\Data\\races.csv")


print(df.show(2))
df1 = (df.withColumnRenamed("raceId", "race_id").withColumnRenamed("year","race_year").withColumnRenamed("circuitid","circuit_id").withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "), col("time")),"yyyy-MM-dd HH:mm:ss")).withColumn("ingestion_date",current_timestamp()))


print(df1.select(["race_id","race_year","round","circuit_id","name","race_timestamp","ingestion_date"]).show(10, truncate=False))


df1.write.mode("overwrite").partitionBy("race_year").parquet("C:\\Users\\sheel\\PycharmProjects\\pyspark\\DataWrite\\races")






