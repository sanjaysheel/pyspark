from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkTest").config("spark.driver.memory", "1g").getOrCreate()

df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("C:\\Users\\sheel\PycharmProjects\\pyspark\\Data\\races.csv")


circuit_df = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("C:\\Users\\sheel\\PycharmProjects\\pyspark\\Data\\circuits.csv")
# print(df.show())
# df1 = df.select([count(when(col(i).isNull(), i )).alias(i) for i in df.columns])
# print(df1.show())
rcdf = df.join(circuit_df, circuit_df.circuitId == df.raceId, "inner")

print(rcdf.count())
rcdf = df.join(circuit_df, circuit_df.circuitId== df.raceId, "left")
print(rcdf.count())
rcdf = df.join(circuit_df, circuit_df.circuitId== df.raceId, "right")
print(rcdf.count())
rcdf = df.join(circuit_df, circuit_df.circuitId== df.raceId, "full")
print(rcdf.count())
rcdf = df.join(circuit_df, circuit_df.circuitId== df.raceId, "semi")
print(rcdf.count())
rcdf = df.join(circuit_df, circuit_df.circuitId== df.raceId, "anti")
print(rcdf.count())
rcdf = df.crossJoin(circuit_df)
print(rcdf.count())



input("enter any input")









