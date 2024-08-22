from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkTest").config("spark.driver.memory", "1g").getOrCreate()

df = spark.read.csv("C:\\Users\\sheel\PycharmProjects\\pyspark\\Data\\races.csv")










