from pyspark.sql import SparkSession

#config is optional but it is good to define this config
spark  = SparkSession.builder.appName("first_spark").config("spark.driver.memory", "1g").master("local[*]").getOrCreate()

# by default inferSchema is false
# by default Header is false
df = spark.read.format('csv').option("header", "true").option("inferSchema","true").load("C:\\Users\\sheel\\PycharmProjects\\pyspark\\Data\\circuits.csv")


# print(df.describe().show())
print(df.show())
print(df.printSchema)

input("enter something")
spark.close()