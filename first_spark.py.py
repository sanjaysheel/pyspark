from pyspark.sql import SparkSession

spark  = SparkSession.builder.appName("first_spark").config("spark.driver.memory", "1g").master("local[*]").getOrCreate()


df = spark.read.format('csv').option("header", "true").option("inferSchema","true").load("C:\\Users\\sheel\\PycharmProjects\\pyspark\\venv\\Data\\circuits.csv")


print(df.describe().show())
print(df.printSchema)

input("enter something")
spark.close()