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
#
# print(df.show(2))
#
# Selecting Columns
# print(df.select((["circuitId","circuitRef", "name", "location","country"])).show())
#
# print(df.select("circuitId","circuitRef", "name", "location","country").show())
#
# print(df.select(df.circuitId,df.circuitRef, df.name, df.location, df.country).show())
#
# print(df.select(df.circuitId,df.circuitRef, df.name, df.location, "country").show())
#
# print(df.select(df["circuitId"],df["circuitRef"], df.name, df.location, "country").show())
#
# selected_df = df.select(col("circuitId"), col("circuitRef"), df["name"], "location", df.country)
#
# using col fucntion as can alias of the columns name
# selected_df = df.select(col("circuitId"), col("circuitRef").alias("CircuitRef"), df["name"].alias("Name"), "location", df.country.alias("Country"))
# print(selected_df.show())
#
#
#
#
#  WithColumnRenamed Return A new DataFrame by renaming the existing columns,
# print(df.show(2))
# df1 = df.withColumnRenamed("name", "Name").withColumnRenamed("lat","latitude").withColumnRenamed("url", "URL").withColumnRenamed("country","Country_Name").withColumnRenamed("circuitId","Circuit_Id").withColumnRenamed("circuitRef","Circuit_Ref")
# print(df1.show(2))
#
#
# df1 = df.withColumn("TimeStamp", current_timestamp()).withColumn("env", lit("productions"))
# print(df1.show(2, truncate=False))
#
# df1.write.mode("overwrite").csv("C:\\Users\\sheel\\PycharmProjects\\pyspark\\DataWrite\\circuits_1.csv")
# df1.write.mode("overwrite").csv("C:\\Users\\sheel\\PycharmProjects\\pyspark\\DataWrite\\circuits_1.csv")
#
# Assignment
# File Ingestion -pi Races (Assignment)
# cav --> Read Data --> Transform Data --> Write Data --> parquet
#
#
#
#
# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# from pyspark.sql.functions import col
# from pyspark.sql.functions import *
#
#
# spark = SparkSession.builder.appName("SparkTest").config("spark.driver.memory", "1g").getOrCreate()
#
# df = spark.read.format("json").option("inferSchema","true").option("header","true").load("C:\\Users\\sheel\\PycharmProjects\\pyspark\\Data\\raw_nyc_phil.json")
#
# # C:\Users\sheel\PycharmProjects\pyspark\Data\SampleFile.json
#
# print(df.printSchema())
# # df1= df.withColumnRenamed("_corrupt_record", "Corrupt_Record").withColumnRenamed("country","Country_Name")
# df_flattened = df.withColumn("program", explode(col("programs"))) \
#     .withColumn("concert", explode(col("program.concerts"))) \
#     .withColumn("work", explode(col("program.works"))) \
#     .withColumn("soloist", explode(col("work.soloists"))) \
#     .select(
#         col("program.id").alias("program_id"),
#         col("program.orchestra").alias("orchestra"),
#         col("program.programID").alias("programID"),
#         col("program.season").alias("season"),
#         col("concert.Date").alias("concert_date"),
#         col("concert.Location").alias("concert_location"),
#         col("concert.Time").alias("concert_time"),
#         col("concert.Venue").alias("concert_venue"),
#         col("concert.eventType").alias("concert_eventType"),
#         col("work.ID").alias("work_id"),
#         col("work.composerName").alias("composer_name"),
#         col("work.conductorName").alias("conductor_name"),
#         col("work.interval").alias("interval"),
#         col("work.movement").alias("movement"),
#         col("work.workTitle").alias("work_title"),
#         col("soloist.soloistInstrument").alias("soloist_instrument"),
#         col("soloist.soloistName").alias("soloist_name"),
#         col("soloist.soloistRoles").alias("soloist_roles")
#     )
# print(df_flattened.show(truncate=False))
#
#
