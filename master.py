# file created from local and sent to main branch of git server
# print("file modified from local and pushed and merged with develop branch")


import pyspark.sql.types
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("Reading csv file").getOrCreate()
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .appName("Reading csv file") \
    .getOrCreate()
wrld_df = spark.read.csv("C:/file/world_population.csv", inferSchema=True, header=True)
# wrld_df.show()
# wrld_df.filter((wrld_df.Country != 'India') & (wrld_df.Continent == 'Asia')).show(truncate=False)
wrld_df.select('Country', 'Continent').show(truncate=False)
# write.csv("C:/Users/SUMIT/Desktop/pyspark/output/output.csv")

# show(truncate=True)
#print("added from editor")
