from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F
import ingest_data, tranform, spark_functions
from pyspark import SparkContext
import logging
import configparser
from datetime import datetime

sc = SparkContext('local', 'Spark SQL')

spark = SparkSession.builder\
            .appName("HelloFreshProject")\
            .getOrCreate()

spark_comman_f = spark_functions.spark_utils()
input_schema = spark_comman_f.get_json_schema("schemas/input_schema.json")

window = Window.orderBy(F.col('prepTime'))

df = sc.textFile("input/")
df1 = df.map(lambda x: x.split('"ingredients": "')).zipWithIndex()\

df2 = df1.map(lambda x: x[1].split('"', 1).replace("\n", ""))
df2.saveAsTextFile("output/temp/")
# df = spark.read \
#     .option("header", True)\
#     .option("inferSchema", True)\
#     .format("JSON").load("input/")\
#     .withColumn('row_number', F.row_number().over(window))

# df1 = df.select("row_number", "prepTime", "cookTime")
#
# df2 = df.select("row_number", "ingredients")
#
# df_final = df1.join(df2, on="row_number", how="inner")
#
#     # .filter("name = 'Golden-Crusted Brussels Sprouts Recipe'")
#
# # df.filter("description like '%I love finding inventive ways to use leftovers, and since my brisket recipe makes so much dadgum meat,%'").show(10, truncate=True)
#
#
# df_final.coalesce(1).write.format("CSV").mode("append").option("header", True).save("output/")

# total 1042
# cookTime = '' and prepTime != '' 23
# prepTime = '' and cookTime != '' 0
# prepTime != '' and cookTime != '' 716