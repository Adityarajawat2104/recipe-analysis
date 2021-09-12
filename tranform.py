import spark_functions
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

class transform_data:

    def __init__(self, spark):
        self.spark = spark

    def complexity_transformation(self, curated_df):
        spark_comman_f = spark_functions.spark_utils()
        calculate_time_udf = F.udf(lambda time_column: spark_comman_f.calculate_time(time_column), IntegerType())
        calculate_complexity_udf = F.udf(lambda time_column: spark_comman_f.calculate_complexity(time_column), StringType())
        new_df = curated_df.withColumn("prep_time_mins", calculate_time_udf(F.col("prepTime"))) \
            .withColumn("cook_time_mins", calculate_time_udf(F.col("cookTime")))\
            .withColumn("avg_total_cooking_time", (F.col("cook_time_mins") + F.col("prep_time_mins")).cast(IntegerType()))\
            .withColumn("difficulty", calculate_complexity_udf(F.col("avg_total_cooking_time")))
        return new_df.select("avg_total_cooking_time", "difficulty")

    # Function to save dataframe
    def save_data(self, file_format, landing_path, name, df):
        df.write.format(file_format).mode("append").option("header", True).save(landing_path + "/" + name)
