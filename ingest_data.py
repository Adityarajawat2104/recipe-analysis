import spark_functions

class ingest:

    def __init__(self, spark):
        self.spark = spark

    # Read raw data from source file
    def read_data(self, file_format, input_schema_path, path):
        spark_comman_f = spark_functions.spark_utils()
        input_schema = spark_comman_f.get_json_schema(input_schema_path)
        return self.spark.read.format(file_format).schema(input_schema).load(path)

    # Refine data - filter null values and keep required columns
    def refine_data(self, df):
        return df.filter("lower(ingredients) like '%beef%'")\
            .filter("prepTime != '' and cookTime != ''")\
            .select("ingredients", "prepTime", "cookTime")

    # Function to save dataframe
    def ingest_data(self, file_format, landing_path, name, df):
        df.write.format(file_format).mode("append").option("header", True).save(landing_path + "/" + name)

