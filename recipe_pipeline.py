from pyspark.sql import SparkSession
import ingest_data, tranform
import logging
import configparser
from datetime import datetime

logging.basicConfig(filename="logs/recipe_pipeline.log", level=logging.INFO, \
                    format="%(levelname)s:%(message)s")


class Pipeline:

    # Initiate spark session
    def create_spark_session(self):
        self.spark = SparkSession.builder\
            .appName("recipe-analysis")\
            .getOrCreate()

    # Main pipeline execution code
    def run_pipelile(self):

        # Reading configs
        config = configparser.ConfigParser()
        config.read(r"projectconfigs\config.ini")
        raw_dataset_location = config.get("complexity_pipeline", "raw_dataset_location")
        raw_dataset_schema = config.get("complexity_pipeline", "raw_dataset_schema")
        raw_dataset_format = config.get("complexity_pipeline", "raw_dataset_format")
        curated_dataset_location = config.get("complexity_pipeline", "curated_dataset_location")
        curated_dateset_name = config.get("complexity_pipeline", "curated_dateset_name")
        curated_dataset_format = config.get("complexity_pipeline", "curated_dataset_format")
        processed_dataset_location = config.get("complexity_pipeline", "processed_dataset_location")
        processed_dataset_name = config.get("complexity_pipeline", "processed_dataset_name")
        processed_dataset_format = config.get("complexity_pipeline", "processed_dataset_format")

        # Initializing ingest and tranform_data class objects
        ingest_process = ingest_data.ingest(self.spark)
        transformation = tranform.transform_data(self.spark)


        # Task-1 Ingestion code
        logging.info(str(datetime.now()) + ' Reading raw data started')
        raw_df = ingest_process.read_data(raw_dataset_format, raw_dataset_schema, raw_dataset_location)

        logging.info(str(datetime.now()) + ' Reading raw data completed')
        logging.info(str(datetime.now()) + ' Refining raw data started')

        refined_df = ingest_process.refine_data(raw_df)
        ingest_process.ingest_data(curated_dataset_format, curated_dataset_location, curated_dateset_name, refined_df)

        logging.info(str(datetime.now()) + ' Refining raw data Completed')


        # Task-2 Transformation
        logging.info(str(datetime.now()) + ' Transformation Completed')

        # Transformation and removing Invalid Cases before writing final results
        final_df = transformation.complexity_transformation(refined_df)\
                    .filter("difficulty != 'Invalid Case'")
        transformation.save_data(processed_dataset_format, processed_dataset_location, processed_dataset_name, final_df)

        logging.info(str(datetime.now()) + ' Transformation Completed')

        self.spark.stop()


if __name__ == '__main__':

    logging.info(str(datetime.now()) + ' Application started')

    pipeline = Pipeline()
    pipeline.create_spark_session()

    logging.info(str(datetime.now()) + ' Spark Session created')
    logging.info(str(datetime.now()) + ' Pipeline started')

    pipeline.run_pipelile()

    logging.info(str(datetime.now()) + ' Pipeline executed')
    logging.info(str(datetime.now()) + ' Application completed')


