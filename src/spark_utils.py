import logging

from pyspark.sql import SparkSession


def configure_logging():
  logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
  )


def create_spark_session(app_name):
  spark = (
    SparkSession.builder
    .appName(app_name)
    .getOrCreate()
  )

  spark.sparkContext.setLogLevel("WARN")

  return spark