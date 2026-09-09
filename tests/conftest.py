import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
  """Crée une session Spark utilisée par les tests, puis l'arrête à la fin."""
  spark = ( 
    SparkSession.builder
    .appName("TradeCorp Tests")
    .master("local[*]")
    .getOrCreate()
  )
  yield spark
  spark.stop()