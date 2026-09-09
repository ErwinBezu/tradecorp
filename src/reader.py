import json
import logging
import os

from pyspark.sql import SparkSession
from azure_utils import get_blob_service_client, download_file
from spark_utils import configure_logging, create_spark_session

logger = logging.getLogger("TradeCorpETL")

FILES = [
  "customers",
  "orders",
  "order_details",
  "products",
  "categories",
  "suppliers",
  "employees",
  "shippers"
]

def read_business_data(spark, container_client, base_dir="/tmp"):
  """Télécharge et charge les fichiers métier CSV dans des DataFrames Spark."""
  dataframes = {}
  os.makedirs(base_dir, exist_ok=True)

  for file in FILES:
    blob_name = f"{file}.csv"
    local_path = os.path.join(base_dir, f"{file}.csv")

    logger.info("Téléchargement de %s depuis Azure", blob_name)
    download_file(container_client, blob_name, local_path)

    logger.info("Lecture de %s avec Spark", blob_name)
    dataframes[file] = spark.read.csv(local_path, header=True, inferSchema=True)

    logger.info("%s chargé avec succès", blob_name)

  return dataframes

def read_reference_data(spark, container_client, base_dir="/tmp"):
  """Télécharge et charge les données de référence utilisées pour l'enrichissement."""
  os.makedirs(base_dir, exist_ok=True)

  local_csv = os.path.join(base_dir, "country_currency.csv")

  logger.info("Téléchargement de reference/country_currency.csv depuis Azure")
  download_file(container_client, "reference/country_currency.csv", local_csv)
  country_currency_df = spark.read.csv(local_csv, header=True, inferSchema=True)
  logger.info("country_currency.csv chargé avec succès")

  local_json = os.path.join(base_dir, "exchange_rates.json")
  logger.info("Téléchargement de reference/exchange_rates.json depuis Azure")
  download_file(container_client, "reference/exchange_rates.json", local_json)

  with open(local_json, "r") as f:
    exchange_rates = json.load(f)

  logger.info("exchange_rates.json chargé avec succès")

  return country_currency_df, exchange_rates


def read_data(spark, base_dir="/tmp"):
  """Lit l'ensemble des données métier et de référence depuis Azure Blob Storage."""
  logger.info("Connexion à Azure Storage")
  blob_service_client = get_blob_service_client()
  container_client = blob_service_client.get_container_client("raw")
  logger.info("Connexion à Azure Storage réussie")

  dataframes = read_business_data(spark, container_client)
  country_currency_df, exchange_rates = read_reference_data(spark, container_client)

  logger.info("Fin du téléchargement")

  return dataframes, country_currency_df, exchange_rates


def main():
  configure_logging()
  spark = create_spark_session("TradeCorpReader")

  try:
    shared_dir = "/home/jovyan/data/tmp/raw"
    tmp_dir = "/home/jovyan/data/tmp"

    logger.info("Téléchargement des données vers le répertoire partagé %s",shared_dir)

    dataframes, country_currency_df, exchange_rates = read_data(spark,base_dir=shared_dir
    )

    for name, df in dataframes.items():
      path = f"{tmp_dir}/{name}"

      logger.info("Écriture intermédiaire de %s dans %s",name,path)

      df.write \
        .mode("overwrite") \
        .parquet(path)

    country_currency_path = f"{tmp_dir}/country_currency"

    logger.info("Écriture de country_currency dans %s",country_currency_path)

    country_currency_df.write \
      .mode("overwrite") \
      .parquet(country_currency_path)

    exchange_rates_path = f"{tmp_dir}/exchange_rates.json"

    logger.info("Écriture de exchange_rates dans %s",exchange_rates_path)

    with open(exchange_rates_path,"w") as f:
      json.dump(exchange_rates, f)

    logger.info("Étape reader terminée avec succès")

  except Exception:
    logger.exception("Erreur pendant l'étape reader")
    raise

  finally:
    spark.stop()

if __name__ == "__main__":
  main()