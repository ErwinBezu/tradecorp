import json
import logging

from azure_utils import get_blob_service_client, download_file

logger = logging.getLogger("TradeCorpETL")

files = [
  "customers",
  "orders",
  "order_details",
  "products",
  "categories",
  "suppliers",
  "employees",
  "shippers"
]

def read_business_data(spark, container_client):
  dataframes = {}

  for file in files:
    blob_name = f"{file}.csv"
    local_path = f"/tmp/{file}.csv"

    logger.info("Téléchargement de %s depuis Azure", blob_name)
    download_file(container_client, blob_name, local_path)

    logger.info("Lecture de %s avec Spark", blob_name)
    dataframes[file] = spark.read.csv(local_path, header=True, inferSchema=True)

    logger.info("%s chargé avec succès", blob_name)

  return dataframes

def read_reference_data(spark, container_client):
  local_csv = "/tmp/country_currency.csv"
  logger.info("Téléchargement de reference/country_currency.csv depuis Azure")
  download_file(container_client, "reference/country_currency.csv", local_csv)
  country_currency_df = spark.read.csv(local_csv, header=True, inferSchema=True)
  logger.info("country_currency.csv chargé avec succès")

  local_json = "/tmp/exchange_rates.json"
  logger.info("Téléchargement de reference/exchange_rates.json depuis Azure")
  download_file(container_client, "reference/exchange_rates.json", local_json)

  with open(local_json, "r") as f:
    exchange_rates = json.load(f)

  logger.info("exchange_rates.json chargé avec succès")

  return country_currency_df, exchange_rates


def read_data(spark):
  logger.info("Connexion à Azure Storage")
  blob_service_client = get_blob_service_client()
  container_client = blob_service_client.get_container_client("raw")
  logger.info("Connexion à Azure Storage réussie")

  dataframes = read_business_data(spark, container_client)
  country_currency_df, exchange_rates = read_reference_data(spark, container_client)

  logger.info("Fin du téléchargement")

  return dataframes, country_currency_df, exchange_rates