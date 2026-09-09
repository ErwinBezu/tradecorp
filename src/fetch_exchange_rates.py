import json
import logging

import requests

from azure_utils import get_blob_service_client

logger = logging.getLogger("TradeCorpETL")

API_URL = "https://api.exchangerate-api.com/v4/latest/USD"

def fetch_exchange_rates():
  """Récupère les taux de change depuis l'API."""
  logger.info("Récupération des taux de change")

  response = requests.get(API_URL, timeout=30)
  response.raise_for_status()

  logger.info("Taux de change récupérés avec succès")

  return response.json()

def upload_exchange_rates(data):
  """Enregistre les taux de change dans le conteneur raw d'Azure Blob Storage."""
  blob_service_client = get_blob_service_client()
  container_client = blob_service_client.get_container_client("raw")

  blob_name = "reference/exchange_rates.json"

  logger.info("Upload des taux de change vers %s", blob_name)

  blob_client = container_client.get_blob_client(blob_name)

  blob_client.upload_blob(json.dumps(data), overwrite=True)

  logger.info("Upload terminé vers raw/%s", blob_name)

def main():
  """Récupère puis envoie les taux de change vers Azure Blob Storage."""
  try:
    data = fetch_exchange_rates()
    upload_exchange_rates(data)

  except Exception:
    logger.exception("Erreur lors de la récupération du taux de change")
    raise


if __name__ == "__main__":
    main()