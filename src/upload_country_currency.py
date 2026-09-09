import logging

from azure_utils import get_blob_service_client

logger = logging.getLogger("TradeCorpETL")

def upload_country_currency():
    """Envoie le fichier de correspondance pays/devise vers Azure Blob Storage."""
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client("raw")

    blob_name = "reference/country_currency.csv"
    local_file = "data/country_currency.csv"

    logger.info("Upload du mapping pays/devise vers %s", blob_name)

    blob_client = container_client.get_blob_client(blob_name)

    with open(local_file, "rb") as file:
      blob_client.upload_blob(
        file,
        overwrite=True
      )

    logger.info("Upload terminé vers raw/%s", blob_name)

if __name__ == "__main__":
  upload_country_currency()