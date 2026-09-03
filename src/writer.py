import logging

from azure_utils import get_blob_service_client, upload_directory

logger = logging.getLogger("TradeCorpETL")


def write_to_clean(df):
  local_path = "/tmp/orders_enriched"
  blob_path = "orders_enriched"

  logger.info("Écriture du DataFrame en Parquet dans %s", local_path)

  df.write \
    .mode("overwrite") \
    .parquet(local_path)

  logger.info("Écriture Parquet terminée")

  blob_service_client = get_blob_service_client()
  container_client = blob_service_client.get_container_client("clean")

  logger.info("Upload des fichiers Parquet vers le conteneur clean")

  upload_directory(
    container_client,
    local_path,
    blob_path
  )

  logger.info("Upload terminé vers clean/%s", blob_path)