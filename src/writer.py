import logging

from azure_utils import get_blob_service_client, upload_directory
from spark_utils import configure_logging


logger = logging.getLogger("TradeCorpETL")
 
def upload_to_clean(
  local_path="/home/jovyan/data/tmp/orders_enriched",
  blob_path="orders_enriched"
):
  """Envoie un dossier Parquet déjà existant vers le conteneur clean (purge l'ancien contenu au passage)."""
  logger.info("Connexion à Azure Storage")
  blob_service_client = get_blob_service_client()
  container_client = blob_service_client.get_container_client("clean")
 
  logger.info("Upload des fichiers Parquet vers clean/%s", blob_path)
 
  upload_directory(
    container_client,
    local_path,
    blob_path
  )
 
  logger.info("Upload terminé vers clean/%s", blob_path)
 
 
def main():
  configure_logging()
 
  try:
    upload_to_clean()
    logger.info("Étape writer terminée avec succès")
 
  except Exception:
    logger.exception("Erreur pendant l'étape writer")
    raise
 
if __name__ == "__main__":
  main()