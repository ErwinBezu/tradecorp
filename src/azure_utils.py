import os
import logging

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger("TradeCorpETL")

load_dotenv("/home/jovyan/.env")

# Connexion a Azure
def get_blob_service_client():
  account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
  account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

  blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=account_key
  )

  return blob_service_client

# Fonction de téléchargement
def download_file(container_client, blob_name, local_path):
  blob_client = container_client.get_blob_client(blob_name)

  with open(local_path, "wb") as file:
    file.write(blob_client.download_blob().readall())

# Fonction d'upload
def upload_directory(container_client, local_directory, blob_directory):
  for file_name in os.listdir(local_directory):

    if not file_name.endswith(".parquet"):
      continue

    local_path = os.path.join(local_directory, file_name)
    blob_name = f"{blob_directory}/{file_name}"

    logger.info("Upload de %s vers %s", file_name, blob_name)

    blob_client = container_client.get_blob_client(blob_name)

    with open(local_path, "rb") as f:
      blob_client.upload_blob(f,overwrite=True)