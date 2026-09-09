import os
import logging

from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger("TradeCorpETL")

load_dotenv("/home/jovyan/.env")

def get_blob_service_client():
  """Crée et retourne un client de connexion à Azure Blob Storage."""
  account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
  account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

  blob_service_client = BlobServiceClient(
    account_url=f"https://{account_name}.blob.core.windows.net",
    credential=account_key
  )

  return blob_service_client

def download_file(container_client, blob_name, local_path):
  """Télécharge un fichier depuis Azure Blob Storage vers un chemin local."""
  blob_client = container_client.get_blob_client(blob_name)

  with open(local_path, "wb") as file:
    file.write(blob_client.download_blob().readall())

def delete_directory(container_client, blob_directory):
  """Supprime tous les blobs existants sous un préfixe donné (purge avant upload)."""
  blobs = container_client.list_blobs(name_starts_with=f"{blob_directory}/")

  for blob in blobs:
    logger.info("Suppression de l'ancien blob %s", blob.name)
    container_client.delete_blob(blob.name)


def upload_directory(container_client, local_directory, blob_directory):
  """Envoie les fichiers Parquet d'un dossier local vers Azure Blob Storage."""
  delete_directory(container_client, blob_directory)

  for file_name in os.listdir(local_directory):

    if not file_name.endswith(".parquet"):
      continue

    local_path = os.path.join(local_directory, file_name)
    blob_name = f"{blob_directory}/{file_name}"

    logger.info("Upload de %s vers %s", file_name, blob_name)

    blob_client = container_client.get_blob_client(blob_name)

    with open(local_path, "rb") as f:
      blob_client.upload_blob(f,overwrite=True)