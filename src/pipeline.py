import logging

from pyspark.sql import SparkSession

from reader import read_data
from transformer import build_enriched
from enrichment import add_currency_column
from writer import write_to_clean

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("TradeCorpETL")

FINAL_COLUMNS = [
  "order_id",
  "customer_id",
  "employee_id",
  "product_id",
  "order_date",
  "required_date",
  "shipped_date",
  "freight",
  "is_shipped",
  "prix_unitaire",
  "quantite",
  "discount",
  "sous_total",
  "customer_name",
  "customer_country",
  "customer_city",
  "product_name",
  "category_name",
  "en_stock",
  "full_name",
  "shipper_name",
  "currency",
  "sous_total_local"
]

def run_pipeline():
  """Exécute le pipeline ETL complet, de la lecture des données à leur écriture dans Azure Blob Storage."""
  spark = (
    SparkSession.builder
    .appName("TradeCorpETL")
    .getOrCreate()
  )
  spark.sparkContext.setLogLevel("WARN")

  try:
    logger.info("Démarrage du pipeline")

    logger.info("Étape 1/4 : lecture des données")
    dataframes, country_currency_df, exchange_rates = read_data(spark)
    logger.info("Extraction terminée")

    logger.info("Étape 2/4 : nettoyage et jointure des tables")
    enriched_df = build_enriched(dataframes)
    logger.info("Transformation terminée")

    logger.info("Étape 3/4 : enrichissement devise et taux de change")
    final_df = add_currency_column(enriched_df, country_currency_df, exchange_rates)

    final_df = final_df.select(*FINAL_COLUMNS)
    logger.info("Enrichissement terminé")

    logger.info("Étape 4/4 : écriture vers la zone clean")
    write_to_clean(final_df)
    logger.info("Écriture terminée")

    logger.info("Pipeline terminé avec succés !")
  
  except Exception:
    logger.exception("Erreur pendant le pipeline ETL")
    raise
  
  finally:
    logger.info("Arrêt de la session Spark")
    spark.stop()

if __name__ == "__main__":
  run_pipeline()