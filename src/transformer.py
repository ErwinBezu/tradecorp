import json
import logging
import os

from utils import (
  trim_column,
  normalize_name,
  normalize_uppercase,
  remove_duplicates,
  cast_column,
  remove_null_rows,
  rename_column,
  rename_columns,
  add_boolean_flag,
  concat_columns,
  select_columns,
  add_boolean_column,
  join_dataframes
)
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, IntegerType
from pyspark.sql.functions import col, round
from spark_utils import configure_logging, create_spark_session
from enrichment import add_currency_column

logger = logging.getLogger("TradeCorpETL")

FILES = [
  "customers",
  "orders",
  "order_details",
  "products",
  "categories",
  "suppliers",
  "employees",
  "shippers",
]


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
  "sous_total_local",
]

def clean_customers(df):
  """Nettoie et normalise les données clients."""
  return (
    df.transform(trim_column,"company_name")
      .transform(normalize_name, "contact_name")
      .transform(normalize_uppercase, "country")
      .transform(remove_duplicates, "customer_id")
      .transform(
        rename_columns,
          {
            "company_name": "customer_name",
            "country": "customer_country",
            "city": "customer_city",
            "phone": "customer_phone"
          }
      )
    )

def clean_orders(df): 
  """Nettoie les commandes et normalise leurs types de données."""
  return (
    df.transform(cast_column, "order_date", DateType())
      .transform(cast_column, "required_date", DateType())
      .transform(cast_column, "shipped_date", DateType())
      .transform(cast_column, "freight", DoubleType())
      .transform(add_boolean_flag, "shipped_date", "is_shipped")
      .transform(remove_null_rows, "shipped_date")
      .transform(rename_column, "ship_via", "shipper_id")
  )

# Fonction du calcul sous_total
def  add_sous_total(df):
  """Calcule le sous-total de chaque ligne de commande après remise."""
  return df.withColumn(
    "sous_total", 
    round(
      col("prix_unitaire") 
      * col("quantite") 
      * (1- col("discount")),
      2
    )
  )

# Fonction de transformation du DF order_details
def clean_order_details(df):
  """Nettoie les détails des commandes et calcule leur sous-total."""
  return (
    df.transform(cast_column, "unit_price", DoubleType())
      .transform(cast_column, "quantity", IntegerType())
      .transform(cast_column, "discount", DoubleType())
      .transform(rename_column, "unit_price", "prix_unitaire")
      .transform(rename_column, "quantity", "quantite")
      .transform(add_sous_total)
  )

# Fonction de transformation du DF employees
def clean_employees(df):
  """Sélectionne et transforme les données des employés."""
  columns = [
    "employee_id",
    "first_name",
    "last_name",
    "title",
    "hire_date",
    "city",
    "country"
  ]

  return (
    df.transform(select_columns, columns)
      .transform(
        concat_columns,
        "full_name",
        ["first_name", "last_name"]
      )
      .transform(
        rename_columns,
        {
          "country": "employee_country",
          "city": "employee_city"
        }
      )
  )
  
def clean_products(df):
  """Nettoie les données produits et indique leur disponibilité en stock."""
  return (
    df.transform(cast_column, "unit_price", "double")
      .transform(add_boolean_column, "en_stock", col("units_in_stock")> 0)
  )

def clean_shippers(df):
  """Normalise les colonnes relatives aux transporteurs."""
  return (
    df.transform(
      rename_columns,
      {
        "company_name": "shipper_name",
        "phone": "shipper_phone"
      }
    )
  )

def build_enriched(dataframes):
  """Nettoie puis joint les données métier pour construire le dataset enrichi."""
  customers = clean_customers(dataframes["customers"])
  orders = clean_orders(dataframes["orders"])
  order_details = clean_order_details(dataframes["order_details"])
  products = clean_products(dataframes["products"])
  employees = clean_employees(dataframes["employees"])
  categories = dataframes["categories"]
  shippers = clean_shippers(dataframes["shippers"])

  columns = [
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
    "shipper_name"
  ]

  return (
    order_details
    .transform(join_dataframes, orders, "order_id", "inner")
    .transform(join_dataframes, customers, "customer_id")
    .transform(join_dataframes, products, "product_id")
    .transform(join_dataframes, categories, "category_id")
    .transform(join_dataframes, employees, "employee_id")
    .transform(join_dataframes, shippers, "shipper_id")
    .transform(select_columns, columns)
  )

def main():
  configure_logging()

  spark = create_spark_session("TradeCorpTransformer")

  raw_dir = "/home/jovyan/data/tmp"
  output_dir = "/home/jovyan/data/tmp/orders_enriched"

  try:
    logger.info("Lecture des données intermédiaires")

    dataframes = {}

    for file in FILES:
      path = f"{raw_dir}/{file}"

      logger.info("Lecture de %s", path)

      dataframes[file] = spark.read.parquet(path)


    country_currency_df = spark.read.parquet(os.path.join(raw_dir, "country_currency"))

    with open(os.path.join(raw_dir, "exchange_rates.json"),"r") as f:
      exchange_rates = json.load(f)

    logger.info("Transformation des données")

    enriched_df = build_enriched(dataframes)

    logger.info("Enrichissement avec les devises")

    final_df = add_currency_column(
      enriched_df,
      country_currency_df,
      exchange_rates
    )

    final_df = final_df.select(*FINAL_COLUMNS)

    logger.info("Écriture du résultat intermédiaire dans %s",output_dir)

    (
      final_df.write
      .mode("overwrite")
      .parquet(output_dir)
    )

    logger.info("Étape transformer terminée avec succès")

  except Exception:
    logger.exception("Erreur pendant l'étape transformer")
    raise

  finally:
    spark.stop()


if __name__ == "__main__":
  main()