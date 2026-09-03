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
from pyspark.sql.types import DateType, DoubleType, IntegerType
from pyspark.sql.functions import col, round

# Fonction de transformation du DF customers
def clean_customers(df):
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

# Fonction de transformation du DF orders
def clean_orders(df): 
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
  
# Fonction de transformation du DF products
def clean_products(df):
  return (
    df.transform(cast_column, "unit_price", "double")
      .transform(add_boolean_column, "en_stock", col("units_in_stock")> 0)
  )

# Fonction de transformation du df shippers
def clean_shippers(df):
  return (
    df.transform(
      rename_columns,
      {
        "company_name": "shipper_name",
        "phone": "shipper_phone"
      }
    )
  )

# FONCTION pour la construction de la table finale enrichie
def build_enriched(dataframes):
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