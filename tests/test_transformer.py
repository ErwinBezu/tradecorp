from transformer import clean_orders, add_sous_total, clean_customers 
from enrichment import add_currency_column

def test_clean_orders_removes_null_shipped_date(spark):
  data = [
    (1, "2024-01-01", "2024-01-05", "2024-01-04", 10.5, 1),
    (2, "2024-01-02", "2024-01-06", None, 12.0, 2),
  ]
  columns = ["order_id", "order_date", "required_date", "shipped_date", "freight", "ship_via"]
  df = spark.createDataFrame(data, columns)

  result = clean_orders(df)

  assert result.count() == 1
  assert result.collect()[0]["order_id"] == 1

def test_add_sous_total(spark):
  data = [(10.0, 2, 0.1)]
  columns = ["prix_unitaire", "quantite", "discount"]
  df = spark.createDataFrame(data, columns)

  result = add_sous_total(df)
  sous_total = result.collect()[0]["sous_total"]

  assert sous_total == 18.0

def test_clean_customers(spark):
  data = [(1, "  Acme Corp  ", "  jean dupont  ", "  france  ", "Paris", "0123456789")]
  columns = ["customer_id", "company_name", "contact_name", "country", "city", "phone"]
  df = spark.createDataFrame(data, columns)

  result = clean_customers(df).collect()[0]

  assert result["contact_name"] == "Jean Dupont"
  assert result["customer_country"] == "FRANCE"

def test_add_currency_column(spark):
  data = [(1, "FRANCE", 100.0)]
  columns = ["order_id", "customer_country", "sous_total"]
  df = spark.createDataFrame(data, columns)

  country_currency_data = [("FRANCE", "EUR")]
  country_currency_df = spark.createDataFrame(
    country_currency_data, ["country", "currency"]
  )

  exchange_rates = {
    "base": "USD",
    "date": "2024-01-01",
    "rates": {"EUR": 0.9, "USD": 1.0}
  }

  result = add_currency_column(df, country_currency_df, exchange_rates).collect()[0]

  assert result["currency"] == "EUR"
  assert result["sous_total_local"] == 90.0