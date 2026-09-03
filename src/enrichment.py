import logging

from pyspark.sql.functions import col, round as spark_round
from utils import rename_column, join_dataframes

logger = logging.getLogger("TradeCorpETL")

def add_currency_column(df, country_currency_df, exchange_rates):
  spark = df.sparkSession

  country_currency = (
    country_currency_df
    .transform(rename_column, "country", "customer_country")
  )

  rates_data = [
    (currency, float(rate))
    for currency, rate in exchange_rates["rates"].items()
  ]
  rates_df = spark.createDataFrame(rates_data, ["currency", "rate"])

  return (
    df
    .transform(join_dataframes, country_currency, "customer_country")
    .transform(join_dataframes, rates_df, "currency")
    .withColumn("sous_total_local", spark_round(col("sous_total") * col("rate"), 2))
    .drop("rate")
  )