from pyspark.sql.functions import col, trim, upper, initcap, concat_ws

def  trim_column(df, column):
  """Supprime les espaces inutiles d'une colonne."""
  return df.withColumn(column, trim(col(column)))

def upper_column(df, column):
  """Convertit une colonne en majuscules."""
  return df.withColumn(column, upper(col(column)))

def initcap_column(df, column):
  """Met en majuscule la première lettre de chaque mot."""
  return df.withColumn(column, initcap(col(column)))

def normalize_uppercase(df, column):
  """Nettoie puis convertit une colonne en majuscules."""
  return upper_column(trim_column(df, column), column)

def normalize_name(df, column):
  """Nettoie et normalise la casse d'un nom."""
  return initcap_column(trim_column(df, column), column)

def cast_column(df, column, data_type):
  """Convertit le type d'une colonne."""
  return df.withColumn(column, col(column).cast(data_type))

def rename_column(df, old_name, new_name):
  """Renomme une colonne."""
  return df.withColumnRenamed(old_name, new_name)

def rename_columns(df, columns):
  """Renomme plusieurs colonnes."""
  return df.withColumnsRenamed(columns)

def remove_duplicates(df, columns):
  """Supprime les doublons selon une colonne."""
  return df.dropDuplicates([columns])

def remove_null_rows(df, column):
  """Supprime les lignes dont la colonne est nulle."""
  return df.filter(col(column).isNotNull())

def add_boolean_flag(df, source_column, new_column):
  """Ajoute un booléen indiquant si une valeur est présente."""
  return df.withColumn(new_column, col(source_column).isNotNull())

def add_boolean_column(df, new_column, condition):
  """Ajoute une colonne booléenne selon une condition."""
  return df.withColumn(new_column, condition)

def concat_columns(df, new_column, columns, separator= " "):
  """Concatène plusieurs colonnes dans une nouvelle colonne."""
  return df.withColumn(
    new_column,
    concat_ws(separator, *[col(column) for column in columns])
  )

def select_columns(df, columns):
  """Sélectionne les colonnes demandées."""
  return df.select(*columns)

def join_dataframes(df_left, df_right, on, how="left"):
  """Joint deux DataFrames selon une clé donnée."""
  return df_left.join(
    df_right,
    on=on,
    how=how
  )