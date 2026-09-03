from pyspark.sql.functions import col, trim, upper, initcap, concat_ws

### Fonctions utilitaires génériques

# Fonction de trim
def  trim_column(df, column):
  return df.withColumn(column, trim(col(column)))

# Fonction d'upperCase
def upper_column(df, column):
  return df.withColumn(column, upper(col(column)))

def initcap_column(df, column):
  return df.withColumn(column, initcap(col(column)))

# Fonction qui regroupe uppercase et trim
def normalize_uppercase(df, column):
  return upper_column(trim_column(df, column), column)

def normalize_name(df, column):
  return initcap_column(trim_column(df, column), column)

def cast_column(df, column, data_type):
  return df.withColumn(column, col(column).cast(data_type))

def rename_column(df, old_name, new_name):
  return df.withColumnRenamed(old_name, new_name)

def rename_columns(df, columns):
  return df.withColumnsRenamed(columns)

def remove_duplicates(df, columns):
  return df.dropDuplicates([columns])

def remove_null_rows(df, column):
  return df.filter(col(column).isNotNull())

def add_boolean_flag(df, source_column, new_column):
  return df.withColumn(new_column, col(source_column).isNotNull())

def add_boolean_column(df, new_column, condition):
  return df.withColumn(new_column, condition)

def concat_columns(df, new_column, columns, separator= " "):
  return df.withColumn(
    new_column,
    concat_ws(separator, *[col(column) for column in columns])
  )

def select_columns(df, columns):
    return df.select(*columns)

def join_dataframes(df_left, df_right, on, how="left"):
  return df_left.join(
    df_right,
    on=on,
    how=how
  )