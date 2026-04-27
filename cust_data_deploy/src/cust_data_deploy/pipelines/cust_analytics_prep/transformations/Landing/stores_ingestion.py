import dlt
from pyspark.sql.functions import *

@dlt.table(name="stores_stg", comment="Stores data")
def stores_stg():
  df = spark.readStream.table('customer_data_analytics.landing.stores')
  return df