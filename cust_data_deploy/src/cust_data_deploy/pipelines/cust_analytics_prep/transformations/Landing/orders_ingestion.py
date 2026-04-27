import dlt
from pyspark.sql.functions import *

@dlt.table(name="orders_stg", comment="Orders data")
def orders_stg():
  df = spark.readStream.table('customer_data_analytics.landing.orders')
  return df