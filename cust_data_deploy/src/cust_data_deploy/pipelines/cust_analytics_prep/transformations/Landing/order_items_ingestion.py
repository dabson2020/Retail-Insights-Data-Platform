import dlt
from pyspark.sql.functions import *

@dlt.table(name="order_items_stg", comment="Order Items data")
def order_items_stg():
  df = spark.readStream.table('customer_data_analytics.landing.order_items')
  return df