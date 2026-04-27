import dlt
from pyspark.sql.functions import *

product_rules = {
  "valid_product_id": "product_id IS NOT NULL",
  "valid_product_name": "product_name IS NOT NULL",
  "valid_category": "category IS NOT NULL",
  "valid_price": "product_price IS NOT NULL",
  "valid_updated_date": "updated_date IS NOT NULL",
}

@dlt.table(name="products_stg", comment="Products data")
def products_stg():
  df = spark.readStream.table('customer_data_analytics.landing.products')
  return df