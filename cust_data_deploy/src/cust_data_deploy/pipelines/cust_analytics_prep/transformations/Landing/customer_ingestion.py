import dlt
from pyspark.sql.functions import *

## Create Customer expectations
customers_rules = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_first_name": "first_name IS NOT NULL",
    "valid_last_name": "last_name IS NOT NULL",
    "valid_created_date": "created_date IS NOT NULL",
  }


@dlt.table(name = 'customer_stg')
@dlt.expect_all_or_drop(customers_rules)
def customer_stg():
    df = spark.readStream.table('customer_data_analytics.landing.customers')
    return df
