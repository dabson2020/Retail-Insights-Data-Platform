import dlt
from pyspark.sql.functions import *

customers_rules_enr = {
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_customer_name": "customer_name IS NOT NULL",
}

@dlt.view(name = 'customer_enr_view')
@dlt.expect_all_or_drop(customers_rules_enr)
def customer_senv_view():
    df = spark.readStream.table('customer_stg')
    df = df.withColumn('customer_name', concat(col('first_name'), lit(' '), col('last_name')))
    df = df.drop('first_name','last_name')
    df = df.select('customer_id','customer_name','city','country','created_date','updated_date')
    return df
 

dlt.create_streaming_table('customer_enriched')
dlt.create_auto_cdc_flow(
target = "customer_enriched",
source = "customer_enr_view",
keys = ["customer_id"],
sequence_by = "updated_date",
ignore_null_updates = None,
apply_as_deletes = None,
apply_as_truncates = None,
column_list = None,
except_column_list = None,
stored_as_scd_type = 1,
track_history_column_list = None,
track_history_except_column_list = None
)