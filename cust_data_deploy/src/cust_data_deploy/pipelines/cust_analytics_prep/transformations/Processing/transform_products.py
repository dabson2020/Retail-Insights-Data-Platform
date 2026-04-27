import dlt
from pyspark.sql.functions import *


@dlt.view(name = 'products_enr_view')

def products_enr_view():
    df = spark.readStream.table('products_stg')
    return df

dlt.create_streaming_table('products_enriched')
dlt.create_auto_cdc_flow(
target = "products_enriched",
source = "products_enr_view",
keys = ["product_id"],
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