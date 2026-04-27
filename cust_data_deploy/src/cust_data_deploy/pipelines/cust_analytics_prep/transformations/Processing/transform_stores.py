import dlt
from pyspark.sql.functions import *


@dlt.view(name = 'stores_enr_view')

def stores_enr_view():
    df = spark.readStream.table('stores_stg')
    return df

dlt.create_streaming_table('stores_enriched')
dlt.create_auto_cdc_flow(
target = "stores_enriched",
source = "stores_enr_view",
keys = ["store_id"],
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