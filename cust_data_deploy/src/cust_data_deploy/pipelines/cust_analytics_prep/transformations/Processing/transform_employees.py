import dlt
from pyspark.sql.functions import *

employees_rules_enr = {
    "valid_employee_id": "employee_id IS NOT NULL",
    "valid_employee_name": "employee_name IS NOT NULL",
}

@dlt.view(name = 'employee_enr_view')
@dlt.expect_all_or_drop(employees_rules_enr)
def employee_enr_view():
    df = spark.readStream.table('employee_stg')
    df = df.withColumn('employee_name', concat(col('first_name'), lit(' '), col('last_name')))
    df = df.drop('first_name','last_name')
    return df
 

dlt.create_streaming_table('employee_enriched')
dlt.create_auto_cdc_flow(
target = "employee_enriched",
source = "employee_enr_view",
keys = ["employee_id"],
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