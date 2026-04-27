import dlt
from pyspark.sql.functions import *

## Create Employee expectations
employees_rules = {
    "valid_employee_id": "employee_id IS NOT NULL",
    "valid_first_name": "first_name IS NOT NULL",
    "valid_last_name": "last_name IS NOT NULL",
    "valid_created_date": "created_date IS NOT NULL",
    "valid_updated_date": "updated_date IS NOT NULL"
  }


@dlt.table(name = 'employee_stg')
@dlt.expect_all_or_drop(employees_rules)
def employee_stg():
    df = spark.readStream.table('customer_data_analytics.landing.employees')
    return df