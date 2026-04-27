import dlt
from pyspark.sql.functions import *

# Creating Employye_performance materialized view

@dlt.table(name = 'employee_performance')

def employee_performance():
    df_emp = spark.read.table('dim_employees')
    df_orders = spark.read.table('fact_orders')
    df_order_items = spark.read.table('order_items')
   

    # Perform joins
    join_df = df_emp.alias("e") \
      .join(df_orders.alias("o"), col("e.employee_id") == col("o.employee_id")) \
      .join(df_order_items.alias("oi"), col("o.order_id") == col("oi.order_id"))

  # Aggregation
    result_df = join_df.groupBy(
      col("e.employee_id"),
      col("e.employee_name")
  ).agg(
      countDistinct(col("o.order_id")).alias("order_count"),
      sum(col("oi.quantity") * col("oi.unit_price")).alias("revenue"),
      max(col("e.created_date")).alias("created_date"),
      max(col("o.updated_date")).alias("updated_date")
  )

    final_df = result_df.orderBy(col("order_count").desc())
    return result_df









