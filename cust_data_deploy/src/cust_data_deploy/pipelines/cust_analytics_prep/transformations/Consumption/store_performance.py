import dlt
from pyspark.sql.functions import *

@dlt.table(name = 'store_performance')
def store_performance():
  store_df = spark.read.table('dim_stores')
  orders_df = spark.read.table('fact_orders')
  order_items_df = spark.read.table('order_items')

  # Perform joins
  df = store_df.alias("s") \
      .join(orders_df.alias("o"), col("s.store_id") == col("o.store_id")) \
      .join(order_items_df.alias("oi"), col("o.order_id") == col("oi.order_id"))

  # Aggregation
  result_df = df.groupBy(
      col("s.store_id"),
      col("s.store_name")
  ).agg(
      countDistinct(col("o.order_id")).alias("total_orders"),
      sum(col("oi.quantity") * col("oi.unit_price")).alias("total_revenue"),
      round(sum(col("oi.quantity") * col("oi.unit_price")) / countDistinct(col("o.order_id")), 2).alias("avg_order_value")
    )

  # Order by order_count descending
  final_df = result_df.orderBy(col("total_orders").desc())
  return final_df