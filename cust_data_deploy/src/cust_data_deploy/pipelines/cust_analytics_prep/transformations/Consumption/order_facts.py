import dlt
from pyspark.sql.functions import *

@dlt.table(name = 'order_facts')
def order_facts():
    orders_df = spark.read.table('fact_orders')
    order_items_df = spark.read.table('order_items')

    # Perform joins
    df = orders_df.alias('o').join(order_items_df.alias('oi'), col('o.order_id') ==col('oi.order_id'))

    # Aggregations
    result_df = df.groupBy(
    col('o.order_id'),
    col('o.customer_id'),
    col('o.employee_id'),
    col('oi.product_id'),
    col('o.order_date'),
    col('o.updated_date')
    ).agg(
    countDistinct(col("o.order_id")).alias("order_count"),
    sum(col("oi.quantity") * col("oi.unit_price")).alias("revenue")
    )

# Order by order_count descending
    final_df = result_df.orderBy(col("order_count").desc())
    return final_df


