import dlt
from pyspark.sql.functions import *

@dlt.table(name = 'customer_orders')
def customer_orders():
    cust_df = spark.read.table('dim_customers')
    orders_df = spark.read.table('fact_orders')

    # Joins
    df = cust_df.alias('c').join(orders_df.alias('o'), col('c.customer_id') ==col('o.customer_id')
)
    # Aggregation
    result_df = df.groupBy(
    col('c.customer_id'),
    col('c.customer_name'),
    col('o.order_date'),
    col('o.updated_date')
    ).agg(countDistinct(col("o.order_id")).alias("total_orders"))

    # orders descending by total_orders

    final_df = result_df.orderBy('total_orders')
    return final_df



