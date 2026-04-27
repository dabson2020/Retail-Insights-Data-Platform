import dlt
from pyspark.sql.functions import countDistinct, col, sum as _sum

@dlt.table(name = 'product_performance')
def product_performance():
    prod_df = spark.table('dim_products')
    order_items_df = spark.table('order_items')

    #joins

    result_df = prod_df.alias('p').join(order_items_df.alias('oi'), col('p.product_id') ==col('oi.product_id'))

    # Aggregation
    result_df = result_df.groupBy(
        col('p.product_id'),
        col('p.product_name'),
        col('oi.unit_price'),
        col('p.updated_date')
        ).agg(countDistinct(col("oi.order_item_id")).alias("total_orders"),
            _sum(col("oi.quantity")).alias("total_quantity_sold"),
            _sum((col("oi.quantity") * col("oi.unit_price"))).alias("revenue") 
        ) 
        

    # order by total orders
    final_df = result_df.orderBy('total_orders')
    return final_df

