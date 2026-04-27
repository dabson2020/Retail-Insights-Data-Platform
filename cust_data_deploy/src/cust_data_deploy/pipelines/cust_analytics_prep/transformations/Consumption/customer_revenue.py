import dlt
from pyspark.sql.functions import *

@dlt.table(name = 'customer_revenue')
def customer_revenue():
    order_facts = dlt.read('order_facts')
    customer_stg = dlt.read('dim_customers')

    #joins
    result_df = customer_stg.alias('c').join(order_facts.alias('o'), col('c.customer_id') == col('o.customer_id'))

    #aggregates
    result_df = result_df.groupBy(col('c.customer_id'), col('c.customer_name')).agg(
        sum(col('o.revenue')).alias('revenue'), 
        countDistinct(col('o.order_id')).alias('order_count'))
    
    final_df = result_df.orderBy(col("order_count").desc())
    return final_df

