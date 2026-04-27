import dlt

from pyspark.sql.functions import col, when, ntile
from pyspark.sql.window import Window

@dlt.table(name = 'customer_revenue_segment')
def customer_revenue_segment():
    
    # Load tables (replace with your actual catalog/schema if needed)
    customer_df = spark.table("dim_customers")
    orders_df = spark.table("order_facts")

    # Step 1: customer_revenue
    customer_revenue_df = (
        customer_df.alias("c")
        .join(orders_df.alias("o"), col("c.customer_id") == col("o.customer_id"))
        .select(
            col("c.customer_id"),
            col("c.customer_name"),
            col("o.revenue").alias("revenue")
        )
    )

    # Step 2: revenue_percentiles (NTILE 5)
    window_spec = Window.orderBy(col("revenue").desc())

    revenue_percentiles_df = (
        customer_revenue_df
        .withColumn("revenue_bucket", ntile(5).over(window_spec))
    )

    # Step 3: Final segmentation
    final_df = (
        revenue_percentiles_df
        .withColumn(
            "segment",
            when(col("revenue_bucket") == 1, "High Value")
            .when(col("revenue_bucket").isin(2, 3, 4), "Medium Value")
            .when(col("revenue_bucket") == 5, "Low Value")
        )
        .select("customer_id", "customer_name", "revenue", "segment")
        .orderBy(col("revenue").desc())
    )

    # Show result
    return final_df