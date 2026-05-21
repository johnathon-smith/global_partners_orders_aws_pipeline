import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, countDistinct, sum, avg, round, lit, current_timestamp, coalesce
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------------------------------------------------------
# Helper: Check Whether Delta Table Exists
# ---------------------------------------------------------

def delta_table_exists(spark, path):
    """
    Returns True if a Delta table exists at the provided S3 path.
    """
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False

# ---------------------------------------------------------
# Main Glue Job
# ---------------------------------------------------------

refined_fact_orders_path = "s3://global-partners-data-bucket-001/refined/fact_orders/"
refined_dim_users_path = "s3://global-partners-data-bucket-001/refined/dim_users/"
curated_loyalty_impact_path = "s3://global-partners-data-bucket-001/curated/loyalty_impact/"

# ---------------------------------------------------------
# Read Refined Tables
# ---------------------------------------------------------

fact_orders_df = (
    spark.read
        .format("delta")
        .load(refined_fact_orders_path)
)

dim_users_df = (
    spark.read
        .format("delta")
        .load(refined_dim_users_path)
)

# ---------------------------------------------------------
# Basic Validation
# ---------------------------------------------------------

fact_required_columns = [
    "user_sk",
    "user_id",
    "order_id",
    "order_total"
]

dim_users_required_columns = [
    "user_sk",
    "is_loyalty"
]

missing_fact_columns = [
    column_name
    for column_name in fact_required_columns
    if column_name not in fact_orders_df.columns
]

missing_dim_user_columns = [
    column_name
    for column_name in dim_users_required_columns
    if column_name not in dim_users_df.columns
]

if missing_fact_columns:
    raise Exception(
        f"Missing required columns in refined fact_orders: {missing_fact_columns}"
    )

if missing_dim_user_columns:
    raise Exception(
        f"Missing required columns in refined dim_users: {missing_dim_user_columns}"
    )

# ---------------------------------------------------------
# Prepare Fact Orders
# ---------------------------------------------------------

orders_df = (
    fact_orders_df
        .filter(col("user_sk").isNotNull())
        .filter(col("user_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .filter(col("order_total").isNotNull())
        .select(
            col("user_sk").cast("string"),
            col("user_id").cast("string"),
            col("order_id").cast("string"),
            col("order_total").cast(DecimalType(18, 2))
        )
)

# ---------------------------------------------------------
# Prepare User Dimension
# ---------------------------------------------------------
# Null loyalty values are treated as False for this portfolio project.
# This keeps the table focused on two comparison groups:
#   is_loyalty = True
#   is_loyalty = False

users_df = (
    dim_users_df
        .filter(col("user_sk").isNotNull())
        .select(
            col("user_sk").cast("string"),
            coalesce(col("is_loyalty").cast("boolean"), lit(False)).alias("is_loyalty")
        )
        .dropDuplicates(["user_sk"])
)

# ---------------------------------------------------------
# Join Orders to Point-in-Time Loyalty Status
# ---------------------------------------------------------
# Because fact_orders already contains the correct SCD2 user_sk,
# this join gives us the customer's loyalty status at the time of the order.

orders_with_loyalty_df = (
    orders_df
        .join(
            users_df,
            on="user_sk",
            how="inner"
        )
)

# ---------------------------------------------------------
# Calculate Group-Level Loyalty Impact Metrics
# ---------------------------------------------------------
# Grain: one row per is_loyalty value

loyalty_group_metrics_df = (
    orders_with_loyalty_df
        .groupBy("is_loyalty")
        .agg(
            countDistinct(col("user_id")).alias("customer_count"),
            countDistinct(col("order_id")).alias("total_orders"),
            round(sum(col("order_total")), 2).alias("total_revenue"),
            round(avg(col("order_total")), 2).alias("avg_order_value")
        )
        .withColumn(
            "avg_orders_per_customer",
            round(col("total_orders") / col("customer_count"), 2)
        )
        .withColumn(
            "revenue_per_customer",
            round(col("total_revenue") / col("customer_count"), 2)
        )
)

# ---------------------------------------------------------
# Calculate Average LTV by Loyalty Status
# ---------------------------------------------------------
# First calculate user-level LTV inside each loyalty group.
#
# Note:
# A user can theoretically appear in both groups if their loyalty status
# changed over time. That is expected because this table is based on
# point-in-time loyalty status at the order level.

user_ltv_by_loyalty_df = (
    orders_with_loyalty_df
        .groupBy(
            "is_loyalty",
            "user_id"
        )
        .agg(
            round(sum(col("order_total")), 2).alias("user_ltv")
        )
)

avg_ltv_by_loyalty_df = (
    user_ltv_by_loyalty_df
        .groupBy("is_loyalty")
        .agg(
            round(avg(col("user_ltv")), 2).alias("avg_ltv")
        )
)

# ---------------------------------------------------------
# Final Curated DataFrame
# ---------------------------------------------------------

final_loyalty_impact_df = (
    loyalty_group_metrics_df
        .join(
            avg_ltv_by_loyalty_df,
            on="is_loyalty",
            how="left"
        )
        .select(
            col("is_loyalty").cast("boolean"),
            col("customer_count").cast("int"),
            col("total_orders").cast("int"),
            col("total_revenue").cast(DecimalType(18, 2)),
            col("avg_ltv").cast(DecimalType(18, 2)),
            col("avg_order_value").cast(DecimalType(18, 2)),
            col("avg_orders_per_customer").cast(DecimalType(18, 2)),
            col("revenue_per_customer").cast(DecimalType(18, 2))
        )
        .withColumn("updated_at", current_timestamp())
)

# ---------------------------------------------------------
# Write / Merge to Curated Delta Table
# ---------------------------------------------------------
# Merge key: is_loyalty
#
# This table should usually have two rows:
#   is_loyalty = true
#   is_loyalty = false

if delta_table_exists(spark, curated_loyalty_impact_path):

    target_delta_table = DeltaTable.forPath(spark, curated_loyalty_impact_path)

    (
        target_delta_table.alias("target")
            .merge(
                final_loyalty_impact_df.alias("source"),
                "target.is_loyalty = source.is_loyalty"
            )
            .whenMatchedUpdate(set={
                "customer_count": "source.customer_count",
                "total_orders": "source.total_orders",
                "total_revenue": "source.total_revenue",
                "avg_ltv": "source.avg_ltv",
                "avg_order_value": "source.avg_order_value",
                "avg_orders_per_customer": "source.avg_orders_per_customer",
                "revenue_per_customer": "source.revenue_per_customer",
                "updated_at": "source.updated_at"
            })
            .whenNotMatchedInsert(values={
                "is_loyalty": "source.is_loyalty",
                "customer_count": "source.customer_count",
                "total_orders": "source.total_orders",
                "total_revenue": "source.total_revenue",
                "avg_ltv": "source.avg_ltv",
                "avg_order_value": "source.avg_order_value",
                "avg_orders_per_customer": "source.avg_orders_per_customer",
                "revenue_per_customer": "source.revenue_per_customer",
                "updated_at": "source.updated_at"
            })
            .execute()
    )

else:

    (
        final_loyalty_impact_df.write
            .format("delta")
            .mode("overwrite")
            .save(curated_loyalty_impact_path)
    )

job.commit()