import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, countDistinct, sum, max, datediff, date_sub, lit, current_timestamp, when, percent_rank, coalesce, round
from pyspark.sql import Window
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
curated_customer_segmentation_path = "s3://global-partners-data-bucket-001/curated/customer_segment/"

# ---------------------------------------------------------
# Read Refined fact_orders
# ---------------------------------------------------------

fact_orders_df = (
    spark.read
        .format("delta")
        .load(refined_fact_orders_path)
)

# ---------------------------------------------------------
# Basic Validation
# ---------------------------------------------------------

required_columns = [
    "user_id",
    "order_id",
    "order_date",
    "order_total"
]

missing_columns = [
    column_name
    for column_name in required_columns
    if column_name not in fact_orders_df.columns
]

if missing_columns:
    raise Exception(
        f"Missing required columns in refined fact_orders: {missing_columns}"
    )

# ---------------------------------------------------------
# Define Analysis Anchor Date
# ---------------------------------------------------------
# Because this project focuses on historical 2023 data, we should not use
# current_date(). If we used the real current date, every customer would look
# inactive/churned because the orders are historical.
#
# Instead, use the max order_date in fact_orders as the analysis date.

anchor_date_row = (
    fact_orders_df
        .select(max(col("order_date")).alias("anchor_date"))
        .collect()[0]
)

anchor_date = anchor_date_row["anchor_date"]

if anchor_date is None:
    raise Exception("Unable to calculate anchor_date because fact_orders has no order_date values.")

# ---------------------------------------------------------
# Prepare Base Orders
# ---------------------------------------------------------

orders_df = (
    fact_orders_df
        .filter(col("user_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .filter(col("order_date").isNotNull())
        .filter(col("order_total").isNotNull())
        .select(
            col("user_id").cast("string"),
            col("order_id").cast("string"),
            col("order_date"),
            col("order_total").cast(DecimalType(18, 2))
        )
)

# ---------------------------------------------------------
# Calculate Last Order Date and Days Since Last Order
# ---------------------------------------------------------

last_order_df = (
    orders_df
        .groupBy("user_id")
        .agg(
            max(col("order_date")).alias("last_order_date")
        )
        .withColumn(
            "days_since_last_order",
            datediff(lit(anchor_date), col("last_order_date"))
        )
)

# ---------------------------------------------------------
# Calculate Last 3 Months Metrics
# ---------------------------------------------------------
# Last 3 months means orders where:
# order_date >= anchor_date - 90 days
#
# This keeps the project simple and avoids month-boundary complexity.

last_3_months_orders_df = (
    orders_df
        .filter(
            col("order_date") >= date_sub(lit(anchor_date), 90)
        )
)

last_3_months_metrics_df = (
    last_3_months_orders_df
        .groupBy("user_id")
        .agg(
            countDistinct(col("order_id")).alias("num_orders_last_3_months"),
            round(sum(col("order_total")), 2).alias("total_spend_last_3_months")
        )
)

# ---------------------------------------------------------
# Combine User Metrics
# ---------------------------------------------------------

customer_metrics_df = (
    last_order_df
        .join(
            last_3_months_metrics_df,
            on="user_id",
            how="left"
        )
        .withColumn(
            "num_orders_last_3_months",
            coalesce(col("num_orders_last_3_months"), lit(0))
        )
        .withColumn(
            "total_spend_last_3_months",
            coalesce(col("total_spend_last_3_months"), lit(0.00))
        )
)

# ---------------------------------------------------------
# Calculate Percentile Rankings for VIP Logic
# ---------------------------------------------------------
# VIP requires:
# - high recent order count
# - high recent spend
# - low days since last order
#
# We define "high" as being in the top 25%.
#
# percent_rank() returns:
# - 0 for the lowest-ranked row
# - 1 for the highest-ranked row
#
# So users with percentile >= 0.75 are in the top 25%.

frequency_window = Window.orderBy(col("num_orders_last_3_months").asc())
spend_window = Window.orderBy(col("total_spend_last_3_months").asc())

customer_ranked_df = (
    customer_metrics_df
        .withColumn(
            "frequency_percentile",
            percent_rank().over(frequency_window)
        )
        .withColumn(
            "spend_percentile",
            percent_rank().over(spend_window)
        )
)

# ---------------------------------------------------------
# Assign Customer Segment
# ---------------------------------------------------------
# Segment priority:
#   1. VIP
#   2. New Customer
#   3. Churn Risk
#   4. Active
#
# Business rules:
#
# VIP:
#   frequency_percentile >= 0.75
#   spend_percentile >= 0.75
#   days_since_last_order <= 30
#
# New Customer:
#   days_since_last_order <= 30
#   num_orders_last_3_months <= 1
#
# Churn Risk:
#   days_since_last_order >= 60
#   num_orders_last_3_months <= 1
#
# Active:
#   everyone else

customer_segmentation_df = (
    customer_ranked_df
        .withColumn(
            "segment",
            when(
                (col("frequency_percentile") >= lit(0.75)) &
                (col("spend_percentile") >= lit(0.75)) &
                (col("days_since_last_order") <= lit(30)),
                lit("VIP")
            )
            .when(
                (col("days_since_last_order") <= lit(30)) &
                (col("num_orders_last_3_months") <= lit(1)),
                lit("New Customer")
            )
            .when(
                (col("days_since_last_order") >= lit(60)) &
                (col("num_orders_last_3_months") <= lit(1)),
                lit("Churn Risk")
            )
            .otherwise(lit("Active"))
        )
)

# ---------------------------------------------------------
# Final Curated DataFrame
# ---------------------------------------------------------

final_customer_segmentation_df = (
    customer_segmentation_df
        .select(
            col("user_id").cast("string"),
            col("days_since_last_order").cast("int"),
            col("num_orders_last_3_months").cast("int"),
            col("total_spend_last_3_months").cast(DecimalType(18, 2)),
            col("segment").cast("string")
        )
        .withColumn("updated_at", current_timestamp())
)

# ---------------------------------------------------------
# Write / Merge to Curated Delta Table
# ---------------------------------------------------------
# Merge key: user_id
#
# This maintains one customer segmentation record per user.
# If the user's segment changes, the existing row is updated.
# If a new user appears, a new row is inserted.

if delta_table_exists(spark, curated_customer_segmentation_path):

    target_delta_table = DeltaTable.forPath(spark, curated_customer_segmentation_path)

    (
        target_delta_table.alias("target")
            .merge(
                final_customer_segmentation_df.alias("source"),
                "target.user_id = source.user_id"
            )
            .whenMatchedUpdate(set={
                "days_since_last_order": "source.days_since_last_order",
                "num_orders_last_3_months": "source.num_orders_last_3_months",
                "total_spend_last_3_months": "source.total_spend_last_3_months",
                "segment": "source.segment",
                "updated_at": "source.updated_at"
            })
            .whenNotMatchedInsert(values={
                "user_id": "source.user_id",
                "days_since_last_order": "source.days_since_last_order",
                "num_orders_last_3_months": "source.num_orders_last_3_months",
                "total_spend_last_3_months": "source.total_spend_last_3_months",
                "segment": "source.segment",
                "updated_at": "source.updated_at"
            })
            .execute()
    )

else:

    (
        final_customer_segmentation_df.write
            .format("delta")
            .mode("overwrite")
            .save(curated_customer_segmentation_path)
    )

job.commit()