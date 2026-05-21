import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Window
from pyspark.sql.functions import col, countDistinct, sum, avg, min, max, datediff, date_trunc, dense_rank, lit, round, current_timestamp
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
curated_location_performance_path = "s3://global-partners-data-bucket-001/curated/location_performance/"

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
    "restaurant_id",
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
# Prepare Base Orders
# ---------------------------------------------------------

orders_df = (
    fact_orders_df
        .filter(col("restaurant_id").isNotNull())
        .filter(col("order_id").isNotNull())
        .filter(col("order_date").isNotNull())
        .filter(col("order_total").isNotNull())
        .select(
            col("restaurant_id").cast("string"),
            col("order_id").cast("string"),
            col("order_date"),
            col("order_total").cast(DecimalType(18, 2))
        )
)

# ---------------------------------------------------------
# Calculate Analysis Period
# ---------------------------------------------------------
# This table is meant to summarize restaurant performance across
# the full available analysis period.
#
# For this project, that should generally be 2023, but calculating
# it from the data makes the job more flexible.

analysis_period_row = (
    orders_df
        .agg(
            min(col("order_date")).alias("min_order_date"),
            max(col("order_date")).alias("max_order_date")
        )
        .collect()[0]
)

min_order_date = analysis_period_row["min_order_date"]
max_order_date = analysis_period_row["max_order_date"]

if min_order_date is None or max_order_date is None:
    raise Exception("Unable to calculate analysis period because order_date values are missing.")

analysis_days_row = (
    orders_df
        .select(
            datediff(
                lit(max_order_date),
                lit(min_order_date)
            ).alias("date_diff")
        )
        .limit(1)
        .collect()[0]
)

analysis_days = analysis_days_row["date_diff"] + 1

analysis_weeks = (
    orders_df
        .select(
            date_trunc("week", col("order_date")).alias("week_start")
        )
        .distinct()
        .count()
)

if analysis_days <= 0:
    raise Exception(f"Invalid analysis_days value: {analysis_days}")

if analysis_weeks <= 0:
    raise Exception(f"Invalid analysis_weeks value: {analysis_weeks}")

# ---------------------------------------------------------
# Aggregate Location Performance Metrics
# ---------------------------------------------------------
# Grain: one row per restaurant_id

location_metrics_df = (
    orders_df
        .groupBy("restaurant_id")
        .agg(
            round(sum(col("order_total")), 2).alias("total_revenue"),
            round(avg(col("order_total")), 2).alias("avg_order_value"),
            countDistinct(col("order_id")).alias("total_orders")
        )
        .withColumn(
            "avg_orders_per_day",
            round(col("total_orders") / lit(analysis_days), 2)
        )
        .withColumn(
            "avg_orders_per_week",
            round(col("total_orders") / lit(analysis_weeks), 2)
        )
)

# ---------------------------------------------------------
# Calculate Revenue Rank
# ---------------------------------------------------------
# dense_rank ensures there are no gaps in ranking values.
#
# Example:
#   Restaurant A: rank 1
#   Restaurant B: rank 2
#   Restaurant C: rank 2
#   Restaurant D: rank 3

revenue_rank_window = Window.orderBy(col("total_revenue").desc())

location_ranked_df = (
    location_metrics_df
        .withColumn(
            "revenue_rank",
            dense_rank().over(revenue_rank_window)
        )
)

# ---------------------------------------------------------
# Final Curated DataFrame
# ---------------------------------------------------------

final_location_performance_df = (
    location_ranked_df
        .select(
            col("restaurant_id").cast("string"),
            col("total_revenue").cast(DecimalType(18, 2)),
            col("avg_order_value").cast(DecimalType(18, 2)),
            col("avg_orders_per_day").cast(DecimalType(18, 2)),
            col("avg_orders_per_week").cast(DecimalType(18, 2)),
            col("revenue_rank").cast("int")
        )
        .withColumn("updated_at", current_timestamp())
)

# ---------------------------------------------------------
# Write / Merge to Curated Delta Table
# ---------------------------------------------------------
# Merge key: restaurant_id
#
# This keeps one performance record per restaurant.
# If the restaurant's metrics change, the existing row is updated.
# If a new restaurant appears, a new row is inserted.

if delta_table_exists(spark, curated_location_performance_path):

    target_delta_table = DeltaTable.forPath(spark, curated_location_performance_path)

    (
        target_delta_table.alias("target")
            .merge(
                final_location_performance_df.alias("source"),
                "target.restaurant_id = source.restaurant_id"
            )
            .whenMatchedUpdate(set={
                "total_revenue": "source.total_revenue",
                "avg_order_value": "source.avg_order_value",
                "avg_orders_per_day": "source.avg_orders_per_day",
                "avg_orders_per_week": "source.avg_orders_per_week",
                "revenue_rank": "source.revenue_rank",
                "updated_at": "source.updated_at"
            })
            .whenNotMatchedInsert(values={
                "restaurant_id": "source.restaurant_id",
                "total_revenue": "source.total_revenue",
                "avg_order_value": "source.avg_order_value",
                "avg_orders_per_day": "source.avg_orders_per_day",
                "avg_orders_per_week": "source.avg_orders_per_week",
                "revenue_rank": "source.revenue_rank",
                "updated_at": "source.updated_at"
            })
            .execute()
    )

else:

    (
        final_location_performance_df.write
            .format("delta")
            .mode("overwrite")
            .save(curated_location_performance_path)
    )

job.commit()