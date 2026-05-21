import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, countDistinct, sum, avg, hour, when, lit, round, current_timestamp, coalesce
from datetime import datetime
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
refined_dim_dates_path = "s3://global-partners-data-bucket-001/refined/dim_dates/"
curated_order_timing_analysis_path = "s3://global-partners-data-bucket-001/curated/order_timing_analysis/"

# ---------------------------------------------------------
# Read Refined Tables
# ---------------------------------------------------------

fact_orders_df = (
    spark.read
        .format("delta")
        .load(refined_fact_orders_path)
)

dim_dates_df = (
    spark.read
        .format("delta")
        .load(refined_dim_dates_path)
)

# ---------------------------------------------------------
# Basic Validation
# ---------------------------------------------------------

fact_required_columns = [
    "order_id",
    "user_id",
    "order_date",
    "order_timestamp",
    "order_total",
    "order_year",
    "order_month"
]

date_required_columns = [
    "date_key",
    "day_of_week",
    "month",
    "is_weekend",
    "is_holiday",
    "holiday_name"
]

missing_fact_columns = [
    column_name
    for column_name in fact_required_columns
    if column_name not in fact_orders_df.columns
]

missing_date_columns = [
    column_name
    for column_name in date_required_columns
    if column_name not in dim_dates_df.columns
]

if missing_fact_columns:
    raise Exception(
        f"Missing required columns in refined fact_orders: {missing_fact_columns}"
    )

if missing_date_columns:
    raise Exception(
        f"Missing required columns in refined dim_dates: {missing_date_columns}"
    )

# ---------------------------------------------------------
# Prepare Fact Orders
# ---------------------------------------------------------

orders_df = (
    fact_orders_df
        .filter(col("order_id").isNotNull())
        .filter(col("order_date").isNotNull())
        .filter(col("order_timestamp").isNotNull())
        .filter(col("order_total").isNotNull())
        .select(
            col("order_id").cast("string"),
            col("user_id").cast("string"),
            col("order_date").cast("date"),
            col("order_timestamp").cast("timestamp"),
            col("order_total").cast(DecimalType(18, 2)),
            col("order_year").cast("int"),
            col("order_month").cast("int")
        )
)

# ---------------------------------------------------------
# Prepare Date Dimension
# ---------------------------------------------------------

dates_df = (
    dim_dates_df
        .filter(col("date_key").isNotNull())
        .select(
            col("date_key").cast("date").alias("date_key"),
            col("day_of_week").cast("string"),
            col("month").cast("string"),
            col("is_weekend").cast("boolean"),
            col("is_holiday").cast("boolean"),
            col("holiday_name").cast("string")
        )
        .dropDuplicates(["date_key"])
)

# ---------------------------------------------------------
# Add Daypart to Orders
# ---------------------------------------------------------
# Daypart business rules:
#
# Morning:
#   6:00 AM through 10:59 AM
#
# Lunch:
#   11:00 AM through 1:59 PM
#
# Afternoon:
#   2:00 PM through 4:59 PM
#
# Dinner:
#   5:00 PM through 8:59 PM
#
# Late Night:
#   9:00 PM through 5:59 AM

orders_with_daypart_df = (
    orders_df
        .withColumn("order_hour", hour(col("order_timestamp")))
        .withColumn(
            "daypart",
            when(
                (col("order_hour") >= lit(6)) &
                (col("order_hour") <= lit(10)),
                lit("Morning")
            )
            .when(
                (col("order_hour") >= lit(11)) &
                (col("order_hour") <= lit(13)),
                lit("Lunch")
            )
            .when(
                (col("order_hour") >= lit(14)) &
                (col("order_hour") <= lit(16)),
                lit("Afternoon")
            )
            .when(
                (col("order_hour") >= lit(17)) &
                (col("order_hour") <= lit(20)),
                lit("Dinner")
            )
            .otherwise(lit("Late Night"))
        )
)

# ---------------------------------------------------------
# Join Orders to Date Dimension
# ---------------------------------------------------------
# This enriches order activity with calendar, weekend, and holiday fields.

orders_with_dates_df = (
    orders_with_daypart_df
        .join(
            dates_df,
            orders_with_daypart_df["order_date"] == dates_df["date_key"],
            how="left"
        )
)

# ---------------------------------------------------------
# Build Order Timing Analysis
# ---------------------------------------------------------
# Grain:
#   one row per order_date + daypart

order_timing_analysis_df = (
    orders_with_dates_df
        .groupBy(
            col("order_date"),
            col("order_year"),
            col("order_month"),
            col("day_of_week"),
            col("month"),
            col("is_weekend"),
            col("is_holiday"),
            col("holiday_name"),
            col("daypart")
        )
        .agg(
            countDistinct(col("order_id")).alias("total_orders"),
            round(sum(col("order_total")), 2).alias("total_revenue"),
            round(avg(col("order_total")), 2).alias("avg_order_value"),
            countDistinct(col("user_id")).alias("unique_customers")
        )
)

# ---------------------------------------------------------
# Final Curated DataFrame
# ---------------------------------------------------------

final_order_timing_analysis_df = (
    order_timing_analysis_df
        .select(
            col("order_date").cast("date"),
            col("order_year").cast("int"),
            col("order_month").cast("int"),
            col("day_of_week").cast("string"),
            col("month").cast("string"),
            coalesce(col("is_weekend"), lit(False)).cast("boolean").alias("is_weekend"),
            coalesce(col("is_holiday"), lit(False)).cast("boolean").alias("is_holiday"),
            col("holiday_name").cast("string"),
            col("daypart").cast("string"),
            col("total_orders").cast("int"),
            col("total_revenue").cast(DecimalType(18, 2)),
            col("avg_order_value").cast(DecimalType(18, 2)),
            col("unique_customers").cast("int")
        )
        .withColumn("updated_at", current_timestamp())
)

# ---------------------------------------------------------
# Write / Merge to Curated Delta Table
# ---------------------------------------------------------
# Merge key:
#   order_date + daypart
#
# This keeps one summary row per date and daypart.
# If metrics change on rerun, the existing row is updated.
# If a new date/daypart appears, a new row is inserted.

if delta_table_exists(spark, curated_order_timing_analysis_path):

    target_delta_table = DeltaTable.forPath(spark, curated_order_timing_analysis_path)

    (
        target_delta_table.alias("target")
            .merge(
                final_order_timing_analysis_df.alias("source"),
                """
                target.order_date = source.order_date
                AND target.daypart = source.daypart
                """
            )
            .whenMatchedUpdate(set={
                "order_year": "source.order_year",
                "order_month": "source.order_month",
                "day_of_week": "source.day_of_week",
                "month": "source.month",
                "is_weekend": "source.is_weekend",
                "is_holiday": "source.is_holiday",
                "holiday_name": "source.holiday_name",
                "total_orders": "source.total_orders",
                "total_revenue": "source.total_revenue",
                "avg_order_value": "source.avg_order_value",
                "unique_customers": "source.unique_customers",
                "updated_at": "source.updated_at"
            })
            .whenNotMatchedInsert(values={
                "order_date": "source.order_date",
                "order_year": "source.order_year",
                "order_month": "source.order_month",
                "day_of_week": "source.day_of_week",
                "month": "source.month",
                "is_weekend": "source.is_weekend",
                "is_holiday": "source.is_holiday",
                "holiday_name": "source.holiday_name",
                "daypart": "source.daypart",
                "total_orders": "source.total_orders",
                "total_revenue": "source.total_revenue",
                "avg_order_value": "source.avg_order_value",
                "unique_customers": "source.unique_customers",
                "updated_at": "source.updated_at"
            })
            .execute()
    )

else:

    (
        final_order_timing_analysis_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("order_year", "order_month")
            .save(curated_order_timing_analysis_path)
    )

job.commit()