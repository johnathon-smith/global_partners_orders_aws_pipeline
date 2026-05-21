import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, count, avg, sum, max, lag, datediff, date_sub, lit, current_timestamp, when, round
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
curated_churn_risk_path = "s3://global-partners-data-bucket-001/curated/customer_churn_risk/"

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
# Because this project uses historical 2023 data, do not use current_date().
# Use the max order_date in fact_orders as the analytical "as of" date.

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
# Calculate Days Since Last Order
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
# Calculate Average Days Between Orders
# ---------------------------------------------------------
# Uses distinct order dates per user.
#
# If a user has only one order date, avg_days_between_orders will be null
# because there is no previous order to compare against.

user_order_dates_df = (
    orders_df
        .select("user_id", "order_date")
        .distinct()
)

user_order_window = Window.partitionBy("user_id").orderBy("order_date")

order_gaps_df = (
    user_order_dates_df
        .withColumn(
            "previous_order_date",
            lag(col("order_date")).over(user_order_window)
        )
        .withColumn(
            "days_between_orders",
            datediff(col("order_date"), col("previous_order_date"))
        )
        .filter(col("previous_order_date").isNotNull())
)

avg_days_between_orders_df = (
    order_gaps_df
        .groupBy("user_id")
        .agg(
            round(avg(col("days_between_orders")), 2).alias("avg_days_between_orders")
        )
)

# ---------------------------------------------------------
# Calculate Percent Change in Spend
# ---------------------------------------------------------
# recent quarter:
#   anchor_date - 89 days through anchor_date
#
# previous quarter:
#   anchor_date - 179 days through anchor_date - 90 days
#
# This creates two rolling 90-day periods.

recent_quarter_start = date_sub(lit(anchor_date), 89)
previous_quarter_start = date_sub(lit(anchor_date), 179)
previous_quarter_end = date_sub(lit(anchor_date), 90)

spend_by_period_df = (
    orders_df
        .groupBy("user_id")
        .agg(
            round(
                sum(
                    when(
                        col("order_date") >= recent_quarter_start,
                        col("order_total")
                    ).otherwise(lit(0.00))
                ),
                2
            ).alias("recent_quarter_spend"),

            round(
                sum(
                    when(
                        (col("order_date") >= previous_quarter_start) &
                        (col("order_date") <= previous_quarter_end),
                        col("order_total")
                    ).otherwise(lit(0.00))
                ),
                2
            ).alias("previous_quarter_spend")
        )
)

spend_change_df = (
    spend_by_period_df
        .withColumn(
            "percent_change_in_spend",
            when(
                col("previous_quarter_spend") == lit(0),
                lit(None).cast(DecimalType(18, 2))
            )
            .otherwise(
                round(
                    (
                        (col("recent_quarter_spend") - col("previous_quarter_spend"))
                        / col("previous_quarter_spend")
                    ) * lit(100),
                    2
                )
            )
        )
)

# ---------------------------------------------------------
# Combine Churn Risk Metrics
# ---------------------------------------------------------

churn_risk_metrics_df = (
    last_order_df
        .join(
            avg_days_between_orders_df,
            on="user_id",
            how="left"
        )
        .join(
            spend_change_df.select(
                "user_id",
                "percent_change_in_spend"
            ),
            on="user_id",
            how="left"
        )
)

# ---------------------------------------------------------
# Calculate at_risk Flag
# ---------------------------------------------------------
# Business rule:
#   if days_since_last_order >= 45, at_risk = True
#   otherwise, at_risk = False

final_churn_risk_df = (
    churn_risk_metrics_df
        .withColumn(
            "at_risk",
            when(
                col("days_since_last_order") >= lit(45),
                lit(True)
            ).otherwise(lit(False))
        )
        .select(
            col("user_id").cast("string"),
            col("days_since_last_order").cast("int"),
            col("avg_days_between_orders").cast(DecimalType(18, 2)),
            col("percent_change_in_spend").cast(DecimalType(18, 2)),
            col("at_risk").cast("boolean")
        )
        .withColumn("updated_at", current_timestamp())
)

# ---------------------------------------------------------
# Write / Merge to Curated Delta Table
# ---------------------------------------------------------
# Merge key: user_id
#
# This keeps one churn risk record per customer.
# If the customer's churn risk metrics change, the existing row is updated.
# If a new customer appears, a new row is inserted.

if delta_table_exists(spark, curated_churn_risk_path):

    target_delta_table = DeltaTable.forPath(spark, curated_churn_risk_path)

    (
        target_delta_table.alias("target")
            .merge(
                final_churn_risk_df.alias("source"),
                "target.user_id = source.user_id"
            )
            .whenMatchedUpdate(set={
                "days_since_last_order": "source.days_since_last_order",
                "avg_days_between_orders": "source.avg_days_between_orders",
                "percent_change_in_spend": "source.percent_change_in_spend",
                "at_risk": "source.at_risk",
                "updated_at": "source.updated_at"
            })
            .whenNotMatchedInsert(values={
                "user_id": "source.user_id",
                "days_since_last_order": "source.days_since_last_order",
                "avg_days_between_orders": "source.avg_days_between_orders",
                "percent_change_in_spend": "source.percent_change_in_spend",
                "at_risk": "source.at_risk",
                "updated_at": "source.updated_at"
            })
            .execute()
    )

else:

    (
        final_churn_risk_df.write
            .format("delta")
            .mode("overwrite")
            .save(curated_churn_risk_path)
    )

job.commit()