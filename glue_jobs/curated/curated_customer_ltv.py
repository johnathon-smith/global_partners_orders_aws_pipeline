import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, countDistinct, sum, avg, round, ntile, when, current_timestamp, lit
from pyspark.sql.window import Window
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
curated_customer_ltv_path = "s3://global-partners-data-bucket-001/curated/customer_ltv/"

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
# Build Customer LTV Aggregation
# ---------------------------------------------------------
# Grain: one row per user_id
#
# ltv = total revenue by user
# num_orders = distinct order count by user
# avg_order_value = average order_total by user

customer_ltv_base_df = (
    fact_orders_df
        .filter(col("user_id").isNotNull())
        .groupBy("user_id")
        .agg(
            round(sum(col("order_total")), 2).alias("ltv"),
            countDistinct(col("order_id")).alias("num_orders"),
            round(avg(col("order_total")), 2).alias("avg_order_value")
        )
)

# ---------------------------------------------------------
# Assign LTV Category
# ---------------------------------------------------------
# Business rule:
#   Top 20% by LTV    = High
#   Middle 60% by LTV = Medium
#   Bottom 20% by LTV = Low
#
# ntile(5) splits users into five ranked buckets by LTV.
# Bucket 1 = top 20%
# Buckets 2, 3, 4 = middle 60%
# Bucket 5 = bottom 20%

ltv_window = Window.orderBy(col("ltv").desc())

customer_ltv_ranked_df = (
    customer_ltv_base_df
        .withColumn("ltv_bucket", ntile(5).over(ltv_window))
        .withColumn(
            "ltv_category",
            when(col("ltv_bucket") == 1, lit("High"))
            .when(col("ltv_bucket") == 5, lit("Low"))
            .otherwise(lit("Medium"))
        )
)

# ---------------------------------------------------------
# Final Curated DataFrame
# ---------------------------------------------------------

customer_ltv_df = (
    customer_ltv_ranked_df
        .select(
            col("user_id").cast("string"),
            col("ltv").cast(DecimalType(18, 2)),
            col("ltv_category").cast("string"),
            col("num_orders").cast("int"),
            col("avg_order_value").cast(DecimalType(18, 2))
        )
        .withColumn("updated_at", current_timestamp())
)

# ---------------------------------------------------------
# Write / Merge to Curated Delta Table
# ---------------------------------------------------------
# Merge key: user_id
#
# This keeps one current customer_ltv record per user.
# If the customer's LTV changes on a future run, the existing row is updated.
# If a new customer appears, a new row is inserted.

if delta_table_exists(spark, curated_customer_ltv_path):

    target_delta_table = DeltaTable.forPath(spark, curated_customer_ltv_path)

    (
        target_delta_table.alias("target")
            .merge(
                customer_ltv_df.alias("source"),
                "target.user_id = source.user_id"
            )
            .whenMatchedUpdate(set={
                "ltv": "source.ltv",
                "ltv_category": "source.ltv_category",
                "num_orders": "source.num_orders",
                "avg_order_value": "source.avg_order_value",
                "updated_at": "source.updated_at"
            })
            .whenNotMatchedInsert(values={
                "user_id": "source.user_id",
                "ltv": "source.ltv",
                "ltv_category": "source.ltv_category",
                "num_orders": "source.num_orders",
                "avg_order_value": "source.avg_order_value",
                "updated_at": "source.updated_at"
            })
            .execute()
    )

else:

    (
        customer_ltv_df.write
            .format("delta")
            .mode("overwrite")
            .save(curated_customer_ltv_path)
    )


job.commit()