import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, to_date, year, month, sum, first, countDistinct, max
from delta.tables import DeltaTable

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------------------------
# Paths
# ------------------------------------------------------------

raw_order_items_path = "s3://global-partners-data-bucket-001/raw/order_items/"
refined_dim_users_path = "s3://global-partners-data-bucket-001/refined/dim_users/"
refined_fact_orders_path = "s3://global-partners-data-bucket-001/refined/fact_orders/"

# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------

def delta_table_exists(spark_session, path):
    try:
        DeltaTable.forPath(spark_session, path)
        return True
    except Exception:
        return False


def merge_delta_table(
    spark_session,
    source_df,
    target_path,
    merge_condition,
    partition_cols=None
):
    if delta_table_exists(spark_session, target_path):
        target = DeltaTable.forPath(spark_session, target_path)

        (
            target.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    else:
        writer = source_df.write.format("delta").mode("overwrite")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(target_path)

# ------------------------------------------------------------
# Read raw order_items
# ------------------------------------------------------------

order_items = spark.read.format("delta").load(raw_order_items_path)

latest_load_date = order_items.agg(max("load_dts").alias("latest_load_date")).collect()[0]["latest_load_date"]

#Process latest raw data only
order_items = order_items.filter(col("load_dts") == latest_load_date)

# ------------------------------------------------------------
# Clean and filter source data
# ------------------------------------------------------------

order_items_2023 = (
    order_items
    .withColumn("creation_time_utc", col("creation_time_utc").cast("timestamp"))
    .withColumn("order_date", to_date(col("creation_time_utc")))
    .filter((col("order_date") >= "2023-01-01") & (col("order_date") < "2024-01-01"))
    .filter(col("app_name") != "Alltown Fresh - DEVELOPMENT")
    .withColumn("item_price", col("item_price").cast("decimal(10,2)"))
    .withColumn("item_quantity", col("item_quantity").cast("int"))
)

# ------------------------------------------------------------
# Aggregate to order-level grain
# ------------------------------------------------------------
# Grain: one row per order_id
#
# order_total = sum(item_price) grouped by order_id
# num_unique_lineitems = count distinct lineitem_id grouped by order_id
# num_order_items = sum(item_quantity) grouped by order_id
# ------------------------------------------------------------

fact_orders_base = (
    order_items_2023
    .groupBy("order_id")
    .agg(
        first("app_name", ignorenulls=True).alias("app_name"),
        first("restaurant_id", ignorenulls=True).alias("restaurant_id"),
        first("user_id", ignorenulls=True).alias("user_id"),
        first("creation_time_utc", ignorenulls=True).alias("order_timestamp"),
        first("order_date", ignorenulls=True).alias("order_date"),
        sum("item_price").alias("order_total"),
        countDistinct("lineitem_id").alias("num_unique_lineitems"),
        sum("item_quantity").alias("num_order_items"),
        first("currency", ignorenulls=True).alias("currency")
    )
    .withColumn("order_year", year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
)

# ------------------------------------------------------------
# Read SCD Type 2 dim_users
# ------------------------------------------------------------

dim_users = (
    spark.read.format("delta").load(refined_dim_users_path)
    .select(
        col("user_sk"),
        col("user_id"),
        col("effective_start_date"),
        col("effective_end_date")
    )
)

# ------------------------------------------------------------
# Join fact_orders to correct SCD2 user version
# ------------------------------------------------------------

fact_orders_with_user_sk = (
    fact_orders_base.alias("fact")
    .join(
        dim_users.alias("users"),
        (
            (col("fact.user_id") == col("users.user_id"))
            & (col("fact.order_date") >= col("users.effective_start_date"))
            & (
                col("users.effective_end_date").isNull()
                | (col("fact.order_date") <= col("users.effective_end_date"))
            )
        ),
        "left"
    )
    .select(
        col("fact.app_name").cast("string").alias("app_name"),
        col("fact.restaurant_id").cast("string").alias("restaurant_id"),
        col("fact.order_id").cast("string").alias("order_id"),
        col("users.user_sk").cast("string").alias("user_sk"),
        col("fact.user_id").cast("string").alias("user_id"),
        col("fact.order_date").cast("date").alias("order_date"),
        col("fact.order_timestamp").cast("timestamp").alias("order_timestamp"),
        col("fact.order_total").cast("decimal(10,2)").alias("order_total"),
        col("fact.num_unique_lineitems").cast("int").alias("num_unique_lineitems"),
        col("fact.num_order_items").cast("int").alias("num_order_items"),
        col("fact.currency").cast("string").alias("currency"),
        col("fact.order_year").cast("int").alias("order_year"),
        col("fact.order_month").cast("int").alias("order_month")
    )
    .dropDuplicates(["order_id"])
    .withColumn("updated_at", current_timestamp())
)

# ------------------------------------------------------------
# Merge into refined fact_orders Delta table
# ------------------------------------------------------------

merge_delta_table(
    spark_session=spark,
    source_df=fact_orders_with_user_sk,
    target_path=refined_fact_orders_path,
    merge_condition="target.order_id = source.order_id",
    partition_cols=["order_year", "order_month"]
)

job.commit()