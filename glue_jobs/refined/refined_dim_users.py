import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, to_date, max, lit, sha2, concat_ws, coalesce, lag, lead, row_number, date_sub
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ------------------------------------------------------------
# Glue setup
# ------------------------------------------------------------

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

# ------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------

def delta_table_exists(spark, path):
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False

def build_user_change_events(order_items_df):
    """
    Creates one row per detected user loyalty state change.
    """
    
    users_raw = (
        order_items_df
        .withColumn("creation_time_utc", col("creation_time_utc").cast("timestamp"))
        .withColumn("order_date", to_date(col("creation_time_utc")))
        .filter((col("order_date") >= "2023-01-01") & (col("order_date") < "2024-01-01"))
        .filter(col("app_name") != "Alltown Fresh - DEVELOPMENT")
        .filter(col("user_id").isNotNull())
        .select(
            col("user_id").cast("string").alias("user_id"),
            col("printed_card_number").cast("string").alias("printed_card_number"),
            col("is_loyalty").cast("boolean").alias("is_loyalty"),
            col("order_date"),
            col("creation_time_utc")
        )
    )
    
    # IF THE SAME USER APPEARS MULTIPLE TIMES ON THE SAME DAY, KEEP THE LATEST STATE OF THAT DAY
    daily_window = (
        Window
        .partitionBy("user_id", "order_date")
        .orderBy(col("creation_time_utc").desc())
    )
    
    users_daily = (
        users_raw
        .withColumn("rn", row_number().over(daily_window))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("printed_card_number"), lit("")),
                    coalesce(col("is_loyalty").cast("string"), lit(""))
                ),
                256
            )
        )
    )
    
    #Detect changes compared to the previous known user state.
    change_window = Window.partitionBy("user_id").orderBy("order_date")
    
    user_changes = (
        users_daily
        .withColumn("previous_record_hash", lag("record_hash").over(change_window))
        .filter(
            col("previous_record_hash").isNull()
            | (col("record_hash") != col("previous_record_hash"))
        )
        .select(
            "user_id",
            "printed_card_number",
            "is_loyalty",
            col("order_date").alias("effective_start_date"),
            "record_hash"
        )
    )
    
    return user_changes
    
# ------------------------------------------------------------
# Read current raw batch
# ------------------------------------------------------------

df = spark.read.format("delta").load(raw_order_items_path)

latest_load_date = df.agg(max("load_dts").alias("latest_load_date")).collect()[0]["latest_load_date"]

latest_df = df.filter(col("load_dts") == latest_load_date)

incoming_changes = build_user_change_events(latest_df)

# ------------------------------------------------------------
# Initial table creation
# ------------------------------------------------------------

if not delta_table_exists(spark, refined_dim_users_path):
    
    version_window = Window.partitionBy("user_id").orderBy("effective_start_date")
    
    dim_users_initial = (
        incoming_changes
        .withColumn(
            "next_effective_start_date",
            lead("effective_start_date").over(version_window)
        )
        .withColumn(
            "effective_end_date",
            date_sub(col("next_effective_start_date"), 1)
        )
        .withColumn(
            "is_current",
            col("next_effective_start_date").isNull()
        )
        .withColumn(
            "user_sk",
            sha2(
                concat_ws(
                    "||",
                    col("user_id"),
                    col("effective_start_date").cast("string"),
                    col("record_hash")
                ),
                256
            )
        )
        .withColumn("updated_at", current_timestamp())
        .select(
            "user_sk",
            "user_id",
            "printed_card_number",
            "is_loyalty",
            "effective_start_date",
            "effective_end_date",
            "is_current",
            "record_hash",
            "updated_at"
        )
    )
    
    dim_users_initial.write.format("delta").mode("overwrite").save(refined_dim_users_path)
    
    job.commit()

else:
    
    # ------------------------------------------------------------
    # Incremental SCD2 processing
    # ------------------------------------------------------------
    
    dim_users_table = DeltaTable.forPath(spark, refined_dim_users_path)
    dim_users_existing = spark.read.format("delta").load(refined_dim_users_path)
    
    current_users = (
        dim_users_existing
        .filter(col("is_current") == True)
        .select(
            "user_id",
            col("printed_card_number").alias("current_printed_card_number"),
            col("is_loyalty").alias("current_is_loyalty"),
            col("effective_start_date").alias("current_effective_start_date"),
            col("record_hash").alias("current_record_hash")
        )
    )
    
    #Bring the current user state into the timeline so we do not insert
    # a duplicate version if the incoming state has not actually changed.
    current_as_events = (
        current_users
        .select(
            col("user_id"),
            col("current_printed_card_number").alias("printed_card_number"),
            col("current_is_loyalty").alias("is_loyalty"),
            col("current_effective_start_date").alias("effective_start_date"),
            col("current_record_hash").alias("record_hash")
        )
    )
    
    affected_user_ids = incoming_changes.select("user_id").distinct()
    
    current_as_events_for_affected_users = (
        current_as_events.join(affected_user_ids, on="user_id", how="inner")
    )
    
    combined_events = (
        current_as_events_for_affected_users
        .unionByName(incoming_changes)
    )
    
    change_window = Window.partitionBy("user_id").orderBy("effective_start_date")
    
    real_changes = (
        combined_events
        .withColumn("previous_record_hash", lag("record_hash").over(change_window))
        .filter(
            col("previous_record_hash").isNull()
            | (col("record_hash") != col("previous_record_hash"))
        )
    )
    
    # Only insert rows that came from the incoming batch.
    new_versions = (
        real_changes
        .join(
            incoming_changes.select("user_id", "effective_start_date", "record_hash"),
            on=["user_id", "effective_start_date", "record_hash"],
            how="inner"
        )
    )
    
    version_window = Window.partitionBy("user_id").orderBy("effective_start_date")
    
    new_versions_final = (
        new_versions.withColumn(
            "next_effective_start_date",
            lead("effective_start_date").over(version_window)
        )
        .withColumn(
            "effective_end_date",
            date_sub(col("next_effective_start_date"), 1)
        )
        .withColumn(
            "is_current",
            col("next_effective_start_date").isNull()
        )
        .withColumn(
            "user_sk",
            sha2(
                concat_ws(
                    "||",
                    col("user_id"),
                    col("effective_start_date").cast("string"),
                    col("record_hash")
                ),
                256
            )
        )
        .withColumn("updated_at", current_timestamp())
        .select(
            "user_sk",
            "user_id",
            "printed_card_number",
            "is_loyalty",
            "effective_start_date",
            "effective_end_date",
            "is_current",
            "record_hash",
            "updated_at"
        )
    )
    
    # Expire the existing current record for users who have a new version.
    first_new_version_per_user = (
        new_versions_final
        .groupBy("user_id")
        .agg({"effective_start_date": "min"})
        .withColumnRenamed("min(effective_start_date)", "first_new_effective_start_date")
        .withColumn(
            "new_effective_end_date",
            date_sub(col("first_new_effective_start_date"), 1)
        )
    )
    
    if first_new_version_per_user.count() > 0:
        (
            dim_users_table.alias("target")
            .merge(
                first_new_version_per_user.alias("source"),
                """
                target.user_id = source.user_id
                AND target.is_current = true
                """
            )
            .whenMatchedUpdate(
                set = {
                    "effective_end_date": "source.new_effective_end_date",
                    "is_current": "false",
                    "updated_at": "current_timestamp()"
                }
            )
            .execute()
        )
        
    # Avoid inserting duplicates if the same job is accidentally rerun.
    existing_user_sks = dim_users_existing.select("user_sk").distinct()
    
    new_versions_to_insert = (
        new_versions_final
        .join(existing_user_sks, on="user_sk", how="left_anti")
    )
    
    if new_versions_to_insert.count() > 0:
        (
            new_versions_to_insert
            .write
            .format("delta")
            .mode("append")
            .save(refined_dim_users_path)
        )
    
    job.commit()