import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, max
from delta.tables import DeltaTable

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

raw_options_path = "s3://global-partners-data-bucket-001/raw/order_item_options/"
refined_items_path = "s3://global-partners-data-bucket-001/refined/dim_order_items/"
refined_dim_options_path = "s3://global-partners-data-bucket-001/refined/dim_options/"

def delta_table_exists(spark, path):
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False

def merge_delta_table(spark, source_df, target_path, merge_condition, partition_cols = None):
    if delta_table_exists(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        
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

options_df = spark.read.format("delta").load(raw_options_path)
items_df = spark.read.format("delta").load(refined_items_path)

options_latest_load_date = options_df.agg(max("load_dts").alias("latest_load_date")).collect()[0]["latest_load_date"]

dim_options = (
    options_df
    .filter(col("load_dts") == options_latest_load_date)
    .join(items_df, on=["order_id","lineitem_id"], how="inner")
    .select(
        col("order_id").cast("string").alias("order_id"),
        col("lineitem_id").cast("string").alias("lineitem_id"),
        col("option_group_name").cast("string").alias("option_group_name"),
        col("option_name").cast("string").alias("option_name"),
        col("option_price").cast("decimal(10,2)").alias("option_price"),
        col("option_quantity").cast("int").alias("option_quantity")
    )
    .dropDuplicates([
        "order_id",
        "lineitem_id",
        "option_group_name",
        "option_name"
    ])
    .withColumn("updated_at", current_timestamp())
)

merge_delta_table(
    spark = spark,
    source_df = dim_options,
    target_path = refined_dim_options_path,
    merge_condition = """
        target.order_id = source.order_id
        AND target.lineitem_id = source.lineitem_id
        AND target.option_group_name = source.option_group_name
        AND target.option_name = source.option_name
    """
)

job.commit()