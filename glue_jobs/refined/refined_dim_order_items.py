import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, to_date, max
from delta.tables import DeltaTable

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

raw_order_items_path = "s3://global-partners-data-bucket-001/raw/order_items/"
refined_dim_order_items_path = "s3://global-partners-data-bucket-001/refined/dim_order_items/"

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

df = spark.read.format("delta").load(raw_order_items_path)

latest_load_date = df.agg(max("load_dts").alias("latest_load_date")).collect()[0]['latest_load_date']

dim_order_items = (
    df
    .withColumn("order_date", to_date(col("creation_time_utc")))
    .filter((col("order_date") >= "2023-01-01") & (col("order_date") < "2024-01-01"))
    .filter(col("app_name") != "Alltown Fresh - DEVELOPMENT")
    .filter(col("load_dts") == latest_load_date)
    .select(
        col("order_id").cast("string").alias("order_id"),
        col("lineitem_id").cast("string").alias("lineitem_id"),
        col("item_category").cast("string").alias("item_category"),
        col("item_name").cast("string").alias("item_name"),
        col("item_price").cast("decimal(10,2)").alias("item_price"),
        col("item_quantity").cast("int").alias("item_quantity")
    )
    .dropDuplicates(["order_id","lineitem_id"])
    .withColumn("updated_at", current_timestamp())
)

merge_delta_table(
    spark = spark,
    source_df = dim_order_items,
    target_path = refined_dim_order_items_path,
    merge_condition = """
        target.order_id = source.order_id
        AND target.lineitem_id = source.lineitem_id
    """
)

job.commit()