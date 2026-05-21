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

raw_date_dim_path = "s3://global-partners-data-bucket-001/raw/date_dim/"
refined_dim_dates_path = "s3://global-partners-data-bucket-001/refined/dim_dates/"

def delta_table_exists(spark, path):
    try:
        DeltaTable.forPath(spark, path)
        return True
    except Exception:
        return False

def merge_delta_table(spark, source_df, target_path, merge_condition, partition_cols=None):
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

df = spark.read.format("delta").load(raw_date_dim_path)

latest_load_date = df.agg(
    max("load_dts").alias("latest_load_date")
).collect()[0]["latest_load_date"]

dim_dates = (
    df
    .filter( (col("year") == 2023) & (col("load_dts") == latest_load_date) )
    .select(
        col("date_key").cast("date").alias("date_key"),
        col("day_of_week").cast("string").alias("day_of_week"),
        col("week").cast("int").alias("week"),
        col("month").cast("string").alias("month"),
        col("year").cast("int").alias("year"),
        col("is_weekend").cast("boolean").alias("is_weekend"),
        col("is_holiday").cast("boolean").alias("is_holiday"),
        col("holiday_name").cast("string").alias("holiday_name")
    )
    .dropDuplicates(["date_key"])
    .withColumn("updated_at", current_timestamp())
)

merge_delta_table(
    spark=spark,
    source_df=dim_dates,
    target_path=refined_dim_dates_path,
    merge_condition="target.date_key = source.date_key"
)

job.commit()