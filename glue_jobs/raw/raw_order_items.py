import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, year, month, to_date

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.parquet(
    "s3://global-partners-data-bucket-001/ingestion/order_items/"
)

df = df.withColumn(
    "creation_time_utc",
    col("creation_time_utc").cast("timestamp")
)

df = df.withColumn(
    "item_price",
    col("item_price").cast("decimal(10,2)")
)

df = df.withColumn(
    "order_date",
    to_date("creation_time_utc")
)

df = df.withColumn(
    "order_year",
    year("order_date")
)

df = df.withColumn(
    "order_month",
    month("order_date")
)

df = df.withColumn(
    "load_dts",
    current_timestamp()
)

df.write.format("delta") \
    .partitionBy("order_year","order_month") \
    .mode("append") \
    .save("s3://global-partners-data-bucket-001/raw/order_items/")

job.commit()