import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.parquet(
    "s3://global-partners-data-bucket-001/ingestion/order_item_options/"
)

df = df.withColumn(
    "option_price",
    col("option_price").cast("decimal(10,2)")
)

df = df.withColumn(
    "load_dts",
    current_timestamp()
)

df.write.format("delta") \
    .mode("append") \
    .save("s3://global-partners-data-bucket-001/raw/order_item_options/")

job.commit()