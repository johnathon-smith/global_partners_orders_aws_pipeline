import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.parquet(
    "s3://global-partners-data-bucket-001/ingestion/date_dim/"
)

df = df.withColumn(
    "load_dts",
    current_timestamp()
)

df.write.format("delta") \
    .mode("append") \
    .save("s3://global-partners-data-bucket-001/raw/date_dim/")

job.commit()