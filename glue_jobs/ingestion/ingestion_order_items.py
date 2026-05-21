import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import json

# Initialize the Secrets Manager client
client = boto3.client("secretsmanager", region_name="us-east-1")

# Retrieve the secret value
get_secret_value_response = client.get_secret_value(SecretId="Global_Partners_Database_Creds")

# Parse the secret value
secret = get_secret_value_response['SecretString']
secret = json.loads(secret)

# Extract the credentials
db_username = secret.get('username')
db_password = secret.get('password')
db_url = secret.get('host')
db_engine = secret.get('engine')
db_port = secret.get('port')

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

jdbc_url = f"jdbc:{db_engine}://{db_url}:{db_port};databaseName=GlobalPartners"
connection_properties = {
    "user": db_username,
    "password": db_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

query = """
(
    SELECT *
    FROM order_items
) AS order_items
"""

df = spark.read.jdbc(
    url=jdbc_url,
    table=query,
    properties=connection_properties
)

df.write.mode("overwrite").parquet(
    "s3://global-partners-data-bucket-001/ingestion/order_items/"
)

job.commit()