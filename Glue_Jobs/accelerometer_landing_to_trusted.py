import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

accelerometer_landing = spark.read.json(
    "s3://accelerometer-s3-landing/accelerometer_landing/"
)

customer_trusted = spark.read.json(
    "s3://customer-s3-landing/customer_trusted/"
)

accelerometer_trusted = accelerometer_landing.join(
    customer_trusted,
    accelerometer_landing["user"] == customer_trusted["email"],
    "inner"
).select(
    accelerometer_landing["user"],
    accelerometer_landing["timestamp"],
    accelerometer_landing["x"],
    accelerometer_landing["y"],
    accelerometer_landing["z"]
)

accelerometer_trusted.write.mode("overwrite").json(
    "s3://accelerometer-s3-landing/accelerometer_trusted/"
)

job.commit()
