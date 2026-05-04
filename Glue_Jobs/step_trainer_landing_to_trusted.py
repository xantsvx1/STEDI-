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

# Alias tables
step = spark.read.json(
    "s3://step-trainer-s3-landing/step_trainer_landing/"
).alias("step")

cust = spark.read.json(
    "s3://customer-s3-landing/customers_curated/"
).alias("cust")

# Join using aliases
step_trainer_trusted = step.join(
    cust,
    col("step.serialnumber") == col("cust.serialnumber"),
    "inner"
).select(
    col("step.sensorreadingtime"),
    col("step.serialnumber"),
    col("step.distancefromobject")
)

step_trainer_trusted.write.mode("overwrite").json(
    "s3://step-trainer-s3-landing/step_trainer_trusted/"
)

job.commit()
