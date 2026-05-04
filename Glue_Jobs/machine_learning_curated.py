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

step = spark.read.json(
    "s3://step-trainer-s3-landing/step_trainer_trusted/"
).alias("step")

accel = spark.read.json(
    "s3://accelerometer-s3-landing/accelerometer_trusted/"
).alias("accel")

machine_learning_curated = step.join(
    accel,
    col("step.sensorreadingtime") == col("accel.timestamp"),
    "inner"
).select(
    col("step.sensorreadingtime"),
    col("step.serialnumber"),
    col("step.distancefromobject"),
    col("accel.user"),
    col("accel.timestamp"),
    col("accel.x"),
    col("accel.y"),
    col("accel.z")
)

machine_learning_curated.write.mode("overwrite").json(
    "s3://step-trainer-s3-landing/machine_learning_curated/"
)

job.commit()
