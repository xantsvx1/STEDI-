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

customer_trusted = spark.read.json(
    "s3://customer-s3-landing/customer_trusted/"
)

accelerometer_trusted = spark.read.json(
    "s3://accelerometer-s3-landing/accelerometer_trusted/"
)

customers_curated = customer_trusted.join(
    accelerometer_trusted,
    customer_trusted["email"] == accelerometer_trusted["user"],
    "inner"
).select(
    customer_trusted["customername"],
    customer_trusted["email"],
    customer_trusted["phone"],
    customer_trusted["birthday"],
    customer_trusted["serialnumber"],
    customer_trusted["registrationdate"],
    customer_trusted["lastupdatedate"],
    customer_trusted["sharewithresearchasofdate"],
    customer_trusted["sharewithpublicasofdate"],
    customer_trusted["sharewithfriendsasofdate"]
).dropDuplicates(["email"])

customers_curated.write.mode("overwrite").json(
    "s3://customer-s3-landing/customers_curated/"
)

job.commit()