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

customer_landing = spark.read.json(
    "s3://customer-s3-landing/customer_landing/"
)

customer_trusted = customer_landing.filter(
    col("shareWithResearchAsOfDate").isNotNull()
).select(
    col("customerName").alias("customername"),
    col("email"),
    col("phone"),
    col("birthDay").alias("birthday"),
    col("serialNumber").alias("serialnumber"),
    col("registrationDate").alias("registrationdate"),
    col("lastUpdateDate").alias("lastupdatedate"),
    col("shareWithResearchAsOfDate").alias("sharewithresearchasofdate"),
    col("shareWithPublicAsOfDate").alias("sharewithpublicasofdate"),
    col("shareWithFriendsAsOfDate").alias("sharewithfriendsasofdate")
)

customer_trusted.write.mode("overwrite").json(
    "s3://customer-s3-landing/customer_trusted/"
)

job.commit()