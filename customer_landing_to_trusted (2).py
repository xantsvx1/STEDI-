import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1777875081133 = glueContext.create_dynamic_frame.from_catalog(database="stediproject", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1777875081133")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AWSGlueDataCatalog_node1777875081133, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1777875064170", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1777875097711 = glueContext.getSink(path="s3://customer-s3-landing/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1777875097711")
AmazonS3_node1777875097711.setCatalogInfo(catalogDatabase="stediproject",catalogTableName="customer_trusted")
AmazonS3_node1777875097711.setFormat("json")
AmazonS3_node1777875097711.writeFrame(AWSGlueDataCatalog_node1777875081133)
job.commit()