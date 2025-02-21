import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node ns-sales-source
nssalessource_node1740064325873 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://ns-de-source-data-bucket/"], "recurse": True}, transformation_ctx="nssalessource_node1740064325873")

# Script generated for node Drop Duplicates
DropDuplicates_node1740064760747 =  DynamicFrame.fromDF(nssalessource_node1740064325873.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1740064760747")

# Script generated for node Drop Fields
DropFields_node1740064823570 = DropFields.apply(frame=DropDuplicates_node1740064760747, paths=["loc", "email"], transformation_ctx="DropFields_node1740064823570")

# Script generated for node ns-sales-destination
EvaluateDataQuality().process_rows(frame=DropFields_node1740064823570, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740062307374", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
nssalesdestination_node1740064963070 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1740064823570, connection_type="s3", format="csv", connection_options={"path": "s3://ns-pip-destination", "partitionKeys": []}, transformation_ctx="nssalesdestination_node1740064963070")
nssalesdestination_node1740064963070 =nssalesdestination_node1740064963070.repartition(1)
job.commit()
