from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName('missingFinder').getOrCreate()

data = [(1,),(2,),(5,),(7,),(8,),(10,)]
columns = ['id']

df = spark.createDataFrame(data,columns)

lisst = range(1,11,1)
new_df = spark.createDataFrame(lisst,IntegerType())
display(new_df.subtract(df))
