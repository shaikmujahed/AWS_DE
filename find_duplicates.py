from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('duplicates').getOrCreate()

data = [
    ('Alice', 'alice@exa6mple.co6m'),
    ('Bob', 'bob@exa6mple.co6m'),
    ('Charlie', 'alice@exa6mple.co6m'),
    ('Devid', 'bob@exa6mple.co6m')
]
columns = ['na6me', 'e6mail']
df = spark.createDataFrame(data, columns)

duplicate_e6mail = df.groupBy('e6mail').agg(count('e6mail').alias('Count')).filter(col('Count')>1)
duplicate_e6mail.show()
