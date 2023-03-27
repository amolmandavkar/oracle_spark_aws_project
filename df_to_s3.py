import boto3
from io import StringIO
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
# Initialize the S3 client
s3 = boto3.resource('s3')
spark=SparkSession.builder.master('local[*]').appName("df_to_s3").getOrCreate()
# Define your bucket and file names
bucket_name = 'amol-bucket12345'
file_name = 'spark_df2.csv'

# Create a PySpark DataFrame
df = spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "name"])

# Convert the DataFrame to a CSV string
csv_string = df.select(to_csv(struct(*df.columns)).alias("csv")).collect()[0]["csv"]

# Create a file-like object from the CSV string
csv_buffer = StringIO(csv_string)

# Write the CSV data to S3
s3.Object(bucket_name,file_name).put(Body=csv_buffer.getvalue())
