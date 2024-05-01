# Databricks notebook source
from pyspark.sql.functions import *
import urllib

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key to safely include it in URLs
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name and mount point for the bucket
AWS_S3_BUCKET = "user-0ec6d756577b-bucket"
MOUNT_NAME = "/mnt/0ec6d756577b-bucket"
# Prepare the source URL for mounting
SOURCE_URL = f"s3a://{AWS_S3_BUCKET}/"

# Check if the directory is already mounted
mounted_dirs = [mount.mountPoint for mount in dbutils.fs.mounts()]
if MOUNT_NAME not in mounted_dirs:
    # Mount the S3 bucket
    dbutils.fs.mount(
        source=SOURCE_URL,
        mount_point=MOUNT_NAME,
        extra_configs={
            "fs.s3a.access.key": ACCESS_KEY,
            "fs.s3a.secret.key": SECRET_KEY
        }
    )

# Confirm the mount is successful
print(f"Mounted {AWS_S3_BUCKET} to {MOUNT_NAME}")


# COMMAND ----------

# Display the contents of the geolocation data directory in the mounted S3 bucket
dbutils.fs.ls("/mnt/0ec6d756577b-bucket/topics/0ec6d756577b.geo/partition=0/")


# COMMAND ----------

# Run this command to unmount a bucket.
#dbutils.fs.unmount("/mnt/S3bucket")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disables format checks during the reading of Delta tables
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

# For Pinterest post data
df_pin = spark.read.format("json") \
    .option("inferSchema", "true") \
    .load("/mnt/0ec6d756577b-bucket/topics/0ec6d756577b.pin/partition=0/*.json")

# For geolocation data
df_geo = spark.read.format("json") \
    .option("inferSchema", "true") \
    .load("/mnt/0ec6d756577b-bucket/topics/0ec6d756577b.geo/partition=0/*.json")

# For user data
df_user = spark.read.format("json") \
    .option("inferSchema", "true") \
    .load("/mnt/0ec6d756577b-bucket/topics/0ec6d756577b.user/partition=0/*.json")

display(df_pin)
display(df_geo)
display(df_user)


