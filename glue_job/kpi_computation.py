import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import boto3
import time

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Parameters
args = job.init_args(sys.argv)
checkpoint_path = "s3://lab7-staging/archived-streams/_checkpoints"
input_path = "s3://lab7-staging/staged-streams/"
archive_path = "s3://lab7-staging/archived-streams/"
kpi_table_path = "s3://lab7-staging/kpi-delta/"

# Enable Glue bookmarks
job.init(bookmark_option="job-bookmark-enable")

# Read staged data with bookmarks
raw_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [input_path],
        "recurse": True
    },
    format="json",
    transformation_ctx="raw_dyf"
)

# Schema validation fallback
expected_columns = {"trip_date", "fare_amount"}
actual_columns = set(raw_dyf.schema().fieldNames())
if not expected_columns.issubset(actual_columns):
    print("Required columns missing, aborting job.")
    job.commit()
    sys.exit(1)

# Convert to DataFrame and calculate KPIs
raw_df = raw_dyf.toDF()

kpi_df = raw_df.groupBy("trip_date").agg(
    F.sum("fare_amount").alias("total_fare"),
    F.count("fare_amount").alias("count_trips"),
    F.max("fare_amount").alias("max_fare"),
    F.min("fare_amount").alias("min_fare")
)

# Write archive with retry logic
max_retries = 3
for attempt in range(max_retries):
    try:
        raw_dyf.write(
            connection_type="s3",
            format="json",
            connection_options={"path": archive_path, "partitionKeys": ["trip_date"]}
        )
        break
    except Exception as e:
        print(f"[Retry {attempt+1}] Failed to archive: {e}")
        time.sleep(2 ** attempt)
else:
    raise Exception("Archiving failed after retries")

# DeltaTable upsert
if DeltaTable.isDeltaTable(spark, kpi_table_path):
    delta_tbl = DeltaTable.forPath(spark, kpi_table_path)
    delta_tbl.alias("t").merge(
        kpi_df.alias("s"),
        "t.trip_date = s.trip_date"
    ).whenMatchedUpdate(set={
        "total_fare": "t.total_fare + s.total_fare",
        "count_trips": "t.count_trips + s.count_trips",
        "max_fare": "GREATEST(t.max_fare, s.max_fare)",
        "min_fare": "LEAST(t.min_fare, s.min_fare)"
    }).whenNotMatchedInsertAll().execute()
else:
    kpi_df.write.format("delta").mode("overwrite").save(kpi_table_path)

# Delta compaction & retention (run daily)
spark.sql(f"OPTIMIZE delta.`{kpi_table_path}` ZORDER BY (trip_date)")
spark.sql(f"VACUUM delta.`{kpi_table_path}` RETAIN 168 HOURS")  # Keep 7 days

job.commit()
