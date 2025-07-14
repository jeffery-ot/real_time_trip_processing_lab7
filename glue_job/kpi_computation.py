import sys
import time
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Initialize Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Get Glue job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# S3 paths
input_path = "s3://lab7-staging/staged-streams/"
archive_path = "s3://lab7-staging/archived-streams/"
kpi_table_path = "s3://lab7-staging/kpi-delta/"

# Read staged JSON data
raw_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path], "recurse": True},
    format="json"
)

# Convert to DataFrame
raw_df = raw_dyf.toDF()

# Flatten nested JSON and extract required fields
flattened_df = raw_df.select(
    F.to_date("start_event.pickup_datetime").alias("trip_date"),
    F.col("end_event.fare_amount").cast("double").alias("fare_amount"),
    *[F.col(field.name) for field in raw_df.schema.fields if field.name not in ["start_event", "end_event"]],
    *[F.col(f"start_event.{field.name}").alias(f"start_event_{field.name}") for field in raw_df.schema["start_event"].dataType],
    *[F.col(f"end_event.{field.name}").alias(f"end_event_{field.name}") for field in raw_df.schema["end_event"].dataType]
).filter(F.col("trip_date").isNotNull() & F.col("fare_amount").isNotNull())

# Abort early if data is empty
if flattened_df.rdd.isEmpty():
    print("No valid rows found. Aborting job.")
    job.commit()
    sys.exit(1)

# KPI calculation
kpi_df = flattened_df.groupBy("trip_date").agg(
    F.sum("fare_amount").alias("total_fare"),
    F.count("fare_amount").alias("count_trips"),
    F.max("fare_amount").alias("max_fare"),
    F.min("fare_amount").alias("min_fare")
)

# Archive raw data with trip_date as top-level partition column
archivable_df = flattened_df.select(
    *[col for col in flattened_df.columns if col != "trip_date"],  # Exclude if already included
    F.col("trip_date")
)

archive_dyf = DynamicFrame.fromDF(archivable_df, glueContext, "archive_dyf")

max_retries = 3
for attempt in range(max_retries):
    try:
        archive_dyf.write(
            connection_type="s3",
            format="json",
            connection_options={"path": archive_path, "partitionKeys": ["trip_date"]}
        )
        break
    except Exception as e:
        print(f"[Retry {attempt + 1}] Failed to archive: {e}")
        time.sleep(2 ** attempt)
else:
    raise Exception("Archiving failed after retries.")

# Delta upsert logic
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

# Delta optimization and retention cleanup
spark.sql(f"OPTIMIZE delta.`{kpi_table_path}` ZORDER BY (trip_date)")
spark.sql(f"VACUUM delta.`{kpi_table_path}` RETAIN 168 HOURS")

job.commit()
