import boto3
import json
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import random

# === CONFIGURATION ===
STREAM_NAME = "trip-data-stream"     # Name of the Kinesis stream
REGION = "us-east-1"                 # AWS Region
BATCH_SIZE = 10                      # Total number of records (start + end)
MAX_WORKERS = 5                      # Number of parallel threads
TIMESTAMP_OFFSET_MINUTES = 5        # Simulated freshness of pickup timestamps

# === LOGGING SETUP ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def load_data():
    """
    Load trip start and end data from CSV files.
    """
    try:
        start_df = pd.read_csv("/app/data/trip_start.csv")
        end_df = pd.read_csv("/app/data/trip_end.csv")
        logger.info(f"Loaded {len(start_df)} start and {len(end_df)} end records")
        return start_df, end_df
    except Exception as e:
        logger.error(f"Failed to load CSV files: {e}")
        raise


def simulate_live_data(record: dict, event_type: str) -> dict:
    """
    Simulate real-time characteristics by adjusting timestamps and jittering numerical values.
    """
    record = record.copy()
    now = datetime.now()

    # Adjust timestamps
    if event_type == "start":
        record["pickup_datetime"] = (now - timedelta(
            minutes=random.randint(1, TIMESTAMP_OFFSET_MINUTES)
        )).isoformat()
    elif event_type == "end":
        record["dropoff_datetime"] = now.isoformat()

    # Add small random jitter to numeric fields
    for key in ["trip_distance", "fare_amount", "tip_amount", "estimated_fare_amount"]:
        if key in record and pd.notnull(record[key]):
            try:
                record[key] = round(float(record[key]) + random.uniform(-1.0, 1.0), 2)
            except ValueError:
                pass

    record["event_type"] = event_type
    return record


def send_record(kinesis_client, record, index):
    """
    Send a single record to the Kinesis stream.
    """
    try:
        data = json.dumps(record, default=str)
        response = kinesis_client.put_record(
            StreamName=STREAM_NAME,
            Data=data,
            PartitionKey=str(record.get("trip_id", index))
        )
        logger.info(f"Record {index} ({record['event_type']}) sent | Trip ID: {record['trip_id']}")
        return True
    except Exception as e:
        logger.error(f"Failed to send record {index}: {e}")
        return False


def stream_batch_data():
    """
    Prepare and send a batch of start and end events for the same trips.
    """
    logger.info("Starting batch data stream with simulated timestamps...")

    # Load the raw data
    start_df, end_df = load_data()

    # Merge on trip_id
    merged_df = pd.merge(start_df, end_df, on="trip_id", how="inner")
    logger.info(f"Merged to {len(merged_df)} matching trip records")

    # Select number of trips to simulate (each trip = 2 events)
    num_trips = min(BATCH_SIZE // 2, len(merged_df))
    sample_df = merged_df.sample(n=num_trips).reset_index(drop=True)

    all_records = []

    for _, row in sample_df.iterrows():
        trip_id = row['trip_id']

        # Create start event
        start_event = {
            "trip_id": trip_id,
            "pickup_location_id": row["pickup_location_id"],
            "dropoff_location_id": row["dropoff_location_id"],
            "vendor_id": row["vendor_id"],
            "pickup_datetime": row["pickup_datetime"],
            "estimated_dropoff_datetime": row["estimated_dropoff_datetime"],
            "estimated_fare_amount": row["estimated_fare_amount"]
        }
        start_event = simulate_live_data(start_event, event_type="start")
        all_records.append(start_event)

        # Create end event
        end_event = {
            "trip_id": trip_id,
            "dropoff_datetime": row["dropoff_datetime"],
            "rate_code": row["rate_code"],
            "passenger_count": row["passenger_count"],
            "trip_distance": row["trip_distance"],
            "fare_amount": row["fare_amount"],
            "tip_amount": row["tip_amount"],
            "payment_type": row["payment_type"],
            "trip_type": row["trip_type"]
        }
        end_event = simulate_live_data(end_event, event_type="end")
        all_records.append(end_event)

    # Shuffle both start/end records together
    random.shuffle(all_records)

    # Send records
    kinesis = boto3.client("kinesis", region_name=REGION)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(
            lambda x: send_record(kinesis, x[1], x[0]),
            enumerate(all_records)
        ))

    logger.info(f"Batch completed: {sum(results)}/{len(all_records)} records sent")


if __name__ == "__main__":
    try:
        stream_batch_data()
        logger.info("Data streaming finished successfully")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
