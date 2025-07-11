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
BATCH_SIZE = 10                      # Number of records to send in each batch
MAX_WORKERS = 5                      # Number of parallel threads
TIMESTAMP_OFFSET_MINUTES = 5        # How far back the 'start' timestamps should be simulated from now

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

    # Simulate timestamp freshness
    if event_type == "start":
        # Pickup is some minutes before now
        record["pickup_datetime"] = (now - timedelta(
            minutes=random.randint(1, TIMESTAMP_OFFSET_MINUTES)
        )).isoformat()
    elif event_type == "end":
        # Dropoff is now
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
        logger.info(f"Record {index} sent | Seq: {response.get('SequenceNumber')}")
        return True
    except Exception as e:
        logger.error(f"Failed to send record {index}: {e}")
        return False


def stream_batch_data():
    """
    Prepare and send a shuffled batch of start and end events to the Kinesis stream.
    """
    logger.info("Starting batch data stream with simulated timestamps...")

    # Load the raw data
    start_df, end_df = load_data()

    # Add event type flags
    start_df["event_type"] = "start"
    end_df["event_type"] = "end"

    # Take equal number of start and end events, then shuffle
    half_batch = min(BATCH_SIZE // 2, len(start_df), len(end_df))
    sampled_start = start_df.sample(n=half_batch).copy()
    sampled_end = end_df.sample(n=half_batch).copy()

    combined_df = pd.concat([sampled_start, sampled_end], ignore_index=True)
    combined_df = combined_df.sample(frac=1).reset_index(drop=True)  # Shuffle rows

    logger.info(f"Prepared {len(combined_df)} mixed records")

    # Create Kinesis client
    kinesis = boto3.client("kinesis", region_name=REGION)

    # Simulate live data
    records = [
        simulate_live_data(row.to_dict(), row["event_type"])
        for _, row in combined_df.iterrows()
    ]

    # Send records in parallel using threads
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(
            lambda x: send_record(kinesis, x[1], x[0]),
            enumerate(records)
        ))

    logger.info(f"Batch completed: {sum(results)}/{len(records)} records sent")


if __name__ == "__main__":
    try:
        stream_batch_data()
        logger.info("Data streaming finished successfully")
    except Exception as e:
        logger.error(f"Script failed: {e}")
        raise
