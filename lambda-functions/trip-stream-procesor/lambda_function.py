import json
import boto3
import base64
from decimal import Decimal
from datetime import datetime, timedelta
import time
import math
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TripEvents')

TTL_DAYS = 30  # Days until record expiration

def parse_number(value):
    """Convert valid floats to Decimal (DynamoDB-safe); skip NaN/Infinity"""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None  # DynamoDB does not support NaN or Infinity
        return Decimal(str(round(value, 5)))
    return value

def parse_iso_date(dt_str):
    """Parse ISO timestamp into a datetime object"""
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None

def log(msg, level="INFO"):
    """Print logs with UTC timestamp and level"""
    print(f"[{level}] [{datetime.utcnow().isoformat()}] {msg}")

def lambda_handler(event, context):
    success = 0
    failed = 0
    skipped = 0

    for record in event['Records']:
        try:
            # Decode Kinesis payload
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            data = json.loads(payload)

            trip_id = data.get('trip_id')
            event_type = data.get('event_type')

            if not trip_id or not event_type:
                log(f"Missing trip_id or event_type: {data}", "WARN")
                failed += 1
                continue

            # TTL setup
            expire_at = int(time.time()) + TTL_DAYS * 24 * 3600
            data['expire_at'] = expire_at

            # Build item excluding nulls, NaN, and Infinity
            item = {}
            for k, v in data.items():
                parsed_value = parse_number(v) if isinstance(v, float) else v
                if parsed_value is not None:
                    item[k] = parsed_value

            # Add keys explicitly (just in case)
            item['trip_id'] = trip_id
            item['event_type'] = event_type

            # Conditional check
            condition_expr = "attribute_not_exists(pickup_datetime)"
            expr_values = {}

            if event_type == 'start' and 'pickup_datetime' in data:
                new_time = parse_iso_date(data['pickup_datetime'])
                if new_time:
                    condition_expr += " OR pickup_datetime < :new_time"
                    expr_values[":new_time"] = data['pickup_datetime']

            elif event_type == 'end' and 'dropoff_datetime' in data:
                new_time = parse_iso_date(data['dropoff_datetime'])
                if new_time:
                    condition_expr = "attribute_not_exists(dropoff_datetime) OR dropoff_datetime < :new_time"
                    expr_values[":new_time"] = data['dropoff_datetime']

            # Put item
            if expr_values:
                table.put_item(
                    Item=item,
                    ConditionExpression=condition_expr,
                    ExpressionAttributeValues=expr_values
                )
            else:
                table.put_item(Item=item)

            log(f"Stored trip_id={trip_id}, event_type={event_type}")
            success += 1

        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                log(f"Skipped outdated record for trip_id={trip_id}, event_type={event_type}", "SKIP")
                skipped += 1
            else:
                log(f"ClientError for record: {e}", "ERROR")
                failed += 1
        except Exception as e:
            log(f"Failed to process record: {e}", "ERROR")
            failed += 1

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Kinesis batch processed',
            'successful_records': success,
            'skipped_records': skipped,
            'failed_records': failed
        })
    }
