import boto3
import os
import json
import logging
from datetime import datetime

# Clients
dynamodb = boto3.resource('dynamodb')
lambda_client = boto3.client('lambda')
sqs = boto3.client('sqs')

# Config
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'GlueJobTracker')
ORCHESTRATOR_LAMBDA_NAME = os.environ.get('ORCHESTRATOR_LAMBDA_NAME')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

table = dynamodb.Table(DYNAMODB_TABLE)

def lambda_handler(event, context):
    logger.info(f"Received Event: {json.dumps(event)}")

    detail = event.get("detail", {})
    job_name = detail.get("jobName")
    job_run_id = detail.get("jobRunId")
    job_state = detail.get("state")

    if not job_run_id or not job_state:
        logger.warning("Missing jobRunId or jobState in event")
        return {"status": "ignored"}

    # Lookup job metadata from tracking table
    try:
        job_info = table.get_item(Key={"JobRunId": job_run_id}).get("Item")
    except Exception as e:
        logger.error(f"Error querying DynamoDB: {str(e)}")
        return {"status": "error", "reason": "dynamodb_failure"}

    if not job_info:
        logger.warning(f"No job record found for JobRunId: {job_run_id}")
        return {"status": "missing"}

    correlation_id = job_info.get("CorrelationId", "unknown")
    next_job = job_info.get("NextGlueJob", "")
    run_id = job_info.get("RunId", "unknown")

    logger.info(f"[{correlation_id}] Glue job {job_name} completed with status: {job_state}")

    # Update job status in DynamoDB
    try:
        table.update_item(
            Key={"JobRunId": job_run_id},
            UpdateExpression="SET #s = :s, CompletedDate = :cd",
            ExpressionAttributeNames={"#s": "Status"},
            ExpressionAttributeValues={
                ":s": job_state,
                ":cd": datetime.utcnow().isoformat()
            }
        )
    except Exception as e:
        logger.error(f"[{correlation_id}] Failed to update status in DynamoDB: {str(e)}")
        return {"status": "update_failed", "correlation_id": correlation_id}

    # If job succeeded, kick off next step
    if job_state == "SUCCEEDED":
        if ORCHESTRATOR_LAMBDA_NAME:
            try:
                logger.info(f"[{correlation_id}] Re-invoking orchestrator Lambda: {ORCHESTRATOR_LAMBDA_NAME}")
                lambda_client.invoke(
                    FunctionName=ORCHESTRATOR_LAMBDA_NAME,
                    InvocationType="Event",
                    Payload=json.dumps({
                        "TriggerSource": "glue-queue",
                        "RunId": run_id,
                        "CorrelationId": correlation_id
                    })
                )
            except Exception as e:
                logger.error(f"[{correlation_id}] Failed to invoke orchestrator: {str(e)}")
                return {"status": "invoke_error", "correlation_id": correlation_id}

        elif SQS_QUEUE_URL:
            try:
                logger.info(f"[{correlation_id}] Enqueuing next step to SQS.")
                sqs.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps({
                        "JobName": next_job,
                        "RunId": run_id,
                        "CorrelationId": correlation_id,
                        "Arguments": {"--correlation_id": correlation_id}
                    })
                )
            except Exception as e:
                logger.error(f"[{correlation_id}] Failed to send message to SQS: {str(e)}")
                return {"status": "sqs_error", "correlation_id": correlation_id}
        else:
            logger.info(f"[{correlation_id}] No ORCHESTRATOR_LAMBDA_NAME or SQS_QUEUE_URL set; ending process.")

    else:
        logger.warning(f"[{correlation_id}] Job failed or was stopped. No next step will be triggered.")

    return {"status": "completed", "correlation_id": correlation_id}
