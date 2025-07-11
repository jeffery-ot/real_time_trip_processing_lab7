import boto3
import json
import uuid
import logging
import os
from datetime import datetime

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
glue = boto3.client('glue')
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')

# Environment Variables
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'MyGlueJob1')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')  # Must be set
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'glue_job_tracking')
MAX_CONCURRENT_RUNS = int(os.environ.get('MAX_CONCURRENT_RUNS', '3'))

# DynamoDB Table resource
table = dynamodb.Table(DYNAMODB_TABLE)


def lambda_handler(event, context):
    if is_sqs_event(event):
        return handle_sqs_message(event['Records'][0])
    else:
        return handle_direct_event(event)


def is_sqs_event(event):
    return (
        'Records' in event
        and event['Records'][0].get('eventSource') == 'aws:sqs'
    )


def handle_direct_event(event):
    correlation_id = str(uuid.uuid4())
    logger.info(f"[{correlation_id}] Handling direct trigger event.")

    message = {
        'JobName': GLUE_JOB_NAME,
        'Arguments': {'--correlation_id': correlation_id},
        'RunId': str(uuid.uuid4()),
        'CorrelationId': correlation_id
    }

    return process_job_request(message, triggered_by='event')


def handle_sqs_message(record):
    body = json.loads(record['Body'])
    correlation_id = body.get('CorrelationId', str(uuid.uuid4()))
    logger.info(f"[{correlation_id}] Handling SQS message.")

    return process_job_request(body, triggered_by='sqs', receipt_handle=record['ReceiptHandle'])


def process_job_request(message, triggered_by, receipt_handle=None):
    job_name = message['JobName']
    arguments = message.get('Arguments', {})
    correlation_id = message['CorrelationId']
    run_id = message.get('RunId', str(uuid.uuid4()))

    # Check Glue job concurrency
    running_jobs = [
        j for j in glue.get_job_runs(JobName=job_name)['JobRuns']
        if j['JobRunState'] == 'RUNNING'
    ]
    logger.info(f"[{correlation_id}] {len(running_jobs)} running jobs found for {job_name}.")

    if len(running_jobs) >= MAX_CONCURRENT_RUNS:
        if triggered_by == 'event':
            sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(message))
            logger.info(f"[{correlation_id}] Job queued due to concurrency limit.")
        else:
            logger.info(f"[{correlation_id}] Job still waiting. Retry later.")
        return {'status': 'queued', 'correlation_id': correlation_id}

    # Start Glue job
    try:
        response = glue.start_job_run(JobName=job_name, Arguments=arguments)
        job_run_id = response['JobRunId']
        logger.info(f"[{correlation_id}] Started Glue job: {job_run_id}")
    except Exception as e:
        logger.error(f"[{correlation_id}] Failed to start Glue job: {str(e)}")
        return {'status': 'error', 'error': str(e), 'correlation_id': correlation_id}

    # Write tracking info to DynamoDB
    try:
        table.put_item(
            Item={
                'JobRunId': job_run_id,
                'ProcessName': 'exampleflow',
                'JobName': job_name,
                'Status': 'InProgress',
                'CreatedDate': datetime.utcnow().isoformat(),
                'CreatedBy': f'LambdaA-{triggered_by}',
                'NextGlueJob': 'MyGlueJob2',
                'RunId': run_id,
                'CorrelationId': correlation_id
            }
        )
        logger.info(f"[{correlation_id}] Tracking info stored in DynamoDB.")
    except Exception as e:
        logger.error(f"[{correlation_id}] Failed to write to DynamoDB: {str(e)}")

    # Delete SQS message if from queue
    if triggered_by == 'sqs' and receipt_handle:
        try:
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            logger.info(f"[{correlation_id}] Deleted message from SQS.")
        except Exception as e:
            logger.warning(f"[{correlation_id}] Failed to delete SQS message: {str(e)}")

    return {'status': 'started', 'JobRunId': job_run_id, 'correlation_id': correlation_id}
