import boto3
import uuid
import os
import logging
import json
from contextlib import suppress

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')


def put_sqs(batches: list) -> bool:
    """
    Args:
        batches (list): items to be put onto SQS que

    items are put onto sqs que 10 at a time (maximum) -- each item in list is one item on que.
    """

    que_url = os.environ['sqs_copy_object_url']
    logger.info(f"Que URL: {que_url}")
    # Batch up SQS Messages
    message_batch = [{'MessageBody': json.dumps(body), "Id": uuid.uuid4().__str__()}
                     for body in batches]

    max_batch_size = 10
    num_messages_success = 0
    num_messages_failed = 0

    # Putting messages onto the Que
    logger.info(f"Putting {len(message_batch)} messages onto Que: {que_url}")
    for k in range(0, len(message_batch), max_batch_size):
        response = sqs_client.send_message_batch(QueueUrl=que_url,
                                                 Entries=message_batch[k:k + max_batch_size])
        num_messages_success += len(response.get('Successful', []))
        num_messages_failed += len(response.get('Failed', []))

    logger.info(f"Successfully sent {num_messages_success} to que")
    if num_messages_success == len(batches):
        return True
    else:
        logger.info(f"FAILED: {num_messages_failed} messages failed to be sent")
        return False


def batch_and_send(message: dict, keys: list):
    """
    Args:
        message['source_bucket'] (str): The bucket name to copy **from**
        message['dest_bucket'] (str): The bucket to copy **to**
        message['prefix'] (str): Limit listing to keys starting with specific prefix (e.g. 'a')
        message['per_lambda'] (int): Number of objects each lambda will copy

    Batches all keys into batches of size <per_lambda> and sends them to put SQS
    """

    per_lambda = message['per_lambda']
    batches = [{"source_bucket": message['source_bucket'],
                "dest_bucket": message['dest_bucket'],
                "keys": keys[i: i + message['per_lambda']]}
               for i in range(0, len(keys), per_lambda)]
    logger.info(f"Created {len(batches)} messages to put onto que")

    return put_sqs(batches)


def list_bucket_and_put_sqs(message):
    """
    Args:
        message['source_bucket'] (str): The bucket name to copy **from**
        message['dest_bucket'] (str): The bucket to copy **to**
        message['prefix'] (str): Limit listing to keys starting with specific prefix (e.g. 'a')
        message['per_lambda'] (int): Number of objects each lambda will copy

    """

    kwargs = {'Bucket': message['source_bucket'], 'Prefix': message['prefix']}
    with suppress(KeyError):
        kwargs['ContinuationToken'] = message['ContinuationToken']

    response = s3_client.list_objects_v2(**kwargs)
    try:
        keys = [content['Key'] for content in response['Contents']]
        logger.info(f'Found {len(keys)} up to {keys[-1:][0][:8]}..., putting on que')
        batch_and_send(message, keys)
        return response['NextContinuationToken']
    except KeyError:
        return False


def main(event, context):
    """
    Args:
        message['source_bucket'] (str): The bucket name to copy **from**
        message['dest_bucket'] (str): The bucket to copy **to**
        message['prefix'] (str): Limit listing to keys starting with specific prefix (e.g. 'a')
        message['per_lambda'] (int): Number of objects each lambda will copy

    Gets all keys in source_bucket with specified prefix, and sends to copy_onjects SQS que
    """

    # recursively invoke this function if less than this many seconds left
    reinvoke_seconds = 60

    try:
        message = json.loads(event['Records'][0]['body'])
    except (json.JSONDecodeError, KeyError):
        logger.info("JSON Decoder error for event: {}".format(event))
        return {'status': 500}

    while True:
        token = list_bucket_and_put_sqs(message)

        if token:
            message['ContinuationToken'] = token

            if context.get_remaining_time_in_millis() < reinvoke_seconds * 1000:
                logger.info(f"Running out of time, invoking new lambda")
                que_url = os.environ['sqs_list_bucket_url']
                response = sqs_client.send_message(QueueUrl=que_url,
                                                   MessageBody=json.dumps(message),
                                                   DelaySeconds=2)
                logger.info(f"Called new message with messageId: {response['MessageId']}")
                break
            else:
                pass

        else:
            logger.info(f"All objects with prefix {message['prefix']} accounted for")
            break

    return {'Status': 200}