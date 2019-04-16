import boto3
import logging
import json
import os
from contextlib import suppress

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')


def list_bucket_and_delete(message):
    """
    Args:
        message['source_bucket'] (str): The bucket name to copy **from**
        message['dest_bucket'] (str): The bucket to copy **to**
        message['prefix'] (str): Limit listing to keys starting with specific prefix (e.g. 'a')
        message['per_lambda'] (int): Number of objects each lambda will copy

    """

    kwargs = {'Bucket': message['bucket'], 'Prefix': message['prefix']}
    with suppress(KeyError):
        kwargs['ContinuationToken'] = message['ContinuationToken']

    response = s3_client.list_objects_v2(**kwargs)

    try:
        keys = [{'Key': content['Key']} for content in response['Contents']]
    except KeyError:
        logger.info(f"No contents in response. Unable to find any objects with prefix {message['prefix']}")
        return False

    logger.info(f"Found {len(keys)} keys up to {keys[-1:][0].get('Key', '')[:8]}..., deleting")
    logger.info(f"Deleting {len(keys)} keys from {message['bucket']}")
    del_response = s3_client.delete_objects(Bucket=message['bucket'],
                                            Delete={'Objects': keys, 'Quiet': True})

    if del_response.get('Errors', False):
        logger.info(f"{len(del_response['Errors'])} failed to be deleted")
        logger.info(json.dumps(del_response))

    try:
        logger.info(f"Requesting next token: {response['NextContinuationToken']}")
        return response['NextContinuationToken']
    except KeyError:
        logger.info("Unable to find NextContinuationToken in response")
        return False


def main(event, context):
    """
    Args:
        message['bucket'] (str): The bucket name delete objects from
        message['prefix'] (str): Limit listing to keys starting with specific prefix (e.g. 'a')

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
        token = list_bucket_and_delete(message)

        if token:
            message['ContinuationToken'] = token

            if context.get_remaining_time_in_millis() < reinvoke_seconds * 1000:
                logger.info(f"Running out of time, invoking new lambda")
                que_url = os.environ['sqs_delete_objects']
                response = sqs_client.send_message(QueueUrl=que_url,
                                                   MessageBody=json.dumps(message),
                                                   DelaySeconds=2)
                logger.info(f"Called new message with messageId: {response['MessageId']}")
                break
            else:
                pass

        else:
            logger.info(f"All objects with prefix {message['prefix']} deleted")
            break

    return {'Status': 200}


# if __name__ == '__main__':
#
#     message = {
#         "bucket": "test-source-keithrozario",
#         "prefix": "a",
#     }
#
#     event = {"Records": []}
#     event['Records'].append({'body': json.dumps(message)})
#
#     main(event, {})
