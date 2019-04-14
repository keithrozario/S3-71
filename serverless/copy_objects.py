import boto3
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)
client = boto3.client('s3')


def main(event, context):

    try:
        message = json.loads(event['Records'][0]['body'])
    except (json.JSONDecodeError, KeyError):
        logger.info("JSON Decoder error for event: {}".format(event))
        return {'status': 500}

    source_bucket = message['source_bucket']
    dest_bucket = message['dest_bucket']

    for index, key in enumerate(message['keys']):
        logger.info(f"Copying {key} from {source_bucket} to {dest_bucket}")
        response = client.copy_object(Bucket=dest_bucket,
                                      CopySource={'Bucket': source_bucket,
                                                  'Key': key},
                                      Key=key)
        try:
            ETag = response['CopyObjectResult']['ETag']
            logger.info(f"Success Copied: {key} with ETag: {ETag}")
        except KeyError:
            logger.info(f"Fail to copy {key}")

    logger.info(f"Completed {len(message['keys'])} uploads")
    return {'Status': 200}