import boto3
import io
import logging
import json

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main(event, context):

    # retrieve que message
    try:
        message = json.loads(event['Records'][0]['body'])
    except json.JSONDecodeError:
        logger.info("JSON Decoder error for event: {}".format(event))
        return {'status': 500}  # return 'successfully' to SQS to prevent retry
    except KeyError:
        logger.info("Missing argument in que message")
        logger.info("Message dump: {}".format(json.dumps(event)))
        return {'status': 500}  # return 'successfully' to SQS to prevent retry

    source_bucket_name = message['source_bucket']
    dest_bucket_name = message['dest_bucket']

    for index, key in enumerate(message['keys']):
        with io.BytesIO() as f:
            s3.download_fileobj(source_bucket_name, key, f)
            f.seek(0)  # reset stream to beginning of file
            s3.upload_fileobj(f, dest_bucket_name, key)
    logger.info("Completed {} uploads")
    return {'Status': 200}
