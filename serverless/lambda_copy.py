import boto3
import io
import logging
import json

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

    s3 = boto3.resource('s3')
    source_bucket = s3.Bucket(message['source_bucket'])
    dest_bucket = s3.Bucket(message['dest_bucket'])

    for index, key in enumerate(message['keys']):
        with io.BytesIO() as byte_stream:
            source_bucket.download_fileobj(key, byte_stream)
            byte_stream.seek(0)  # reset stream to beginning of file
            dest_bucket.upload_fileobj(byte_stream, key)
            logger.info(f"Uploaded {key} with size {byte_stream.__sizeof__()}")
    logger.info(f"Completed {len(message['keys'])} uploads")
    return {'Status': 200}
