#!/usr/bin/env python

import boto3
import uuid
import json
import logging
import argparse
import time
from yaml import load, Loader


def get_config():
    """
    Gets configuration from serverless.yml file
    """

    with open('serverless/serverless.yml', 'r') as config_file:
        data = load(config_file, Loader=Loader)

    return data


if __name__ == '__main__':

    """
    copies all files from source to destination bucket
    """

    # Logging setup
    logging.basicConfig(filename='scan.log',
                        filemode='a',
                        level=logging.INFO,
                        format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger(__name__)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logger.addHandler(console)

    # Command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source_bucket",
                        help="Source Bucket Name",
                        default='ocr-domains-default')
    parser.add_argument("-d", "--dest_bucket",
                        help="Destination Bucket Name",
                        default='ocr-domains-prod')
    args = parser.parse_args()
    source_bucket = args.source_bucket
    dest_bucket = args.dest_bucket

    # Get Configuration
    config = get_config()
    region = config['provider']['region']
    queue_name = config['custom']['sqs']

    # Setup Clients & Resources
    client = boto3.client('sqs', region_name=region)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(source_bucket)
    downloaded_keys = []

    # Get all keys in Source Bucket
    logger.info(f"Getting all keys from bucket:{source_bucket}")
    keys = [obj.key for obj in bucket.objects.all()]
    logger.info(f'Found {len(keys)} keys')

    # Setup Que Batches
    per_lambda = 10
    batches = [{"source_bucket": source_bucket,
                "dest_bucket": dest_bucket,
                "keys": keys[i: i + per_lambda]}
               for i in range(0, len(keys), per_lambda)]

    message_batch = [{'MessageBody': json.dumps(body), "Id": uuid.uuid4().__str__()}
                     for body in batches]
    max_batch_size = 10
    num_messages_success = 0
    num_messages_failed = 0

    # Putting messages onto the Que
    que_url = "https://sqs.us-east-2.amazonaws.com/820756113164/bucketKeys"
    que_dl_url = client.get_queue_url(QueueName=f"{queue_name}-dl")['QueueUrl']
    logger.info(f"Putting {len(message_batch)} messages onto Que: {que_url}")
    for k in range(0, len(message_batch), max_batch_size):
        response = client.send_message_batch(QueueUrl=que_url,
                                             Entries=message_batch[k:k + max_batch_size])
        num_messages_success += len(response.get('Successful', []))
        num_messages_failed += len(response.get('Failed', []))
    logger.info(f"Total Messages: {len(message_batch)}")
    logger.info(f"Successfully sent: {num_messages_success}")
    logger.info(f"Failed to send: {num_messages_failed}")

    # Check Queue
    logger.info("Checking SQS Que....")
    while True:
        time.sleep(10)
        response = client.get_queue_attributes(QueueUrl=que_url,
                                               AttributeNames=['ApproximateNumberOfMessages',
                                                               'ApproximateNumberOfMessagesNotVisible'])
        num_messages_on_que = int(response['Attributes']['ApproximateNumberOfMessages'])
        num_messages_hidden = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

        logger.info(f"{num_messages_on_que} messages left on Que, {num_messages_hidden} messages not visible")
        if num_messages_hidden == 0:
            break

    logger.info("No messages left on SQS Que, checking DLQ:")
    response = client.get_queue_attributes(QueueUrl=que_dl_url,
                                           AttributeNames=['ApproximateNumberOfMessages',
                                                           'ApproximateNumberOfMessagesNotVisible'])
    num_dead_letters = int(response['Attributes']['ApproximateNumberOfMessages'])
    if num_dead_letters == 0:
        logger.info("No Dead Letters found. All Que messages successfully processed")
    else:
        logger.info(f"{num_dead_letters} messages failed. Check dead letter que for more info")

    calling_keys = []
    for batch in message_batch:
        body = json.loads(batch['MessageBody'])
        calling_keys.extend(body['keys'])

    logger.info(f"\n\nCalled: {len(calling_keys)}")

    bucket = s3.Bucket(dest_bucket)
    dest_keys = [obj.key for obj in bucket.objects.all()]
    logger.info(f"Found {len(dest_keys)} in destination bucket")

    missing_keys = [key for key in calling_keys if key not in dest_keys]
    logger.info(f"{missing_keys}")
    logger.info("end")