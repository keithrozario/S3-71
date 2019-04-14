#!/usr/bin/env python

import boto3
import uuid
import json
import logging
import argparse
import time
import string

from yaml import load, Loader


def get_config():
    """
    Gets configuration from serverless.yml file
    """

    with open('serverless/serverless.yml', 'r') as config_file:
        data = load(config_file, Loader=Loader)

    return data


def check_queue(queue_name):

    que_url = client.get_queue_url(QueueName=f"{queue_name}")['QueueUrl']
    response = client.get_queue_attributes(QueueUrl=que_url,
                                           AttributeNames=['ApproximateNumberOfMessages',
                                                           'ApproximateNumberOfMessagesNotVisible'])
    num_messages_on_que = int(response['Attributes']['ApproximateNumberOfMessages'])
    num_messages_hidden = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

    logger.info(f"{num_messages_on_que} messages left on Que, {num_messages_hidden} messages not visible")

    return num_messages_on_que, num_messages_hidden


def check_dead_letter(queue_name):
    """
    Args:
        queue : queue_name of the dead letter queue
    """

    que_dl_url = client.get_queue_url(QueueName=f"{queue_name}")['QueueUrl']
    response = client.get_queue_attributes(QueueUrl=que_dl_url,
                                           AttributeNames=['ApproximateNumberOfMessages',
                                                           'ApproximateNumberOfMessagesNotVisible'])
    num_dead_letters = int(response['Attributes']['ApproximateNumberOfMessages'])
    if num_dead_letters == 0:
        logger.info("No Dead Letters found. All Que messages successfully processed")
    else:
        logger.info(f"{num_dead_letters} messages failed. Check dead letter que for more info")

    return num_dead_letters


def put_sqs(message_batch, queue_name):
    """
    Args:
        message_batch : list of messages to be sent to the que
        queue_name : name of que to be put on

    """
    max_batch_size = 10
    num_messages_success = 0
    num_messages_failed = 0
    que_url = client.get_queue_url(QueueName=f"{queue_name}")['QueueUrl']
    logger.info(f"Putting {len(message_batch)} messages onto Que: {que_url}")
    for k in range(0, len(message_batch), max_batch_size):
        response = client.send_message_batch(QueueUrl=que_url,
                                             Entries=message_batch[k:k + max_batch_size])
        num_messages_success += len(response.get('Successful', []))
        num_messages_failed += len(response.get('Failed', []))
    logger.info(f"Total Messages: {len(message_batch)}")
    logger.info(f"Successfully sent: {num_messages_success}")
    logger.info(f"Failed to send: {num_messages_failed}")

    logger.info("Checking SQS Que....")
    while True:
        time.sleep(10)
        response = client.get_queue_attributes(QueueUrl=que_url,
                                               AttributeNames=['ApproximateNumberOfMessages',
                                                               'ApproximateNumberOfMessagesNotVisible'])
        num_messages_on_que = int(response['Attributes']['ApproximateNumberOfMessages'])
        num_messages_hidden = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

        logger.info(f"{num_messages_on_que} messages left on Que, {num_messages_hidden} messages not visible")
        if num_messages_on_que == 0 and num_messages_hidden == 0:
            break

    return num_messages_success


if __name__ == '__main__':

    """
    copies all files from source to destination bucket
    """

    # Logging setup
    logging.basicConfig(filename='scan.log',
                        filemode='a',
                        level=logging.DEBUG,
                        format='%(asctime)s %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger(__name__)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s %(message)s', "%H:%M:%S"))
    logger.addHandler(console)

    # Command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--source_bucket",
                        help="Source Bucket Name",
                        default='∂')
    parser.add_argument("-d", "--dest_bucket",
                        help="Destination Bucket Name",
                        default='test-dest-keithrozario')
    parser.add_argument("-p", "--per_lambda",
                        help="number of files to transfer per lambda",
                        default=50)
    args = parser.parse_args()

    # Get Configuration
    config = get_config()
    region = config['provider']['region']
    service_name = config['service']
    list_queue_name = config['custom']['sqs_list_bucket'].replace('${self:service}', service_name)
    copy_queue_name = config['custom']['sqs_copy_objects'].replace('${self:service}', service_name)
    logger.info(f'Copying contents of {args.source_bucket} to {args.dest_bucket}')
    logger.debug(f'Using Serverless deployment {service_name}')
    logger.debug(f'Using SQS Queue: {list_queue_name}, {copy_queue_name}')

    # Setup Clients & Resources
    client = boto3.client('sqs', region_name=region)

    message = {"source_bucket": args.source_bucket,
               "dest_bucket": args.dest_bucket,
               "per_lambda": 50}

    prefixes = 'abcdef0123456789'
    message_batch = []
    for prefix in prefixes:
        message['prefix'] = prefix
        message_batch.append({'MessageBody': json.dumps(message), "Id": uuid.uuid4().__str__()})

    # Putting messages onto the Que
    put_sqs(message_batch, list_queue_name)

    # Check Queue
    logger.info("No messages left on SQS Que, checking DLQ:")
    check_dead_letter(f"{service_name}-dl")

    logger.info('Checking copy Queue')
    while True:

        num_messages_on_que, num_messages_hidden = check_queue(copy_queue_name)
        if num_messages_on_que == 0 and num_messages_hidden == 0:
            break
        else:
            time.sleep(30)

    if check_dead_letter(f"{service_name}-dl") > 0:
        logger.info(f"Errors found, please refer to {service_name}-dl for more info")
    else:
        logger.info("All Done")
