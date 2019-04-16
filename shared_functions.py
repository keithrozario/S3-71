import boto3
import logging
import time

from yaml import load, Loader


def get_config():
    """
    Gets configuration from serverless.yml file
    """

    with open('serverless/serverless.yml', 'r') as config_file:
        data = load(config_file, Loader=Loader)

    return data


def check_queue(queue_name):
    """

    Args:
        queue_name : queue_name of the queue

    Checks queue for messages, logs queue status of messages left on que and hidden messages
    returns only when queue is empty

    """
    region = get_config()['provider']['region']
    client = boto3.client('sqs', region_name=region)
    logger = logging.getLogger('__main__')

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
        queue_name : queue_name of the dead letter queue
    """

    region = get_config()['provider']['region']
    client = boto3.client('sqs', region_name=region)
    logger = logging.getLogger('__main__')

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

    region = get_config()['provider']['region']
    client = boto3.client('sqs', region_name=region)
    logger = logging.getLogger('__main__')

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
