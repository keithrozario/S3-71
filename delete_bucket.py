#!/usr/bin/env python3

import uuid
import json
import logging
import argparse
import string

from copy_bucket import get_config, check_dead_letter, put_sqs

if __name__ == '__main__':

    """
    deletes all files from source to destination bucket
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
    console.setFormatter(logging.Formatter('%(asctime)s %(message)s', "%H:%M:%S"))
    logger.addHandler(console)

    # Command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--bucket",
                        help="Bucket Name",
                        default='test-dest-keithrozario')
    args = parser.parse_args()
    bucket_name = args.bucket

    # Get Configuration
    config = get_config()
    region = config['provider']['region']
    service_name = config['service']
    delete_queue_name = config['custom']['sqs_delete_objects'].replace('${self:service}', service_name)

    logger.info(f"Deleting all objects in {bucket_name}")
    logger.info(f'Using Serverless deployment {service_name}')
    logger.info(f'Using SQS Queue: {delete_queue_name}')

    message = {"bucket": args.bucket}

    prefixes = string.printable
    message_batch = []
    for prefix in prefixes:
        message['prefix'] = prefix
        message_batch.append({'MessageBody': json.dumps(message), "Id": uuid.uuid4().__str__()})

    # Putting messages onto the Que
    put_sqs(message_batch, delete_queue_name)

    # Check Queue
    logger.info(f"No messages left on SQS Que, all objects beginning with the following chars were deleted: {prefixes}")
    check_dead_letter(f"{service_name}-dl")

    if check_dead_letter(f"{service_name}-dl") > 0:
        logger.info(f"Errors found, please refer to {service_name}-dl for more info")
    else:
        logger.info("All Done")
