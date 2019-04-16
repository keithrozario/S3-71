import boto3
import copy_bucket
import time

if __name__ == '__main__':
    region = copy_bucket.get_config()['provider']['region']
    client = boto3.client('lambda', region_name=region)

    for x in range(199):
        time.sleep(0.01)
        response = client.invoke(FunctionName='cps3-prod-populate_s3',
                                 InvocationType='Event')

