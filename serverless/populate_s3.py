import boto3
import uuid
import io
import json

s3_client = boto3.client('s3')


def main(event, context):

    dummy_content = {"foo": "bar"}
    dest_bucket = 'test-source-keithrozario'

    for x in range(5000):
        file_obj = io.BytesIO(json.dumps(dummy_content).encode('utf-8'))
        s3_client.upload_fileobj(file_obj, dest_bucket, uuid.uuid4().__str__())

    return {"status": 200}
