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
        file_name = uuid.uuid4().__str__()
        key = f"{file_name[:1]}/{file_name}"
        s3_client.upload_fileobj(file_obj, dest_bucket, key)

    return {"status": 200}
