import boto3 as b3
from botocore.exceptions import ClientError
import requests

client = b3.client('s3',
                   aws_access_key_id=ACCESS_KEY,
                   aws_secret_access_key=SECRET_KEY,
                   region_name=REGION)
s3 = b3.resource('s3',
                 aws_access_key_id=ACCESS_KEY,
                 aws_secret_access_key=SECRET_KEY,
                 region_name=REGION
                 )


# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name
    
    try:
        response = client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e.response['Error']['Message'])
        return False
    return True


def upload_from_urls(url_list):
    if not url_list:
        raise ValueError("Url list is empty!")

    for url in url_list:
        req_for_image = requests.get(url, stream=True)

        file_object_from_req = req_for_image.raw
        req_data = file_object_from_req.read()

        file_name = url.rsplit('/', 1)[-1]
        s3.Bucket('artistimg').put_object(Key=file_name, Body=req_data)

