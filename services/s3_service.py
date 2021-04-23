import boto3 as b3
from botocore.exceptions import ClientError

client = b3.client('s3')


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


