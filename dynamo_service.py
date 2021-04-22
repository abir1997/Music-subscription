import boto3 as b3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

client = b3.client('dynamodb')
DB = b3.resource('dynamodb')

def get_all_logins():
    table = DB.Table('login')

    response = table.scan()
    print(response)