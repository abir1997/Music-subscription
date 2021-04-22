'''
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
'''

import boto3 as b3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

client = b3.client('dynamodb')
DB = b3.resource('dynamodb')


def get_login(email):
    table = DB.Table('login')
    try:
        response = table.get_item(Key={'email': email})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        login = response.get('Item')
        return login


def get_all_logins():
    table = DB.Table('login')

    response = table.scan()
    print(response)


def create_music_table():
    partition_key_type = 'HASH'
    sort_key_type = 'RANGE'
    table = DB.create_table(
        TableName='music',
        KeySchema=[
            {
                'AttributeName': 'artist',
                'KeyType': partition_key_type
            },
            {
                'AttributeName': 'title',
                'KeyType': sort_key_type
            },
            {
                'AttributeName': 'year',
                'KeyType': sort_key_type
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'artist',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'year',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
