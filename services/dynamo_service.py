'''
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
'''

import boto3 as b3
from botocore.exceptions import ClientError
import json
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

client = b3.client('dynamodb')
DB = b3.resource('dynamodb')
LOGIN_TABLE = DB.Table('login')
MUSIC_TABLE = DB.Table('music')


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


def insert_login(email, user_name, password):
    if email is None or user_name is None or password is None:
        raise ValueError("Args cannot be null.")

    response = LOGIN_TABLE.put_item(
        Item={
            'email': email,
            'password': password,
            'user_name': user_name
        }
    )

    return response


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
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'artist',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }

    )
    return table


def load_music():
    if music_data_exists():
        print("Music data already exists.")
    else:
        music_data = None
        with open("a2.json") as json_file:
            music_data = json.load(json_file, parse_float=Decimal)

        for data in music_data['songs']:
            year = int(data['year'])
            title = data['title']
            print("Adding music:", year, title)
            MUSIC_TABLE.put_item(Item=data)


def music_data_exists():
    response = MUSIC_TABLE.scan()
    return response.get('Count') != 0


def get_all_img_urls():
    response = MUSIC_TABLE.scan()
    url_list = []
    for data in response['Items']:
        url_list.append(data['img_url'])

    return url_list


def get_music(artist=None, title=None, year=None):
    if title is None and artist is None and year is None:
        response = MUSIC_TABLE.scan()
        return response.get('Items')

    if title is not None and artist is not None and year is None:
        response = MUSIC_TABLE.query(
            KeyConditionExpression=Key('artist').eq(artist) & Key('title').eq(title)
        )
        print(str(response['Count']) + "items returned.")
        return response.get('Items')


