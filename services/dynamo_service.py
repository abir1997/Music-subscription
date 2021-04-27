'''
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
'''

import boto3 as b3
from botocore.exceptions import ClientError
import json
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
import re

client = b3.client('dynamodb')
DB = b3.resource('dynamodb')
LOGIN_TABLE = DB.Table('login')
MUSIC_TABLE = DB.Table('music')
SUBSCRIPTION_TABLE = DB.Table('subscription')


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


def put_login(email, user_name, password):
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


def get_music(artist="", title="", year=""):
    response = None
    filtered_list = []
    if not title and not artist and not year:
        response = MUSIC_TABLE.scan()
        filtered_list = response.get('Items')

    elif title and artist and not year:
        response = MUSIC_TABLE.query(
            KeyConditionExpression=Key('artist').eq(artist) & Key('title').eq(title)
        )
        filtered_list = response.get('Items')
    elif title and not artist and not year:
        response = MUSIC_TABLE.scan()
        filtered_list = get_filtered_music_by_title(title, response.get('Items'))
    elif not title and artist and not year:
        response = MUSIC_TABLE.query(
            KeyConditionExpression=Key('artist').eq(artist)
        )
        filtered_list = response.get('Items')
    elif title and artist and year:
        response = MUSIC_TABLE.query(
            KeyConditionExpression=Key('artist').eq(artist) & Key('title').eq(title)
        )
        filtered_list = get_filtered_music_by_year(year, response.get('Items'))
    elif title and not artist and year:
        response = MUSIC_TABLE.scan()
        filtered_list = get_filtered_music_by_title(title, response.get('Items'))
        filtered_list = get_filtered_music_by_year(year)
    elif not title and not artist and year:
        response = MUSIC_TABLE.scan()
        filtered_list = get_filtered_music_by_year(year, response.get('Items'))

    return filtered_list


def get_filtered_music_by_year(year, music_list):
    filtered_list = []
    for item in music_list:
        if item['year'] == year:
            filtered_list.append(item)
    return filtered_list


def get_filtered_music_by_title(title, music_list):
    filtered_list = []
    for item in music_list:
        if item['title'] == title:
            filtered_list.append(item)
    return filtered_list


def put_subscription(email, subscription):
    if email is None or subscription is None:
        raise ValueError("Args cannot be null.")

    response = SUBSCRIPTION_TABLE.put_item(
        Item={
            'email': email,
            'subscription': subscription
        }
    )

    print(subscription + " added to " + email)
    return response


def get_all_subscriptions(email):
    sub_list = []
    if email is None:
        raise ValueError("Arg cannot be null.")

    response = SUBSCRIPTION_TABLE.query(
        KeyConditionExpression=Key('email').eq(email)
    )

    print("Returned " + str(response.get('Count')) + " items")
    items = response.get('Items')
    # convert subscriptions to dict or just return dict??
    for item in items:
        subscription_string = item.get('subscription')
        json_str = subscription_string.replace("\'", "\"")

        # convert json to dict
        sub_list.append(json.loads(json_str))

    return sub_list
