'''
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
'''

import boto3 as b3
from botocore.exceptions import ClientError
import json
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

client = b3.client('dynamodb',
                   aws_access_key_id=ACCESS_KEY,
                   aws_secret_access_key=SECRET_KEY,
                   region_name=REGION
                   )

DB = b3.resource('dynamodb',
                 aws_access_key_id=ACCESS_KEY,
                 aws_secret_access_key=SECRET_KEY,
                 region_name=REGION
                 )

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


def put_login(email, password, user_name):
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
        return filtered_list

    elif title and artist and not year:
        response = MUSIC_TABLE.scan()
        filtered_list = get_filtered_music_by_artist_and_year(artist, title)

    elif title and not artist and not year:
        response = MUSIC_TABLE.scan()
        filtered_list = get_filtered_music_by_title(title, response.get('Items'))

    elif not title and artist and not year:
        response = MUSIC_TABLE.scan()
        filtered_list = get_filtered_music_by_artist(artist, response.get('Items'))

    elif not title and artist and year:
        response = MUSIC_TABLE.scan()
        filtered_list = get_filtered_music_by_artist_and_year(artist, year, response.get('Items'))

    elif title and artist and year:
        response = MUSIC_TABLE.scan()
        filtered_list = get_filtered_music_by_filters(artist, title, year, response.get('Items'))

    elif title and not artist and year:
        response = MUSIC_TABLE.scan()
        filtered_list = get_filtered_music_by_title_and_year(title, year, response.get('Items'))

    elif not title and not artist and year:
        response = MUSIC_TABLE.scan()
        filtered_list = get_filtered_music_by_year(year, response.get('Items'))

    return filtered_list


def get_filtered_music_by_artist_and_year(artist, year, music_list):
    filtered_list = []
    for item in music_list:
        if artist in item['artist'] and item['year'] == year:
            filtered_list.append(item)
    return filtered_list


def get_filtered_music_by_title_and_artist(artist, title, music_list):
    filtered_list = []
    for item in music_list:
        if artist in item['artist'] and title in item['title']:
            filtered_list.append(item)
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
        if title in item['title']:
            filtered_list.append(item)
    return filtered_list


def get_filtered_music_by_artist(artist, music_list):
    filtered_list = []
    for item in music_list:
        if artist in item['artist']:
            filtered_list.append(item)
    return filtered_list


def get_filtered_music_by_title_and_year(title, year, music_list):
    filtered_list = []
    for item in music_list:
        if title in item['title'] and item['year'] == year:
            filtered_list.append(item)
    return filtered_list


def get_filtered_music_by_filters(artist, title, year, music_list):
    filtered_list = []
    for item in music_list:
        if title in item['title'] and artist in item['artist'] and item['year'] == year:
            filtered_list.append(item)
    return filtered_list


def put_subscription(email, subscription):
    if email is None or subscription is None:
        raise ValueError("Args cannot be null.")

    try:
        response = SUBSCRIPTION_TABLE.get_item(Key={'email': email})
    except ClientError as e:
        print(e.response['Error']['Message'])

    if not response.get('Item'):
        subscription_set = {subscription}
        response = SUBSCRIPTION_TABLE.put_item(
            Item={
                'email': email,
                'subscription': subscription_set
            }
        )
    else:
        item = response.get('Item')
        item['subscription'].add(subscription)
        SUBSCRIPTION_TABLE.put_item(Item=item)

        print(subscription + " added to " + email)
        return response


def remove_subscription(email, sub):
    if sub is None:
        raise ValueError("Args cannot be null.")
    try:
        response = SUBSCRIPTION_TABLE.get_item(Key={'email': email})
    except ClientError as e:
        print(e.response['Error']['Message'])

    item = response.get('Item')
    if not item:
        raise ValueError("No item returned from Dynamodb.")
    else:
        item['subscription'].remove(sub)
        # Delete entry from table if last subscription because DynamoDB does not allow empty set.
        if len(item['subscription']) == 0:
            print("Last subscription removed. Deleting entry for user : " + email)
            SUBSCRIPTION_TABLE.delete_item(Key={'email': email})
        else:
            SUBSCRIPTION_TABLE.put_item(Item=item)
            print(sub + " has been removed from subscriptions.")


def get_all_subscriptions(email):
    sub_list = []
    if email is None:
        raise ValueError("Arg cannot be null.")

    response = SUBSCRIPTION_TABLE.query(
        KeyConditionExpression=Key('email').eq(email)
    )

    print("Returned " + str(response.get('Count')) + " items")
    items = response.get('Items')
    # get subscription set from response
    for item in items:
        subscription_set = item.get('subscription')
        for subscription in subscription_set:
            # convert json to dict
            json_str = subscription.replace("\'", "\"")
            sub_list.append(json.loads(json_str))

    return sub_list
