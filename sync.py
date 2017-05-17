import os
import json
from collections import OrderedDict
from copy import copy
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestError, RequestsHttpConnection
from boto.utils import get_instance_metadata
import boto3


def meta_constructor(metadata):
    internal_meta = copy(metadata)

    data_geometry = {
        'type': 'Polygon',
        'crs': {
            'type': 'name',
            'properties': {
                'name': 'urn:ogc:def:crs:EPSG:8.9:4326'
            }
        },
        'coordinates': [[
            [metadata.get('upperRightCornerLongitude'), metadata.get('upperRightCornerLatitude')],
            [metadata.get('upperLeftCornerLongitude'), metadata.get('upperLeftCornerLatitude')],
            [metadata.get('lowerLeftCornerLongitude'), metadata.get('lowerLeftCornerLatitude')],
            [metadata.get('lowerRightCornerLongitude'), metadata.get('lowerRightCornerLatitude')],
            [metadata.get('upperRightCornerLongitude'), metadata.get('upperRightCornerLatitude')]
        ]]
    }

    body = OrderedDict([
        ('scene_id', metadata.get('sceneID')),
        ('satellite_name', 'landsat-8'),
        ('cloud_coverage', metadata.get('cloudCoverFull', 100)),
        ('date', metadata.get('acquisitionDate')),
        ('thumbnail', metadata.get('browseURL')),
        ('data_geometry', data_geometry)
    ])

    body.update(internal_meta)

    return body


def get_credentials():
    obj = get_instance_metadata()
    return obj['iam']['security-credentials'].values()[0]


def connection_to_es(es_host, es_port, aws=False):
    args = {}

    cred = get_credentials()
    access_key = cred['AccessKeyId']
    secret_access = cred['SecretAccessKey']
    token = cred['Token']
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    awsauth = AWS4Auth(access_key, secret_access, region, 'es',
                       session_token=token)

    args = {
        'http_auth': awsauth,
        'use_ssl': True,
        'verify_certs': True,
        'connection_class': RequestsHttpConnection
    }

    es = Elasticsearch(hosts=[{
        'host': es_host,
        'port': es_port
    }], **args)

    return es

def get_items(limit=100, last_key=None):
    """Gets items from DynamoDB"""

    items = []
    list_key = None
    client = boto3.client('dynamodb', region_name='us-east-1')

    args = {
        'TableName': 'landsat',
        'Limit': limit
    }

    if last_key:
        args['ExclusiveStartKey'] = last_key

    response = client.scan(**args)

    if response['Count'] > 0:
        for item in response['Items']:
            items.append(json.loads(item['body']['S']))

        return (items, response['LastEvaluatedKey'])
    else:
        raise Execption('No record found')

def bulk_updater(records):
    data = []

    for record in records:
        data.append({
            'index': {
                '_index': 'sat-api',
                '_type': 'landsat8',
                '_id': record['sceneID']
            }
        })
        data.append(meta_constructor(record))

    es = connection_to_es(os.getenv('ES_HOST'), 443)
    elasticsearch.helpers.parallel_bulk(es, data)
    # r = es.bulk(index='sat-api', body=data, refresh=True)
    # print(r)


def update_es():
    counter = 0
    limit = 200

    # get items from DynamoDB
    items, last_key = get_items(limit)
    if last_key:
        while True:
            bulk_updater(items)
            items, last_key = get_items(limit, last_key)
            counter = counter + limit
            print(str(counter) + '\r')

            if not last_key:
                break
    else:
        bulk_updater(items)

update_es()

#obj = get_instance_metadata()
#print(obj['iam']['security-credentials'].values())

