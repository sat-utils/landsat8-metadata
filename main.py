import os
import json
import boto
import urllib2
from boto.s3.key import Key
import boto3
import click
import logging
from copy import copy
from collections import OrderedDict
from datetime import date, timedelta
from elasticsearch import Elasticsearch, RequestError, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import requests

from reader import csv_reader

logger = logging.getLogger('landsat8.meta')
bucket_name = os.getenv('BUCKETNAME', 'landsat8-meta')
thumbs_bucket_name = os.getenv('THUMBS_BUCKETNAME', 'ad-thumbnails')
s3 = boto3.resource('s3')
es_index = 'sat-api'
es_type = 'landsat8'


def get_url(url, j=False):
    req = urllib2.Request(url)
    r = urllib2.urlopen(req).read()
    if j:
        obj = json.loads(r)
    else:
        obj = r
    return obj


def get_role():
    url = 'http://169.254.169.254/latest/meta-data/iam/security-credentials/'
    obj = get_url(url)
    return obj


def get_credentials():
    role = get_role()
    url = 'http://169.254.169.254/latest/meta-data/iam/security-credentials/' + role
    obj = get_url(url, True)
    return obj


def connection_to_es(es_host, es_port, aws=False):

    args = {}

    if aws:
        cred = get_credentials()

        access_key = cred['AccessKeyId']
        secret_access = cred['SecretAccessKey']
        token = cred['token']
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


def create_index(index_name, doc_type, es_host, es_port, **kwargs):

    body = {
        doc_type: {
            'properties': {
                'scene_id': {'type': 'string', 'index': 'not_analyzed'},
                'satellite_name': {'type': 'string'},
                'cloud_coverage': {'type': 'float'},
                'date': {'type': 'date'},
                'data_geometry': {
                    'type': 'geo_shape',
                    'tree': 'quadtree',
                    'precision': '5mi'}
            }
        }
    }
    es = connection_to_es(es_host, es_port, kwargs['aws'])

    es.indices.create(index=index_name, ignore=400)

    es.indices.put_mapping(
        doc_type=doc_type,
        body=body,
        index=index_name
    )


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


def elasticsearch_updater(product_dir, metadata, **kwargs):

    try:
        es = connection_to_es(kwargs['es_host'], kwargs['es_port'], kwargs['aws'])

        body = meta_constructor(metadata)

        logger.info('Pushing to Elasticsearch')

        try:
            es.index(index=es_index, doc_type=es_type, id=body['scene_id'],
                     body=body)
        except RequestError as e:
            body['data_geometry'] = None
            es.index(index=es_index, doc_type=es_type, id=body['scene_id'],
                     body=body)

    except Exception as e:
        logger.error('Unhandled error occured while writing to elasticsearch')
        logger.error('Details: %s' % e.__str__())


def dynamodb_updater(product_dir, metadata, **kwargs):
    client = boto3.client('dynamodb', region_name='us-east-1')
    client.put_item(
        TableName='landsat',
        Item={
            'scene_id': {
                'S': metadata['sceneID']
            },
            'body': {
                'S': json.dumps(metadata)
            }
        }
    )

    print('Posted %s to DynamoDB' % metadata['sceneID'])


def thumbnail_writer(product_dir, metadata, **kwargs):
    """
    Extra function to download images from USGS, then upload to S3 and call
    the ES metadata writer afterwards.
    """

    # Download original thumbnail
    output_file = metadata['sceneID'] + '.jpg'
    thumbnail = 'https://' + thumbs_bucket_name + '.s3.amazonaws.com/' + output_file

    new_thumb = requests.get(thumbnail)
    if new_thumb.status_code != 200:

        # Upload thumbnail to S3
        thumbs_bucket_name2 = os.getenv('THUMBS_BUCKETNAME', 'ad-thumbnails')
        try:
            print('uploading %s' % output_file)
            c = boto.connect_s3()
            b = c.get_bucket(thumbs_bucket_name2)
            k = Key(b, name=output_file)
            k.set_metadata('Content-Type', 'image/jpeg')
            r = requests.get(metadata['browseURL'])
            k.set_contents_from_string(r.content, policy='public-read')
        except Exception as e:
            print(e)

    # Update metadata record
    metadata['thumbnail'] = thumbnail

    dynamodb_updater(product_dir, metadata, **kwargs)
    return


def file_writer(product_dir, metadata):
    body = meta_constructor(metadata)

    if not os.path.exists(product_dir):
        os.makedirs(product_dir)

    f = open(os.path.join(product_dir, body['scene_id'] + '.json'), 'w')
    f.write(json.dumps(body))
    logger.info('saving to disk at %s' % product_dir)
    f.close()


def s3_writer(product_dir, metadata):
    # make sure product_dir doesn't start with slash (/) or dot (.)
    if product_dir.startswith('.'):
        product_dir = product_dir[1:]

    if product_dir.startswith('/'):
        product_dir = product_dir[1:]

    body = meta_constructor(metadata)

    key = os.path.join(product_dir, body['scene_id'] + '.json')
    s3.Object(bucket_name, key).put(Body=json.dumps(body), ACL='public-read', ContentType='application/json')

    logger.info('saving to s3 at %s')


def last_updated(today):
    """ Gets the latest time a product added to Elasticsearch """

    bucket = s3.Bucket(bucket_name)

    start_day = today.day
    start_month = today.month

    yr_counter = 0
    while True:
        m_counter = 0
        year = today.year - yr_counter
        if year < 2015:
            break
        while True:
            month = start_month - m_counter
            if month == 0:
                start_month = 12
                break
            d_counter = 0
            while True:
                day = start_day - d_counter
                if day == 0:
                    start_day = 31
                    break
                path = os.path.join(str(year), str(month), str(day))
                logger.info('checking %s' % path)
                objs = bucket.objects.filter(Prefix=path).limit(1)
                if list(objs):
                    return date(year, month, day)
                d_counter += 1
            m_counter += 1
        yr_counter += 1

    return None


@click.command()
@click.argument('ops', metavar='<operations: choices: s3 | es | disk | thumbs>', nargs=-1)
@click.option('--start', default=None, help='Start Date. Format: YYYY-MM-DD')
@click.option('--end', default=None, help='End Date. Format: YYYY-MM-DD')
@click.option('--es-host', default='localhost', help='Elasticsearch host address')
@click.option('--es-port', default=9200, type=int, help='Elasticsearch port number')
@click.option('--folder', default='.', help='Destination folder if is written to disk')
@click.option('--download', is_flag=True,
              help='Sets the updater to download the metadata file first instead of streaming it')
@click.option('--aws', is_flag=True, default=False,
              help='Uses AWS STS to obtain aws credentials for accessing the various services')
@click.option('--download-folder', default=None,
              help='The folder to save the downloaded metadata to. Defaults to a temp folder')
@click.option('-v', '--verbose', is_flag=True)
@click.option('--concurrency', default=20, type=int, help='Process concurrency. Default=20')
def main(ops, start, end, es_host, es_port, folder, download,
         aws, download_folder, verbose, concurrency):

    if not ops:
        raise click.UsageError('No Argument provided. Use --help if you need help')

    accepted_args = {
        'es': elasticsearch_updater,
        's3': s3_writer,
        'disk': file_writer,
        'thumbs': thumbnail_writer,
        'db': dynamodb_updater
    }

    writers = []
    for op in ops:
        if op in accepted_args.keys():
            writers.append(accepted_args[op])
        else:
            raise click.UsageError('Operation (%s) is not supported' % op)

    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()

    if verbose:
        ch.setLevel(logging.INFO)
    else:
        ch.setLevel(logging.ERROR)

    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if 'es' in ops or 'thumbs' in ops:
        create_index(es_index, es_type, es_host, es_port, aws=aws)

    if not start and not end:
        delta = timedelta(days=3)
        start = date.today() - delta
        start = '{0}-{1}-{2}'.format(start.year, start.month, start.day)

    csv_reader(folder, writers, start_date=start, end_date=end, download=download, download_path=download_folder,
               num_worker_threads=concurrency, es_host=es_host, es_port=es_port, aws=aws)


if __name__ == '__main__':
    main()
