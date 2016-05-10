import os
import boto3
import requests

thumbs_bucket_name = os.getenv('THUMBS_BUCKETNAME', 'ad-thumbnails')
s3 = boto3.resource('s3')


def thumbnail_writer(product_dir, metadata):
    """
    Extra function to download images from USGS, then upload to S3 and call
    the ES metadata writer afterwards.
    """

    from main import elasticsearch_updater
    # Download original thumbnail
    orig_url = metadata['browseURL']
    r = requests.get(orig_url)
    output_file = metadata['sceneID'] + '.jpg'

    # Upload thumbnail to S3
    s3.Object(thumbs_bucket_name, output_file).put(Body=r.content,
                                                   ACL='public-read',
                                                   ContentType='image/jpeg')

    # Update metadata record
    metadata['thumbnail'] = 'https://' + thumbs_bucket_name + \
        '.s3.amazonaws.com/' + output_file
    elasticsearch_updater(product_dir, metadata)
