import os
import time
import logging
import requests
from homura import download as fetch
from tempfile import mkdtemp
from datetime import datetime
from collections import OrderedDict

logger = logging.getLogger('landsat8.meta')


def convert_date(value):
    return datetime.strptime(value, '%Y-%m-%d').date()


def download_meta(url, download_path):
    dpath = download_path if download_path else mkdtemp()
    dpath = os.path.join(dpath, 'LANDSAT_8.csv')

    # don't download if the file is downloaded in the last 6 hours
    if os.path.isfile(dpath):
        mtime = os.path.getmtime(dpath)
        if time.time() - mtime < (6 * 60 * 60):
            return open(dpath, 'r')

    fetch(url, dpath)
    return open(dpath, 'r')


def csv_reader(dst, writers, start_date=None, end_date=None, url=None,
               download=False, download_path=None):
    """ Reads landsat8 metadata from a csv file stored on USGS servers
    and applys writer functions on the data """

    if not url:
        url = 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_8.csv'

    # download the whole file
    if download:
        logger.info('Downloading landsat8 metadata file')

        # don't download if the file is downloaded in the last 6 hours
        f = download_meta(url, download_path)
        liner = f.readlines

    # or read line by line
    else:
        logger.info('Streaming landsat8 metadata file')
        r = requests.get(url, stream=True)
        liner = r.iter_lines

    header = None
    start_write = False

    for line in liner():
        row = line.split(',')

        # first line is the header
        if not header:
            header = row

        # other lines have values
        else:
            for j, v in enumerate(row):
                try:
                    row[j] = float(v)
                except ValueError:
                    pass

            # generate the record
            record = OrderedDict(zip(header, row))

            # apply filter
            # if there is an enddate, stops the process when the end date is reached
            if not end_date:
                start_write = True

            if end_date and record['acquisitionDate'] == end_date:
                start_write = True

            if start_date and record['acquisitionDate'] == start_date:
                break

            # if condition didn't match, generate path and apply writers and go to the next line
            if start_write:
                date = convert_date(record['acquisitionDate'])
                path = os.path.join(dst, str(date.year), str(date.month), str(date.day))

                logger.info('processing %s' % record['sceneID'])
                for w in writers:
                    w(path, record)
