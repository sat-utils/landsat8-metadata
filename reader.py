import os
import time
import logging
import requests
from Queue import Queue
from concurrent import futures
from datetime import datetime
from homura import download as fetch
from tempfile import mkdtemp
from collections import OrderedDict

logger = logging.getLogger('landsat8.meta')


class ReachedEndOfProcess(Exception):
    pass


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


def row_processor(record, date, dst, writers):

    path = os.path.join(dst, str(date.year), str(date.month), str(date.day))

    logger.info('processing %s' % record['sceneID'])
    for w in writers:
        w(path, record)


def csv_reader(dst, writers, start_date=None, end_date=None, url=None,
               download=False, download_path=None, num_worker_threads=1):
    """ Reads landsat8 metadata from a csv file stored on USGS servers
    and applys writer functions on the data """

    threaded = False
    threads = []

    if num_worker_threads > 0:
        threaded = True
        queue = Queue()

    if not url:
        url = 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_8.csv'

    # download the whole file
    if download:
        logger.info('Downloading landsat8 metadata file')

        # don't download if the file is downloaded in the last 6 hours
        f = download_meta(url, download_path)
        liner = f

    # or read line by line
    else:
        logger.info('Streaming landsat8 metadata file')
        r = requests.get(url, stream=True)
        liner = r.iter_lines

    if start_date:
        start_date = convert_date(start_date)

    if end_date:
        end_date = convert_date(end_date)

    header = None

    # read the header
    line = liner.next()
    header = line.split(',')

    def gen(line):
        row = line.split(',')

        for j, v in enumerate(row):
            try:
                row[j] = float(v)
            except ValueError:
                pass

        # generate the record
        record = OrderedDict(zip(header, row))
        date = convert_date(record['acquisitionDate'])

        # apply filter
        # if there is an enddate, stops the process when the end date is reached
        if end_date and date > end_date:
            return

        if start_date and date < start_date:
            return

        row_processor(record, date, dst, writers)

    with futures.ThreadPoolExecutor(max_workers=num_worker_threads) as executor:
        try:
            results = executor.map(gen, liner, timeout=30)
        except futures.TimeoutError:
            print('skipped')

        #for future in futures.as_completed(results):
        #    print('saved')

