import os
import json
import logging
import requests
from collections import OrderedDict

logger = logging.getLogger('landsat8.meta')


def file_writer(path, metadata):
    if not os.path.exists(path):
        os.makedirs(path)

    f = open(os.path.join(path, metadata['tile_name'] + '.json'), 'w')
    f.write(json.dumps(metadata))
    f.close()


def csv_reader(dst, writers, start_date=None, end_date=None, url=None):
    """ Reads landsat8 metadata from a csv file stored on USGS servers
    and applys writer functions on the data """

    if not url:
        url = 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_8.csv'
    r = requests.get(url, stream=True)

    header = None
    start_write = False

    for line in r.iter_lines():
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
                logger.info('processing %s' % record['sceneID'])
                for w in writers:
                    w(dst, record)
