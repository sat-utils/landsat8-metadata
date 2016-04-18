### Landsat8 Metadata Generator

This small library helps with generating landsat 8 metadata and upload it to Amazon S3 and/or ElasticSearch

#### Installation

    $ pip install -r requirements.txt

#### Usage

```
    $ python main.py --help

    Usage: main.py [OPTIONS] <operations: choices: s3 | es | disk>

    Options:
      --start TEXT           Start Date. Format: YYYY-MM-DD
      --end TEXT             End Date. Format: YYYY-MM-DD
      --es-host TEXT         Elasticsearch host address
      --es-port INTEGER      Elasticsearch port number
      --folder TEXT          Destination folder if is written to disk
      -v, --verbose
      --help                 Show this message and exit.
```

Example:

    $ python main.py s3 es --start='2016-01-01' --verbose
