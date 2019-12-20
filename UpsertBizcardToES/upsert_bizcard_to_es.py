#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import json
import os
import base64
import traceback
import hashlib

import boto3
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from requests_aws4auth import AWS4Auth

ES_INDEX, ES_TYPE = (os.getenv('ES_INDEX', 'octember_bizcard'), os.getenv('ES_TYPE', 'bizcard'))
ES_HOST = os.getenv('ES_HOST')

AWS_REGION = os.getenv('REGION_NAME', 'us-east-1')

session = boto3.Session(region_name=AWS_REGION)
credentials = session.get_credentials()
credentials = credentials.get_frozen_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key
token = credentials.token

aws_auth = AWS4Auth(
    access_key,
    secret_key,
    AWS_REGION,
    'es',
    session_token=token
)

es_client = Elasticsearch(
    hosts = [{'host': ES_HOST, 'port': 443}],
    http_auth=aws_auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)
print('[INFO] ElasticSearch Service', json.dumps(es_client.info(), indent=2), file=sys.stderr)


def lambda_handler(event, context):
  import collections

  counter = collections.OrderedDict([('reads', 0),
      ('writes', 0),
      ('invalid', 0),
      ('errors', 0)])

  doc_list = []
  for record in event['Records']:
    try:
      counter['reads'] += 1
      payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
      json_data = json.loads(payload)

      if not all([json_data.get(k, None) for k in ('data', 'owner', 's3_key')]):
        counter['invalid'] += 1
        continue

      image_id = os.path.basename(json_data['s3_key'])
      doc = json_data['data']
      doc['doc_id'] = hashlib.md5(image_id.encode('utf-8')).hexdigest()[:8]
      doc['image_id'] = image_id
      doc['owner'] = json_data['owner']
      doc['is_alive'] = 1

      #XXX: deduplicate contents
      content_id = ':'.join('{}'.format(doc.get(k, '').lower()) for k in ('name', 'email', 'phone_number'))
      doc['content_id'] = hashlib.md5(content_id.encode('utf-8')).hexdigest()[:8]

      es_index_action_meta = {"index": {"_index": ES_INDEX, "_type": ES_TYPE, "_id": doc['doc_id']}}
      doc_list.append(es_index_action_meta)
      doc_list.append(doc)

      counter['writes'] += 1
    except Exception as ex:
      counter['errors'] += 1
      traceback.print_exc()

  print('[INFO]', ', '.join(['{}={}'.format(k, v) for k, v in counter.items()]), file=sys.stderr)

  try:
    es_bulk_body = '\n'.join([json.dumps(e) for e in doc_list])
    res = es_client.bulk(body=es_bulk_body, index=ES_INDEX, refresh=True)
  except Exception as ex:
    traceback.print_exc()


if __name__ == '__main__':
  kinesis_data = [
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/edy_bizcard_0046.jpg", "owner": "edy", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "edy@amazon.com", "phone_number": "(+82 10) 1025 7049", "company": "aws", "name": "Edy Kim", "job_title": "Specialist Solutions Architect", "created_at": "2019-10-25T01:12:54Z"}}''',
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/edy_bizcard_0071.jpg", "owner": "edy", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "crong@amazon.com", "phone_number": "(+82 10) 7433 9352", "company": "aws", "name": "Crong Lee", "job_title": "Associate Solutions Architect", "created_at": "2019-10-25T01:12:54Z"}}''',
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/edy_bizcard_0044.jpg", "owner": "edy", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "harry@amazon.com", "phone_number": "(+82 10) 4218 8396", "company": "aws", "name": "Harry Jang", "job_title": "Partner Solutions Architect", "created_at": "2019-10-25T01:12:54Z"}}''',
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/edy_bizcard_0050.jpg", "owner": "edy", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "poby@amazon.com", "phone_number": "(+82 10) 6430 0671", "company": "aws", "name": "Poby Kim", "job_title": "Solutions Architect", "created_at": "2019-10-25T01:12:54Z"}}''',
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/poby_bizcard_0050.jpg", "owner": "poby", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "poby@amazon.com", "phone_number": "(+82 10) 6430 0671", "company": "aws", "name": "Poby Kim", "job_title": "Solutions Architect", "created_at": "2019-10-25T01:12:54Z"}}''',
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/poby_bizcard_0046.jpg", "owner": "poby", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "edy@amazon.com", "phone_number": "(+82 10) 1025 7049", "company": "aws", "name": "Edy Kim", "job_title": "Specialist Solutions Architect", "created_at": "2019-10-25T01:12:54Z"}}''',
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/poby_bizcard_0054.jpg", "owner": "poby", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "pororo@amazon.com", "phone_number": "(+82 10) 0388 1679", "company": "aws", "name": "Pororo Kim", "job_title": "SA Manager", "created_at": "2019-10-25T01:12:54Z"}}''',
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/poby_bizcard_0001.jpg", "owner": "poby", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "rody@amazon.com", "phone_number": "(+82 10) 4323 7890", "company": "aws", "name": "Rody Park", "job_title": "Solutions Architect", "created_at": "2019-10-25T01:12:54Z"}}''',
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/pororo_bizcard_0093.jpg", "owner": "pororo", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "pororo@amazon.com", "phone_number": "(+82 10) 8957 0150", "company": "aws", "name": "Pororo Kim", "job_title": "SA Manager", "created_at": "2019-10-25T01:12:54Z"}}''',
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/pororo_bizcard_0041.jpg", "owner": "pororo", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "crong@amazon.com", "phone_number": "(+82 10) 7433 9352", "company": "aws", "name": "Crong Lee", "job_title": "Associate Solutions Architect", "created_at": "2019-10-25T01:12:54Z"}}''',
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/pororo_bizcard_0030.jpg", "owner": "pororo", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "harry@amazon.com", "phone_number": "(+82 10) 4218 8396", "company": "aws", "name": "Harry Jang", "job_title": "Partner Solutions Architect", "created_at": "2019-10-25T01:12:54Z"}}''',
  ]

  records = [{
    "eventID": "shardId-000000000000:49545115243490985018280067714973144582180062593244200961",
    "eventVersion": "1.0",
    "kinesis": {
      "approximateArrivalTimestamp": 1428537600,
      "partitionKey": "partitionKey-3",
      "data": base64.b64encode(e.encode('utf-8')),
      "kinesisSchemaVersion": "1.0",
      "sequenceNumber": "49545115243490985018280067714973144582180062593244200961"
    },
    "invokeIdentityArn": "arn:aws:iam::EXAMPLE",
    "eventName": "aws:kinesis:record",
    "eventSourceARN": "arn:aws:kinesis:EXAMPLE",
    "eventSource": "aws:kinesis",
    "awsRegion": "us-east-1"
    } for e in kinesis_data]
  event = {"Records": records}
  lambda_handler(event, {})

