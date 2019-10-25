#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import json
import os
import urllib.parse
import re
import base64
import traceback
import datetime
import hashlib

import boto3

KINESIS_STREAM_NAME = 'octember-bizcard-text'

textract_client = boto3.client('textract')

def parse_textract_data(lines):
  def _get_email(s):
    email_re = re.compile(r'[a-zA-Z0-9+_\-\.]+@[0-9a-zA-Z][.-0-9a-zA-Z]*.[a-zA-Z]+')
    emails = email_re.findall(s)
    return emails[0] if emails else ''

  def _get_addr(s):
    ko_addr_stopwords = ['-gu', '-ro', '-do', ' gu', ' ro', ' do', ' seoul', ' korea']
    addr_txt = s.lower()
    score = sum([1 if e in addr_txt else 0 for e in ko_addr_stopwords])
    return s if score >= 3 else ''

  def _get_phone_number(s):
    #phone_number_re = re.compile(r'(?:\+ *)?\d[\d\- ]{7,}\d')
    phone_number_re = re.compile(r'\({0,1}\+{0,1}[\d ]*[\d]{2,}\){0,1}[\d\- ]{7,}')
    phones = phone_number_re.findall(s)
    return phones[0] if phones else ''

  funcs = {
    'email': _get_email,
    'addr': _get_addr,
    'phone_number': _get_phone_number
  }

  doc = {}
  for line in lines:
    for k in ['email', 'addr', 'phone_number']:
      ret = funcs[k](line)
      if ret:
        doc[k] = ret

  #TODO: assume that a biz card dispaly company, name, job title in order
  company_name, name, job_title = lines[:3]
  doc['company'] = company_name
  doc['name'] = name
  doc['job_title'] = job_title

  return doc


def get_textract_data(bucketName, documentKey):
  print('[DEBUG] Loading get_textract_data', file=sys.stderr)

  response = textract_client.detect_document_text(
  Document={
    'S3Object': {
    'Bucket': bucketName,
    'Name': documentKey
    }
  })

  detected_text_list = [item['Text'] for item in response['Blocks'] if item['BlockType'] == 'LINE']
  return detected_text_list


def write_records_to_kinesis(records, kinesis_stream_name):
  import random
  random.seed(47)

  def gen_records():
    record_list = []
    for rec in records:
      payload = json.dumps(rec, ensure_ascii=False)
      partition_key = 'part-{:05}'.format(random.randint(1, 1024))
      record_list.append({'Data': payload, 'PartitionKey': partition_key})
    return record_list

  MAX_RETRY_COUNT = 3
  kinesis_client = boto3.client('kinesis')

  record_list = gen_records()
  for i in range(MAX_RETRY_COUNT):
    try:
      response = kinesis_client.put_records(Records=record_list, StreamName=kinesis_stream_name)
      print('[DEBUG]', response, file=sys.stderr)
      break
    except Exception as ex:
      import time

      traceback.print_exc()
      time.sleep(2)
  else:
    raise RuntimeError('[ERROR] Failed to put_records into kinesis stream: {}'.format(kinesis_stream_name))


def lambda_handler(event, context):
  import collections

  counter = collections.OrderedDict([('reads', 0),
      ('writes', 0), ('errors', 0)])

  for record in event['Records']:
    try:
      counter['reads'] += 1

      payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
      json_data = json.loads(payload)

      bucket, key = (json_data['s3_bucket'], json_data['s3_key'])
      detected_text = get_textract_data(bucket, key)

      doc = parse_textract_data(detected_text)
      doc['created_at'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

      owner = os.path.basename(key).split('_')[0]
      text_data = {'s3_bucket': bucket, 's3_key': key, 'owner': owner, 'data': doc}
      print('[DEBUG]', json.dumps(text_data), file=sys.stderr)

      write_records_to_kinesis([text_data], KINESIS_STREAM_NAME)
      counter['writes'] += 1
    except Exception as ex:
      counter['errors'] += 1
      print('[ERROR] getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket), file=sys.stderr)
      traceback.print_exc()
  print('[INFO]', ', '.join(['{}={}'.format(k, v) for k, v in counter.items()]), file=sys.stderr)


if __name__ == '__main__':
  kinesis_data = [
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/sungmk_20191025_1622.jpg"}''',
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

