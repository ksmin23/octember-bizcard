#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import json
import os
import urllib.parse
import re
import base64
import traceback
import datetime
import hashlib

import boto3

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
    #print('phone', phones)
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
  #print('Loading get_textract_data')

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
      print(response) #debug
      break
    except Exception as ex:
      import time

      traceback.print_exc()
      time.sleep(2)
  else:
    raise RuntimeError('[ERROR] Failed to put_records into kinesis stream: {}'.format(kinesis_stream_name))


def lambda_handler(event, context):
  #TODO: get event from kinesis; should process multiple events
  # Get the object from the event and show its content type
  bucket = event['Records'][0]['s3']['bucket']['name']
  key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
  try:
    detectedText = get_textract_data(bucket, key)
    doc = parse_textract_data(detectedText)
    owner = os.path.basename(key).split('_')[0] 
    payload = {'s3_bucket': bucket, 's3_key': key, 'owner': owner, 'data': doc}
    #TODO: send records to kinesis
    #write_records_to_kinesis([payload], kinesis_stream_name)
    return payload
  except Exception as ex:
    print(ex)
    print('[ERROR] getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
    raise ex

def lambda_handler_for_kinesis_event(event, context):
  import collections

  counter = collections.OrderedDict([('reads', 0),
      ('writes', 0), ('errors', 0)])

  #TODO: get event from kinesis; should process multiple events
  for record in event['Records']:
    try:
      counter['reads'] += 1
      payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
      json_data = json.loads(payload)
      bucket, key = (json_data['s3_bucket'], json_data['s3_key'])
      detected_text = get_textract_data(bucket, key)
      doc = parse_textract_data(detected_text)

      doc['doc_id'] = hashlib.md5(os.path.basename(key).encode('utf-8')).hexdigest()[:8]
      doc['created_at'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
      doc['is_alive'] = True

      #XXX: deduplicate contents
      content_id = ':'.join('{}'.format(doc.get(k, '').lower()) for k in ('name', 'email', 'phone_number'))
      doc['content_id'] = hashlib.md5(content_id.encode('utf-8')).hexdigest()[:8]

      owner = os.path.basename(key).split('_')[0]
      text_data = {'s3_bucket': bucket, 's3_key': key, 'owner': owner, 'data': doc}
      #TODO: send records to kinesis
      #write_records_to_kinesis([text_data], kinesis_stream_name)
      counter['writes'] += 1
      return text_data
    except Exception as ex:
      counter['errors'] += 1
      print('[ERROR] getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket), file=sys.stderr)
      traceback.print_exc()
  print('[INFO]', ', '.join(['{}={}'.format(k, v) for k, v in counter.items()]), file=sys.stderr)


def test_for_kinesis_event():
  kinesis_data = [
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/sungmk_bizcard.jpg"}''',
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
  res = lambda_handler_for_kinesis_event(event,{})
  print(json.dumps(res, indent=2))


def test():
  event = '''{
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "octember-use1",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:aws:s3:::octember-use1"
        },
        "object": {
          "key": "bizcard-raw-img/sungmk_bizcard.jpg",
          "size": 638,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}'''

  res = lambda_handler(json.loads(event), {})
  print(json.dumps(res))


def test3():
  event = '''{
  "Records": [
    {
      "eventVersion": "2.0",
      "eventSource": "aws:s3",
      "awsRegion": "us-east-1",
      "eventTime": "1970-01-01T00:00:00.000Z",
      "eventName": "ObjectCreated:Put",
      "userIdentity": {
        "principalId": "EXAMPLE"
      },
      "requestParameters": {
        "sourceIPAddress": "127.0.0.1"
      },
      "responseElements": {
        "x-amz-request-id": "EXAMPLE123456789",
        "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
      },
      "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "testConfigRule",
        "bucket": {
          "name": "octember-use1",
          "ownerIdentity": {
            "principalId": "EXAMPLE"
          },
          "arn": "arn:aws:s3:::octember-use1"
        },
        "object": {
          "key": "bizcard-raw-img/sungmk_bizcard.jpg",
          "size": 638,
          "eTag": "0123456789abcdef0123456789abcdef",
          "sequencer": "0A1B2C3D4E5F678901"
        }
      }
    }
  ]
}'''

  event = json.loads(event)
  bucket = event['Records'][0]['s3']['bucket']['name']
  key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
  record = {'s3_bucket': bucket, 's3_key': key}
  records = [record]
  kinesis_stream_name = 'octember-bizcard-img'
  write_records_to_kinesis(records, kinesis_stream_name)


def test2():
  record = {"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/sungmk_bizcard.jpg", "owner": "sungmk", "data": {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "sungmk@amazon.com", "phone_number": "2710 9704", "company": "aws", "name": "Sungmin Kim", "job_title": "Solutions Architect"}}
  records = [record]
  kinesis_stream_name = 'octember-bizcard-text'
  write_records_to_kinesis(records, kinesis_stream_name)


if __name__ == '__main__':
  #test3()
  test_for_kinesis_event()

