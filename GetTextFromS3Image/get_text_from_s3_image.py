#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import json
import os
import re
import base64
import traceback
import datetime

import boto3

AWS_REGION = os.getenv('REGION_NAME', 'us-east-1')
KINESIS_STREAM_NAME = os.getenv('KINESIS_STREAM_NAME', 'octember-bizcard-text')
DDB_TABLE_NAME = os.getenv('DDB_TABLE_NAME', 'OctemberBizcardImg')

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


def get_textract_data(textract_client, bucketName, documentKey):
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


def write_records_to_kinesis(kinesis_client, kinesis_stream_name, records):
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


def update_process_status(ddb_client, table_name, item):
  def ddb_update_item():
    s3_bucket = item['s3_bucket']
    s3_key = item['s3_key']
    image_id = os.path.basename(s3_key)
    status = item['status']
    modified_time = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')

    response = ddb_client.update_item(
      TableName=table_name,
      Key={
        "image_id": {
          "S": image_id
        }
      },
      UpdateExpression="SET s3_bucket = :s3_bucket, s3_key = :s3_key, mts = :mts, #status = :status",
      ExpressionAttributeNames={
        '#status': 'status'
      },
      ExpressionAttributeValues={
        ":s3_bucket": {
          "S": s3_bucket
        },
        ":s3_key": {
          "S":  s3_key
        },
        ":mts": {
          "N": "{}".format(modified_time)
        },
        ":status": {
          "S": status
        }
      }
    )
    return response

  try:
    res = ddb_update_item()
    print('[DEBUG]', res, file=sys.stderr)
  except Exception as ex:
    traceback.print_exc()
    print('[ERROR]', res, file=sys.stderr)
    raise ex


def copy_bizcard_to_user_photo_album(s3_client, params):
  src_bucket, src_key, owner = params['s3_bucket'], params['s3_key'], params['owner']
  copy_source = {
    'Bucket': src_bucket,
    'Key': src_key
  }

  image_id = os.path.basename(src_key)
  dest_s3_bucket = src_bucket
  dest_s3_key = 'bizcard-by-user/{owner}/{image_id}'.format(owner=owner, image_id=image_id)
  s3_client.copy(copy_source, dest_s3_bucket, dest_s3_key)
  return {'s3_bucket': dest_s3_bucket, 's3_key': dest_s3_key, 'owner': owner}


def lambda_handler(event, context):
  import collections

  textract_client = boto3.client('textract', region_name=AWS_REGION)
  kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)
  ddb_client = boto3.client('dynamodb', region_name=AWS_REGION)
  s3_client = boto3.client('s3', region_name=AWS_REGION)

  counter = collections.OrderedDict([('reads', 0),
      ('writes', 0), ('errors', 0)])

  for record in event['Records']:
    try:
      counter['reads'] += 1

      payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
      json_data = json.loads(payload)

      bucket, key = (json_data['s3_bucket'], json_data['s3_key'])
      update_process_status(ddb_client, DDB_TABLE_NAME, {'s3_bucket': bucket, 's3_key': key, 'status': 'PROCESS'})

      detected_text = get_textract_data(textract_client, bucket, key)

      doc = parse_textract_data(detected_text)
      doc['created_at'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

      owner = os.path.basename(key).split('_')[0]
      text_data = {'s3_bucket': bucket, 's3_key': key, 'owner': owner, 'data': doc}
      print('[DEBUG]', json.dumps(text_data), file=sys.stderr)

      write_records_to_kinesis(kinesis_client, KINESIS_STREAM_NAME, [text_data])
      ret = copy_bizcard_to_user_photo_album(s3_client, {'s3_bucket': bucket, 's3_key': key, 'owner': owner})

      update_process_status(ddb_client, DDB_TABLE_NAME, {'s3_bucket': ret['s3_bucket'], 's3_key': ret['s3_key'], 'status': 'END'})

      counter['writes'] += 1
    except Exception as ex:
      counter['errors'] += 1
      print('[ERROR] getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket), file=sys.stderr)
      traceback.print_exc()
  print('[INFO]', ', '.join(['{}={}'.format(k, v) for k, v in counter.items()]), file=sys.stderr)


if __name__ == '__main__':
  kinesis_data = [
    '''{"s3_bucket": "octember-use1", "s3_key": "bizcard-raw-img/sungmk_20191029.jpg"}''',
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

