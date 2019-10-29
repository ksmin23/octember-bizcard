#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import json
import os
import urllib.parse
import traceback
import datetime

import boto3

DRY_RUN = (os.getenv('DRY_RUN', 'false') == 'true')

AWS_REGION = os.getenv('REGION_NAME', 'us-east-1')
KINESIS_STREAM_NAME = os.getenv('KINESIS_STREAM_NAME', 'octember-bizcard-img')
DDB_TABLE_NAME = os.getenv('DDB_TABLE_NAME', 'OctemberBizcardImg')


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
  for _ in range(MAX_RETRY_COUNT):
    try:
      response = kinesis_client.put_records(Records=record_list, StreamName=kinesis_stream_name)
      print("[DEBUG]", response, file=sys.stderr)
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
      ConditionExpression = "attribute_not_exists(image_id) OR mts < :mts",
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
    raise ex


def lambda_handler(event, context):
  kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)
  ddb_client = boto3.client('dynamodb', region_name=AWS_REGION)

  for record in event['Records']:
    try:
      bucket = record['s3']['bucket']['name']
      key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')

      record = {'s3_bucket': bucket, 's3_key': key}
      print("[INFO]", record, file=sys.stderr)
      write_records_to_kinesis(kinesis_client, KINESIS_STREAM_NAME, [record])
      update_process_status(ddb_client, DDB_TABLE_NAME, {'s3_bucket': bucket, 's3_key': key, 'status': 'START'})
    except Exception as ex:
      traceback.print_exc()


if __name__ == '__main__':
  s3_event = '''{
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

  event = json.loads(s3_event)
  lambda_handler(event, {})

