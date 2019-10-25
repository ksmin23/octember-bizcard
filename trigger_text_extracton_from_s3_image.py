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


KINESIS_STREAM_NAME = 'octember-bizcard-img'

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
      print("[INFO]", response, file=sys.stderr) #debug
      break
    except Exception as ex:
      import time

      traceback.print_exc()
      time.sleep(2)
  else:
    raise RuntimeError('[ERROR] Failed to put_records into kinesis stream: {}'.format(kinesis_stream_name))


def lambda_handler(event, context):
  try:
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    record = {'s3_bucket': bucket, 's3_key': key}
    print("[INFO]", record, file=sys.stderr)
    write_records_to_kinesis([record], KINESIS_STREAM_NAME)
    #TODO: update image processing status into DynamoDB
  except Exception as ex:
    traceback.print_exc()


if __name__ == '__main__':
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

  lambda_handler(json.loads(event), {})

