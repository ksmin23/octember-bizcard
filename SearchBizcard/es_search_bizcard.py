#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import json
import os
import traceback
import pprint

import boto3
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from requests_aws4auth import AWS4Auth

ES_INDEX, ES_TYPE = (os.getenv('ES_INDEX', 'octember_bizcard'), os.getenv('ES_TYPE', 'bizcard'))
ES_HOST = os.getenv('ES_HOST', 'vpc-octember-kfwwunjrm422d44nr7dnhvjsw4.us-east-1.es.amazonaws.com')

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
  try:
    query_keywords = event["queryStringParameters"]["query"]
    user_name = event["queryStringParameters"]["user"]

    es_query_body = {
      "query": {
        "multi_match" : {
          "query":  query_keywords,
          "fields": [ "name^3", "company", "job_title" ]
        }
      }
    }

    res = es_client.search(index=ES_INDEX, body=es_query_body)
    print("[INFO] Got {} Hits:".format(res['hits']['total']['value']), file=sys.stderr)

    #XXX: https://aws.amazon.com/ko/premiumsupport/knowledge-center/malformed-502-api-gateway/
    response = {
      'statusCode': 200,
      'body': json.dumps(res),
      'isBase64Encoded': False
    }
    return response
  except Exception as ex:
    traceback.print_exc()

    response = {
      'statusCode': 404,
      'body': 'Not Found'
      'isBase64Encoded': False
    }
    return response


if __name__ == '__main__':
  event = {
    "resource": "/search",
    "path": "/search",
    "httpMethod": "GET",
    "headers": None,
    "multiValueHeaders": None,
    "queryStringParameters": {
      "query": "sungmin",
      "user": "sungmk"
    },
    "multiValueQueryStringParameters": {
      "query": [
        "sungmin"
      ],
      "user": [
        "sungmk"
      ]
    },
    "pathParameters": None,
    "stageVariables": None,
    "requestContext": {
      "resourceId": "rbszcr",
      "resourcePath": "/search",
      "httpMethod": "GET",
      "extendedRequestId": "CHz0dGu7oAMFk1Q=",
      "requestTime": "25/Oct/2019:14:01:00 +0000",
      "path": "/search",
      "accountId": "819320734790",
      "protocol": "HTTP/1.1",
      "stage": "test-invoke-stage",
      "domainPrefix": "testPrefix",
      "requestTimeEpoch": 1572012060411,
      "requestId": "a25a5212-4c46-47c0-aea5-a975fe8fac3d",
      "identity": {
        "cognitoIdentityPoolId": None,
        "cognitoIdentityId": None,
        "apiKey": "test-invoke-api-key",
        "principalOrgId": None,
        "cognitoAuthenticationType": None,
        "userArn": "arn:aws:iam::819320734790:user/ksmin23",
        "apiKeyId": "test-invoke-api-key-id",
        "userAgent": "aws-internal/3 aws-sdk-java/1.11.641 Linux/4.9.184-0.1.ac.235.83.329.metal1.x86_64 OpenJDK_64-Bit_Server_VM/25.222-b10 java/1.8.0_222 vendor/Oracle_Corporation",
        "accountId": "819320734790",
        "caller": "AIDA35Q2SIRDDNCR7ZAIN",
        "sourceIp": "test-invoke-source-ip",
        "accessKey": "ASIA35Q2SIRDGDANYOSZ",
        "cognitoAuthenticationProvider": None,
        "user": "AIDA35Q2SIRDDNCR7ZAIN"
      },
      "domainName": "testPrefix.testDomainName",
      "apiId": "h02uojhcic"
    },
    "body": None,
    "isBase64Encoded": False
  }

  res = lambda_handler(event, {})
  pprint.pprint(res)

