#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import os
import json
import hashlib
import traceback
import pprint

import boto3
import redis

from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import T, P, Operator, Scope, Column, Order
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

AWS_REGION = os.getenv('REGION_NAME', 'us-east-1')
NEPTUNE_ENDPOINT = os.getenv('NEPTUNE_ENDPOINT')
NEPTUNE_PORT = int(os.getenv('NEPTUNE_PORT', '8182'))

NEPTUNE_CONN = None

ELASTICACHE_HOST = os.getenv('ELASTICACHE_HOST')
redis_client = redis.Redis(host=ELASTICACHE_HOST, port=6379, db=0)

def graph_traversal(neptune_endpoint=None, neptune_port=NEPTUNE_PORT, show_endpoint=True, connection=None):
  def _remote_connection(neptune_endpoint=None, neptune_port=None, show_endpoint=True):
    neptune_gremlin_endpoint = '{protocol}://{neptune_endpoint}:{neptune_port}/{suffix}'.format(protocol='ws',
      neptune_endpoint=neptune_endpoint, neptune_port=neptune_port, suffix='gremlin')

    if show_endpoint:
      print('[INFO] gremlin: {}'.format(neptune_gremlin_endpoint), file=sys.stderr)
    retry_count = 0
    while True:
      try:
        return DriverRemoteConnection(neptune_gremlin_endpoint, 'g')
      except HTTPError as ex:
        exc_info = sys.exc_info()
        if retry_count < 3:
          retry_count += 1
          print('[DEBUG] Connection timeout. Retrying...', file=sys.stderr)
        else:
          raise exc_info[0].with_traceback(exc_info[1], exc_info[2])

  if connection is None:
    connection = _remote_connection(neptune_endpoint, neptune_port, show_endpoint)
  return traversal().withRemote(connection)


def people_you_may_know(g, user_name, limit=10):
  from gremlin_python.process.traversal import Scope, Column, Order

  recommendations = (g.V().hasLabel('person').has('_name', user_name.lower()).as_('person').
    both('knows').aggregate('friends').
    both('knows').
      where(P.neq('person')).where(P.without('friends')).
    groupCount().by('id').
    order(Scope.local).by(Column.values, Order.decr).
    next())

  vertex_scores = [(key, score) for key, score in recommendations.items()][:limit]
  res = []
  for key, score in vertex_scores:
    value = {k: v for k, v in g.V(key).valueMap().next().items() if not (k == 'id' or k.startswith('_'))}
    value['score'] = float(score)
    res.append(value)
  return res


def lambda_handler(event, context):
  global NEPTUNE_CONN

  if NEPTUNE_CONN is None:
    NEPTUNE_CONN = graph_traversal(NEPTUNE_ENDPOINT, NEPTUNE_PORT, connection=None)
  graph_db = NEPTUNE_CONN
 
  try:
    user_name = event['queryStringParameters']['user']
    limit = int(event['queryStringParameters'].get('limit', 10))

    query_hash_code = hashlib.md5(user_name.lower().encode('utf-8')).hexdigest()[:8]
    query_id = 'pymk:query_id:{}'.format(query_hash_code)
    print('[DEBUG] PYMK query id: {}'.format(query_id))

    results = redis_client.get(query_id)
    results = results.decode('utf-8') if results != None else None
    if results is None:
      ret = people_you_may_know(graph_db, user_name, limit)
      total_count = len(ret)
      print("[INFO] Got {} Hits:".format(total_count), file=sys.stderr)
      results = json.dumps(ret)
      if total_count > 0:
        redis_client.set(query_id, results, ex=10*60, nx=True)

    #XXX: https://aws.amazon.com/ko/premiumsupport/knowledge-center/malformed-502-api-gateway/
    response = {
      'statusCode': 200,
      'body': results,
      'isBase64Encoded': False
    }
    return response
  except Exception as ex:
    traceback.print_exc()

    response = {
      'statusCode': 200,
      'body': '[]',
      'isBase64Encoded': False
    }
    return response


if __name__ == '__main__':
  event = {
    "resource": "/pymk",
    "path": "/pymk",
    "httpMethod": "GET",
    "headers": None,
    "multiValueHeaders": None,
    "queryStringParameters": {
      "user": "sungmin kim"
    },
    "multiValueQueryStringParameters": {
      "user": [
        "sungmin kim"
      ]
    },
    "pathParameters": None,
    "stageVariables": None,
    "requestContext": {
      "resourceId": "rbszcr",
      "resourcePath": "/pymk",
      "httpMethod": "GET",
      "extendedRequestId": "CHz0dGu7oAMFk1Q=",
      "requestTime": "25/Oct/2019:14:01:00 +0000",
      "path": "/search",
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
        "userArn": "arn:aws:iam::123456789012:user/USER",
        "apiKeyId": "test-invoke-api-key-id",
        "userAgent": "aws-internal/3 aws-sdk-java/1.11.641 Linux/4.9.184-0.1.ac.235.83.329.metal1.x86_64 OpenJDK_64-Bit_Server_VM/25.222-b10 java/1.8.0_222 vendor/Oracle_Corporation",
        "caller": "AKIAIOSFODNN7EXAMPLE",
        "sourceIp": "test-invoke-source-ip",
        "accessKey": "AKIAIOSFODNN7EXAMPLE",
        "cognitoAuthenticationProvider": None,
        "user": "AKIAIOSFODNN7EXAMPLE"
      },
      "domainName": "testPrefix.testDomainName",
      "apiId": "h02uojhcic"
    },
    "body": None,
    "isBase64Encoded": False
  }

  res = lambda_handler(event, {})
  pprint.pprint(res)

