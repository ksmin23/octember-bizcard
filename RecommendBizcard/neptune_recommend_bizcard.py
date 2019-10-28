#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import json
import traceback

import boto3

from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import T, P, Operator, Scope, Column, Order
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

#TODO: should change
AWS_REGION = 'us-east-1'
NEPTUNE_ENDPOINT = 'octemberbizcard.cnrh6fettief.us-east-1.neptune.amazonaws.com'
NEPTUNE_PORT = 8182

NEPTUNE_CONN = None

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


def people_you_may_know(g, user_name):
  from gremlin_python.process.traversal import Scope, Column, Order

  recommendations = (g.V().hasLabel('person').has('name', user_name).as_('person').
    both('knows').aggregate('friends').
    both('knows').
      where(P.neq('person')).where(P.without('friends')).
    groupCount().by('name').
    order(Scope.local).by(Column.values, Order.decr).
    next())
  return [{'name': key, 'score': score} for key, score in recommendations.items()]


def lambda_handler(event, context):
  if NEPTUNE_CONN is None:
    graph_db = graph_traversal(neptune_endpoint, neptune_port, connection=NEPTUNE_CONN)
    NEPTUNE_CONN = graph_db
  else:
    graph_db = NEPTUNE_CONN
 
  user_name = event["queryStringParameters"]["user"]
  res = people_you_may_know(graph_db, user_name)

  response = {
    'statusCode': 200,
    'body': json.dumps(res)
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
        "query": "Sungmin",
        "user": "sungmk"
    },
    "multiValueQueryStringParameters": {
        "user": [
            "Sungmin Kim"
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

  lambda_handler(event, {})

