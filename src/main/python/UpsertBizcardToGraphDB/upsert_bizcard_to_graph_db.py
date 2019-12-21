#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import json
import os
import base64
import time
import traceback
import random
import hashlib

from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import T, P, Operator
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

random.seed(47)

AWS_REGION = os.getenv('REGION_NAME', 'us-east-1')
NEPTUNE_ENDPOINT = os.getenv('NEPTUNE_ENDPOINT')
NEPTUNE_PORT = int(os.getenv('NEPTUNE_PORT', '8182'))


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


def clear_graph(neptune_endpoint=None, neptune_port=NEPTUNE_PORT, batch_size=200, edge_batch_size=None, vertex_batch_size=None):
  if edge_batch_size is None:
    edge_batch_size = batch_size

  if vertex_batch_size is None:
    vertex_batch_size = batch_size

  g = graph_traversal(neptune_endpoint, neptune_port, False) 

  has_edges = True
  edge_count = None
  while has_edges:
    if edge_count is None:
      print('[DEBUG] clearing property graph data [edge_batch_size={}, edge_count=Unknown]...'.format(edge_batch_size))
    else:
      print('[DEBUG] clearing property graph data [edge_batch_size={}, edge_count={}]...'.format(edge_batch_size, edge_count))
    g.E().limit(edge_batch_size).drop().toList()
    edge_count = g.E().count().next()
    has_edges = (edge_count > 0)

  has_vertices = True
  vertex_count = None
  while has_vertices:
    if vertex_count is None:
      print('[DEBUG] clearing property graph data [vertex_batch_size={}, vertex_count=Unknown]...'.format(vertex_batch_size), file=sys.stderr)
    else:
      print('[DEBUG] clearing property graph data [vertex_batch_size={}, vertex_count={}]...'.format(vertex_batch_size, vertex_count), file=sys.stderr)
    g.V().limit(vertex_batch_size).drop().toList()
    vertex_count = g.V().count().next()
    has_vertices = (vertex_count > 0)


def get_person(g, person_id):
  person = g.V(person_id).toList()
  return None if not person else person[-1]


def upsert_person(g, person):
  person_vertex = get_person(g, person['id'])
  elem = g.addV('person').property(T.id, person['id']).next() if not person_vertex else g.V(person_vertex).next()
  for k in ('id', 'name', 'email', 'phone_number', 'company', 'job_title'):
    g.V(elem).property(k, person[k]).next()
  g.V(elem).property('_name', person['name'].lower()).next()

  _from_person_id = hashlib.md5(person['owner'].encode('utf-8')).hexdigest()[:8]
  _to_person_id = person['id']
  if _from_person_id != _to_person_id:
    from_person_vertex = get_person(g, _from_person_id)
    to_person_vertex = get_person(g, _to_person_id)
    weight = 1.0
    for _ in range(3):
      try:
        if g.V(from_person_vertex).outE('knows').filter(__.inV().is_(to_person_vertex)).toList():
          print('[DEBUG] Updating relationship: [{} -> {}]'.format(_from_person_id, _to_person_id), file=sys.stderr)
          g.V(from_person_vertex).outE('knows').filter(__.inV().is_(to_person_vertex)).property('weight', weight).next()
        else:
          print('[DEBUG] Creating relationship: [{} -> {}]'.format(_from_person_id, _to_person_id), file=sys.stderr)
          g.V(from_person_vertex).addE('knows').to(to_person_vertex).property('weight', weight).next()
        break
      except Exception as ex:
        traceback.print_exc()
        time.sleep(0.01)


def _print_all_vertices(g):
  import pprint
  all_persons = [{**node.__dict__, **properties} for node in g.V()
              for properties in g.V(node).valueMap()]
  pprint.pprint(all_persons)

# pylint: disable=unused-argument
def lambda_handler(event, context):
  import collections

  counter = collections.OrderedDict([('reads', 0),
      ('writes', 0),
      ('invalid', 0),
      ('errors', 0)])

  neptune_endpoint, neptune_port = (NEPTUNE_ENDPOINT, NEPTUNE_PORT)
  graph_db = graph_traversal(neptune_endpoint, neptune_port)

  for record in event['Records']:
    try:
      counter['reads'] += 1
      payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
      json_data = json.loads(payload)

      if not all([json_data.get(k, None) for k in ('data', 'owner', 's3_key')]):
        counter['invalid'] += 1
        continue

      record = json_data['data']
      person = {
        "id": hashlib.md5(record['email'].split('@')[0].encode('utf-8')).hexdigest()[:8],
        "name": record['name'],
        "email": record['email'],
        "phone_number": record['phone_number'],
        "company": record['company'],
        "job_title": record['job_title'],
        "owner": json_data['owner']
      }
      #print(json.dumps(person, indent=2))
      upsert_person(graph_db, person)

      counter['writes'] += 1
    except Exception as _:
      counter['errors'] += 1
      traceback.print_exc()


if __name__ == '__main__':
  # pylint: disable=invalid-name
  kinesis_data = [
    # pylint: disable=bad-indentation
    # pylint: disable=line-too-long
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

  #pylint: disable=bad-indentation
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

