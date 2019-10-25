#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import sys
import json

import boto3
from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection
from requests_aws4auth import AWS4Auth

my_region = 'us-east-1'
my_service = 'es'
my_es_host = 'vpc-movies-fycobrhs4jyxpq4k3cumu7cgli.us-east-1.es.amazonaws.com'

session = boto3.Session(region_name=my_region) # thanks Leon
credentials = session.get_credentials()
credentials = credentials.get_frozen_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key
token = credentials.token

aws_auth = AWS4Auth(
    access_key,
    secret_key,
    my_region,
    my_service,
    session_token=token
)

es = Elasticsearch(
    hosts = [{'host': my_es_host, 'port': 443}],
    http_auth=aws_auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

print(json.dumps(es.info(), indent=2))

action_meta = {"index": {"_index": "octember_bizcard", "_type": "bizcard", "_id": "4fa229ed"}}
doc_source = {"addr": "1 2Floor GS Tower, 508 Nonhyeon-ro, Gangnam-gu, Seoul 06141, Korea", "email": "sungmk@amazon.com", "phone_number": "(+82 10) 2710 9704 ", "company": "aws", "name": "Sungmin Kim", "job_title": "Solutions Architect", "created_at": "2019-10-25T01:12:54Z", "owner": "sungmk", "doc_id": "4fa229ed", "is_alive": True, "content_id": "644b0d2c"}

s1 = json.dumps(action_meta)
s2 = json.dumps(doc_source)
body = '\n'.join([s1, s2])

res = es.bulk(body=body, index='octember_bizcard', refresh=True)
print(res)
