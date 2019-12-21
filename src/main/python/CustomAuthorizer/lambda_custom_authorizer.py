#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

#***************************************************************************************/
#*
#*  Title: signS3POST.py
#*  Description: Lambda Function for Signing POST request to S3(Direct Uploads)
#*  Author: Beomi
#*  Availability: https://gist.github.com/Beomi/ac9d34dbfa9a6bdaf4a0426e8b83b4e3
#*
#***************************************************************************************/

import sys
import base64
import hashlib
import hmac
import json
import logging
import os

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

# Key derivation functions. See:
# http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
def sign(key, msg):
  return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def getSignatureKey(key, date_stamp, regionName, serviceName):
  kDate = sign(('AWS4' + key).encode('utf-8'), date_stamp)
  kRegion = sign(kDate, regionName)
  kService = sign(kRegion, serviceName)
  kSigning = sign(kService, 'aws4_request')
  return kSigning


def sign_policy(policy, credential):
  """ Sign and return the policy document for a simple upload.
  http://aws.amazon.com/articles/1434/#signyours3postform """
  base64_policy = base64.b64encode(policy)
  parts = credential.split('/')
  date_stamp = parts[1]
  region = parts[2]
  service = parts[3]

  signedKey = getSignatureKey(SECRET_KEY, date_stamp, region, service)
  signature = hmac.new(signedKey, base64_policy, hashlib.sha256).hexdigest()

  base64_policy = base64_policy.decode('utf-8')
  return {'policy': base64_policy, 'signature': signature}


def sign_headers(headers):
  """ Sign and return the headers for a chunked upload. """
  # headers = str(bytearray(headers, 'utf-8'))  # hmac doesn't want unicode
  parts = headers.split('\n')
  print('[INFO] headers: ', parts, file=sys.stderr)
  canonical_request = ('\n'.join(parts[3:]))
  algorithm = parts[0]
  amz_date = parts[1]
  credential_scope = parts[2]
  string_to_sign = algorithm + '\n' + amz_date + '\n' + credential_scope + '\n' + hashlib.sha256(
    canonical_request.encode('utf-8')).hexdigest()

  cred_parts = credential_scope.split('/')
  date_stamp = cred_parts[0]
  region = cred_parts[1]
  service = cred_parts[2]
  signed_key = getSignatureKey(SECRET_KEY, date_stamp, region, service)
  signature = hmac.new(signed_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()

  return {'signature': signature}


def lambda_handler(event, context):
  """ Route for signing the policy document or REST headers. """
  print('[INFO] event: ', event, file=sys.stderr)
  request_payload = event
  if request_payload.get('headers'):
    response_data = sign_headers(request_payload['headers'])
  else:
    credential = list([c for c in request_payload['conditions'] if 'x-amz-credential' in c][0].values())[0]
    print('[DEBUG]', credential, type(credential), file=sys.stderr)
    credential = str(credential)
    response_data = sign_policy(json.dumps(event).encode('utf-8'), credential)
  return response_data

