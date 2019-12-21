#!/usr/bin/env python3
import os

from aws_cdk import core

from octember_bizcard.octember_bizcard_stack import OctemberBizcardStack

ACCOUNT = os.getenv('CDK_DEFAULT_ACCOUNT', '')
REGION = os.getenv('CDK_DEFAULT_REGION', 'us-east-1')
AWS_ENV = core.Environment(account=ACCOUNT, region=REGION)

app = core.App()
OctemberBizcardStack(app, "octember-bizcard", env=AWS_ENV)

app.synth()
