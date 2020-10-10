#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
# vim: tabstop=2 shiftwidth=2 softtabstop=2 expandtab

import os

from aws_cdk import core

from octember_bizcard.octember_bizcard_stack import OctemberBizcardStack

app = core.App()
OctemberBizcardStack(app, "octember-bizcard")

app.synth()
