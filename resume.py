#!/usr/bin/env python3

import json
import filelock
import os
import sys
import time

import common

logger, config = common.get_common('resume')

# Retrieve the list of hosts to resume
try:
    hostlist = sys.argv[1]
    logger.info('Hostlist: %s' %hostlist)
except:
    logger.critical('Missing hostlist argument')
    sys.exit(1)

logger.debug('Nodes to resume: %s', hostlist)

